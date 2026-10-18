package langserver

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/google/go-cmp/cmp"
	"github.com/kitagry/bqls/langserver/internal/bigquery"
	"github.com/kitagry/bqls/langserver/internal/bigquery/mock_bigquery"
	"github.com/kitagry/bqls/langserver/internal/lint"
	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/kitagry/bqls/langserver/internal/source"
	"github.com/sirupsen/logrus"
	"github.com/sourcegraph/jsonrpc2"
)

func TestHandler_handleWorkspaceDidChangeConfiguration_LintOnly(t *testing.T) {
	const uri lsp.DocumentURI = "file:///a.sql"
	h := newTestHandlerWithFile(t, uri, "SELECT 1")
	h.diagnosticRequest = make(chan lsp.DocumentURI, 3)
	h.initializeParams.InitializationOptions = InitializeOption{ProjectID: "p", Location: "US"}
	project := h.project

	params := json.RawMessage(`{"settings": {"project_id": "p", "location": "US", "lint": {"rules": {"null-comparison": false}}}}`)
	_, err := h.handleWorkspaceDidChangeConfiguration(t.Context(), nil, &jsonrpc2.Request{Params: &params})
	if err != nil {
		t.Fatalf("handleWorkspaceDidChangeConfiguration() error = %v", err)
	}

	if h.project != project {
		t.Error("project was rebuilt, want it kept when only lint settings change")
	}
	want := lint.Config{Rules: map[string]bool{"null-comparison": false}}
	if diff := cmp.Diff(want, h.lintConfig()); diff != "" {
		t.Errorf("lintConfig() diff (-want +got):\n%s", diff)
	}

	select {
	case got := <-h.diagnosticRequest:
		if got != uri {
			t.Errorf("diagnostics requested for %s, want %s", got, uri)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("diagnostics were not re-requested for the open document")
	}
}

func TestHandler_rediagnoseOpenFiles_AfterClose(t *testing.T) {
	ctrl := gomock.NewController(t)
	bqClient := mock_bigquery.NewMockClient(ctrl)
	bqClient.EXPECT().Close().Return(nil)
	p := source.NewProjectWithBQClient("/", bqClient, logrus.New())
	if err := p.UpdateFile("file:///a.sql", "SELECT 1", 1); err != nil {
		t.Fatal(err)
	}
	// No scheduler consumes diagnosticRequest, so the re-diagnose sender
	// stays blocked until Close runs.
	h := newHandler(logrus.New(), "test")
	h.project = p
	h.diagnosticRequest = make(chan lsp.DocumentURI)

	h.rediagnoseOpenFiles()
	time.Sleep(20 * time.Millisecond)
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	// A send on the closed channel would panic the test binary here.
	time.Sleep(50 * time.Millisecond)
}

func TestMergeSettings(t *testing.T) {
	prev := InitializeOption{
		ProjectID:                        "p",
		Location:                         "US",
		SupportsAsyncVirtualTextDocument: true,
		Lint:                             lint.Config{Rules: map[string]bool{"a": false}},
	}

	tests := map[string]struct {
		raw  string
		want InitializeOption
	}{
		"settings without lint keep previous rules": {
			raw: `{"project_id": "q", "location": "EU"}`,
			want: InitializeOption{
				ProjectID:                        "q",
				Location:                         "EU",
				SupportsAsyncVirtualTextDocument: true,
				Lint:                             lint.Config{Rules: map[string]bool{"a": false}},
			},
		},
		"lint-only settings keep project, location and async flag": {
			raw: `{"lint": {"rules": {"a": true}}}`,
			want: InitializeOption{
				ProjectID:                        "p",
				Location:                         "US",
				SupportsAsyncVirtualTextDocument: true,
				Lint:                             lint.Config{Rules: map[string]bool{"a": true}},
			},
		},
		"lint rules are replaced, not merged": {
			raw: `{"lint": {"rules": {"b": false}}}`,
			want: InitializeOption{
				ProjectID:                        "p",
				Location:                         "US",
				SupportsAsyncVirtualTextDocument: true,
				Lint:                             lint.Config{Rules: map[string]bool{"b": false}},
			},
		},
		"empty lint object resets rules to defaults": {
			raw: `{"lint": {}}`,
			want: InitializeOption{
				ProjectID:                        "p",
				Location:                         "US",
				SupportsAsyncVirtualTextDocument: true,
			},
		},
		"empty rules object resets rules to defaults": {
			raw: `{"lint": {"rules": {}}}`,
			want: InitializeOption{
				ProjectID:                        "p",
				Location:                         "US",
				SupportsAsyncVirtualTextDocument: true,
				Lint:                             lint.Config{Rules: map[string]bool{}},
			},
		},
		"capitalized Lint key still replaces rules": {
			raw: `{"Lint": {"rules": {"b": true}}}`,
			want: InitializeOption{
				ProjectID:                        "p",
				Location:                         "US",
				SupportsAsyncVirtualTextDocument: true,
				Lint:                             lint.Config{Rules: map[string]bool{"b": true}},
			},
		},
		"null lint resets rules to defaults": {
			raw: `{"lint": null}`,
			want: InitializeOption{
				ProjectID:                        "p",
				Location:                         "US",
				SupportsAsyncVirtualTextDocument: true,
			},
		},
		"null project_id keeps the previous project": {
			raw:  `{"project_id": null}`,
			want: prev,
		},
		"explicit empty project_id resets to the default project": {
			raw: `{"project_id": ""}`,
			want: InitializeOption{
				ProjectID:                        "",
				Location:                         "US",
				SupportsAsyncVirtualTextDocument: true,
				Lint:                             lint.Config{Rules: map[string]bool{"a": false}},
			},
		},
		"explicit false async flag overrides": {
			raw: `{"supports_async_virtual_text_document": false}`,
			want: InitializeOption{
				ProjectID: "p",
				Location:  "US",
				Lint:      lint.Config{Rules: map[string]bool{"a": false}},
			},
		},
		"null settings change nothing": {
			raw:  `null`,
			want: prev,
		},
		"omitted settings change nothing": {
			raw:  ``,
			want: prev,
		},
		"empty settings change nothing": {
			raw:  `{}`,
			want: prev,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			got, err := mergeSettings(prev, json.RawMessage(tt.raw))
			if err != nil {
				t.Fatalf("mergeSettings() error = %v", err)
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("mergeSettings() diff (-want +got):\n%s", diff)
			}
		})
	}
}

func TestMergeSettings_DoesNotShareRulesWithPrevious(t *testing.T) {
	prevRules := map[string]bool{"a": false}
	prev := InitializeOption{Lint: lint.Config{Rules: prevRules}}

	got, err := mergeSettings(prev, json.RawMessage(`{"project_id": "q"}`))
	if err != nil {
		t.Fatalf("mergeSettings() error = %v", err)
	}
	got.Lint.Rules["b"] = true

	if diff := cmp.Diff(map[string]bool{"a": false}, prevRules); diff != "" {
		t.Errorf("previous rules were mutated through the merged settings (-want +got):\n%s", diff)
	}
}

func TestMergeSettings_ReturnsErrorOnInvalidJSON(t *testing.T) {
	_, err := mergeSettings(InitializeOption{}, json.RawMessage(`{"project_id": 1}`))
	if err == nil {
		t.Error("mergeSettings() error = nil, want an error for a non-string project_id")
	}
}

func TestHandler_handleWorkspaceDidChangeConfiguration_PartialLintOnlyKeepsProject(t *testing.T) {
	const uri lsp.DocumentURI = "file:///a.sql"
	h := newTestHandlerWithFile(t, uri, "SELECT 1")
	h.diagnosticRequest = make(chan lsp.DocumentURI, 3)
	h.initializeParams.InitializationOptions = InitializeOption{ProjectID: "p", Location: "US"}
	project := h.project

	params := json.RawMessage(`{"settings": {"lint": {"rules": {"null-comparison": false}}}}`)
	_, err := h.handleWorkspaceDidChangeConfiguration(t.Context(), nil, &jsonrpc2.Request{Params: &params})
	if err != nil {
		t.Fatalf("handleWorkspaceDidChangeConfiguration() error = %v", err)
	}

	if h.project != project {
		t.Error("project was rebuilt, want it kept when settings omit project_id and location")
	}
	want := InitializeOption{ProjectID: "p", Location: "US", Lint: lint.Config{Rules: map[string]bool{"null-comparison": false}}}
	if diff := cmp.Diff(want, h.initializeParams.InitializationOptions); diff != "" {
		t.Errorf("InitializationOptions diff (-want +got):\n%s", diff)
	}

	select {
	case got := <-h.diagnosticRequest:
		if got != uri {
			t.Errorf("diagnostics requested for %s, want %s", got, uri)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("diagnostics were not re-requested for the open document")
	}
}

func TestHandler_handleWorkspaceDidChangeConfiguration_RebuildKeepsOpenDocuments(t *testing.T) {
	const uri lsp.DocumentURI = "file:///a.sql"
	const src = "SELECT 1"
	h, _ := newRebuildableHandler(t)
	if err := h.project.UpdateFile(uri, src, 1); err != nil {
		t.Fatal(err)
	}
	h.diagnosticRequest = make(chan lsp.DocumentURI, 3)
	h.initializeParams.InitializationOptions = InitializeOption{ProjectID: "p", Location: "US"}
	project := h.project

	ctrl := gomock.NewController(t)
	var gotProjectID, gotLocation string
	h.newBQClient = func(_ context.Context, projectID, location string) (bigquery.Client, error) {
		gotProjectID, gotLocation = projectID, location
		return mock_bigquery.NewMockClient(ctrl), nil
	}

	params := json.RawMessage(`{"settings": {"project_id": "q"}}`)
	_, err := h.handleWorkspaceDidChangeConfiguration(t.Context(), nil, &jsonrpc2.Request{Params: &params})
	if err != nil {
		t.Fatalf("handleWorkspaceDidChangeConfiguration() error = %v", err)
	}

	if h.project == project {
		t.Fatal("project was kept, want it rebuilt when project_id changes")
	}
	if gotProjectID != "q" || gotLocation != "US" {
		t.Errorf("BigQuery client created for (%q, %q), want (%q, %q)", gotProjectID, gotLocation, "q", "US")
	}
	got, ok := h.project.GetFile(uri)
	if !ok {
		t.Fatalf("open document %s was dropped by the rebuild", uri)
	}
	if got != src {
		t.Errorf("GetFile(%s) = %q, want %q", uri, got, src)
	}

	select {
	case got := <-h.diagnosticRequest:
		if got != uri {
			t.Errorf("diagnostics requested for %s, want %s", got, uri)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("diagnostics were not re-requested for the open document")
	}
}

// newRebuildableHandler returns a handler whose current project uses a mock
// client that signals on the returned channel when it is closed.
func newRebuildableHandler(t *testing.T) (*Handler, <-chan struct{}) {
	t.Helper()
	ctrl := gomock.NewController(t)
	oldClient := mock_bigquery.NewMockClient(ctrl)
	closed := make(chan struct{})
	oldClient.EXPECT().Close().DoAndReturn(func() error {
		close(closed)
		return nil
	})
	h := newHandler(logrus.New(), "test")
	h.project = source.NewProjectWithBQClient("/", oldClient, logrus.New())
	h.bqClient = oldClient
	h.newBQClient = func(context.Context, string, string) (bigquery.Client, error) {
		return mock_bigquery.NewMockClient(ctrl), nil
	}
	// The previous project is closed asynchronously, so wait for it before
	// the mock controller verifies its expectations.
	t.Cleanup(func() {
		select {
		case <-closed:
		case <-time.After(5 * time.Second):
		}
	})
	return h, closed
}

func TestHandler_setupByInitializeParams_ClosesPreviousProject(t *testing.T) {
	h, closed := newRebuildableHandler(t)

	if err := h.setupByInitializeParams(); err != nil {
		t.Fatalf("setupByInitializeParams() error = %v", err)
	}

	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("previous project was not closed after the rebuild")
	}
}

func TestHandler_setupByInitializeParams_ClosesPreviousProjectAfterInFlightUse(t *testing.T) {
	h, closed := newRebuildableHandler(t)
	_, release := h.acquireProject()

	if err := h.setupByInitializeParams(); err != nil {
		t.Fatalf("setupByInitializeParams() error = %v", err)
	}

	select {
	case <-closed:
		t.Fatal("previous project was closed while still in use")
	case <-time.After(50 * time.Millisecond):
	}

	release()
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("previous project was not closed after its last user released it")
	}
}

func TestHandler_diagnose_ConcurrentWithRebuild(t *testing.T) {
	const uri lsp.DocumentURI = "file:///a.sql"
	h, _ := newRebuildableHandler(t)
	if err := h.project.UpdateFile(uri, "SELECT 1", 1); err != nil {
		t.Fatal(err)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		for range 100 {
			if _, err := h.diagnose(t.Context(), uri); err != nil {
				t.Errorf("diagnose() error = %v", err)
				return
			}
		}
	}()
	if err := h.setupByInitializeParams(); err != nil {
		t.Fatalf("setupByInitializeParams() error = %v", err)
	}
	<-done
}
