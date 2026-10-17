package langserver

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/google/go-cmp/cmp"
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
