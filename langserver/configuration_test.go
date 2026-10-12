package langserver

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/kitagry/bqls/langserver/internal/lint"
	"github.com/kitagry/bqls/langserver/internal/lsp"
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
