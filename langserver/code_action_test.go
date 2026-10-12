package langserver

import (
	"encoding/json"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/sourcegraph/jsonrpc2"
)

func TestHandler_handleTextDocumentCodeAction_Lint(t *testing.T) {
	const uri lsp.DocumentURI = "file:///a.sql"
	const src = "SELECT x\nFROM UNNEST([1]) AS x\n  WHERE x = CAST(NULL AS INT64)"
	diagRange := lsp.Range{Start: lsp.Position{Line: 2, Character: 8}, End: lsp.Position{Line: 2, Character: 31}}
	lintDiag := lsp.Diagnostic{
		Range:    diagRange,
		Severity: lsp.Warning,
		Code:     "null-comparison",
		Source:   "bqls-lint",
		Message:  "Comparison with NULL using = always yields NULL; use IS NULL",
	}
	commands := []any{
		lsp.Command{Title: "Execute Query", Command: CommandExecuteQuery, Arguments: []any{uri}},
		lsp.Command{Title: "List Personal Job Histories", Command: CommandListJobHistories},
		lsp.Command{Title: "List Project Job Histories", Command: CommandListJobHistories, Arguments: []any{"--all-user"}},
	}

	tests := map[string]struct {
		rng            lsp.Range
		literalSupport bool
		want           []any
	}{
		"cursor on lint diagnostic offers fix and ignore": {
			literalSupport: true,
			rng:            lsp.Range{Start: lsp.Position{Line: 2, Character: 10}, End: lsp.Position{Line: 2, Character: 10}},
			want: append([]any{
				lsp.CodeAction{
					Title:       "Replace with IS NULL",
					Kind:        lsp.CAKQuickFix,
					Diagnostics: []lsp.Diagnostic{lintDiag},
					IsPreferred: true,
					Edit: &lsp.WorkspaceEdit{Changes: map[string][]lsp.TextEdit{
						string(uri): {{Range: diagRange, NewText: "x IS NULL"}},
					}},
				},
				// The ignore comment goes on its own line above, keeping the
				// diagnostic line's indentation.
				lsp.CodeAction{
					Title:       "Ignore null-comparison on this line",
					Kind:        lsp.CAKQuickFix,
					Diagnostics: []lsp.Diagnostic{lintDiag},
					Edit: &lsp.WorkspaceEdit{Changes: map[string][]lsp.TextEdit{
						string(uri): {{
							Range:   lsp.Range{Start: lsp.Position{Line: 2}, End: lsp.Position{Line: 2}},
							NewText: "  -- bqls:ignore null-comparison\n",
						}},
					}},
				},
			}, commands...),
		},
		"range without lint diagnostic offers only commands": {
			literalSupport: true,
			rng:            lsp.Range{Start: lsp.Position{Line: 0, Character: 0}, End: lsp.Position{Line: 0, Character: 3}},
			want:           commands,
		},
		"client without code action literal support gets only commands": {
			rng:  lsp.Range{Start: lsp.Position{Line: 2, Character: 10}, End: lsp.Position{Line: 2, Character: 10}},
			want: commands,
		},
	}

	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			h := newTestHandlerWithFile(t, uri, src)
			if tt.literalSupport {
				h.initializeParams.Capabilities.TextDocument.CodeAction.CodeActionLiteralSupport.CodeActionKind.ValueSet = []lsp.CodeActionKind{lsp.CAKQuickFix}
			}
			params, err := json.Marshal(lsp.CodeActionParams{TextDocument: lsp.TextDocumentIdentifier{URI: uri}, Range: tt.rng})
			if err != nil {
				t.Fatal(err)
			}
			raw := json.RawMessage(params)

			got, err := h.handleTextDocumentCodeAction(t.Context(), nil, &jsonrpc2.Request{Params: &raw})
			if err != nil {
				t.Fatalf("handleTextDocumentCodeAction() error = %v", err)
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("code actions diff (-want +got):\n%s", diff)
			}
		})
	}
}

func TestHandler_handleTextDocumentCodeAction_ExtendsExistingIgnore(t *testing.T) {
	const uri lsp.DocumentURI = "file:///a.sql"
	const src = "SELECT x\nFROM UNNEST([1]) AS x\n  -- bqls:ignore other-rule -- legacy\n  WHERE x = CAST(NULL AS INT64)"
	h := newTestHandlerWithFile(t, uri, src)
	h.initializeParams.Capabilities.TextDocument.CodeAction.CodeActionLiteralSupport.CodeActionKind.ValueSet = []lsp.CodeActionKind{lsp.CAKQuickFix}

	pos := lsp.Position{Line: 3, Character: 10}
	actions := h.lintCodeActions(t.Context(), uri, lsp.Range{Start: pos, End: pos})

	var got []lsp.TextEdit
	for _, a := range actions {
		if ca, ok := a.(lsp.CodeAction); ok && ca.Title == "Ignore null-comparison on this line" {
			got = ca.Edit.Changes[string(uri)]
		}
	}
	insertAt := lsp.Position{Line: 2, Character: 27}
	want := []lsp.TextEdit{{Range: lsp.Range{Start: insertAt, End: insertAt}, NewText: ", null-comparison"}}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("ignore edit diff (-want +got):\n%s", diff)
	}
}
