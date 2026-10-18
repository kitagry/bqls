package langserver

import (
	"encoding/json"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/sourcegraph/jsonrpc2"
)

func TestHandler_handleTextDocumentFormatting(t *testing.T) {
	const uri lsp.DocumentURI = "file:///a.sql"
	tests := map[string]struct {
		src  string
		want string
	}{
		"pipe query": {
			src:  "FROM `project.dataset.table` |> WHERE id > 1 |> AGGREGATE COUNT(*) AS c GROUP BY name",
			want: "FROM\n  `project.dataset.table`\n|> WHERE\n     id > 1\n|> AGGREGATE\n     COUNT(*) AS c\n   GROUP BY name;",
		},
	}

	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			h := newTestHandlerWithFile(t, uri, tt.src)
			params, err := json.Marshal(lsp.DocumentFormattingParams{TextDocument: lsp.TextDocumentIdentifier{URI: uri}})
			if err != nil {
				t.Fatal(err)
			}
			raw := json.RawMessage(params)

			got, err := h.handleTextDocumentFormatting(t.Context(), nil, &jsonrpc2.Request{Params: &raw})
			if err != nil {
				t.Fatalf("handleTextDocumentFormatting() error = %v", err)
			}
			if diff := cmp.Diff(ComputeEdits(uri, tt.src, tt.want), got); diff != "" {
				t.Errorf("formatting edits diff (-want +got):\n%s", diff)
			}
		})
	}
}

// Formatting edits are whole lines (Character 0), so they stay correct in
// UTF-16 units even when lines contain multibyte text.
func TestHandler_handleTextDocumentFormatting_MultibyteText(t *testing.T) {
	const uri lsp.DocumentURI = "file:///a.sql"
	const src = "SELECT 'あいう' AS s,\t'😀' AS e FROM t WHERE x = 'あ'"
	h := newTestHandlerWithFile(t, uri, src)
	params, err := json.Marshal(lsp.DocumentFormattingParams{TextDocument: lsp.TextDocumentIdentifier{URI: uri}})
	if err != nil {
		t.Fatal(err)
	}
	raw := json.RawMessage(params)

	got, err := h.handleTextDocumentFormatting(t.Context(), nil, &jsonrpc2.Request{Params: &raw})
	if err != nil {
		t.Fatalf("handleTextDocumentFormatting() error = %v", err)
	}
	for _, e := range got.([]lsp.TextEdit) {
		if e.Range.Start.Character != 0 || e.Range.End.Character != 0 {
			t.Errorf("edit %v is not line granular", e.Range)
		}
	}
	want := "SELECT\n  'あいう' AS s,\n  '😀' AS e\nFROM\n  t\nWHERE\n  x = 'あ';"
	if fixed := applyUTF16Edits(t, src, got.([]lsp.TextEdit)); fixed != want {
		t.Errorf("formatted text = %q, want %q", fixed, want)
	}
}
