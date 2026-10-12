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
