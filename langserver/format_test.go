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

// Formatting edits are whole lines (Character 0), except that the end of a
// document without a trailing newline is in UTF-16 units of its last line.
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
	docEnd := lsp.Position{Line: 0, Character: 49}
	for _, e := range got.([]lsp.TextEdit) {
		for _, p := range []lsp.Position{e.Range.Start, e.Range.End} {
			if p.Character != 0 && p != docEnd {
				t.Errorf("edit %v is neither line granular nor at the document end %v", e.Range, docEnd)
			}
		}
	}
	want := "SELECT\n  'あいう' AS s,\n  '😀' AS e\nFROM\n  t\nWHERE\n  x = 'あ';"
	if fixed := applyUTF16Edits(t, src, got.([]lsp.TextEdit)); fixed != want {
		t.Errorf("formatted text = %q, want %q", fixed, want)
	}
}

// FormatSql drops comments, so a document with comments must not be formatted
// rather than losing them silently.
func TestHandler_handleTextDocumentFormatting_KeepsComments(t *testing.T) {
	const uri lsp.DocumentURI = "file:///a.sql"
	tests := map[string]string{
		"line comments":  "-- head comment\nSELECT long -- trailing\nFROM t\n",
		"hash comment":   "SELECT long # trailing\nFROM t\n",
		"block comment":  "SELECT /* note */ long FROM t\n",
		"pipe query":     "FROM t -- source\n|> WHERE id > 1\n",
		"comment at EOF": "SELECT long FROM t\n-- end",
	}
	for n, src := range tests {
		t.Run(n, func(t *testing.T) {
			got := formatDocument(t, uri, src)
			if got != nil {
				t.Errorf("handleTextDocumentFormatting() = %v, want no edits", got)
			}
		})
	}
}

func TestHandler_handleTextDocumentFormatting_CommentMarkersInStrings(t *testing.T) {
	const uri lsp.DocumentURI = "file:///a.sql"
	const src = "SELECT '-- a' AS s, \"# b\" AS h, '/* c */' AS c FROM t"
	got := formatDocument(t, uri, src)
	edits, ok := got.([]lsp.TextEdit)
	if !ok || len(edits) == 0 {
		t.Fatalf("handleTextDocumentFormatting() = %v, want edits", got)
	}
	want := "SELECT\n  '-- a' AS s,\n  \"# b\" AS h,\n  '/* c */' AS c\nFROM\n  t;"
	if fixed := applyUTF16Edits(t, src, edits); fixed != want {
		t.Errorf("formatted text = %q, want %q", fixed, want)
	}
}

func TestComputeEdits_NoTrailingNewline(t *testing.T) {
	const uri lsp.DocumentURI = "file:///a.sql"
	tests := map[string]struct {
		before, after string
		end           lsp.Position
	}{
		"one line": {
			before: "SELECT 1",
			after:  "SELECT\n  1;",
			end:    lsp.Position{Line: 0, Character: 8},
		},
		"multibyte last line": {
			before: "SELECT 1,\n'あ😀'",
			after:  "SELECT\n  1,\n  'あ😀';",
			end:    lsp.Position{Line: 1, Character: 5},
		},
	}
	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			edits := ComputeEdits(uri, tt.before, tt.after)
			for _, e := range edits {
				for _, p := range []lsp.Position{e.Range.Start, e.Range.End} {
					if p.Line > tt.end.Line || (p.Line == tt.end.Line && p.Character > tt.end.Character) {
						t.Errorf("edit %v points past the end of the document %v", e.Range, tt.end)
					}
				}
			}
			if fixed := applyUTF16Edits(t, tt.before, edits); fixed != tt.after {
				t.Errorf("edited text = %q, want %q", fixed, tt.after)
			}
		})
	}
}

func formatDocument(t *testing.T, uri lsp.DocumentURI, src string) any {
	t.Helper()
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
	return got
}
