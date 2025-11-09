package completion

import "github.com/kitagry/bqls/langserver/internal/lsp"

type CompletionItem struct {
	Kind          lsp.CompletionItemKind
	NewText       string
	SnippetText   string // Optional: if set, used when snippets are supported
	Documentation lsp.MarkupContent
	TypedPrefix   string
}

func (c CompletionItem) ToLspCompletionItem(position lsp.Position, supportSnippet bool) lsp.CompletionItem {
	if !supportSnippet {
		return lsp.CompletionItem{
			InsertTextFormat: lsp.ITFPlainText,
			Kind:             c.Kind,
			Label:            c.NewText,
			Documentation:    c.Documentation,
		}
	}

	// Use SnippetText if available, otherwise fall back to NewText
	textToInsert := c.NewText
	if c.SnippetText != "" {
		textToInsert = c.SnippetText
	}

	startPosition := position
	startPosition.Character -= len(c.TypedPrefix)
	return lsp.CompletionItem{
		InsertTextFormat: lsp.ITFSnippet,
		Kind:             c.Kind,
		Label:            c.NewText,
		Documentation:    c.Documentation,
		TextEdit: &lsp.TextEdit{
			NewText: textToInsert,
			Range: lsp.Range{
				Start: startPosition,
				End:   position,
			},
		},
	}
}
