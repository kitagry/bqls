package completion

import "github.com/kitagry/bqls/langserver/internal/lsp"

type CompletionItem struct {
	Kind          lsp.CompletionItemKind
	NewText       string
	Documentation string
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

	startPosition := position
	startPosition.Character -= len(c.TypedPrefix)
	return lsp.CompletionItem{
		InsertTextFormat: lsp.ITFSnippet,
		Kind:             c.Kind,
		Label:            c.NewText,
		Documentation:    c.Documentation,
		TextEdit: &lsp.TextEdit{
			NewText: c.NewText,
			Range: lsp.Range{
				Start: startPosition,
				End:   position,
			},
		},
	}
}
