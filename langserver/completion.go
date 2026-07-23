package langserver

import (
	"context"

	"github.com/kitagry/bqls/langserver/internal/lsp"
)

func (h *Handler) handleTextDocumentCompletion(ctx context.Context, params lsp.TextDocumentPositionParams) ([]lsp.CompletionItem, error) {
	items, err := h.project.Complete(ctx, params.TextDocument.URI, params.Position)
	if err != nil {
		return nil, err
	}

	completionItems := make([]lsp.CompletionItem, len(items))
	for i, item := range items {
		completionItems[i] = item.ToLspCompletionItem(params.Position, h.clientSupportSnippets())
	}

	return completionItems, nil
}

func (h *Handler) handleCompletionItemResolve(ctx context.Context, params lsp.CompletionItem) (lsp.CompletionItem, error) {
	item, err := h.project.ResolveCompletionItem(ctx, params)
	if err != nil {
		return lsp.CompletionItem{}, err
	}
	return item, nil
}

func (h *Handler) clientSupportSnippets() bool {
	return h.initializeParams.Capabilities.TextDocument.Completion.CompletionItem.SnippetSupport
}
