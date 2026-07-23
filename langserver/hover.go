package langserver

import (
	"context"

	"github.com/kitagry/bqls/langserver/internal/lsp"
)

func (h *Handler) handleTextDocumentHover(ctx context.Context, params lsp.TextDocumentPositionParams) (lsp.Hover, error) {
	return h.documentIdent(ctx, params.TextDocument.URI, params.Position)
}

func (h *Handler) documentIdent(ctx context.Context, uri lsp.DocumentURI, position lsp.Position) (lsp.Hover, error) {
	result, err := h.project.TermDocument(ctx, uri, position)
	if err != nil {
		return lsp.Hover{}, err
	}

	return lsp.Hover{Contents: result}, nil
}
