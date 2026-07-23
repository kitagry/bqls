package langserver

import (
	"context"

	"github.com/kitagry/bqls/langserver/internal/lsp"
)

func (h *Handler) handleTextDocumentDefinition(ctx context.Context, params lsp.TextDocumentPositionParams) ([]lsp.Location, error) {
	return h.project.LookupIdent(ctx, params.TextDocument.URI, params.Position)
}
