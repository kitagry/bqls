package langserver

import (
	"context"

	"github.com/kitagry/bqls/langserver/internal/lsp"
)

func (h *Handler) handleWorkspaceDidChangeConfiguration(ctx context.Context, params lsp.DidChangeConfigurationParams[InitializeOption]) (struct{}, error) {
	h.initializeParams.InitializationOptions = params.Settings

	if err := h.setupByInitializeParams(); err != nil {
		return struct{}{}, err
	}

	return struct{}{}, nil
}
