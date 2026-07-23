package langserver

import (
	"context"

	jsonrpc "github.com/gumeniukcom/golang-jsonrpc2/v2"
	"github.com/kitagry/bqls/langserver/internal/lsp"
)

type InitializeOption struct {
	ProjectID string `json:"project_id"`
	Location  string `json:"location"`
}

func (h *Handler) handleInitialize(ctx context.Context, params lsp.InitializeParams[InitializeOption]) (lsp.InitializeResult, error) {
	// Capture the transport's pusher so background goroutines can send
	// server-initiated notifications for the life of the connection.
	if p, ok := jsonrpc.PusherFromContext(ctx); ok {
		h.pusher = p
	}

	h.initializeParams = params

	if err := h.setupByInitializeParams(); err != nil {
		return lsp.InitializeResult{}, err
	}

	return lsp.InitializeResult{
		Capabilities: lsp.ServerCapabilities{
			TextDocumentSync: &lsp.TextDocumentSyncOptionsOrKind{
				Kind: toPtr(lsp.TDSKFull),
			},
			DefinitionProvider:         true,
			DocumentFormattingProvider: true,
			HoverProvider:              true,
			CodeActionProvider:         true,
			CompletionProvider: &lsp.CompletionOptions{
				ResolveProvider:   true,
				TriggerCharacters: []string{"*", "."},
			},
			ExecuteCommandProvider: &lsp.ExecuteCommandOptions{
				Commands: []string{
					CommandExecuteQuery,
					CommandListDatasets,
					CommandListTables,
					CommandListJobHistories,
					CommandSaveResult,
				},
			},
		},
	}, nil
}

func toPtr[T any](s T) *T {
	return &s
}
