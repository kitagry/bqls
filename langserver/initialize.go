package langserver

import (
	"context"
	"encoding/json"

	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/kitagry/bqls/langserver/internal/source"
	"github.com/sourcegraph/jsonrpc2"
)

type InitializeOption struct {
	ProjectID string `json:"project_id"`
}

func (h *Handler) handleInitialize(ctx context.Context, conn *jsonrpc2.Conn, req *jsonrpc2.Request) (result interface{}, err error) {
	if req.Params == nil {
		return nil, &jsonrpc2.Error{Code: jsonrpc2.CodeInvalidParams}
	}

	h.conn = conn

	var params lsp.InitializeParams[InitializeOption]
	if err := json.Unmarshal(*req.Params, &params); err != nil {
		return nil, err
	}
	h.initializeParams = params

	p, err := source.NewProject(context.Background(), params.RootPath, params.InitializationOptions.ProjectID, h.logger)
	if err != nil {
		return nil, err
	}
	h.project = p

	return lsp.InitializeResult{
		Capabilities: lsp.ServerCapabilities{
			TextDocumentSync: &lsp.TextDocumentSyncOptionsOrKind{
				Kind: toPtr(lsp.TDSKFull),
			},
			DocumentFormattingProvider: true,
			HoverProvider:              true,
			CodeActionProvider:         true,
			CompletionProvider: &lsp.CompletionOptions{
				ResolveProvider:   false,
				TriggerCharacters: []string{"*", "."},
			},
			ExecuteCommandProvider: &lsp.ExecuteCommandOptions{
				Commands: []string{
					CommandListDatasets,
					CommandListTables,
				},
			},
		},
	}, nil
}

func toPtr[T any](s T) *T {
	return &s
}
