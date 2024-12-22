package langserver

import (
	"context"
	"encoding/json"

	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/sourcegraph/jsonrpc2"
)

func (h *Handler) handleWorkspaceDidChangeConfiguration(ctx context.Context, conn *jsonrpc2.Conn, req *jsonrpc2.Request) (result any, err error) {
	if req.Params == nil {
		return nil, &jsonrpc2.Error{Code: jsonrpc2.CodeInvalidParams}
	}

	var params lsp.DidChangeConfigurationParams[InitializeOption]
	if err := json.Unmarshal(*req.Params, &params); err != nil {
		return nil, err
	}

	h.initializeParams.InitializationOptions = params.Settings

	err = h.setupByInitializeParams()
	if err != nil {
		return nil, err
	}

	return nil, nil
}
