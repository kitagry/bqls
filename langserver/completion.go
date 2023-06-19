package langserver

import (
	"context"
	"encoding/json"

	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/sourcegraph/jsonrpc2"
)

func (h *Handler) handleTextDocumentCompletion(ctx context.Context, conn *jsonrpc2.Conn, req *jsonrpc2.Request) (result interface{}, err error) {
	if req.Params == nil {
		return nil, &jsonrpc2.Error{Code: jsonrpc2.CodeInvalidParams}
	}

	var params lsp.TextDocumentPositionParams
	if err := json.Unmarshal(*req.Params, &params); err != nil {
		return nil, err
	}

	items, err := h.project.Complete(ctx, documentURIToURI(params.TextDocument.URI), params.Position, h.clientSupportSnippets())
	if err != nil {
		return nil, err
	}

	return items, nil
}

func (h *Handler) clientSupportSnippets() bool {
	return h.initializeParams.Capabilities.TextDocument.Completion.CompletionItem.SnippetSupport
}
