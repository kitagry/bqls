package langserver

import (
	"context"
	"encoding/json"

	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/sourcegraph/jsonrpc2"
)

func (h *Handler) handleTextDocumentHover(ctx context.Context, conn *jsonrpc2.Conn, req *jsonrpc2.Request) (result any, err error) {
	if req.Params == nil {
		return nil, &jsonrpc2.Error{Code: jsonrpc2.CodeInvalidParams}
	}

	var params lsp.TextDocumentPositionParams
	if err := json.Unmarshal(*req.Params, &params); err != nil {
		return nil, err
	}

	return h.documentIdent(ctx, params.TextDocument.URI, params.Position)
}

func (h *Handler) documentIdent(ctx context.Context, uri lsp.DocumentURI, position lsp.Position) (lsp.Hover, error) {
	result, err := h.project.TermDocument(documentURIToURI(uri), position)
	if err != nil {
		return lsp.Hover{}, err
	}

	return lsp.Hover{Contents: result}, nil
}
