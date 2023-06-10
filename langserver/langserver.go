package langserver

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/kitagry/bqls/langserver/internal/source"
	"github.com/sourcegraph/jsonrpc2"
)

type handler struct {
	conn   *jsonrpc2.Conn
	logger *log.Logger

	project *source.Project

	diagnosticRequest chan lsp.DocumentURI
	initializeParams  lsp.InitializeParams
}

func NewHandler() jsonrpc2.Handler {
	handler := &handler{
		logger:            log.New(os.Stderr, "", log.LstdFlags),
		diagnosticRequest: make(chan lsp.DocumentURI, 3),
	}
	go handler.diagnostic()
	return jsonrpc2.HandlerWithError(handler.handle)
}

func (h *handler) handle(ctx context.Context, conn *jsonrpc2.Conn, req *jsonrpc2.Request) (result interface{}, err error) {
	switch req.Method {
	case "initialize":
		return h.handleInitialize(ctx, conn, req)
	case "initialized":
		return
	case "textDocument/didOpen":
		return h.handleTextDocumentDidOpen(ctx, conn, req)
	case "textDocument/didChange":
		return h.handleTextDocumentDidChange(ctx, conn, req)
	case "textDocument/didClose":
		return h.handleTextDocumentDidClose(ctx, conn, req)
	case "textDocument/didSave":
		return h.handleTextDocumentDidSave(ctx, conn, req)
	}
	return nil, &jsonrpc2.Error{Code: jsonrpc2.CodeMethodNotFound, Message: fmt.Sprintf("method not supported: %s", req.Method)}
}

func uriToDocumentURI(uri string) lsp.DocumentURI {
	return lsp.DocumentURI(fmt.Sprintf("file://%s", uri))
}

func documentURIToURI(duri lsp.DocumentURI) string {
	return string(duri)[len("file://"):]
}
