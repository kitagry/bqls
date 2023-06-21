package langserver

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"

	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/kitagry/bqls/langserver/internal/source"
	"github.com/sourcegraph/jsonrpc2"
)

type Handler struct {
	conn   *jsonrpc2.Conn
	logger *log.Logger

	project *source.Project

	diagnosticRequest chan lsp.DocumentURI
	initializeParams  lsp.InitializeParams
}

var _ jsonrpc2.Handler = (*Handler)(nil)

func NewHandler() *Handler {
	handler := &Handler{
		logger:            log.New(os.Stderr, "", log.LstdFlags),
		diagnosticRequest: make(chan lsp.DocumentURI, 3),
	}
	go handler.diagnostic()
	return handler
}

func (h *Handler) Handle(ctx context.Context, conn *jsonrpc2.Conn, req *jsonrpc2.Request) {
	jsonrpc2.HandlerWithError(h.handle).Handle(ctx, conn, req)
}

func (h *Handler) Close() error {
	var errs []error
	errs = append(errs, h.conn.Close())
	errs = append(errs, h.project.Close())
	close(h.diagnosticRequest)
	return errors.Join(errs...)
}

func (h *Handler) handle(ctx context.Context, conn *jsonrpc2.Conn, req *jsonrpc2.Request) (result interface{}, err error) {
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
	case "textDocument/formatting":
		return h.handleTextDocumentFormatting(ctx, conn, req)
	case "textDocument/hover":
		return h.handleTextDocumentHover(ctx, conn, req)
	case "textDocument/completion":
		return h.handleTextDocumentCompletion(ctx, conn, req)
	}
	return nil, &jsonrpc2.Error{Code: jsonrpc2.CodeMethodNotFound, Message: fmt.Sprintf("method not supported: %s", req.Method)}
}

func uriToDocumentURI(uri string) lsp.DocumentURI {
	return lsp.DocumentURI(fmt.Sprintf("file://%s", uri))
}

func documentURIToURI(duri lsp.DocumentURI) string {
	return string(duri)[len("file://"):]
}
