package langserver

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime/debug"

	"github.com/kitagry/bqls/langserver/internal/bigquery"
	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/kitagry/bqls/langserver/internal/source"
	"github.com/sirupsen/logrus"
	"github.com/sourcegraph/jsonrpc2"
)

type Handler struct {
	conn   *jsonrpc2.Conn
	logger *logrus.Logger

	bqClient bigquery.Client
	project  *source.Project

	diagnosticRequest chan lsp.DocumentURI
	dryrunRequest     chan lsp.DocumentURI
	initializeParams  lsp.InitializeParams[InitializeOption]
}

var _ jsonrpc2.Handler = (*Handler)(nil)

func NewHandler(isDebug bool) *Handler {
	logger := logrus.New()
	logger.Out = os.Stderr
	if isDebug {
		logger.SetLevel(logrus.DebugLevel)
	} else {
		logger.SetLevel(logrus.InfoLevel)
	}

	handler := &Handler{
		logger:            logger,
		diagnosticRequest: make(chan lsp.DocumentURI, 3),
		dryrunRequest:     make(chan lsp.DocumentURI, 3),
	}
	go handler.scheduleDiagnostics()
	go handler.scheduleDryRun()
	return handler
}

func (h *Handler) setupByInitializeParams() error {
	bqClient, err := bigquery.New(
		context.Background(),
		h.initializeParams.InitializationOptions.ProjectID,
		h.initializeParams.InitializationOptions.Location,
		true,
		h.logger,
	)
	if err != nil {
		return err
	}

	p := source.NewProject(context.Background(), h.initializeParams.RootPath, bqClient, h.logger)

	h.bqClient = bqClient
	h.project = p
	return nil
}

func (h *Handler) Handle(ctx context.Context, conn *jsonrpc2.Conn, req *jsonrpc2.Request) {
	defer func() {
		if err := recover(); err != nil {
			h.logger.Errorf("panic! %#v", err)
			h.logger.Errorf("stack trace: %s", string(debug.Stack()))
		}
	}()
	jsonrpc2.HandlerWithError(h.handle).Handle(ctx, conn, req)
}

func (h *Handler) Close() error {
	var errs []error
	if h.conn != nil {
		errs = append(errs, h.conn.Close())
	}
	if h.project != nil {
		errs = append(errs, h.project.Close())
	}
	close(h.diagnosticRequest)
	close(h.dryrunRequest)
	return errors.Join(errs...)
}

func (h *Handler) handle(ctx context.Context, conn *jsonrpc2.Conn, req *jsonrpc2.Request) (result any, err error) {
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
	case "completionItem/resolve":
		return h.handleCompletionItemResolve(ctx, conn, req)
	case "textDocument/definition":
		return h.handleTextDocumentDefinition(ctx, conn, req)
	case "textDocument/codeAction":
		return h.handleTextDocumentCodeAction(ctx, conn, req)
	case "workspace/executeCommand":
		return h.handleWorkspaceExecuteCommand(ctx, conn, req)
	case "workspace/didChangeConfiguration":
		return h.handleWorkspaceDidChangeConfiguration(ctx, conn, req)
	case "bqls/virtualTextDocument":
		return h.handleVirtualTextDocument(ctx, conn, req)
	}
	return nil, &jsonrpc2.Error{Code: jsonrpc2.CodeMethodNotFound, Message: fmt.Sprintf("method not supported: %s", req.Method)}
}
