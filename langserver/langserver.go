package langserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/kitagry/bqls/langserver/internal/source"
	"github.com/sirupsen/logrus"
	"github.com/sourcegraph/jsonrpc2"
)

type Handler struct {
	conn   *jsonrpc2.Conn
	logger *logrus.Logger

	project *source.Project

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

func (h *Handler) Handle(ctx context.Context, conn *jsonrpc2.Conn, req *jsonrpc2.Request) {
	defer func() {
		err := recover()
		if err != nil {
			h.logger.Errorf("panic: %v", err)
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

func (h *Handler) handle(ctx context.Context, conn *jsonrpc2.Conn, req *jsonrpc2.Request) (result interface{}, err error) {
	switch req.Method {
	case "initialize":
		return h.handleInitialize(ctx, conn, req)
	case "initialized":
		return
	case "textDocument/didOpen":
		return ignoreMiddleware(h.handleTextDocumentDidOpen)(ctx, conn, req)
	case "textDocument/didChange":
		return ignoreMiddleware(h.handleTextDocumentDidChange)(ctx, conn, req)
	case "textDocument/didClose":
		return ignoreMiddleware(h.handleTextDocumentDidClose)(ctx, conn, req)
	case "textDocument/didSave":
		return ignoreMiddleware(h.handleTextDocumentDidSave)(ctx, conn, req)
	case "textDocument/formatting":
		return ignoreMiddleware(h.handleTextDocumentFormatting)(ctx, conn, req)
	case "textDocument/hover":
		return ignoreMiddleware(h.handleTextDocumentHover)(ctx, conn, req)
	case "textDocument/completion":
		return ignoreMiddleware(h.handleTextDocumentCompletion)(ctx, conn, req)
	case "textDocument/codeAction":
		return h.handleTextDocumentCodeAction(ctx, conn, req)
	case "workspace/executeCommand":
		return h.handleWorkspaceExecuteCommand(ctx, conn, req)
	}
	return nil, &jsonrpc2.Error{Code: jsonrpc2.CodeMethodNotFound, Message: fmt.Sprintf("method not supported: %s", req.Method)}
}

type HandleFunc func(ctx context.Context, conn *jsonrpc2.Conn, req *jsonrpc2.Request) (result any, err error)

// In order to use bqls.nvim, we need to ignore requests that are not SQL files.
// For example, when we open neo-tree to show the project-dataset-table structure, it sends a request to the language server.
// But we don't need to handle it.
func ignoreMiddleware(next HandleFunc) HandleFunc {
	type TStruct struct {
		TextDocument lsp.TextDocumentIdentifier `json:"textDocument"`
	}

	return func(ctx context.Context, conn *jsonrpc2.Conn, req *jsonrpc2.Request) (result any, err error) {
		var s TStruct
		if err := json.Unmarshal(*req.Params, &s); err != nil {
			return nil, err
		}

		// When buffer is not a SQL file, ignore the request
		if !strings.HasSuffix(string(s.TextDocument.URI), ".sql") {
			return nil, nil
		}
		return next(ctx, conn, req)
	}
}

func uriToDocumentURI(uri string) lsp.DocumentURI {
	return lsp.DocumentURI(fmt.Sprintf("file://%s", uri))
}

func documentURIToURI(duri lsp.DocumentURI) string {
	return string(duri)[len("file://"):]
}
