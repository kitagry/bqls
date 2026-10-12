package langserver

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/kitagry/bqls/langserver/internal/bigquery"
	"github.com/kitagry/bqls/langserver/internal/lint"
	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/kitagry/bqls/langserver/internal/source"
	"github.com/sirupsen/logrus"
	"github.com/sourcegraph/jsonrpc2"
)

// jsonrpcConn is the subset of *jsonrpc2.Conn the handler needs, factored
// out so tests can substitute a fake and assert on notifications sent.
type jsonrpcConn interface {
	Notify(ctx context.Context, method string, params interface{}, opts ...jsonrpc2.CallOption) error
	Close() error
}

type Handler struct {
	conn    jsonrpcConn
	logger  *logrus.Logger
	version string

	bqClient bigquery.Client
	project  *source.Project

	diagnosticRequest          chan lsp.DocumentURI
	dryrunRequest              chan lsp.DocumentURI
	virtualTextDocumentRequest chan lsp.DocumentURI
	// closing is closed by Close so background senders can stop; closeMu and
	// closed keep them from sending on request channels Close has closed.
	closing chan struct{}
	closeMu sync.RWMutex
	closed  bool

	shutdown atomic.Bool
	// exit is buffered so the exit notification never blocks the handler,
	// even if nobody is waiting on ExitNotify.
	exit chan int

	// settingsMu guards initializeParams, which diagnostics goroutines read
	// while workspace/didChangeConfiguration may replace it.
	settingsMu       sync.RWMutex
	initializeParams lsp.InitializeParams[InitializeOption]
}

func (h *Handler) lintConfig() lint.Config {
	h.settingsMu.RLock()
	defer h.settingsMu.RUnlock()
	return h.initializeParams.InitializationOptions.Lint
}

var _ jsonrpc2.Handler = (*Handler)(nil)

func NewHandler(isDebug bool, version string) *Handler {
	logger := logrus.New()
	logger.Out = os.Stderr
	if isDebug {
		logger.SetLevel(logrus.DebugLevel)
	} else {
		logger.SetLevel(logrus.InfoLevel)
	}

	handler := newHandler(logger, version)
	go handler.scheduleDiagnostics()
	go handler.scheduleDryRun()
	go handler.scheduleVirtualTextDocument()
	return handler
}

// newHandler builds a Handler without starting its schedulers.
func newHandler(logger *logrus.Logger, version string) *Handler {
	return &Handler{
		logger:                     logger,
		version:                    version,
		diagnosticRequest:          make(chan lsp.DocumentURI, 3),
		dryrunRequest:              make(chan lsp.DocumentURI, 3),
		virtualTextDocumentRequest: make(chan lsp.DocumentURI, 3),
		closing:                    make(chan struct{}),
		exit:                       make(chan int, 1),
	}
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
	// Unblock background senders first, then wait for them to leave before
	// closing the channels they send on.
	close(h.closing)
	h.closeMu.Lock()
	h.closed = true
	close(h.diagnosticRequest)
	close(h.dryrunRequest)
	close(h.virtualTextDocumentRequest)
	h.closeMu.Unlock()
	return errors.Join(errs...)
}

// ExitNotify delivers the process exit code once the client sends the exit
// notification: 0 if shutdown was received first, otherwise 1.
func (h *Handler) ExitNotify() <-chan int {
	return h.exit
}

func (h *Handler) handle(ctx context.Context, conn *jsonrpc2.Conn, req *jsonrpc2.Request) (result any, err error) {
	if h.shutdown.Load() && req.Method != "exit" {
		if req.Notif {
			return nil, nil
		}
		return nil, &jsonrpc2.Error{Code: jsonrpc2.CodeInvalidRequest, Message: fmt.Sprintf("server is shutting down: %s", req.Method)}
	}

	switch req.Method {
	case "initialize":
		return h.handleInitialize(ctx, conn, req)
	case "initialized":
		return
	case "shutdown":
		h.shutdown.Store(true)
		return nil, nil
	case "exit":
		code := 1
		if h.shutdown.Load() {
			code = 0
		}
		select {
		case h.exit <- code:
		default:
		}
		return nil, nil
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
	// Per the LSP spec, "$/"-prefixed notifications (e.g. $/setTrace,
	// $/cancelRequest) are protocol-implementation-dependent and may be
	// freely ignored by servers that don't implement them; only "$/"
	// requests must still be errored with MethodNotFound.
	if req.Notif && strings.HasPrefix(req.Method, "$/") {
		return nil, nil
	}
	return nil, &jsonrpc2.Error{Code: jsonrpc2.CodeMethodNotFound, Message: fmt.Sprintf("method not supported: %s", req.Method)}
}
