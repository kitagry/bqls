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

	// projectMu guards bqClient and project, which background goroutines
	// read while workspace/didChangeConfiguration may replace them, and
	// projectUsers, which counts the goroutines still using each project so
	// a replaced one is closed only after they finish.
	projectMu    sync.Mutex
	bqClient     bigquery.Client
	project      *source.Project
	projectUsers map[*source.Project]*sync.WaitGroup
	// newBQClient creates the client for a (re)built project; nil means
	// bigquery.New. Tests replace it to avoid real credentials.
	newBQClient func(ctx context.Context, projectID, location string) (bigquery.Client, error)

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

	lintConfigFiles lintConfigFiles
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
	newBQClient := h.newBQClient
	if newBQClient == nil {
		newBQClient = func(ctx context.Context, projectID, location string) (bigquery.Client, error) {
			return bigquery.New(ctx, projectID, location, true, h.logger)
		}
	}
	bqClient, err := newBQClient(
		context.Background(),
		h.initializeParams.InitializationOptions.ProjectID,
		h.initializeParams.InitializationOptions.Location,
	)
	if err != nil {
		return err
	}

	p := source.NewProject(context.Background(), h.initializeParams.RootPath, bqClient, h.logger)
	if h.project != nil {
		// The client keeps these documents open and won't resend them, so a
		// rebuilt project must start with them.
		for _, uri := range h.project.OpenFiles() {
			text, ok := h.project.GetFile(uri)
			if !ok {
				continue
			}
			if err := p.UpdateFile(uri, text, 0); err != nil {
				return err
			}
		}
	}

	h.projectMu.Lock()
	prev := h.project
	prevUsers := h.projectUsers[prev]
	delete(h.projectUsers, prev)
	h.bqClient = bqClient
	h.project = p
	h.projectMu.Unlock()

	if prev != nil {
		// Waiting here would stall the JSON-RPC handler until in-flight
		// queries such as dry runs finish.
		go func() {
			if prevUsers != nil {
				prevUsers.Wait()
			}
			if err := prev.Close(); err != nil {
				h.logger.Errorf("failed to close the previous project: %v", err)
			}
		}()
	}
	return nil
}

// acquireProject returns the current project for use off the JSON-RPC
// handler goroutine. release must be called once the project, including
// anything derived from it, is no longer used.
func (h *Handler) acquireProject() (p *source.Project, release func()) {
	h.projectMu.Lock()
	defer h.projectMu.Unlock()
	if h.project == nil {
		return nil, func() {}
	}
	users, ok := h.projectUsers[h.project]
	if !ok {
		if h.projectUsers == nil {
			h.projectUsers = make(map[*source.Project]*sync.WaitGroup)
		}
		users = &sync.WaitGroup{}
		h.projectUsers[h.project] = users
	}
	users.Add(1)
	return h.project, users.Done
}

func (h *Handler) Handle(ctx context.Context, conn *jsonrpc2.Conn, req *jsonrpc2.Request) {
	jsonrpc2.HandlerWithError(h.handleWithRecover).Handle(ctx, conn, req)
}

// handleWithRecover turns a panic into an InternalError so that
// HandlerWithError still replies and the client doesn't wait forever.
func (h *Handler) handleWithRecover(ctx context.Context, conn *jsonrpc2.Conn, req *jsonrpc2.Request) (result any, err error) {
	defer func() {
		if r := recover(); r != nil {
			h.logger.Errorf("panic in %s: %v", req.Method, r)
			h.logger.Errorf("stack trace: %s", string(debug.Stack()))
			result = nil
			err = &jsonrpc2.Error{Code: jsonrpc2.CodeInternalError, Message: fmt.Sprintf("internal error while handling %s: %v", req.Method, r)}
		}
	}()
	return h.handle(ctx, conn, req)
}

func (h *Handler) Close() error {
	var errs []error
	if h.conn != nil {
		errs = append(errs, h.conn.Close())
	}
	h.projectMu.Lock()
	p := h.project
	h.projectMu.Unlock()
	if p != nil {
		errs = append(errs, p.Close())
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
