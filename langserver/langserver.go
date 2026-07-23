package langserver

import (
	"context"
	"errors"
	"os"

	jsonrpc "github.com/gumeniukcom/golang-jsonrpc2/v2"
	"github.com/kitagry/bqls/langserver/internal/bigquery"
	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/kitagry/bqls/langserver/internal/source"
	"github.com/sirupsen/logrus"
)

type Handler struct {
	// pusher sends server-initiated notifications (publishDiagnostics,
	// $/progress, window/showMessage). It is nil until captured from the
	// request context in handleInitialize.
	pusher jsonrpc.Pusher
	logger *logrus.Logger

	bqClient bigquery.Client
	project  *source.Project

	diagnosticRequest chan lsp.DocumentURI
	dryrunRequest     chan lsp.DocumentURI
	initializeParams  lsp.InitializeParams[InitializeOption]
}

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

// Register wires every supported LSP method onto the dispatcher. Method-not-found
// and panic recovery are handled by the library, so no fallback case is needed.
// Dispatch is sequential (the transport default), matching LSP ordering.
func (h *Handler) Register(rpc *jsonrpc.JSONRPC) {
	must := func(err error) {
		if err != nil {
			h.logger.Fatalf("failed to register method: %v", err)
		}
	}

	must(jsonrpc.RegisterTyped(rpc, "initialize", h.handleInitialize))
	must(jsonrpc.RegisterTyped(rpc, "initialized", h.handleInitialized))
	must(jsonrpc.RegisterTyped(rpc, "textDocument/didOpen", h.handleTextDocumentDidOpen))
	must(jsonrpc.RegisterTyped(rpc, "textDocument/didChange", h.handleTextDocumentDidChange))
	must(jsonrpc.RegisterTyped(rpc, "textDocument/didClose", h.handleTextDocumentDidClose))
	must(jsonrpc.RegisterTyped(rpc, "textDocument/didSave", h.handleTextDocumentDidSave))
	must(jsonrpc.RegisterTyped(rpc, "textDocument/formatting", h.handleTextDocumentFormatting))
	must(jsonrpc.RegisterTyped(rpc, "textDocument/hover", h.handleTextDocumentHover))
	must(jsonrpc.RegisterTyped(rpc, "textDocument/completion", h.handleTextDocumentCompletion))
	must(jsonrpc.RegisterTyped(rpc, "completionItem/resolve", h.handleCompletionItemResolve))
	must(jsonrpc.RegisterTyped(rpc, "textDocument/definition", h.handleTextDocumentDefinition))
	must(jsonrpc.RegisterTyped(rpc, "textDocument/codeAction", h.handleTextDocumentCodeAction))
	must(jsonrpc.RegisterTyped(rpc, "workspace/executeCommand", h.handleWorkspaceExecuteCommand))
	must(jsonrpc.RegisterTyped(rpc, "workspace/didChangeConfiguration", h.handleWorkspaceDidChangeConfiguration))
	must(jsonrpc.RegisterTyped(rpc, "bqls/virtualTextDocument", h.handleVirtualTextDocument))
}

// handleInitialized is the no-op handler for the "initialized" notification.
func (h *Handler) handleInitialized(ctx context.Context, _ struct{}) (struct{}, error) {
	return struct{}{}, nil
}

func (h *Handler) Close() error {
	var errs []error
	// The stdio transport now owns the connection lifecycle, so there is no
	// connection to close here.
	if h.project != nil {
		errs = append(errs, h.project.Close())
	}
	close(h.diagnosticRequest)
	close(h.dryrunRequest)
	return errors.Join(errs...)
}
