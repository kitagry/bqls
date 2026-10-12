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

	h.settingsMu.Lock()
	prev := h.initializeParams.InitializationOptions
	h.initializeParams.InitializationOptions = params.Settings
	h.settingsMu.Unlock()

	// Rebuilding the project recreates the BigQuery client and drops its
	// caches, which is needless when only lint settings changed.
	if h.project != nil && !requiresProjectRebuild(prev, params.Settings) {
		h.rediagnoseOpenFiles()
		return nil, nil
	}

	err = h.setupByInitializeParams()
	if err != nil {
		return nil, err
	}

	return nil, nil
}

func requiresProjectRebuild(prev, next InitializeOption) bool {
	return prev.ProjectID != next.ProjectID || prev.Location != next.Location
}

func (h *Handler) rediagnoseOpenFiles() {
	uris := h.project.OpenFiles()
	// Sent asynchronously because diagnosticRequest is a small buffered
	// channel and blocking here would stall the JSON-RPC handler.
	go func() {
		h.closeMu.RLock()
		defer h.closeMu.RUnlock()
		if h.closed {
			return
		}
		for _, uri := range uris {
			select {
			case h.diagnosticRequest <- uri:
			case <-h.closing:
				return
			}
		}
	}()
}
