package langserver

import (
	"context"
	"encoding/json"
	"maps"
	"slices"
	"strings"

	"github.com/kitagry/bqls/langserver/internal/lint"
	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/sourcegraph/jsonrpc2"
)

func (h *Handler) handleWorkspaceDidChangeConfiguration(ctx context.Context, conn *jsonrpc2.Conn, req *jsonrpc2.Request) (result any, err error) {
	if req.Params == nil {
		return nil, &jsonrpc2.Error{Code: jsonrpc2.CodeInvalidParams}
	}

	var params lsp.DidChangeConfigurationParams[json.RawMessage]
	if err := json.Unmarshal(*req.Params, &params); err != nil {
		return nil, err
	}

	h.settingsMu.Lock()
	prev := h.initializeParams.InitializationOptions
	next, err := mergeSettings(prev, params.Settings)
	if err != nil {
		h.settingsMu.Unlock()
		return nil, err
	}
	h.initializeParams.InitializationOptions = next
	h.settingsMu.Unlock()

	// Rebuilding the project recreates the BigQuery client and drops its
	// caches, which is needless when only lint settings changed.
	if h.project != nil && !requiresProjectRebuild(prev, next) {
		h.rediagnoseOpenFiles()
		return nil, nil
	}

	err = h.setupByInitializeParams()
	if err != nil {
		return nil, err
	}

	return nil, nil
}

// mergeSettings applies the fields present in raw on top of prev, so clients
// can send only the settings they changed.
func mergeSettings(prev InitializeOption, raw json.RawMessage) (InitializeOption, error) {
	if len(raw) == 0 {
		return prev, nil
	}
	var present map[string]json.RawMessage
	if err := json.Unmarshal(raw, &present); err != nil {
		return prev, err
	}

	// encoding/json matches field names case-insensitively, so presence
	// must too or a "Lint" key would be merged into the previous rules.
	hasLint := slices.ContainsFunc(slices.Collect(maps.Keys(present)), func(k string) bool {
		return strings.EqualFold(k, "lint")
	})

	next := prev
	if hasLint {
		// Decoding into a non-nil map would add keys to the previous rules;
		// a present lint setting is the complete rule set (an empty one
		// restores the defaults).
		next.Lint = lint.Config{}
	} else {
		// lintConfig() hands the rules map to concurrent readers, so the
		// merged settings must not share it.
		next.Lint.Rules = maps.Clone(prev.Lint.Rules)
	}
	if err := json.Unmarshal(raw, &next); err != nil {
		return prev, err
	}
	return next, nil
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
