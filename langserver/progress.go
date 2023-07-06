package langserver

import (
	"context"

	"github.com/kitagry/bqls/langserver/internal/lsp"
)

func (h *Handler) workDoneProgressBegin(ctx context.Context, token lsp.ProgressToken, params lsp.WorkDoneProgressBegin) error {
	if !h.initializeParams.Capabilities.Window.WorkDoneProgress {
		return nil
	}
	return h.conn.Notify(ctx, "$/progress", lsp.ProgressParams[lsp.WorkDoneProgressBegin]{
		Token: token,
		Value: &params,
	})
}

func (h *Handler) workDoneProgressReport(ctx context.Context, token lsp.ProgressToken, params lsp.WorkDoneProgressReport) error {
	if !h.initializeParams.Capabilities.Window.WorkDoneProgress {
		return nil
	}
	return h.conn.Notify(ctx, "$/progress", lsp.ProgressParams[lsp.WorkDoneProgressReport]{
		Token: token,
		Value: &params,
	})
}

func (h *Handler) workDoneProgressEnd(ctx context.Context, token lsp.ProgressToken, params lsp.WorkDoneProgressEnd) error {
	if !h.initializeParams.Capabilities.Window.WorkDoneProgress {
		return nil
	}
	return h.conn.Notify(ctx, "$/progress", lsp.ProgressParams[lsp.WorkDoneProgressEnd]{
		Token: token,
		Value: &params,
	})
}
