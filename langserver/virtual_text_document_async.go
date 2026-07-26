package langserver

import (
	"context"

	"github.com/kitagry/bqls/langserver/internal/lsp"
)

// scheduleVirtualTextDocument mirrors scheduleDiagnostics/scheduleDryRun:
// requests for the same uri supersede any still-running fetch for that uri,
// and the result is pushed back via bqls/publishVirtualTextDocument instead
// of being returned synchronously.
func (h *Handler) scheduleVirtualTextDocument() {
	running := make(map[lsp.DocumentURI]context.CancelFunc)

	for {
		uri, ok := <-h.virtualTextDocumentRequest
		if !ok {
			break
		}

		if cancel, ok := running[uri]; ok {
			cancel()
		}

		ctx, cancel := context.WithCancel(context.Background())
		running[uri] = cancel

		go func() {
			defer func() {
				if err := recover(); err != nil {
					h.logger.Errorf("panic in virtual text document: %v", err)
				}
			}()

			info, err := uri.VirtualTextDocumentInfo()
			if err != nil {
				h.publishVirtualTextDocumentError(ctx, uri, err)
				return
			}

			workDoneToken := lsp.ProgressToken("virtual_text_document:" + string(uri))
			h.workDoneProgressBegin(ctx, workDoneToken, lsp.WorkDoneProgressBegin{
				Title:   "Virtual text document",
				Message: "Loading virtual text document info...",
			})
			defer h.workDoneProgressEnd(ctx, workDoneToken, lsp.WorkDoneProgressEnd{})

			// Details are usually much cheaper to fetch than preview rows, so
			// publish them as soon as they're ready instead of waiting for
			// preview too.
			details, err := h.resolveVirtualTextDocumentDetails(ctx, workDoneToken, info)
			if ctx.Err() != nil {
				return // superseded by a newer request for the same uri
			}
			if err != nil {
				h.publishVirtualTextDocumentError(ctx, uri, err)
				return
			}
			h.publishVirtualTextDocumentDetails(ctx, uri, details.contents, details.schema)

			result, err := h.resolveVirtualTextDocumentPreview(ctx, details)
			if ctx.Err() != nil {
				return
			}
			if err != nil {
				h.logger.Printf("failed to build query result: %v", err)
			}
			h.publishVirtualTextDocumentPreview(ctx, uri, result)
		}()
	}
}

func (h *Handler) publishVirtualTextDocumentDetails(ctx context.Context, uri lsp.DocumentURI, contents []lsp.MarkedString, schema []lsp.FieldSchema) {
	err := h.conn.Notify(ctx, "bqls/publishVirtualTextDocument", lsp.PublishVirtualTextDocumentParams{
		TextDocument: lsp.TextDocumentIdentifier{URI: uri},
		Kind:         lsp.VirtualTextDocumentKindDetails,
		Contents:     contents,
		Schema:       schema,
	})
	if err != nil {
		h.logger.Errorf(`failed to send "bqls/publishVirtualTextDocument" (details) for %s: %v`, uri, err)
	}
}

func (h *Handler) publishVirtualTextDocumentPreview(ctx context.Context, uri lsp.DocumentURI, result lsp.QueryResult) {
	err := h.conn.Notify(ctx, "bqls/publishVirtualTextDocument", lsp.PublishVirtualTextDocumentParams{
		TextDocument: lsp.TextDocumentIdentifier{URI: uri},
		Kind:         lsp.VirtualTextDocumentKindPreview,
		Result:       &result,
	})
	if err != nil {
		h.logger.Errorf(`failed to send "bqls/publishVirtualTextDocument" (preview) for %s: %v`, uri, err)
	}
}

func (h *Handler) publishVirtualTextDocumentError(ctx context.Context, uri lsp.DocumentURI, err error) {
	notifyErr := h.conn.Notify(ctx, "bqls/publishVirtualTextDocument", lsp.PublishVirtualTextDocumentParams{
		TextDocument: lsp.TextDocumentIdentifier{URI: uri},
		Error:        err.Error(),
	})
	if notifyErr != nil {
		h.logger.Errorf(`failed to send "bqls/publishVirtualTextDocument" error for %s: %v`, uri, notifyErr)
	}
}
