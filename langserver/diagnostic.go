package langserver

import (
	"cmp"
	"context"
	"errors"
	"fmt"

	bq "cloud.google.com/go/bigquery"

	"github.com/kitagry/bqls/langserver/internal/lint"
	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/kitagry/bqls/langserver/internal/source/file"
	"github.com/kitagry/bqls/langserver/internal/source/helper"
)

func (h *Handler) scheduleDiagnostics() {
	running := make(map[lsp.DocumentURI]context.CancelFunc)

	for {
		uri, ok := <-h.diagnosticRequest
		if !ok {
			break
		}

		if uri.IsVirtualTextDocument() {
			continue
		}

		cancel, ok := running[uri]
		if ok {
			cancel()
		}

		ctx, cancel := context.WithCancel(context.Background())
		running[uri] = cancel

		go func() {
			defer func() {
				if err := recover(); err != nil {
					h.logger.Errorf("panic in diagnostics: %v", err)
					h.clearDiagnosticsAfterPanic(ctx, uri)
				}
			}()
			diagnostics, err := h.diagnose(ctx, uri)
			if err != nil {
				h.logger.Println(err)
				return
			}
			// A newer request for uri is running and will publish; this
			// result may predate the text or dry-run result that one sees.
			if ctx.Err() != nil {
				return
			}

			for uri, d := range diagnostics {
				err := h.publishDiagnostics(ctx, uri, d)
				if err != nil {
					h.logger.Errorf(`failed to send "textDocument/publishDiagnostics" for %s: %v`, uri, err)
				}
			}
		}()
	}
}

func (h *Handler) scheduleDryRun() {
	running := make(map[lsp.DocumentURI]context.CancelFunc)

	for {
		uri, ok := <-h.dryrunRequest
		if !ok {
			break
		}

		if uri.IsVirtualTextDocument() {
			continue
		}

		cancel, ok := running[uri]
		if ok {
			cancel()
		}

		ctx, cancel := context.WithCancel(context.Background())
		running[uri] = cancel

		go func() {
			defer func() {
				if err := recover(); err != nil {
					h.logger.Errorf("panic in dryrun: %v", err)
					h.clearDiagnosticsAfterPanic(ctx, uri)
				}
			}()

			js, err := h.dryrun(ctx, uri)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return
				}
				sendErr := h.showMessage(ctx, lsp.MTError, errors.Unwrap(err).Error())
				if sendErr != nil {
					h.logger.Errorf("failed to dryrun: %v", err)
				}
				return
			}

			if js == nil {
				return
			}
			// Publishing goes through scheduleDiagnostics, which cancels an
			// older run for uri that may have started before the dry-run
			// result was stored and would otherwise publish without it last.
			h.requestDiagnostics(ctx, uri)
			if js.Statistics == nil {
				return
			}
			totalProcessed := helper.ByteSize(js.Statistics.TotalBytesProcessed).String()
			err = h.showMessage(ctx, lsp.Info, fmt.Sprintf("This query will process %s when run.", totalProcessed))
			if err != nil {
				h.logger.Debugf(`failed to send "window/showMessageRequest": %v`, err)
			}
		}()
	}
}

// clearDiagnosticsAfterPanic drops whatever was published before, since
// diagnostics from an earlier version of the file would point at stale
// positions and hide that analysis is currently failing.
func (h *Handler) clearDiagnosticsAfterPanic(ctx context.Context, uri lsp.DocumentURI) {
	if ctx.Err() != nil {
		return // superseded; clearing could wipe the newer request's result
	}
	if err := h.publishDiagnostics(ctx, uri, []lsp.Diagnostic{}); err != nil {
		h.logger.Errorf(`failed to send "textDocument/publishDiagnostics" for %s: %v`, uri, err)
	}
}

func (h *Handler) publishDiagnostics(ctx context.Context, uri lsp.DocumentURI, diagnostics []lsp.Diagnostic) error {
	return h.conn.Notify(ctx, "textDocument/publishDiagnostics", lsp.PublishDiagnosticsParams{
		URI:         uri,
		Diagnostics: diagnostics,
	})
}

func (h *Handler) showMessage(ctx context.Context, level lsp.MessageType, message string) error {
	return h.conn.Notify(ctx, "window/showMessage", lsp.ShowMessageParams{
		Type:    level,
		Message: message,
	})
}

func (h *Handler) diagnose(ctx context.Context, uri lsp.DocumentURI) (map[lsp.DocumentURI][]lsp.Diagnostic, error) {
	result := make(map[lsp.DocumentURI][]lsp.Diagnostic)

	p, release := h.acquireProject()
	defer release()
	diags, ok := p.Diagnose(ctx, uri, h.lintConfigFor(uri))
	if !ok {
		return result, nil
	}
	result[uri] = append(convertErrorsToDiagnostics(diags.Src, diags.Errors), convertLintDiagnostics(diags.Src, diags.Lint)...)

	return result, nil
}

const lintDiagnosticSource = "bqls-lint"

func convertLintDiagnostics(src string, diags []lint.Diagnostic) []lsp.Diagnostic {
	return lintDiagnosticsToLsp(helper.NewPositionConverter(src), diags)
}

func lintDiagnosticsToLsp(conv helper.PositionConverter, diags []lint.Diagnostic) []lsp.Diagnostic {
	result := make([]lsp.Diagnostic, len(diags))
	for i, d := range diags {
		result[i] = lsp.Diagnostic{
			Range:    conv.RangeToUTF16(d.Range),
			Severity: d.Severity,
			Code:     d.Rule,
			Source:   lintDiagnosticSource,
			Message:  d.Message,
		}
	}
	return result
}

func (h *Handler) dryrun(ctx context.Context, uri lsp.DocumentURI) (*bq.JobStatus, error) {
	p, release := h.acquireProject()
	js, err := p.Dryrun(ctx, uri)
	release()
	if err != nil {
		return nil, err
	}
	if js != nil {
		for _, err := range js.Errors {
			h.logger.Info("error location", err.Location)
		}
	}
	return js, nil
}

// requestDiagnostics queues uri for scheduleDiagnostics without blocking past
// Close or the cancellation of ctx, since diagnosticRequest is a small
// buffered channel.
func (h *Handler) requestDiagnostics(ctx context.Context, uri lsp.DocumentURI) {
	h.closeMu.RLock()
	defer h.closeMu.RUnlock()
	if h.closed {
		return
	}
	select {
	case h.diagnosticRequest <- uri:
	case <-h.closing:
	case <-ctx.Done():
	}
}

func convertErrorsToDiagnostics(src string, errs []file.Error) []lsp.Diagnostic {
	return errorsToLspDiagnostics(helper.NewPositionConverter(src), errs)
}

func errorsToLspDiagnostics(conv helper.PositionConverter, errs []file.Error) []lsp.Diagnostic {
	result := make([]lsp.Diagnostic, len(errs))
	for i, err := range errs {
		endPosition := err.Position
		endPosition.Character += err.TermLength
		result[i] = lsp.Diagnostic{
			Range:    conv.RangeToUTF16(lsp.Range{Start: err.Position, End: endPosition}),
			Message:  err.Msg,
			Severity: cmp.Or(err.Severity, lsp.Error),
		}
	}
	return result
}
