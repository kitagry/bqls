package langserver

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"math"
	"strings"

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

			diagnostics, totalProcessed, err := h.dryrun(ctx, uri)
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

			for uri, d := range diagnostics {
				err := h.publishDiagnostics(ctx, uri, d)
				if err != nil {
					h.logger.Debugf(`failed to send "textDocument/publishDiagnostics" for %s: %v`, uri, err)
				}
			}
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
	diags, ok := p.Diagnose(ctx, uri, h.lintConfig())
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

func (h *Handler) dryrun(ctx context.Context, uri lsp.DocumentURI) (errs map[lsp.DocumentURI][]lsp.Diagnostic, totalProcessed string, err error) {
	p, release := h.acquireProject()
	js, err := p.Dryrun(ctx, uri)
	release()
	if err != nil {
		return nil, "", err
	}
	if js == nil {
		return nil, "", err
	}
	defer js.Done()

	errs, _ = h.diagnose(ctx, uri)
	for _, err := range js.Errors {
		h.logger.Info("error location", err.Location)
	}

	totalProcessed = bytesConvert(js.Statistics.TotalBytesProcessed)

	return errs, totalProcessed, nil
}

func bytesConvert(bytes int64) string {
	if bytes == 0 {
		return "0 bytes"
	}

	base := math.Floor(math.Log(float64(bytes)) / math.Log(1024))
	units := []string{"bytes", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB", "ZiB"}

	stringVal := fmt.Sprintf("%.2f", float64(bytes)/math.Pow(1024, base))
	stringVal = strings.TrimSuffix(stringVal, ".00")
	return fmt.Sprintf("%s %v",
		stringVal,
		units[int(base)],
	)
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
