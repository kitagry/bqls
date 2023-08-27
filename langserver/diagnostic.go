package langserver

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"

	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/kitagry/bqls/langserver/internal/source/file"
)

func (h *Handler) scheduleDiagnostics() {
	running := make(map[lsp.DocumentURI]context.CancelFunc)

	for {
		uri, ok := <-h.diagnosticRequest
		if !ok {
			break
		}

		cancel, ok := running[uri]
		if ok {
			cancel()
		}

		ctx, cancel := context.WithCancel(context.Background())
		running[uri] = cancel

		go func() {
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

		cancel, ok := running[uri]
		if ok {
			cancel()
		}

		ctx, cancel := context.WithCancel(context.Background())
		running[uri] = cancel

		go func() {
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

	pathToErrs := h.project.GetErrors(documentURIToURI(uri))
	for path, errs := range pathToErrs {
		uri := uriToDocumentURI(path)
		result[uri] = convertErrorsToDiagnostics(errs)
	}

	return result, nil
}

func (h *Handler) dryrun(ctx context.Context, uri lsp.DocumentURI) (errs map[lsp.DocumentURI][]lsp.Diagnostic, totalProcessed string, err error) {
	js, err := h.project.Dryrun(ctx, documentURIToURI(uri))
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

func convertErrorsToDiagnostics(errs []file.Error) []lsp.Diagnostic {
	result := make([]lsp.Diagnostic, len(errs))
	for i, err := range errs {
		endPosition := err.Position
		endPosition.Character += err.TermLength
		result[i] = lsp.Diagnostic{
			Range: lsp.Range{
				Start: err.Position,
				End:   endPosition,
			},
			Message: err.Msg,
		}
	}
	return result
}
