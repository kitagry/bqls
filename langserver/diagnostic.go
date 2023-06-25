package langserver

import (
	"context"

	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/kitagry/bqls/langserver/internal/source"
)

func (h *Handler) diagnostic() {
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
				h.conn.Notify(ctx, "textDocument/publishDiagnostics", lsp.PublishDiagnosticsParams{
					URI:         uri,
					Diagnostics: d,
				})
			}
		}()
	}
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

func convertErrorsToDiagnostics(errs []source.Error) []lsp.Diagnostic {
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
