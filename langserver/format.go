package langserver

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/goccy/go-zetasql"
	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/sourcegraph/jsonrpc2"
)

func (h *Handler) handleTextDocumentFormatting(ctx context.Context, conn *jsonrpc2.Conn, req *jsonrpc2.Request) (result any, err error) {
	if req.Params == nil {
		return nil, &jsonrpc2.Error{Code: jsonrpc2.CodeInvalidParams}
	}

	var params lsp.DocumentFormattingParams
	if err := json.Unmarshal(*req.Params, &params); err != nil {
		return nil, err
	}

	rawText, ok := h.project.GetFile(documentURIToURI(params.TextDocument.URI))
	if !ok {
		return nil, fmt.Errorf("failed to find document %s", params.TextDocument.URI)
	}

	formatted, err := zetasql.FormatSQL(rawText)
	if !ok {
		return nil, fmt.Errorf("failed to format: %w", err)
	}

	if len(formatted) == 0 {
		return nil, nil
	}

	return ComputeEdits(params.TextDocument.URI, rawText, string(formatted)), nil
}

// ComputeEdits computes diff edits from 2 string inputs
func ComputeEdits(uri lsp.DocumentURI, before, after string) []lsp.TextEdit {
	ops := operations(splitLines(before), splitLines(after))
	edits := make([]lsp.TextEdit, 0, len(ops))
	for _, op := range ops {
		switch op.Kind {
		case Delete:
			// Delete: unformatted[i1:i2] is deleted.
			edits = append(edits, lsp.TextEdit{Range: lsp.Range{
				Start: lsp.Position{Line: op.I1, Character: 0},
				End:   lsp.Position{Line: op.I2, Character: 0},
			}})
		case Insert:
			// Insert: formatted[j1:j2] is inserted at unformatted[i1:i1].
			if content := strings.Join(op.Content, ""); content != "" {
				edits = append(edits, lsp.TextEdit{
					Range: lsp.Range{
						Start: lsp.Position{Line: op.I1, Character: 0},
						End:   lsp.Position{Line: op.I2, Character: 0},
					},
					NewText: content,
				})
			}
		}
	}
	return edits
}
