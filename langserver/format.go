package langserver

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	googlesql "github.com/goccy/go-googlesql"
	"github.com/kitagry/bqls/langserver/internal/lint"
	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/kitagry/bqls/langserver/internal/source/helper"
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

	rawText, ok := h.project.GetFile(params.TextDocument.URI)
	if !ok {
		return nil, fmt.Errorf("failed to find document %s", params.TextDocument.URI)
	}

	// FormatSql drops comments and go-googlesql does not expose the
	// comment-preserving LenientFormatSql, so leave such documents untouched
	// rather than silently deleting the user's comments.
	if lint.HasComment(rawText) {
		return nil, nil
	}

	formatted, err := googlesql.FormatSql(rawText)
	if err != nil {
		return nil, fmt.Errorf("failed to format: %w", err)
	}

	if len(formatted) == 0 {
		return nil, nil
	}

	return ComputeEdits(params.TextDocument.URI, rawText, string(formatted)), nil
}

// ComputeEdits computes diff edits from 2 string inputs
func ComputeEdits(uri lsp.DocumentURI, before, after string) []lsp.TextEdit {
	beforeLines := splitLines(before)
	ops := operations(beforeLines, splitLines(after))
	edits := make([]lsp.TextEdit, 0, len(ops))
	// Without a trailing newline, the line after the last one does not exist,
	// so a position there must be the end of the last line instead.
	pos := func(line int) lsp.Position {
		if line < len(beforeLines) || strings.HasSuffix(before, "\n") || before == "" {
			return lsp.Position{Line: line, Character: 0}
		}
		last := len(beforeLines) - 1
		return helper.NewPositionConverter(before).ToUTF16(lsp.Position{Line: last, Character: len(beforeLines[last])})
	}
	for _, op := range ops {
		switch op.Kind {
		case Delete:
			// Delete: unformatted[i1:i2] is deleted.
			edits = append(edits, lsp.TextEdit{Range: lsp.Range{
				Start: pos(op.I1),
				End:   pos(op.I2),
			}})
		case Insert:
			// Insert: formatted[j1:j2] is inserted at unformatted[i1:i1].
			if content := strings.Join(op.Content, ""); content != "" {
				edits = append(edits, lsp.TextEdit{
					Range: lsp.Range{
						Start: pos(op.I1),
						End:   pos(op.I2),
					},
					NewText: content,
				})
			}
		}
	}
	return edits
}
