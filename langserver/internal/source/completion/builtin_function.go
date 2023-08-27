package completion

import (
	"context"
	"strings"

	"github.com/goccy/go-zetasql/ast"
	"github.com/kitagry/bqls/langserver/internal/function"
	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/kitagry/bqls/langserver/internal/source/file"
)

func (c *completor) completeBuiltinFunction(ctx context.Context, parsedFile file.ParsedFile, position lsp.Position) []CompletionItem {
	termOffset := parsedFile.TermOffset(position)

	// When the cursor is in the middle of the table path, do not suggest the built-in functions.
	tablePathNode, ok := file.SearchAstNode[*ast.TablePathExpressionNode](parsedFile.Node, parsedFile.TermOffset(position))
	if ok && tablePathNode.ParseLocationRange().End().ByteOffset() != termOffset {
		return []CompletionItem{}
	}

	incompleteColumnName := parsedFile.FindIncompleteColumnName(position)

	result := make([]CompletionItem, 0)
	for _, f := range function.BuiltInFunctions {
		if strings.HasPrefix(f.Name, incompleteColumnName) {
			result = append(result, CompletionItem{
				Kind:        lsp.CIKFunction,
				NewText:     f.Name,
				TypedPrefix: incompleteColumnName,
				Documentation: lsp.MarkupContent{
					Kind:  lsp.MKMarkdown,
					Value: f.Description,
				},
			})
		}
	}
	return result
}
