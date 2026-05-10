package completion

import (
	"context"
	"strings"

	googlesql "github.com/goccy/go-googlesql"
	"github.com/kitagry/bqls/langserver/internal/function"
	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/kitagry/bqls/langserver/internal/source/file"
)

func (c *completor) completeBuiltinFunction(ctx context.Context, parsedFile file.ParsedFile, position lsp.Position) []CompletionItem {
	termOffset := parsedFile.TermOffset(position)

	// When parse failed (node is nil), check if cursor is inside a backtick-quoted table path.
	// If so, no function completions are appropriate.
	if parsedFile.Node == nil {
		src := parsedFile.Src
		if termOffset <= len(src) {
			if strings.Count(src[:termOffset], "`")%2 == 1 {
				return []CompletionItem{}
			}
		}
	}

	// When the cursor is in the middle of the table path, do not suggest the built-in functions.
	tablePathNode, ok := file.SearchAstNode[*googlesql.ASTTablePathExpression](parsedFile.Node, parsedFile.TermOffset(position))
	if ok {
		loc, _ := tablePathNode.GetParseLocationRange()
		endOff := file.ParseLocEnd(loc)
		if endOff != termOffset {
			return []CompletionItem{}
		}
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
