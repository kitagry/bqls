package completion

import (
	"context"
	"fmt"
	"strings"

	"github.com/goccy/go-zetasql"
	"github.com/goccy/go-zetasql/ast"
	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/kitagry/bqls/langserver/internal/source/file"
)

func (c *completor) completeDeclaration(ctx context.Context, parsedFile file.ParsedFile, position lsp.Position) []CompletionItem {
	termOffset := parsedFile.TermOffset(position)

	// When the cursor is in the middle of the table path, do not suggest the built-in functions.
	tablePathNode, ok := file.SearchAstNode[*ast.TablePathExpressionNode](parsedFile.Node, parsedFile.TermOffset(position))
	if ok && tablePathNode.ParseLocationRange().End().ByteOffset() != termOffset {
		return []CompletionItem{}
	}

	incompleteColumnName := parsedFile.FindIncompleteColumnName(position)

	declarations := file.ListAstNode[*ast.VariableDeclarationNode](parsedFile.Node)

	result := make([]CompletionItem, 0, len(declarations))
	for _, d := range declarations {
		iList := d.VariableList().IdentifierList()
		for _, l := range iList {
			if !strings.HasPrefix(l.Name(), incompleteColumnName) {
				continue
			}
			result = append(result, CompletionItem{
				Kind:    lsp.CIKVariable,
				NewText: l.Name(),
				Documentation: lsp.MarkupContent{
					Kind:  lsp.MKMarkdown,
					Value: fmt.Sprintf("```sql\n%s```", zetasql.Unparse(d)),
				},
			})
		}
	}
	return result
}
