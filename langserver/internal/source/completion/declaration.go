package completion

import (
	"context"
	"fmt"
	"strings"

	googlesql "github.com/goccy/go-googlesql"
	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/kitagry/bqls/langserver/internal/source/file"
)

func (c *completor) completeDeclaration(ctx context.Context, parsedFile file.ParsedFile, position lsp.Position) []CompletionItem {
	termOffset := parsedFile.TermOffset(position)

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

	declarations := file.ListAstNode[*googlesql.ASTVariableDeclaration](parsedFile.Node)

	result := make([]CompletionItem, 0, len(declarations))
	for _, d := range declarations {
		varList, err := d.VariableList()
		if err != nil || varList == nil {
			continue
		}
		n, _ := varList.NumChildren()
		for i := int32(0); i < n; i++ {
			ident, err := varList.IdentifierList(i)
			if err != nil {
				continue
			}
			name, err := ident.GetAsString()
			if err != nil {
				continue
			}
			if !strings.HasPrefix(name, incompleteColumnName) {
				continue
			}
			unparsed, _ := googlesql.Unparse(d)
			result = append(result, CompletionItem{
				Kind:    lsp.CIKVariable,
				NewText: name,
				Documentation: lsp.MarkupContent{
					Kind:  lsp.MKMarkdown,
					Value: fmt.Sprintf("```sql\n%s```", unparsed),
				},
			})
		}
	}
	return result
}
