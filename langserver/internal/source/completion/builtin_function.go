package completion

import (
	"context"
	"strings"

	"github.com/kitagry/bqls/langserver/internal/function"
	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/kitagry/bqls/langserver/internal/source/file"
)

func (c *completor) completeBuiltinFunction(ctx context.Context, parsedFile file.ParsedFile, position lsp.Position) []CompletionItem {
	incompleteColumnName := parsedFile.FindIncompleteColumnName(position)

	result := make([]CompletionItem, 0)
	for _, f := range function.BuiltInFunctions {
		if strings.HasPrefix(f.Name, incompleteColumnName) {
			result = append(result, CompletionItem{
				Kind:          lsp.CIKFunction,
				NewText:       f.Name,
				TypedPrefix:   incompleteColumnName,
				Documentation: f.Description,
			})
		}
	}
	return result
}
