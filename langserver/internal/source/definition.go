package source

import (
	"context"
	"strings"

	"github.com/goccy/go-zetasql/ast"
	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/kitagry/bqls/langserver/internal/source/file"
)

func (p *Project) LookupIdent(ctx context.Context, uri lsp.DocumentURI, position lsp.Position) ([]lsp.Location, error) {
	sql := p.cache.Get(uri)
	parsedFile := p.analyzer.ParseFile(uri, sql.RawText)

	termOffset := parsedFile.TermOffset(position)

	tablePathExpression, ok := file.SearchAstNode[*ast.TablePathExpressionNode](parsedFile.Node, termOffset)
	if ok {
		return p.lookupTablePathExpressionNode(uri, parsedFile, tablePathExpression)
	}

	return nil, nil
}

func (p *Project) lookupTablePathExpressionNode(uri lsp.DocumentURI, parsedFile file.ParsedFile, tablePathExpression *ast.TablePathExpressionNode) ([]lsp.Location, error) {
	pathExpr := tablePathExpression.PathExpr()
	if pathExpr == nil {
		return nil, nil
	}

	tableNames := make([]string, len(pathExpr.Names()))
	for i, n := range tablePathExpression.PathExpr().Names() {
		tableNames[i] = n.Name()
	}
	tableName := strings.Join(tableNames, ".")

	// Search table name in table path expression
	result := listupTargetWithClauseEntries(parsedFile, uri, tableName)

	return result, nil
}

func listupTargetWithClauseEntries(parsedFile file.ParsedFile, uri lsp.DocumentURI, tableName string) []lsp.Location {
	withClauseEntries := file.ListAstNode[*ast.WithClauseEntryNode](parsedFile.Node)
	result := make([]lsp.Location, 0, len(withClauseEntries))
	for _, entry := range withClauseEntries {
		if entry.Alias().Name() != tableName {
			continue
		}
		locRange := entry.Alias().ParseLocationRange()
		if locRange == nil {
			continue
		}
		r, ok := parsedFile.ToLspRange(locRange)
		if !ok {
			continue
		}
		result = append(result, lsp.Location{
			URI:   uri,
			Range: r,
		})
	}
	return result
}
