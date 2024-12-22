package source

import (
	"context"
	"fmt"
	"strings"

	"github.com/goccy/go-zetasql/ast"
	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/kitagry/bqls/langserver/internal/source/file"
)

func (p *Project) LookupIdent(ctx context.Context, uri string, position lsp.Position) ([]lsp.Location, error) {
	sql := p.cache.Get(uri)
	parsedFile := p.analyzer.ParseFile(uri, sql.RawText)

	termOffset := parsedFile.TermOffset(position)

	tablePathExpression, ok := file.SearchAstNode[*ast.TablePathExpressionNode](parsedFile.Node, termOffset)
	if !ok {
		return nil, fmt.Errorf("not found")
	}

	pathExpr := tablePathExpression.PathExpr()
	if pathExpr == nil {
		return nil, fmt.Errorf("not found")
	}

	tableNames := make([]string, len(pathExpr.Names()))
	for i, n := range tablePathExpression.PathExpr().Names() {
		tableNames[i] = n.Name()
	}
	tableName := strings.Join(tableNames, ".")

	withClauseEntries := file.ListAstNode[*ast.WithClauseEntryNode](parsedFile.Node)
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

		return []lsp.Location{{
			URI:   lsp.NewDocumentURI(uri),
			Range: r,
		}}, nil
	}

	return nil, fmt.Errorf("not found")
}
