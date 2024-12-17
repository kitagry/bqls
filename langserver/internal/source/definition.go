package source

import (
	"context"
	"fmt"
	"strings"

	"github.com/goccy/go-zetasql/ast"
	"github.com/goccy/go-zetasql/types"
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

		return []lsp.Location{{
			URI:   lsp.NewDocumentURI(uri),
			Range: bqlsLocationToLspLocation(locRange, sql.RawText),
		}}, nil
	}

	return nil, fmt.Errorf("not found")
}

func bqlsLocationToLspLocation(loc *types.ParseLocationRange, file string) lsp.Range {
	return lsp.Range{
		Start: bqlsPointToLspPoint(loc.Start(), file),
		End:   bqlsPointToLspPoint(loc.End(), file),
	}
}

func bqlsPointToLspPoint(point *types.ParseLocationPoint, file string) lsp.Position {
	return offsetToLspPoint(point.ByteOffset(), file)
}

func offsetToLspPoint(offset int, file string) lsp.Position {
	toEndText := file[:offset]
	line := strings.Count(toEndText, "\n")
	newLineInd := strings.LastIndex(toEndText, "\n")
	var char int
	if newLineInd == -1 {
		char = len(toEndText)
	} else {
		char = len(toEndText[newLineInd:]) - 1
	}
	return lsp.Position{
		Line:      line,
		Character: char,
	}
}
