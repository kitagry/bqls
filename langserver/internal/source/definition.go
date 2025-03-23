package source

import (
	"context"
	"strings"

	"github.com/goccy/go-zetasql/ast"
	rast "github.com/goccy/go-zetasql/resolved_ast"
	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/kitagry/bqls/langserver/internal/source/file"
)

func (p *Project) LookupIdent(ctx context.Context, uri lsp.DocumentURI, position lsp.Position) ([]lsp.Location, error) {
	sql := p.cache.Get(uri)
	parsedFile := p.analyzer.ParseFile(uri, sql.RawText)

	termOffset := parsedFile.TermOffset(position)

	tablePathExpression, ok := file.SearchAstNode[*ast.TablePathExpressionNode](parsedFile.Node, termOffset)
	if ok {
		// NOTE: ignore rast.ScanNode because it is not resolved
		output, ok := parsedFile.FindTargetAnalyzeOutput(termOffset)
		var tablePathRastNode rast.ScanNode
		if ok {
			tablePathRastNode, _ = file.SearchResolvedAstNode[rast.ScanNode](output, termOffset)
		}
		return p.lookupTablePathExpressionNode(ctx, uri, parsedFile, tablePathExpression, tablePathRastNode)
	}

	return nil, nil
}

func (p *Project) lookupTablePathExpressionNode(ctx context.Context, uri lsp.DocumentURI, parsedFile file.ParsedFile, tablePathExpression *ast.TablePathExpressionNode, tablePathRastNode rast.ScanNode) ([]lsp.Location, error) {
	p.logger.Println("lookupTablePathExpressionNode", tablePathExpression, tablePathRastNode)
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
	result := p.listupTargetTableExpressionEntries(ctx, parsedFile, uri, tableName, tablePathRastNode)

	// Search table name in with clause
	result = append(result, p.listupTargetWithClauseEntries(parsedFile, uri, tableName)...)

	return result, nil
}

func (p *Project) listupTargetTableExpressionEntries(ctx context.Context, parsedFile file.ParsedFile, uri lsp.DocumentURI, tableName string, scanNode rast.ScanNode) []lsp.Location {
	tableScanNode, ok := scanNode.(*rast.TableScanNode)
	if !ok {
		return nil
	}

	metadata, err := p.analyzer.GetTableMetadataFromPath(ctx, tableScanNode.Table().Name())
	if err != nil {
		return nil
	}

	projectID, datasetID, tableID, ok := extractTableIDsFromMedatada(metadata)
	if !ok {
		return nil
	}

	return []lsp.Location{
		{
			URI: lsp.NewTableVirtualTextDocumentURI(projectID, datasetID, tableID),
			Range: lsp.Range{
				Start: lsp.Position{
					Line:      0,
					Character: 0,
				},
				End: lsp.Position{
					Line:      0,
					Character: 0,
				},
			},
		},
	}
}

func (p *Project) listupTargetWithClauseEntries(parsedFile file.ParsedFile, uri lsp.DocumentURI, tableName string) []lsp.Location {
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
