package source

import (
	"context"
	"fmt"
	"strings"

	"cloud.google.com/go/bigquery"
	"github.com/goccy/go-zetasql"
	"github.com/goccy/go-zetasql/ast"
	rast "github.com/goccy/go-zetasql/resolved_ast"
	"github.com/kitagry/bqls/langserver/internal/cache"
	"github.com/kitagry/bqls/langserver/internal/lsp"
)

func (p *Project) Complete(ctx context.Context, uri string, position lsp.Position, supportSunippet bool) ([]lsp.CompletionItem, error) {
	result := make([]lsp.CompletionItem, 0)
	sql := p.cache.Get(uri)
	termOffset := positionToByteOffset(sql.RawText, position)

	if hasSelectListEmptyError(sql.Errors) {
		// inserted text
		// before:
		//   SELECT | FROM table_name
		// after:
		//   SELECT * FROM table_name
		rawText := sql.RawText[:termOffset] + "*" + sql.RawText[termOffset:]
		dummySQL := cache.NewSQL(rawText)
		if len(dummySQL.Errors) > 0 {
			return nil, nil
		}
		sql = dummySQL
	}

	var statementNode ast.StatementNode
	for _, stmt := range sql.GetStatementNodes() {
		loc := stmt.ParseLocationRange()
		if loc == nil {
			continue
		}
		startOffset := loc.Start().ByteOffset()
		endOffset := loc.End().ByteOffset()
		if startOffset <= termOffset && termOffset <= endOffset {
			statementNode = stmt
			break
		}
	}
	output, err := p.analyzeStatement(sql.RawText, statementNode)
	if err != nil {
		return nil, nil
	}

	node, ok := searchResolvedAstNode[*rast.ProjectScanNode]([]*zetasql.AnalyzerOutput{output}, termOffset)
	if !ok {
		// In some case, *rast.ProjectScanNode.ParseLocationRange() returns nil.
		// So, if we cannot find *rast.ProjectScanNode, we search *rast.ProjectScanNode which ParseLocationRange returns nil.
		rast.Walk(output.Statement(), func(n rast.Node) error {
			sNode, ok := n.(*rast.ProjectScanNode)
			if !ok {
				return nil
			}
			lRange := sNode.ParseLocationRange()
			if lRange == nil {
				node = sNode
			}
			return nil
		})
		if node == nil {
			return nil, nil
		}
	}

	columns := node.InputScan().ColumnList()
	for _, column := range columns {
		item, ok := p.createCompletionItemFromColumn(ctx, column, position, supportSunippet)
		if ok {
			result = append(result, item)
		}
	}

	return result, nil
}

func hasSelectListEmptyError(errs []error) bool {
	for _, err := range errs {
		if strings.Contains(err.Error(), "Syntax error: SELECT list must not be empty") {
			return true
		}
	}
	return false
}

func (p *Project) createCompletionItemFromColumn(ctx context.Context, column *rast.Column, cursorPosition lsp.Position, supportSunippet bool) (lsp.CompletionItem, bool) {
	tableMetadata, err := p.getTableMetadataFromPath(ctx, column.TableNameID())
	if err != nil {
		// cannot find table metadata
		return createCompletionItemFromColumn(column, cursorPosition, supportSunippet), true
	}

	for _, c := range tableMetadata.Schema {
		if column.Name() == c.Name {
			return createCompletionItemFromSchema(c, cursorPosition, supportSunippet), true
		}
	}

	return lsp.CompletionItem{}, false
}

func createCompletionItemFromColumn(column *rast.Column, cursorPosition lsp.Position, supportSunippet bool) lsp.CompletionItem {
	if !supportSunippet {
		return lsp.CompletionItem{
			InsertTextFormat: lsp.ITFPlainText,
			Kind:             lsp.CIKField,
			Label:            column.Name(),
			Detail:           column.Type().Kind().String(),
		}
	}

	return lsp.CompletionItem{
		InsertTextFormat: lsp.ITFSnippet,
		Kind:             lsp.CIKField,
		Label:            column.Name(),
		Detail:           column.Type().Kind().String(),
		TextEdit: &lsp.TextEdit{
			NewText: column.Name(),
			Range: lsp.Range{
				Start: cursorPosition,
				End:   cursorPosition,
			},
		},
	}
}

func createCompletionItemFromSchema(schema *bigquery.FieldSchema, cursorPosition lsp.Position, supportSunippet bool) lsp.CompletionItem {
	detail := string(schema.Type)
	if schema.Description != "" {
		detail += "\n" + schema.Description
	}
	if !supportSunippet {
		return lsp.CompletionItem{
			InsertTextFormat: lsp.ITFPlainText,
			Kind:             lsp.CIKField,
			Label:            schema.Name,
			Detail:           detail,
		}
	}

	return lsp.CompletionItem{
		InsertTextFormat: lsp.ITFSnippet,
		Kind:             lsp.CIKField,
		Label:            schema.Name,
		Detail:           detail,
		TextEdit: &lsp.TextEdit{
			NewText: schema.Name,
			Range: lsp.Range{
				Start: cursorPosition,
				End:   cursorPosition,
			},
		},
	}
}

func (p *Project) getTableMetadataFromTablePathExpressionNode(ctx context.Context, tableNode *ast.TablePathExpressionNode) (*bigquery.TableMetadata, error) {
	pathExpr := tableNode.PathExpr()
	if pathExpr == nil {
		return nil, fmt.Errorf("invalid table path expression")
	}
	pathNames := make([]string, len(pathExpr.Names()))
	for i, n := range pathExpr.Names() {
		pathNames[i] = n.Name()
	}
	targetTable, err := p.getTableMetadataFromPath(ctx, strings.Join(pathNames, "."))
	if err != nil {
		return nil, fmt.Errorf("failed to get table metadata: %w", err)
	}
	return targetTable, nil
}

func listAstNode[T ast.Node](node ast.Node) []T {
	targetNodes := make([]T, 0)
	ast.Walk(node, func(n ast.Node) error {
		node, ok := n.(T)
		if ok {
			targetNodes = append(targetNodes, node)
		}
		return nil
	})
	return targetNodes
}
