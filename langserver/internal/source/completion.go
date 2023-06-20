package source

import (
	"context"
	"fmt"
	"strings"

	"cloud.google.com/go/bigquery"
	"github.com/goccy/go-zetasql/ast"
	"github.com/kitagry/bqls/langserver/internal/cache"
	"github.com/kitagry/bqls/langserver/internal/lsp"
)

func (p *Project) Complete(ctx context.Context, uri string, position lsp.Position, supportSunippet bool) ([]lsp.CompletionItem, error) {
	result := make([]lsp.CompletionItem, 0)
	sql := p.cache.Get(uri)

	if hasSelectListEmptyError(sql.Errors) {
		bytesOffset := positionToByteOffset(sql.RawText, position)
		// inserted text
		// before:
		//   SELECT | FROM table_name
		// after:
		//   SELECT * FROM table_name
		rawText := sql.RawText[:bytesOffset] + "*" + sql.RawText[bytesOffset:]
		dummySQL := cache.NewSQL(rawText)
		if len(dummySQL.Errors) > 0 {
			return nil, nil
		}
		sql = dummySQL
	}

	fromNodes := listAstNode[*ast.FromClauseNode](sql.Node)
	for _, fromNode := range fromNodes {
		tableExpr := fromNode.TableExpression()
		if tableExpr == nil {
			continue
		}

		switch tableExpr := tableExpr.(type) {
		case *ast.TablePathExpressionNode:
			tableMeta, err := p.getTableMetadataFromTablePathExpressionNode(ctx, tableExpr)
			if err != nil {
				return nil, fmt.Errorf("failed to get table metadata: %w", err)
			}

			for _, schema := range tableMeta.Schema {
				result = append(result, createCompletionItemFromSchema(schema, position, supportSunippet))
			}
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
