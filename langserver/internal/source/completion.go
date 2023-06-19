package source

import (
	"context"
	"fmt"
	"strings"

	"cloud.google.com/go/bigquery"
	"github.com/goccy/go-zetasql/ast"
	"github.com/kitagry/bqls/langserver/internal/lsp"
)

func (p *Project) Complete(ctx context.Context, uri string, position lsp.Position, supportSunippet bool) ([]lsp.CompletionItem, error) {
	result := make([]lsp.CompletionItem, 0)
	sql := p.cache.Get(uri)

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
				detail := string(schema.Type)
				if schema.Description != "" {
					detail += "\n" + schema.Description
				}
				if !supportSunippet {
					result = append(result, lsp.CompletionItem{
						InsertTextFormat: lsp.ITFPlainText,
						Kind:             lsp.CIKField,
						Label:            schema.Name,
						Detail:           detail,
					})
					continue
				}

				result = append(result, lsp.CompletionItem{
					InsertTextFormat: lsp.ITFSnippet,
					Kind:             lsp.CIKField,
					Label:            schema.Name,
					Detail:           detail,
					TextEdit: &lsp.TextEdit{
						NewText: schema.Name,
						Range: lsp.Range{
							Start: position,
							End:   position,
						},
					},
				})
			}
		}
	}

	return result, nil
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
