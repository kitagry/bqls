package completion

import (
	"context"
	"fmt"
	"strings"

	"github.com/goccy/go-zetasql/ast"
	rast "github.com/goccy/go-zetasql/resolved_ast"
	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/kitagry/bqls/langserver/internal/source/file"
)

type tablePathParams struct {
	ProjectID string
	DatasetID string
	TableID   string
}

func (c *completor) completeTablePath(ctx context.Context, parsedFile file.ParsedFile, position lsp.Position) ([]CompletionItem, error) {
	termOffset := parsedFile.TermOffset(position)

	// cursor is on table name
	tablePathNode, ok := file.SearchAstNode[*ast.TablePathExpressionNode](parsedFile.Node, termOffset)
	if !ok || tablePathNode.ParseLocationRange().End().ByteOffset() == termOffset {
		return nil, nil
	}

	tablePath, ok := file.CreateTableNameFromTablePathExpressionNode(tablePathNode)
	if !ok {
		return nil, nil
	}

	splittedTablePath := strings.Split(tablePath, ".")
	params := tablePathParams{}
	if len(splittedTablePath) >= 1 {
		params.ProjectID = splittedTablePath[0]
	}
	if len(splittedTablePath) >= 2 {
		params.DatasetID = splittedTablePath[1]
	}
	if len(splittedTablePath) >= 3 {
		params.TableID = splittedTablePath[2]
	}

	switch len(splittedTablePath) {
	case 0, 1:
		return c.completeProjectForTablePath(ctx, params)
	case 2:
		return c.completeDatasetForTablePath(ctx, params)
	case 3:
		return c.completeTableForTablePath(ctx, params)
	}

	return nil, nil
}

func (c *completor) completeTableScanField(ctx context.Context, tableScanNode *rast.TableScanNode, incompleteColumnName string) []CompletionItem {
	if tableScanNode.Alias() == "" {
		return nil
	}

	if strings.HasPrefix(tableScanNode.Alias(), incompleteColumnName) {
		return []CompletionItem{
			{
				Kind:    lsp.CIKField,
				NewText: tableScanNode.Alias(),
				Documentation: lsp.MarkupContent{
					Kind:  lsp.MKPlainText,
					Value: tableScanNode.Table().FullName(),
				},
				TypedPrefix: incompleteColumnName,
			},
		}
	}

	if !strings.HasPrefix(incompleteColumnName, tableScanNode.Alias()+".") {
		return nil
	}

	result := make([]CompletionItem, 0)
	afterWord := strings.TrimPrefix(incompleteColumnName, tableScanNode.Alias()+".")
	columns := tableScanNode.ColumnList()
	for _, column := range columns {
		if !strings.HasPrefix(column.Name(), afterWord) {
			continue
		}
		item, ok := c.createCompletionItemFromColumn(ctx, afterWord, column)
		if !ok {
			continue
		}

		result = append(result, item)
	}
	return result
}

func (c *completor) completeWithScanField(ctx context.Context, withScanNode *rast.WithRefScanNode, incompleteColumnName string) []CompletionItem {
	if strings.HasPrefix(withScanNode.WithQueryName(), incompleteColumnName) {
		return []CompletionItem{
			{
				Kind:        lsp.CIKField,
				NewText:     withScanNode.WithQueryName(),
				TypedPrefix: incompleteColumnName,
			},
		}
	}

	if !strings.HasPrefix(incompleteColumnName, withScanNode.WithQueryName()+".") {
		return nil
	}

	result := make([]CompletionItem, 0)
	afterWord := strings.TrimPrefix(incompleteColumnName, withScanNode.WithQueryName()+".")
	columns := withScanNode.ColumnList()
	for _, column := range columns {
		if !strings.HasPrefix(column.Name(), afterWord) {
			continue
		}
		item, ok := c.createCompletionItemFromColumn(ctx, afterWord, column)
		if !ok {
			continue
		}

		result = append(result, item)
	}
	return result
}

func (c *completor) completeProjectForTablePath(ctx context.Context, param tablePathParams) ([]CompletionItem, error) {
	projects, err := c.bqClient.ListProjects(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to ListProjects: %w", err)
	}

	result := make([]CompletionItem, 0)
	for _, p := range projects {
		if !strings.HasPrefix(p.ProjectId, param.ProjectID) {
			continue
		}

		result = append(result, CompletionItem{
			Kind:    lsp.CIKModule,
			NewText: p.ProjectId,
			Documentation: lsp.MarkupContent{
				Kind:  lsp.MKPlainText,
				Value: p.Name,
			},
			TypedPrefix: param.ProjectID,
		})
	}

	return result, nil
}

func (c *completor) completeDatasetForTablePath(ctx context.Context, param tablePathParams) ([]CompletionItem, error) {
	datasets, err := c.bqClient.ListDatasets(ctx, param.ProjectID)
	if err != nil {
		return nil, fmt.Errorf("failed to ListDatasets: %w", err)
	}

	result := make([]CompletionItem, 0)
	for _, d := range datasets {
		if !strings.HasPrefix(d.DatasetID, param.DatasetID) {
			continue
		}

		result = append(result, CompletionItem{
			Kind:    lsp.CIKModule,
			NewText: d.DatasetID,
			Documentation: lsp.MarkupContent{
				Kind:  lsp.MKPlainText,
				Value: fmt.Sprintf("%s.%s", d.ProjectID, d.DatasetID),
			},
			TypedPrefix: param.DatasetID,
		})
	}

	return result, nil
}

func (c *completor) completeTableForTablePath(ctx context.Context, param tablePathParams) ([]CompletionItem, error) {
	tables, err := c.bqClient.ListTables(ctx, param.ProjectID, param.DatasetID, true)
	if err != nil {
		return nil, fmt.Errorf("failed to bqClient.ListTables: %w", err)
	}

	result := make([]CompletionItem, 0)
	for _, t := range tables {
		if !strings.HasPrefix(t.TableID, param.TableID) {
			continue
		}

		result = append(result, CompletionItem{
			Kind:    lsp.CIKModule,
			NewText: t.TableID,
			Documentation: lsp.MarkupContent{
				Kind:  lsp.MKPlainText,
				Value: fmt.Sprintf("%s.%s.%s", t.ProjectID, t.DatasetID, t.TableID),
			},
			TypedPrefix: param.TableID,
		})
	}

	return result, nil
}
