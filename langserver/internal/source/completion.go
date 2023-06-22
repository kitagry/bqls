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

func (p *Project) Complete(ctx context.Context, uri string, position lsp.Position, supportSnippet bool) ([]lsp.CompletionItem, error) {
	result := make([]lsp.CompletionItem, 0)
	sql := p.cache.Get(uri)
	termOffset := positionToByteOffset(sql.RawText, position)

	if hasSelectListEmptyError(sql.Errors) {
		// inserted text
		// before:
		//   SELECT | FROM table_name
		// after:
		//   SELECT * FROM table_name
		rawText := sql.RawText[:termOffset] + "1" + sql.RawText[termOffset:]
		dummySQL := cache.NewSQL(rawText)
		if len(dummySQL.Errors) > 0 {
			return nil, nil
		}
		sql = dummySQL
	}

	// cursor is on table name
	if node, ok := searchAstNode[*ast.TablePathExpressionNode](sql.Node, termOffset); ok {
		return p.completeTablePath(ctx, node, position, supportSnippet)
	}

	output, incompleteColumnName, err := p.forceAnalyzeStatement(sql, termOffset)
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
		if !strings.HasPrefix(column.Name(), incompleteColumnName) {
			continue
		}
		item, ok := p.createCompletionItemFromColumn(ctx, column, position, supportSnippet)
		if !ok {
			continue
		}

		if supportSnippet {
			item.TextEdit.Range.Start.Character -= len(incompleteColumnName)
		}
		result = append(result, item)
	}

	return result, nil
}

type tablePathParams struct {
	ProjectID string
	DatasetID string
	TableID   string
}

func (p *Project) completeTablePath(ctx context.Context, node *ast.TablePathExpressionNode, position lsp.Position, supportSnippet bool) ([]lsp.CompletionItem, error) {
	tablePath, ok := createTableNameFromTablePathExpressionNode(node)
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
	case 3:
		return p.completeTableForTablePath(ctx, params, position, supportSnippet)
	}

	return nil, nil
}

func (p *Project) completeTableForTablePath(ctx context.Context, param tablePathParams, position lsp.Position, supportSnippet bool) ([]lsp.CompletionItem, error) {
	tables, err := p.listLatestSuffixTables(ctx, param.ProjectID, param.DatasetID)
	if err != nil {
		return nil, fmt.Errorf("failed to listLatestSuffixTables: %w", err)
	}

	result := make([]lsp.CompletionItem, 0)
	for _, t := range tables {
		if !strings.HasPrefix(t.TableID, param.TableID) {
			continue
		}

		if !supportSnippet {
			result = append(result, lsp.CompletionItem{
				InsertTextFormat: lsp.ITFPlainText,
				Kind:             lsp.CIKFile,
				Label:            t.TableID,
				Detail:           fmt.Sprintf("%s.%s.%s", t.ProjectID, t.DatasetID, t.TableID),
			})
		} else {
			startPosition := position
			startPosition.Character -= len(param.TableID)
			result = append(result, lsp.CompletionItem{
				InsertTextFormat: lsp.ITFSnippet,
				Kind:             lsp.CIKFile,
				Label:            t.TableID,
				Detail:           fmt.Sprintf("%s.%s.%s", t.ProjectID, t.DatasetID, t.TableID),
				TextEdit: &lsp.TextEdit{
					NewText: t.TableID,
					Range: lsp.Range{
						Start: startPosition,
						End:   position,
					},
				},
			})
		}
	}

	return result, nil
}

func (p *Project) forceAnalyzeStatement(sql *cache.SQL, termOffset int) (output *zetasql.AnalyzerOutput, incompleteColumnName string, err error) {
	findTargetStatement := func(sql *cache.SQL, termOffset int) (ast.StatementNode, bool) {
		for _, stmt := range sql.GetStatementNodes() {
			loc := stmt.ParseLocationRange()
			if loc == nil {
				continue
			}
			startOffset := loc.Start().ByteOffset()
			endOffset := loc.End().ByteOffset()
			if startOffset <= termOffset && termOffset <= endOffset {
				return stmt, true
			}
		}
		return nil, false
	}

	statementNode, ok := findTargetStatement(sql, termOffset)
	if !ok {
		return nil, "", fmt.Errorf("failed to find target statementNode")
	}

	output, err = p.analyzeStatement(sql.RawText, statementNode)
	if err == nil {
		return output, "", nil
	}
	ind := strings.Index(err.Error(), "Unrecognized name: ")
	if ind == -1 {
		return nil, "", fmt.Errorf("Failed to analyze statement: %w", err)
	}

	// When the error is "Unrecognized name: ", we should complete the name.
	// But it can't be analyzed by zetasql, we should replace the name to '*'
	// Error message example is `INVALID_ARGUMENT: Unrecognized name: i [at 1:8]`
	parsedErr := parseZetaSQLError(err)
	incompleteColumnName = strings.TrimSpace(parsedErr.Msg[ind+len("Unrecognized name: "):])
	errOffset := positionToByteOffset(sql.RawText, parsedErr.Position)
	if errOffset == len(sql.RawText) {
		return nil, "", fmt.Errorf("Failed to analyze statement: %w", parsedErr)
	}

	replacedSQL := sql.RawText[:errOffset] + "1," + sql.RawText[errOffset+len(incompleteColumnName):]
	sql = cache.NewSQL(replacedSQL)
	stmt, ok := findTargetStatement(sql, errOffset)
	if !ok {
		return nil, "", fmt.Errorf("failed to find target statementNode")
	}
	output, err = p.analyzeStatement(sql.RawText, stmt)
	if err != nil {
		return nil, "", fmt.Errorf("Failed to analyze statement: %w", err)
	}
	return output, incompleteColumnName, nil
}

func hasSelectListEmptyError(errs []error) bool {
	for _, err := range errs {
		if strings.Contains(err.Error(), "Syntax error: SELECT list must not be empty") {
			return true
		}
	}
	return false
}

func (p *Project) createCompletionItemFromColumn(ctx context.Context, column *rast.Column, cursorPosition lsp.Position, supportSnippet bool) (lsp.CompletionItem, bool) {
	tableMetadata, err := p.getTableMetadataFromPath(ctx, column.TableNameID())
	if err != nil {
		// cannot find table metadata
		return createCompletionItemFromColumn(column, cursorPosition, supportSnippet), true
	}

	for _, c := range tableMetadata.Schema {
		if column.Name() == c.Name {
			return createCompletionItemFromSchema(c, cursorPosition, supportSnippet), true
		}
	}

	return lsp.CompletionItem{}, false
}

func createCompletionItemFromColumn(column *rast.Column, cursorPosition lsp.Position, supportSnippet bool) lsp.CompletionItem {
	if !supportSnippet {
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

func createCompletionItemFromSchema(schema *bigquery.FieldSchema, cursorPosition lsp.Position, supportSnippet bool) lsp.CompletionItem {
	detail := string(schema.Type)
	if schema.Description != "" {
		detail += "\n" + schema.Description
	}
	if !supportSnippet {
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
