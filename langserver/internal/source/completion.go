package source

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"strings"

	"cloud.google.com/go/bigquery"
	"github.com/goccy/go-zetasql"
	"github.com/goccy/go-zetasql/ast"
	rast "github.com/goccy/go-zetasql/resolved_ast"
	"github.com/goccy/go-zetasql/types"
	"github.com/kitagry/bqls/langserver/internal/cache"
	"github.com/kitagry/bqls/langserver/internal/lsp"
)

var lastDotRegex = regexp.MustCompile(`\w+\.\s`)

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
		fmt.Fprintln(os.Stderr, err)
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
		item, ok := p.createCompletionItemFromColumn(ctx, incompleteColumnName, column, position, supportSnippet)
		if !ok {
			continue
		}

		result = append(result, item)
	}

	// for record column completion
	for _, column := range columns {
		if !strings.HasPrefix(incompleteColumnName, column.Name()) {
			continue
		}
		items := p.createCompletionItemForRecordType(ctx, incompleteColumnName, column, position, supportSnippet)

		result = append(result, items...)
	}

	// for table alias completion
	result = append(result, p.completeScanField(ctx, node.InputScan(), incompleteColumnName, position, supportSnippet)...)

	return result, nil
}

type tablePathParams struct {
	ProjectID string
	DatasetID string
	TableID   string
}

func (p *Project) completeScanField(ctx context.Context, node rast.ScanNode, incompleteColumnName string, position lsp.Position, supportSnippet bool) []lsp.CompletionItem {
	switch n := node.(type) {
	case *rast.TableScanNode:
		return p.completeTableScanField(ctx, n, incompleteColumnName, position, supportSnippet)
	case *rast.WithRefScanNode:
		return p.completeWithScanField(ctx, n, incompleteColumnName, position, supportSnippet)
	}
	return nil
}

func (p *Project) completeTableScanField(ctx context.Context, tableScanNode *rast.TableScanNode, incompleteColumnName string, position lsp.Position, supportSnippet bool) []lsp.CompletionItem {
	if tableScanNode.Alias() == "" {
		return nil
	}

	if strings.HasPrefix(tableScanNode.Alias(), incompleteColumnName) {
		if !supportSnippet {
			return []lsp.CompletionItem{
				{
					InsertTextFormat: lsp.ITFPlainText,
					Kind:             lsp.CIKField,
					Label:            tableScanNode.Alias(),
					Detail:           tableScanNode.Table().FullName(),
				},
			}
		}

		startPosition := position
		startPosition.Character -= len(incompleteColumnName)
		return []lsp.CompletionItem{
			{
				InsertTextFormat: lsp.ITFSnippet,
				Kind:             lsp.CIKField,
				Label:            tableScanNode.Alias(),
				Detail:           tableScanNode.Table().FullName(),
				TextEdit: &lsp.TextEdit{
					NewText: tableScanNode.Alias(),
					Range: lsp.Range{
						Start: startPosition,
						End:   position,
					},
				},
			},
		}
	}

	if !strings.HasPrefix(incompleteColumnName, tableScanNode.Alias()+".") {
		return nil
	}

	result := make([]lsp.CompletionItem, 0)
	afterWord := strings.TrimPrefix(incompleteColumnName, tableScanNode.Alias()+".")
	columns := tableScanNode.ColumnList()
	for _, column := range columns {
		if !strings.HasPrefix(column.Name(), afterWord) {
			continue
		}
		item, ok := p.createCompletionItemFromColumn(ctx, afterWord, column, position, supportSnippet)
		if !ok {
			continue
		}

		result = append(result, item)
	}
	return result
}

func (p *Project) completeWithScanField(ctx context.Context, withScanNode *rast.WithRefScanNode, incompleteColumnName string, position lsp.Position, supportSnippet bool) []lsp.CompletionItem {
	if strings.HasPrefix(withScanNode.WithQueryName(), incompleteColumnName) {
		if !supportSnippet {
			return []lsp.CompletionItem{
				{
					InsertTextFormat: lsp.ITFPlainText,
					Kind:             lsp.CIKField,
					Label:            withScanNode.WithQueryName(),
				},
			}
		}

		startPosition := position
		startPosition.Character -= len(incompleteColumnName)
		return []lsp.CompletionItem{
			{
				InsertTextFormat: lsp.ITFSnippet,
				Kind:             lsp.CIKField,
				Label:            withScanNode.WithQueryName(),
				TextEdit: &lsp.TextEdit{
					NewText: withScanNode.WithQueryName(),
					Range: lsp.Range{
						Start: startPosition,
						End:   position,
					},
				},
			},
		}
	}

	if !strings.HasPrefix(incompleteColumnName, withScanNode.WithQueryName()+".") {
		return nil
	}

	result := make([]lsp.CompletionItem, 0)
	afterWord := strings.TrimPrefix(incompleteColumnName, withScanNode.WithQueryName()+".")
	columns := withScanNode.ColumnList()
	for _, column := range columns {
		if !strings.HasPrefix(column.Name(), afterWord) {
			continue
		}
		item, ok := p.createCompletionItemFromColumn(ctx, afterWord, column, position, supportSnippet)
		if !ok {
			continue
		}

		result = append(result, item)
	}
	return result
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
	case 0, 1:
		return p.completeProjectForTablePath(ctx, params, position, supportSnippet)
	case 2:
		return p.completeDatasetForTablePath(ctx, params, position, supportSnippet)
	case 3:
		return p.completeTableForTablePath(ctx, params, position, supportSnippet)
	}

	return nil, nil
}

func (p *Project) completeProjectForTablePath(ctx context.Context, param tablePathParams, position lsp.Position, supportSnippet bool) ([]lsp.CompletionItem, error) {
	projects, err := p.bqClient.ListProjects(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to ListProjects: %w", err)
	}

	result := make([]lsp.CompletionItem, 0)
	for _, p := range projects {
		if !strings.HasPrefix(p.ProjectId, param.ProjectID) {
			continue
		}

		if !supportSnippet {
			result = append(result, lsp.CompletionItem{
				InsertTextFormat: lsp.ITFPlainText,
				Kind:             lsp.CIKFile,
				Label:            p.ProjectId,
				Detail:           p.Name,
			})
		} else {
			startPosition := position
			startPosition.Character -= len(param.ProjectID)
			result = append(result, lsp.CompletionItem{
				InsertTextFormat: lsp.ITFSnippet,
				Kind:             lsp.CIKFile,
				Label:            p.ProjectId,
				Detail:           p.Name,
				TextEdit: &lsp.TextEdit{
					NewText: p.ProjectId,
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

func (p *Project) completeDatasetForTablePath(ctx context.Context, param tablePathParams, position lsp.Position, supportSnippet bool) ([]lsp.CompletionItem, error) {
	datasets, err := p.bqClient.ListDatasets(ctx, param.ProjectID)
	if err != nil {
		return nil, fmt.Errorf("failed to ListDatasets: %w", err)
	}

	result := make([]lsp.CompletionItem, 0)
	for _, d := range datasets {
		if !strings.HasPrefix(d.DatasetID, param.DatasetID) {
			continue
		}

		if !supportSnippet {
			result = append(result, lsp.CompletionItem{
				InsertTextFormat: lsp.ITFPlainText,
				Kind:             lsp.CIKFile,
				Label:            d.DatasetID,
				Detail:           fmt.Sprintf("%s.%s", d.ProjectID, d.DatasetID),
			})
		} else {
			startPosition := position
			startPosition.Character -= len(param.DatasetID)
			result = append(result, lsp.CompletionItem{
				InsertTextFormat: lsp.ITFSnippet,
				Kind:             lsp.CIKFile,
				Label:            d.DatasetID,
				Detail:           fmt.Sprintf("%s.%s", d.ProjectID, d.DatasetID),
				TextEdit: &lsp.TextEdit{
					NewText: d.DatasetID,
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
	// Find like e.x. SELECT item. FROM table
	//
	// zetasql returns odd error.
	// e.x.)
	// panic: INVALID_ARGUMENT: Unrecognized name: param [at 1:8]
	// SELECT param.id, param. FROM `project.dataset.table`
	//        ^
	if loc := lastDotRegex.FindIndex([]byte(sql.RawText)); len(loc) == 2 {
		// sql.Rawtext[loc[1]] is a space.
		targetWord := sql.RawText[loc[0] : loc[1]-1]
		replacedSQL := sql.RawText[:loc[0]] + "1," + sql.RawText[loc[1]-1:]
		sql = cache.NewSQL(replacedSQL)
		stmt, ok := findTargetStatement(sql, termOffset)
		if !ok {
			return nil, "", fmt.Errorf("failed to find target statementNode")
		}
		output, err = p.analyzeStatement(sql.RawText, stmt)
		if err != nil {
			return nil, "", fmt.Errorf("Failed to analyze statement: %w", err)
		}
		return output, targetWord, nil
	}

	statementNode, ok := findTargetStatement(sql, termOffset)
	if !ok {
		return nil, "", fmt.Errorf("failed to find target statementNode")
	}

	output, err = p.analyzeStatement(sql.RawText, statementNode)
	if err == nil {
		return output, "", nil
	}

	parsedErr := parseZetaSQLError(err)
	if strings.Contains(parsedErr.Msg, "Unrecognized name: ") {
		return p.forceAnalyzeUnrecognizedNameStatement(sql, parsedErr)
	}

	if strings.Contains(parsedErr.Msg, "does not exist in STRUCT") {
		// if record errror, we should return incompleteColumnName which contains record name.
		node, ok := searchAstNode[*ast.IdentifierNode](sql.Node, termOffset)
		if !ok {
			return nil, "", fmt.Errorf("failed to search ast node")
		}
		incompleteNode, ok := lookupNode[*ast.PathExpressionNode](node)
		if !ok {
			return nil, "", fmt.Errorf("failed to lookup node")
		}

		output, _, err := p.forceAnalyzeFieldDoesNotExitInStructStatement(sql, parsedErr)
		if err != nil {
			return nil, "", fmt.Errorf("failed to forceAnalyzeFieldDoesNotExitInStructStatement: %w", err)
		}
		names := make([]string, 0)
		for _, n := range incompleteNode.Names() {
			names = append(names, n.Name())
		}
		return output, strings.Join(names, "."), nil
	}

	return nil, "", fmt.Errorf("Failed to analyze statement: %w", err)
}

// When the error is "Unrecognized name: ", we should complete the name.
// But it can't be analyzed by zetasql, we should replace the name to '1,'
// Error message example is `INVALID_ARGUMENT: Unrecognized name: i [at 1:8]`
func (p *Project) forceAnalyzeUnrecognizedNameStatement(sql *cache.SQL, parsedErr Error) (output *zetasql.AnalyzerOutput, incompleteColumnName string, err error) {
	ind := strings.Index(parsedErr.Msg, "Unrecognized name: ")

	incompleteColumnName = strings.TrimSpace(parsedErr.Msg[ind+len("Unrecognized name: "):])
	errOffset := positionToByteOffset(sql.RawText, parsedErr.Position)
	if errOffset == 0 || errOffset == len(sql.RawText) {
		return nil, "", fmt.Errorf("Failed to analyze statement: %w", parsedErr)
	}

	if sql.RawText[errOffset+len(incompleteColumnName)] == '.' {
		incompleteColumnName += "."
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

func (p *Project) forceAnalyzeFieldDoesNotExitInStructStatement(sql *cache.SQL, parsedErr Error) (output *zetasql.AnalyzerOutput, incompleteColumnName string, err error) {
	ind := strings.Index(parsedErr.Msg, "Field name ")
	if ind == -1 {
		return nil, "", parsedErr
	}
	incompleteColumnName = parsedErr.Msg[ind+len("Field name "):]
	incompleteColumnName = incompleteColumnName[:strings.Index(incompleteColumnName, " ")]

	errOffset := positionToByteOffset(sql.RawText, parsedErr.Position)
	if errOffset == 0 || errOffset == len(sql.RawText) {
		return nil, "", fmt.Errorf("Failed to analyze statement: %w", parsedErr)
	}

	replacedSQL := sql.RawText[:errOffset] + "*" + sql.RawText[errOffset+len(incompleteColumnName):]
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

func findTargetStatement(sql *cache.SQL, termOffset int) (ast.StatementNode, bool) {
	stmts := sql.GetStatementNodes()
	if len(stmts) == 1 {
		return stmts[0], true
	}

	for _, stmt := range stmts {
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

func hasSelectListEmptyError(errs []error) bool {
	for _, err := range errs {
		if strings.Contains(err.Error(), "Syntax error: SELECT list must not be empty") {
			return true
		}
	}
	return false
}

func (p *Project) createCompletionItemFromColumn(ctx context.Context, incompleteColumnName string, column *rast.Column, cursorPosition lsp.Position, supportSnippet bool) (lsp.CompletionItem, bool) {
	tableMetadata, err := p.getTableMetadataFromPath(ctx, column.TableNameID())
	if err != nil {
		// cannot find table metadata
		return createCompletionItemFromColumn(column, cursorPosition, supportSnippet, len(incompleteColumnName)), true
	}

	for _, c := range tableMetadata.Schema {
		if column.Name() == c.Name {
			return createCompletionItemFromSchema(c, cursorPosition, supportSnippet, len(incompleteColumnName)), true
		}
	}

	return lsp.CompletionItem{}, false
}

func (p *Project) createCompletionItemForRecordType(ctx context.Context, incompleteColumnName string, column *rast.Column, cursorPosition lsp.Position, supportSnippet bool) []lsp.CompletionItem {
	if !column.Type().IsStruct() {
		return nil
	}

	splittedIncompleteColumnName := strings.Split(incompleteColumnName, ".")
	if len(splittedIncompleteColumnName) <= 1 {
		return nil
	}
	afterRecord := splittedIncompleteColumnName[1]

	tableMetadata, err := p.getTableMetadataFromPath(ctx, column.TableNameID())
	items := make([]lsp.CompletionItem, 0)
	if err != nil {
		fields := column.Type().AsStruct().Fields()
		for _, field := range fields {
			if !strings.HasPrefix(field.Name(), afterRecord) {
				continue
			}
			items = append(items, createCompletionItemFromColumn(field, cursorPosition, supportSnippet, len(afterRecord)))
		}
		return items
	}

	for _, c := range tableMetadata.Schema {
		if column.Name() == c.Name {
			if c.Type != bigquery.RecordFieldType {
				return nil
			}

			for _, field := range c.Schema {
				if !strings.HasPrefix(field.Name, afterRecord) {
					continue
				}
				items = append(items, createCompletionItemFromSchema(field, cursorPosition, supportSnippet, len(afterRecord)))
			}
			return items
		}
	}

	return nil
}

type columnInterface interface {
	Name() string
	Type() types.Type
}

func createCompletionItemFromColumn(column columnInterface, cursorPosition lsp.Position, supportSnippet bool, startOffset int) lsp.CompletionItem {
	if !supportSnippet {
		return lsp.CompletionItem{
			InsertTextFormat: lsp.ITFPlainText,
			Kind:             lsp.CIKField,
			Label:            column.Name(),
			Detail:           column.Type().Kind().String(),
		}
	}

	startPosition := cursorPosition
	startPosition.Character = startPosition.Character - startOffset
	return lsp.CompletionItem{
		InsertTextFormat: lsp.ITFSnippet,
		Kind:             lsp.CIKField,
		Label:            column.Name(),
		Detail:           column.Type().Kind().String(),
		TextEdit: &lsp.TextEdit{
			NewText: column.Name(),
			Range: lsp.Range{
				Start: startPosition,
				End:   cursorPosition,
			},
		},
	}
}

func createCompletionItemFromSchema(schema *bigquery.FieldSchema, cursorPosition lsp.Position, supportSnippet bool, startOffset int) lsp.CompletionItem {
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

	startPosition := cursorPosition
	startPosition.Character = startPosition.Character - startOffset
	return lsp.CompletionItem{
		InsertTextFormat: lsp.ITFSnippet,
		Kind:             lsp.CIKField,
		Label:            schema.Name,
		Detail:           detail,
		TextEdit: &lsp.TextEdit{
			NewText: schema.Name,
			Range: lsp.Range{
				Start: startPosition,
				End:   cursorPosition,
			},
		},
	}
}
