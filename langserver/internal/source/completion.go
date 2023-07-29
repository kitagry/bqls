package source

import (
	"context"
	"fmt"
	"strings"

	"cloud.google.com/go/bigquery"
	"github.com/goccy/go-zetasql"
	"github.com/goccy/go-zetasql/ast"
	rast "github.com/goccy/go-zetasql/resolved_ast"
	"github.com/goccy/go-zetasql/types"
	"github.com/kitagry/bqls/langserver/internal/lsp"
)

type CompletionItem struct {
	Kind        lsp.CompletionItemKind
	NewText     string
	Detail      string
	TypedPrefix string
}

func (c CompletionItem) ToLspCompletionItem(position lsp.Position, supportSnippet bool) lsp.CompletionItem {
	if !supportSnippet {
		return lsp.CompletionItem{
			InsertTextFormat: lsp.ITFPlainText,
			Kind:             c.Kind,
			Label:            c.NewText,
			Detail:           c.Detail,
		}
	}

	startPosition := position
	startPosition.Character -= len(c.TypedPrefix)
	return lsp.CompletionItem{
		InsertTextFormat: lsp.ITFSnippet,
		Kind:             c.Kind,
		Label:            c.NewText,
		Detail:           c.Detail,
		TextEdit: &lsp.TextEdit{
			NewText: c.NewText,
			Range: lsp.Range{
				Start: startPosition,
				End:   position,
			},
		},
	}
}

func (p *Project) Complete(ctx context.Context, uri string, position lsp.Position) ([]CompletionItem, error) {
	result := make([]CompletionItem, 0)
	sql := p.cache.Get(uri)
	termOffset := positionToByteOffset(sql.RawText, position)

	parsedFile := p.ParseFile(uri, sql.RawText)
	termOffset = parsedFile.fixTermOffsetForNode(termOffset)

	// cursor is on table name
	tablePathNode, ok := searchAstNode[*ast.TablePathExpressionNode](parsedFile.Node, termOffset)
	if ok && tablePathNode.ParseLocationRange().End().ByteOffset() != termOffset {
		return p.completeTablePath(ctx, tablePathNode)
	}

	output, ok := parsedFile.FindTargetAnalyzeOutput(termOffset)
	if !ok {
		p.logger.Debug("not found analyze output")
		return nil, nil
	}
	incompleteColumnName := parsedFile.FindIncompleteColumnName(position)

	node, ok := findScanNode(output, termOffset)
	if node == nil {
		p.logger.Debug("not found project scan node")
		return nil, nil
	}

	if pScanNode, ok := node.(*rast.ProjectScanNode); ok {
		node = pScanNode.InputScan()
	}
	if oScanNode, ok := node.(*rast.OrderByScanNode); ok {
		node = oScanNode.InputScan()
	}
	if aScanNode, ok := node.(*rast.AggregateScanNode); ok {
		node = aScanNode.InputScan()
	}

	columns := node.ColumnList()
	for _, column := range columns {
		if !strings.HasPrefix(column.Name(), incompleteColumnName) {
			continue
		}
		item, ok := p.createCompletionItemFromColumn(ctx, incompleteColumnName, column)
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
		items := p.createCompletionItemForRecordType(ctx, incompleteColumnName, column)

		result = append(result, items...)
	}

	// for table alias completion
	result = append(result, p.completeScanField(ctx, node, incompleteColumnName)...)

	return result, nil
}

func findScanNode(output *zetasql.AnalyzerOutput, termOffset int) (node rast.ScanNode, ok bool) {
	node, ok = searchResolvedAstNode[*rast.ProjectScanNode](output, termOffset)
	if ok {
		return node, true
	}

	node, ok = searchResolvedAstNode[*rast.OrderByScanNode](output, termOffset)
	if ok {
		return node, true
	}

	// In some case, *rast.ProjectScanNode.ParseLocationRange() returns nil.
	// So, if we cannot find *rast.ProjectScanNode, we search *rast.ProjectScanNode which ParseLocationRange returns nil.
	rast.Walk(output.Statement(), func(n rast.Node) error {
		if !n.IsScan() {
			return nil
		}

		sNode := n.(rast.ScanNode)

		lRange := n.ParseLocationRange()
		if lRange == nil {
			node = sNode
			return nil
		}
		// if the cursor is on the end of the node, the cursor is out of the node
		// So, we need to permit the some offset.
		if lRange.End().ByteOffset() < termOffset+5 {
			node = sNode
			return nil
		}
		return nil
	})

	if node == nil {
		return nil, false
	}
	return node, true
}

type tablePathParams struct {
	ProjectID string
	DatasetID string
	TableID   string
}

func (p *Project) completeScanField(ctx context.Context, node rast.ScanNode, incompleteColumnName string) []CompletionItem {
	switch n := node.(type) {
	case *rast.TableScanNode:
		return p.completeTableScanField(ctx, n, incompleteColumnName)
	case *rast.WithRefScanNode:
		return p.completeWithScanField(ctx, n, incompleteColumnName)
	case *rast.JoinScanNode:
		leftResult := p.completeScanField(ctx, n.LeftScan(), incompleteColumnName)
		rightResult := p.completeScanField(ctx, n.RightScan(), incompleteColumnName)
		return append(leftResult, rightResult...)
	case *rast.FilterScanNode:
		return p.completeScanField(ctx, n.InputScan(), incompleteColumnName)
	}
	return nil
}

func (p *Project) completeTableScanField(ctx context.Context, tableScanNode *rast.TableScanNode, incompleteColumnName string) []CompletionItem {
	if tableScanNode.Alias() == "" {
		return nil
	}

	if strings.HasPrefix(tableScanNode.Alias(), incompleteColumnName) {
		return []CompletionItem{
			{
				Kind:        lsp.CIKField,
				NewText:     tableScanNode.Alias(),
				Detail:      tableScanNode.Table().FullName(),
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
		item, ok := p.createCompletionItemFromColumn(ctx, afterWord, column)
		if !ok {
			continue
		}

		result = append(result, item)
	}
	return result
}

func (p *Project) completeWithScanField(ctx context.Context, withScanNode *rast.WithRefScanNode, incompleteColumnName string) []CompletionItem {
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
		item, ok := p.createCompletionItemFromColumn(ctx, afterWord, column)
		if !ok {
			continue
		}

		result = append(result, item)
	}
	return result
}

func (p *Project) completeTablePath(ctx context.Context, node *ast.TablePathExpressionNode) ([]CompletionItem, error) {
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
		return p.completeProjectForTablePath(ctx, params)
	case 2:
		return p.completeDatasetForTablePath(ctx, params)
	case 3:
		return p.completeTableForTablePath(ctx, params)
	}

	return nil, nil
}

func (p *Project) completeProjectForTablePath(ctx context.Context, param tablePathParams) ([]CompletionItem, error) {
	projects, err := p.bqClient.ListProjects(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to ListProjects: %w", err)
	}

	result := make([]CompletionItem, 0)
	for _, p := range projects {
		if !strings.HasPrefix(p.ProjectId, param.ProjectID) {
			continue
		}

		result = append(result, CompletionItem{
			Kind:        lsp.CIKModule,
			NewText:     p.ProjectId,
			Detail:      p.Name,
			TypedPrefix: param.ProjectID,
		})
	}

	return result, nil
}

func (p *Project) completeDatasetForTablePath(ctx context.Context, param tablePathParams) ([]CompletionItem, error) {
	datasets, err := p.bqClient.ListDatasets(ctx, param.ProjectID)
	if err != nil {
		return nil, fmt.Errorf("failed to ListDatasets: %w", err)
	}

	result := make([]CompletionItem, 0)
	for _, d := range datasets {
		if !strings.HasPrefix(d.DatasetID, param.DatasetID) {
			continue
		}

		result = append(result, CompletionItem{
			Kind:        lsp.CIKModule,
			NewText:     d.DatasetID,
			Detail:      fmt.Sprintf("%s.%s", d.ProjectID, d.DatasetID),
			TypedPrefix: param.DatasetID,
		})
	}

	return result, nil
}

func (p *Project) completeTableForTablePath(ctx context.Context, param tablePathParams) ([]CompletionItem, error) {
	tables, err := p.listLatestSuffixTables(ctx, param.ProjectID, param.DatasetID)
	if err != nil {
		return nil, fmt.Errorf("failed to listLatestSuffixTables: %w", err)
	}

	result := make([]CompletionItem, 0)
	for _, t := range tables {
		if !strings.HasPrefix(t.TableID, param.TableID) {
			continue
		}

		result = append(result, CompletionItem{
			Kind:        lsp.CIKModule,
			NewText:     t.TableID,
			Detail:      fmt.Sprintf("%s.%s.%s", t.ProjectID, t.DatasetID, t.TableID),
			TypedPrefix: param.TableID,
		})
	}

	return result, nil
}

func (p *Project) createCompletionItemFromColumn(ctx context.Context, incompleteColumnName string, column *rast.Column) (CompletionItem, bool) {
	tableMetadata, err := p.getTableMetadataFromPath(ctx, column.TableName())
	if err != nil {
		// cannot find table metadata
		return createCompletionItemFromColumn(column, incompleteColumnName), true
	}

	for _, c := range tableMetadata.Schema {
		if column.Name() == c.Name {
			return createCompletionItemFromSchema(c, incompleteColumnName), true
		}
	}

	return CompletionItem{}, false
}

func (p *Project) createCompletionItemForRecordType(ctx context.Context, incompleteColumnName string, column *rast.Column) []CompletionItem {
	if !column.Type().IsStruct() {
		return nil
	}

	splittedIncompleteColumnName := strings.Split(incompleteColumnName, ".")
	if len(splittedIncompleteColumnName) <= 1 {
		return nil
	}
	afterRecord := strings.Join(splittedIncompleteColumnName[1:], ".")

	tableMetadata, err := p.getTableMetadataFromPath(ctx, column.TableName())
	if err != nil {
		return p.createCompletionItemForType(ctx, afterRecord, column.Type())
	}

	return p.createCompletionItemForBigquerySchema(ctx, incompleteColumnName, tableMetadata.Schema)
}

func (p *Project) createCompletionItemForType(ctx context.Context, incompleteColumnName string, typ types.Type) []CompletionItem {
	if !typ.IsStruct() {
		return nil
	}

	fields := typ.AsStruct().Fields()

	inCompleteColumns := strings.Split(incompleteColumnName, ".")
	if len(inCompleteColumns) > 1 {
		for _, field := range fields {
			if field.Name() == inCompleteColumns[0] {
				if !field.Type().IsStruct() {
					return nil
				}
				return p.createCompletionItemForType(ctx, strings.Join(inCompleteColumns[1:], "."), field.Type())
			}
		}
		return nil
	}

	items := make([]CompletionItem, 0)
	for _, field := range fields {
		if !strings.HasPrefix(field.Name(), incompleteColumnName) {
			continue
		}
		items = append(items, createCompletionItemFromColumn(field, incompleteColumnName))
	}
	return items
}

func (p *Project) createCompletionItemForBigquerySchema(ctx context.Context, incompleteColumnName string, schema bigquery.Schema) []CompletionItem {
	inCompleteColumns := strings.Split(incompleteColumnName, ".")
	if len(inCompleteColumns) > 1 {
		for _, field := range schema {
			if field.Name == inCompleteColumns[0] {
				return p.createCompletionItemForBigquerySchema(ctx, strings.Join(inCompleteColumns[1:], "."), field.Schema)
			}
		}
		return nil
	}

	items := make([]CompletionItem, 0)
	for _, field := range schema {
		if !strings.HasPrefix(field.Name, incompleteColumnName) {
			continue
		}
		items = append(items, createCompletionItemFromSchema(field, incompleteColumnName))
	}
	return items
}

type columnInterface interface {
	Name() string
	Type() types.Type
}

func createCompletionItemFromColumn(column columnInterface, incompleteColumnName string) CompletionItem {
	return CompletionItem{
		Kind:        lsp.CIKField,
		NewText:     column.Name(),
		Detail:      column.Type().TypeName(types.ProductExternal),
		TypedPrefix: incompleteColumnName,
	}
}

func createCompletionItemFromSchema(schema *bigquery.FieldSchema, incompleteColumnName string) CompletionItem {
	detail := string(schema.Type)
	if schema.Description != "" {
		detail += "\n" + schema.Description
	}
	return CompletionItem{
		Kind:        lsp.CIKField,
		NewText:     schema.Name,
		Detail:      detail,
		TypedPrefix: incompleteColumnName,
	}
}
