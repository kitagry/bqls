package completion

import (
	"context"
	"strings"

	bq "cloud.google.com/go/bigquery"
	rast "github.com/goccy/go-zetasql/resolved_ast"
	"github.com/goccy/go-zetasql/types"
	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/kitagry/bqls/langserver/internal/source/file"
)

func (c *completor) completeColumns(ctx context.Context, parsedFile file.ParsedFile, position lsp.Position) []CompletionItem {
	termOffset := parsedFile.TermOffset(position)

	output, ok := parsedFile.FindTargetAnalyzeOutput(termOffset)
	if !ok {
		c.logger.Debug("not found analyze output")
		return nil
	}
	incompleteColumnName := parsedFile.FindIncompleteColumnName(position)

	node, ok := findScanNode(output, termOffset)
	if !ok {
		c.logger.Debug("not found project scan node")
		return nil
	}

	if sNode, ok := c.getMostNarrowInputScanNode(node, termOffset); ok {
		node = sNode
	}

	if pScanNode, ok := node.(*rast.ProjectScanNode); ok {
		node = pScanNode.InputScan()
	}

	result := make([]CompletionItem, 0)
	columns := node.ColumnList()
	for _, column := range columns {
		if !strings.HasPrefix(column.Name(), incompleteColumnName) {
			continue
		}
		item, ok := c.createCompletionItemFromColumn(ctx, incompleteColumnName, column)
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
		items := c.createCompletionItemForRecordType(ctx, incompleteColumnName, column)

		result = append(result, items...)
	}

	// for table alias completion
	result = append(result, c.completeScanField(ctx, node, incompleteColumnName)...)

	return result
}

func (c *completor) createCompletionItemFromColumn(ctx context.Context, incompleteColumnName string, column *rast.Column) (CompletionItem, bool) {
	tableMetadata, err := c.analyzer.GetTableMetadataFromPath(ctx, column.TableName())
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

func (c *completor) createCompletionItemForRecordType(ctx context.Context, incompleteColumnName string, column *rast.Column) []CompletionItem {
	if !column.Type().IsStruct() {
		return nil
	}

	splittedIncompleteColumnName := strings.Split(incompleteColumnName, ".")
	if len(splittedIncompleteColumnName) <= 1 {
		return nil
	}
	afterRecord := strings.Join(splittedIncompleteColumnName[1:], ".")

	tableMetadata, err := c.analyzer.GetTableMetadataFromPath(ctx, column.TableName())
	if err != nil {
		return c.createCompletionItemForType(ctx, afterRecord, column.Type())
	}

	return c.createCompletionItemForBigquerySchema(ctx, incompleteColumnName, tableMetadata.Schema)
}

func (c *completor) createCompletionItemForType(ctx context.Context, incompleteColumnName string, typ types.Type) []CompletionItem {
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
				return c.createCompletionItemForType(ctx, strings.Join(inCompleteColumns[1:], "."), field.Type())
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

func (c *completor) createCompletionItemForBigquerySchema(ctx context.Context, incompleteColumnName string, schema bq.Schema) []CompletionItem {
	inCompleteColumns := strings.Split(incompleteColumnName, ".")
	if len(inCompleteColumns) > 1 {
		for _, field := range schema {
			if field.Name == inCompleteColumns[0] {
				return c.createCompletionItemForBigquerySchema(ctx, strings.Join(inCompleteColumns[1:], "."), field.Schema)
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

func (c *completor) completeScanField(ctx context.Context, node rast.ScanNode, incompleteColumnName string) []CompletionItem {
	switch n := node.(type) {
	case *rast.TableScanNode:
		return c.completeTableScanField(ctx, n, incompleteColumnName)
	case *rast.WithRefScanNode:
		return c.completeWithScanField(ctx, n, incompleteColumnName)
	case *rast.JoinScanNode:
		leftResult := c.completeScanField(ctx, n.LeftScan(), incompleteColumnName)
		rightResult := c.completeScanField(ctx, n.RightScan(), incompleteColumnName)
		return append(leftResult, rightResult...)
	case *rast.FilterScanNode:
		return c.completeScanField(ctx, n.InputScan(), incompleteColumnName)
	}
	return nil
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

func (c *completor) getMostNarrowInputScanNode(node rast.ScanNode, termOffset int) (rast.ScanNode, bool) {
	for {
		switch n := node.(type) {
		case *rast.ProjectScanNode:
			node, ok := c.getMostNarrowInputScanNode(n.InputScan(), termOffset)
			if ok {
				return node, true
			}

			lRange := n.ParseLocationRange()
			if lRange == nil {
				return n, true
			}

			if lRange.Start().ByteOffset() <= termOffset && termOffset <= lRange.End().ByteOffset() {
				return n, true
			}
			return nil, false
		case *rast.AggregateScanNode:
			node, ok := c.getMostNarrowInputScanNode(n.InputScan(), termOffset)
			if ok {
				return node, true
			}

			lRange := n.ParseLocationRange()
			if lRange == nil {
				return n, true
			}

			if lRange.Start().ByteOffset() <= termOffset && termOffset <= lRange.End().ByteOffset() {
				return n, true
			}
			return nil, false
		case *rast.FilterScanNode:
			node, ok := c.getMostNarrowInputScanNode(n.InputScan(), termOffset)
			if ok {
				return node, true
			}

			lRange := n.ParseLocationRange()
			if lRange == nil {
				return n, true
			}

			if lRange.Start().ByteOffset() <= termOffset && termOffset <= lRange.End().ByteOffset() {
				return n, true
			}
			return nil, false
		case *rast.TableScanNode:
			lRange := n.ParseLocationRange()
			if lRange == nil {
				return n, true
			}

			if lRange.Start().ByteOffset() <= termOffset && termOffset <= lRange.End().ByteOffset() {
				return n, true
			}
			return nil, false
		case *rast.WithScanNode:
			wel := n.WithEntryList()
			replaced := false
			for _, we := range wel {
				lRange := we.ParseLocationRange()
				if lRange == nil {
					continue
				}

				startOffset := lRange.Start().ByteOffset()
				endOffset := lRange.End().ByteOffset()
				if termOffset < startOffset || endOffset < termOffset {
					continue
				}

				node = we.WithSubquery()
				replaced = true
			}

			if !replaced {
				node = n.Query()
			}

			result, ok := c.getMostNarrowInputScanNode(node, termOffset)
			if ok {
				return result, true
			}
			return node, true
		case *rast.WithRefScanNode:
			return nil, false
		case *rast.JoinScanNode:
			node1, ok := c.getMostNarrowInputScanNode(n.LeftScan(), termOffset)
			if ok {
				return node1, true
			}
			node2, ok := c.getMostNarrowInputScanNode(n.RightScan(), termOffset)
			if ok {
				return node2, true
			}

			lRange := n.ParseLocationRange()
			if lRange == nil {
				return n, true
			}

			if lRange.Start().ByteOffset() <= termOffset && termOffset <= lRange.End().ByteOffset() {
				return n, true
			}
			return nil, false
		case *rast.OrderByScanNode:
			node, ok := c.getMostNarrowInputScanNode(n.InputScan(), termOffset)
			if ok {
				return node, true
			}

			lRange := n.ParseLocationRange()
			if lRange == nil {
				return n, true
			}

			if lRange.Start().ByteOffset() <= termOffset && termOffset <= lRange.End().ByteOffset() {
				return n, true
			}
			return nil, false
		case *rast.LimitOffsetScanNode:
			node, ok := c.getMostNarrowInputScanNode(n.InputScan(), termOffset)
			if ok {
				return node, true
			}
			return nil, false
		default:
			c.logger.Printf("unknown scan node type: %T\n", n)
			return nil, false
		}
	}
}
