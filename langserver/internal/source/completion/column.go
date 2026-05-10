package completion

import (
	"context"
	"strings"

	bq "cloud.google.com/go/bigquery"
	googlesql "github.com/goccy/go-googlesql"
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

	if pScanNode, ok := node.(*googlesql.ResolvedProjectScan); ok {
		inputScan, err := pScanNode.InputScan()
		if err == nil && inputScan != nil {
			node = inputScan
		}
	}

	result := make([]CompletionItem, 0)
	columns, _ := node.MutableColumnList()
	for _, column := range columns {
		name, _ := column.Name()
		if !strings.HasPrefix(name, incompleteColumnName) {
			continue
		}
		item, ok := c.createCompletionItemFromColumn(ctx, incompleteColumnName, column)
		if !ok {
			continue
		}

		result = append(result, item)
	}

	// for record column completion
	columns, _ = node.MutableColumnList()
	for _, column := range columns {
		name, _ := column.Name()
		if !strings.HasPrefix(incompleteColumnName, name) {
			continue
		}
		items := c.createCompletionItemForRecordType(ctx, incompleteColumnName, column)

		result = append(result, items...)
	}

	// for table alias completion
	result = append(result, c.completeScanField(ctx, node, incompleteColumnName)...)

	return result
}

func (c *completor) createCompletionItemFromColumn(ctx context.Context, incompleteColumnName string, column *googlesql.ResolvedColumn) (CompletionItem, bool) {
	tableName, _ := column.TableName()
	tableMetadata, err := c.analyzer.GetTableMetadataFromPath(ctx, tableName)
	if err != nil {
		// cannot find table metadata
		return createCompletionItemFromResolvedColumn(column, incompleteColumnName), true
	}

	colName, _ := column.Name()
	for _, sc := range tableMetadata.Schema {
		if colName == sc.Name {
			return createCompletionItemFromSchema(sc, incompleteColumnName), true
		}
	}

	return CompletionItem{}, false
}

func (c *completor) createCompletionItemForRecordType(ctx context.Context, incompleteColumnName string, column *googlesql.ResolvedColumn) []CompletionItem {
	typ, _ := column.Type()
	if typ == nil {
		return nil
	}
	isStruct, _ := typ.IsStruct()
	if !isStruct {
		return nil
	}

	splittedIncompleteColumnName := strings.Split(incompleteColumnName, ".")
	if len(splittedIncompleteColumnName) <= 1 {
		return nil
	}
	afterRecord := strings.Join(splittedIncompleteColumnName[1:], ".")

	tableName, _ := column.TableName()
	tableMetadata, err := c.analyzer.GetTableMetadataFromPath(ctx, tableName)
	if err != nil {
		return c.createCompletionItemForType(ctx, afterRecord, typ)
	}

	return c.createCompletionItemForBigquerySchema(ctx, incompleteColumnName, tableMetadata.Schema)
}

func (c *completor) createCompletionItemForType(ctx context.Context, incompleteColumnName string, typ googlesql.Googlesql_TypeNode) []CompletionItem {
	if typ == nil {
		return nil
	}
	isStruct, _ := typ.IsStruct()
	if !isStruct {
		return nil
	}

	structType, err := typ.AsStruct()
	if err != nil || structType == nil {
		return nil
	}
	fields, err := structType.Fields()
	if err != nil {
		return nil
	}

	inCompleteColumns := strings.Split(incompleteColumnName, ".")
	if len(inCompleteColumns) > 1 {
		for _, field := range fields {
			if field.Name == inCompleteColumns[0] {
				isFieldStruct, _ := field.Type_.IsStruct()
				if !isFieldStruct {
					return nil
				}
				return c.createCompletionItemForType(ctx, strings.Join(inCompleteColumns[1:], "."), field.Type_)
			}
		}
		return nil
	}

	items := make([]CompletionItem, 0)
	for _, field := range fields {
		if !strings.HasPrefix(field.Name, incompleteColumnName) {
			continue
		}
		items = append(items, createCompletionItemFromStructField(field, incompleteColumnName))
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

func (c *completor) completeScanField(ctx context.Context, node googlesql.ResolvedScanNode, incompleteColumnName string) []CompletionItem {
	switch n := node.(type) {
	case *googlesql.ResolvedTableScan:
		return c.completeTableScanField(ctx, n, incompleteColumnName)
	case *googlesql.ResolvedWithRefScan:
		return c.completeWithScanField(ctx, n, incompleteColumnName)
	case *googlesql.ResolvedJoinScan:
		leftScan, _ := n.LeftScan()
		rightScan, _ := n.RightScan()
		var leftResult, rightResult []CompletionItem
		if leftScan != nil {
			leftResult = c.completeScanField(ctx, leftScan, incompleteColumnName)
		}
		if rightScan != nil {
			rightResult = c.completeScanField(ctx, rightScan, incompleteColumnName)
		}
		return append(leftResult, rightResult...)
	case *googlesql.ResolvedFilterScan:
		inputScan, _ := n.InputScan()
		if inputScan != nil {
			return c.completeScanField(ctx, inputScan, incompleteColumnName)
		}
	}
	return nil
}

func (c *completor) completeTableScanField(ctx context.Context, tableScanNode *googlesql.ResolvedTableScan, incompleteColumnName string) []CompletionItem {
	alias, _ := tableScanNode.Alias()
	if alias == "" {
		return nil
	}

	if strings.HasPrefix(alias, incompleteColumnName) {
		table, _ := tableScanNode.Table()
		fullName := ""
		if table != nil {
			fullName, _ = table.FullName()
		}
		return []CompletionItem{
			{
				Kind:    lsp.CIKField,
				NewText: alias,
				Documentation: lsp.MarkupContent{
					Kind:  lsp.MKPlainText,
					Value: fullName,
				},
				TypedPrefix: incompleteColumnName,
			},
		}
	}

	if !strings.HasPrefix(incompleteColumnName, alias+".") {
		return nil
	}

	result := make([]CompletionItem, 0)
	afterWord := strings.TrimPrefix(incompleteColumnName, alias+".")
	columns, _ := tableScanNode.ColumnList()
	for _, column := range columns {
		name, _ := column.Name()
		if !strings.HasPrefix(name, afterWord) {
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

func (c *completor) completeWithScanField(ctx context.Context, withScanNode *googlesql.ResolvedWithRefScan, incompleteColumnName string) []CompletionItem {
	withQueryName, _ := withScanNode.WithQueryName()
	if strings.HasPrefix(withQueryName, incompleteColumnName) {
		return []CompletionItem{
			{
				Kind:        lsp.CIKField,
				NewText:     withQueryName,
				TypedPrefix: incompleteColumnName,
			},
		}
	}

	if !strings.HasPrefix(incompleteColumnName, withQueryName+".") {
		return nil
	}

	result := make([]CompletionItem, 0)
	afterWord := strings.TrimPrefix(incompleteColumnName, withQueryName+".")
	columns, _ := withScanNode.ColumnList()
	for _, column := range columns {
		name, _ := column.Name()
		if !strings.HasPrefix(name, afterWord) {
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

func (c *completor) getMostNarrowInputScanNode(node googlesql.ResolvedScanNode, termOffset int) (googlesql.ResolvedScanNode, bool) {
	for {
		switch n := node.(type) {
		case *googlesql.ResolvedProjectScan:
			inputScan, _ := n.InputScan()
			if inputScan != nil {
				inner, ok := c.getMostNarrowInputScanNode(inputScan, termOffset)
				if ok {
					return inner, true
				}
			}

			lRange, _ := n.GetParseLocationRangeOrNULL()
			if lRange == nil {
				return n, true
			}

			startOff := file.ParseLocStart(lRange)
			endOff := file.ParseLocEnd(lRange)
			if startOff <= termOffset && termOffset <= endOff {
				return n, true
			}
			return nil, false
		case *googlesql.ResolvedAggregateScan:
			inputScan, _ := n.InputScan()
			if inputScan != nil {
				inner, ok := c.getMostNarrowInputScanNode(inputScan, termOffset)
				if ok {
					return inner, true
				}
			}

			lRange, _ := n.GetParseLocationRangeOrNULL()
			if lRange == nil {
				return n, true
			}

			startOff := file.ParseLocStart(lRange)
			endOff := file.ParseLocEnd(lRange)
			if startOff <= termOffset && termOffset <= endOff {
				return n, true
			}
			return nil, false
		case *googlesql.ResolvedFilterScan:
			inputScan, _ := n.InputScan()
			if inputScan != nil {
				inner, ok := c.getMostNarrowInputScanNode(inputScan, termOffset)
				if ok {
					return inner, true
				}
			}

			lRange, _ := n.GetParseLocationRangeOrNULL()
			if lRange == nil {
				return n, true
			}

			startOff := file.ParseLocStart(lRange)
			endOff := file.ParseLocEnd(lRange)
			if startOff <= termOffset && termOffset <= endOff {
				return n, true
			}
			return nil, false
		case *googlesql.ResolvedTableScan:
			lRange, _ := n.GetParseLocationRangeOrNULL()
			if lRange == nil {
				return n, true
			}

			startOff := file.ParseLocStart(lRange)
			endOff := file.ParseLocEnd(lRange)
			if startOff <= termOffset && termOffset <= endOff {
				return n, true
			}
			return nil, false
		case *googlesql.ResolvedWithScan:
			wel, _ := n.WithEntryList()
			replaced := false
			var nextNode googlesql.ResolvedScanNode
			for _, we := range wel {
				lRange, _ := we.GetParseLocationRangeOrNULL()
				if lRange == nil {
					continue
				}

				startOff := file.ParseLocStart(lRange)
				endOff := file.ParseLocEnd(lRange)
				if termOffset < startOff || endOff < termOffset {
					continue
				}

				subquery, _ := we.WithSubquery()
				nextNode = subquery
				replaced = true
			}

			if !replaced {
				nextNode, _ = n.Query()
			}

			if nextNode == nil {
				return node, true
			}

			result, ok := c.getMostNarrowInputScanNode(nextNode, termOffset)
			if ok {
				return result, true
			}
			return nextNode, true
		case *googlesql.ResolvedWithRefScan:
			return nil, false
		case *googlesql.ResolvedJoinScan:
			leftScan, _ := n.LeftScan()
			rightScan, _ := n.RightScan()
			if leftScan != nil {
				inner, ok := c.getMostNarrowInputScanNode(leftScan, termOffset)
				if ok {
					return inner, true
				}
			}
			if rightScan != nil {
				inner, ok := c.getMostNarrowInputScanNode(rightScan, termOffset)
				if ok {
					return inner, true
				}
			}

			lRange, _ := n.GetParseLocationRangeOrNULL()
			if lRange == nil {
				return n, true
			}

			startOff := file.ParseLocStart(lRange)
			endOff := file.ParseLocEnd(lRange)
			if startOff <= termOffset && termOffset <= endOff {
				return n, true
			}
			return nil, false
		case *googlesql.ResolvedOrderByScan:
			inputScan, _ := n.InputScan()
			if inputScan != nil {
				inner, ok := c.getMostNarrowInputScanNode(inputScan, termOffset)
				if ok {
					return inner, true
				}
			}

			lRange, _ := n.GetParseLocationRangeOrNULL()
			if lRange == nil {
				return n, true
			}

			startOff := file.ParseLocStart(lRange)
			endOff := file.ParseLocEnd(lRange)
			if startOff <= termOffset && termOffset <= endOff {
				return n, true
			}
			return nil, false
		case *googlesql.ResolvedLimitOffsetScan:
			inputScan, _ := n.InputScan()
			if inputScan != nil {
				inner, ok := c.getMostNarrowInputScanNode(inputScan, termOffset)
				if ok {
					return inner, true
				}
			}
			return nil, false
		default:
			c.logger.Printf("unknown scan node type: %T\n", n)
			return nil, false
		}
	}
}
