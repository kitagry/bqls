package source

import (
	"context"
	"fmt"
	"math"
	"strings"

	"cloud.google.com/go/bigquery"
	googlesql "github.com/goccy/go-googlesql"
	"github.com/kitagry/bqls/langserver/internal/function"
	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/kitagry/bqls/langserver/internal/source/file"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
)

func (p *Project) TermDocument(ctx context.Context, uri lsp.DocumentURI, position lsp.Position) ([]lsp.MarkedString, error) {
	sql := p.cache.Get(uri)
	parsedFile := p.analyzer.ParseFile(uri, sql.RawText)
	defer parsedFile.Close()

	termOffset := parsedFile.TermOffset(position)
	targetNode, ok := file.SearchAstNode[*googlesql.ASTPathExpression](parsedFile.Node, termOffset)
	if !ok {
		p.logger.Debug("not found target node")
		return nil, nil
	}

	output, ok := parsedFile.FindTargetAnalyzeOutput(termOffset)
	if !ok {
		// Fallback: table hover from AST.
		if tpeNode, ok := file.LookupNode[*googlesql.ASTTablePathExpression](targetNode); ok {
			result, ok := p.termDocumentFromAstNode(ctx, tpeNode)
			if ok {
				return result, nil
			}
		}
		// Fallback: column info from table schemas when analysis failed.
		if result, found := p.termDocumentFromASTColumnRef(ctx, parsedFile, targetNode, termOffset); found {
			return result, nil
		}
		// Fallback: function documentation from AST when analysis failed.
		// Only triggers when cursor is on the function name, not its arguments.
		if fnCallNode, ok := file.LookupNode[*googlesql.ASTFunctionCall](targetNode); ok {
			if isFunctionNameNode(fnCallNode, targetNode) {
				result, found := p.termDocumentFromASTFunctionCall(fnCallNode)
				if found {
					return result, nil
				}
			}
		}
		p.logger.Debug("not found target analyze output")
		return nil, nil
	}

	// lookup table metadata
	if tpeNode, ok := file.LookupNode[*googlesql.ASTTablePathExpression](targetNode); ok {
		result, ok := p.termDocumentForInputScan(ctx, termOffset, tpeNode, output, parsedFile)
		if ok {
			return result, nil
		}
	}

	if node, ok := file.SearchResolvedAstNode[*googlesql.ResolvedFunctionCall](output, termOffset); ok {
		fn, err := node.Function()
		if err == nil && fn != nil {
			fnName, _ := fn.Name()
			// Skip ZetaSQL internal operators (e.g. "$equal", "$add") — not user-facing functions.
			if !strings.HasPrefix(fnName, "$") {
				if result, found := p.functionDocumentation(fn); found {
					return result, nil
				}
			}
		}
	}

	if node, ok := file.SearchResolvedAstNode[*googlesql.ResolvedGetStructField](output, termOffset); ok {
		typ, err := node.Type()
		if err != nil || typ == nil {
			return nil, nil
		}
		typeName, _ := typ.DebugString(false)
		return []lsp.MarkedString{
			{
				Language: "markdown",
				Value:    typeName,
			},
		}, nil
	}

	if term, ok := file.SearchResolvedAstNode[*googlesql.ResolvedColumnRef](output, termOffset); ok {
		column, err := term.Column()
		if err != nil || column == nil {
			return nil, fmt.Errorf("failed to find term: %v", term)
		}

		tableName, _ := column.TableName()
		tableMetadata, err := p.analyzer.GetTableMetadataFromPath(ctx, tableName)
		if err != nil {
			// cannot find table metadata
			return []lsp.MarkedString{
				{
					Language: "yaml",
					Value:    createColumnYamlString(column),
				},
			}, nil
		}

		colName, _ := column.Name()
		for _, f := range tableMetadata.Schema {
			if colName == f.Name {
				return []lsp.MarkedString{
					{
						Language: "yaml",
						Value:    createBigQueryFieldYamlString(f, 0),
					},
				}, nil
			}
		}
	}

	if selectColumnNode, ok := file.LookupNode[*googlesql.ASTSelectColumn](targetNode); ok {
		column, err := p.getSelectColumnNodeToAnalyzedOutputColumnNode(output, selectColumnNode, termOffset)
		if err != nil {
			return nil, fmt.Errorf("failed to get column info: %w", err)
		}

		tableName, _ := column.TableName()
		tableMetadata, err := p.analyzer.GetTableMetadataFromPath(ctx, tableName)
		if err != nil {
			return []lsp.MarkedString{
				{
					Language: "yaml",
					Value:    createColumnYamlString(column),
				},
			}, nil
		}

		colName, _ := column.Name()
		for _, f := range tableMetadata.Schema {
			if colName == f.Name {
				return []lsp.MarkedString{
					{
						Language: "yaml",
						Value:    createBigQueryFieldYamlString(f, 0),
					},
				}, nil
			}
		}
	}

	return nil, nil
}

func (p *Project) termDocumentFromAstNode(ctx context.Context, targetNode *googlesql.ASTTablePathExpression) ([]lsp.MarkedString, bool) {
	name, ok := file.CreateTableNameFromTablePathExpressionNode(targetNode)
	if !ok {
		return nil, false
	}

	targetTable, err := p.analyzer.GetTableMetadataFromPath(ctx, name)
	if err != nil {
		return nil, false
	}

	result, err := buildBigQueryTableMetadataMarkedString(targetTable)
	if err != nil {
		return nil, false
	}
	return result, true
}

// isFunctionNameNode returns true when node is the path expression that names fnCall (not an argument).
func isFunctionNameNode(fnCall *googlesql.ASTFunctionCall, node *googlesql.ASTPathExpression) bool {
	fnExpr, err := fnCall.Function()
	if err != nil || fnExpr == nil {
		return false
	}
	fnLoc, err := fnExpr.GetParseLocationRange()
	if err != nil || fnLoc == nil {
		return false
	}
	nodeLoc, err := node.GetParseLocationRange()
	if err != nil || nodeLoc == nil {
		return false
	}
	return file.ParseLocStart(fnLoc) == file.ParseLocStart(nodeLoc) &&
		file.ParseLocEnd(fnLoc) == file.ParseLocEnd(nodeLoc)
}

// functionDocumentation looks up built-in docs or signature info for a resolved function node.
func (p *Project) functionDocumentation(fn *googlesql.Function) ([]lsp.MarkedString, bool) {
	fnName, _ := fn.Name()
	fnSQLName, _ := fn.SQLName()
	builtinFunction, ok := function.FindBuiltInFunction(fnName)
	if !ok {
		sigs, _ := fn.Signatures()
		sigStrs := make([]string, 0, len(sigs))
		for _, sig := range sigs {
			s, _ := sig.DebugString(fnSQLName, true)
			sigStrs = append(sigStrs, s)
		}
		return []lsp.MarkedString{
			{
				Language: "markdown",
				Value:    fmt.Sprintf("## %s\n\n%s", fnSQLName, strings.Join(sigStrs, "\n")),
			},
		}, true
	}

	result := make([]lsp.MarkedString, 0, len(builtinFunction.ExampleSQLs)+1)
	result = append(result, lsp.MarkedString{
		Language: "markdown",
		Value:    fmt.Sprintf("%s\n\n[bigquery documentation](%s)", builtinFunction.Description, builtinFunction.URL),
	})
	for _, sql := range builtinFunction.ExampleSQLs {
		result = append(result, lsp.MarkedString{
			Language: "sql",
			Value:    sql,
		})
	}
	return result, true
}

// termDocumentFromASTFunctionCall returns function documentation by looking up the function name
// from the AST node, used as a fallback when analysis fails.
func (p *Project) termDocumentFromASTFunctionCall(fnCallNode *googlesql.ASTFunctionCall) ([]lsp.MarkedString, bool) {
	pathExpr, err := fnCallNode.Function()
	if err != nil || pathExpr == nil {
		return nil, false
	}
	names, err := pathExpr.ToIdentifierVector()
	if err != nil || len(names) == 0 {
		return nil, false
	}
	fnName := strings.Join(names, ".")
	builtinFn, ok := function.FindBuiltInFunction(fnName)
	if !ok {
		return nil, false
	}
	result := make([]lsp.MarkedString, 0, len(builtinFn.ExampleSQLs)+1)
	result = append(result, lsp.MarkedString{
		Language: "markdown",
		Value:    fmt.Sprintf("%s\n\n[bigquery documentation](%s)", builtinFn.Description, builtinFn.URL),
	})
	for _, sql := range builtinFn.ExampleSQLs {
		result = append(result, lsp.MarkedString{
			Language: "sql",
			Value:    sql,
		})
	}
	return result, true
}

// termDocumentFromASTColumnRef is a fallback that looks up column info from preloaded table schemas
// when analysis failed (e.g. due to type mismatch in function arguments).
func (p *Project) termDocumentFromASTColumnRef(ctx context.Context, parsedFile file.ParsedFile, targetNode *googlesql.ASTPathExpression, termOffset int) ([]lsp.MarkedString, bool) {
	// Get column name from path expression.
	names, err := targetNode.ToIdentifierVector()
	if err != nil || len(names) == 0 {
		return nil, false
	}
	colName := names[len(names)-1]

	// Find all table path expressions in the enclosing statement.
	stmtNode, ok := parsedFile.FindTargetStatementNode(termOffset)
	if !ok {
		return nil, false
	}
	tableNames := make([]string, 0)
	file.Walk(stmtNode, func(n googlesql.ASTNode) error { //nolint
		tpe, ok := n.(*googlesql.ASTTablePathExpression)
		if !ok {
			return nil
		}
		name, ok := file.CreateTableNameFromTablePathExpressionNode(tpe)
		if ok {
			tableNames = append(tableNames, name)
		}
		return nil
	})

	for _, tblName := range tableNames {
		meta, err := p.analyzer.GetTableMetadataFromPath(ctx, tblName)
		if err != nil {
			continue
		}
		for _, f := range meta.Schema {
			if f.Name == colName {
				return []lsp.MarkedString{
					{
						Language: "yaml",
						Value:    createBigQueryFieldYamlString(f, 0),
					},
				}, true
			}
		}
	}
	return nil, false
}

func (p *Project) termDocumentForInputScan(ctx context.Context, termOffset int, targetNode *googlesql.ASTTablePathExpression, output *googlesql.AnalyzerOutput, parsedFile file.ParsedFile) ([]lsp.MarkedString, bool) {
	stmt, err := output.ResolvedStatement()
	if err != nil || stmt == nil {
		return nil, false
	}
	targetScanNode, ok := getMostNarrowScanNode(termOffset, stmt)
	if !ok {
		return nil, false
	}

	name, ok := file.CreateTableNameFromTablePathExpressionNode(targetNode)
	if !ok {
		p.logger.Debug("not found table name")
		return nil, false
	}

	scanNode, ok := p.findInputScan(name, targetScanNode)
	if !ok {
		p.logger.Debug("not found scan node")
		return nil, false
	}

	switch node := scanNode.(type) {
	case *googlesql.ResolvedTableScan:
		result, err := p.createTableMarkedString(ctx, node)
		if err != nil {
			return nil, false
		}
		if len(result) > 0 {
			return result, true
		}
	case *googlesql.ResolvedWithRefScan:
		withEntries := file.ListResolvedAstNode[*googlesql.ResolvedWithEntry](output)
		if len(withEntries) == 0 {
			p.logger.Debug("not found with entries")
			return nil, false
		}

		columns, _ := node.MutableColumnList()
		result := []lsp.MarkedString{
			{
				Language: "yaml",
				Value:    createColumnListYamlString(columns),
			},
		}
		withQueryName, _ := node.WithQueryName()
		for _, entry := range withEntries {
			entryName, _ := entry.WithQueryName()
			if entryName == name {
				subQuery, _ := entry.WithSubquery()
				if subQuery == nil {
					break
				}
				switch n := subQuery.(type) {
				case *googlesql.ResolvedProjectScan:
					loc, _ := n.GetParseLocationRangeOrNULL()
					sql, ok := parsedFile.ExtractSQL(loc)
					if !ok {
						break
					}
					// Strip surrounding parens added by ZetaSQL's location range.
					sql = strings.TrimSpace(sql)
					if strings.HasPrefix(sql, "(") && strings.HasSuffix(sql, ")") {
						sql = strings.TrimSpace(sql[1 : len(sql)-1])
					}
					sql = fmt.Sprintf("WITH %s AS (\n%s\n)", withQueryName, sql)
					result = append(result, lsp.MarkedString{
						Language: "sql",
						Value:    sql,
					})
				}
			}
		}
		return result, true
	}
	return nil, false
}

func (p *Project) createTableMarkedString(ctx context.Context, node *googlesql.ResolvedTableScan) ([]lsp.MarkedString, error) {
	table, err := node.Table()
	if err != nil || table == nil {
		return nil, fmt.Errorf("failed to get table")
	}
	tableName, err := table.Name()
	if err != nil {
		return nil, fmt.Errorf("failed to get table name: %w", err)
	}
	targetTable, err := p.analyzer.GetTableMetadataFromPath(ctx, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to get table metadata: %w", err)
	}

	return buildBigQueryTableMetadataMarkedString(targetTable)
}

func (p *Project) getSelectColumnNodeToAnalyzedOutputColumnNode(output *googlesql.AnalyzerOutput, column *googlesql.ASTSelectColumn, termOffset int) (*googlesql.ResolvedColumn, error) {
	stmt, err := output.ResolvedStatement()
	if err != nil || stmt == nil {
		return nil, fmt.Errorf("failed to find statement")
	}
	targetScanNode, ok := getMostNarrowScanNode(termOffset, stmt)
	if !ok {
		return nil, fmt.Errorf("failed to find scan node")
	}

	refNames := p.listRefNamesForScanNode(targetScanNode)

	columnName, ok := getSelectColumnName(column)
	if !ok {
		return nil, fmt.Errorf("failed getSelectColumnName: %v", column)
	}

	// remove table prefix
	for _, refName := range refNames {
		tablePrefix := fmt.Sprintf("%s.", refName)
		if strings.HasPrefix(columnName, tablePrefix) {
			columnName = strings.TrimPrefix(columnName, tablePrefix)
			break
		}
	}

	columns, _ := targetScanNode.MutableColumnList()
	for _, c := range columns {
		name, _ := c.Name()
		if name == columnName {
			return c, nil
		}
	}
	return nil, fmt.Errorf("failed to find column info")
}

func (p *Project) listRefNamesForScanNode(scanNode googlesql.ResolvedScanNode) []string {
	switch n := scanNode.(type) {
	case *googlesql.ResolvedProjectScan:
		inputScan, _ := n.InputScan()
		if inputScan != nil {
			return p.listRefNamesForScanNode(inputScan)
		}
	case *googlesql.ResolvedWithScan:
		query, _ := n.Query()
		if query != nil {
			return p.listRefNamesForScanNode(query)
		}
	case *googlesql.ResolvedOrderByScan:
		inputScan, _ := n.InputScan()
		if inputScan != nil {
			return p.listRefNamesForScanNode(inputScan)
		}
	case *googlesql.ResolvedAggregateScan:
		inputScan, _ := n.InputScan()
		if inputScan != nil {
			return p.listRefNamesForScanNode(inputScan)
		}
	case *googlesql.ResolvedFilterScan:
		inputScan, _ := n.InputScan()
		if inputScan != nil {
			return p.listRefNamesForScanNode(inputScan)
		}
	case *googlesql.ResolvedArrayScan:
		inputScan, _ := n.InputScan()
		if inputScan != nil {
			return p.listRefNamesForScanNode(inputScan)
		}
	case *googlesql.ResolvedAnalyticScan:
		inputScan, _ := n.InputScan()
		if inputScan != nil {
			return p.listRefNamesForScanNode(inputScan)
		}
	case *googlesql.ResolvedJoinScan:
		leftScan, _ := n.LeftScan()
		rightScan, _ := n.RightScan()
		var left, right []string
		if leftScan != nil {
			left = p.listRefNamesForScanNode(leftScan)
		}
		if rightScan != nil {
			right = p.listRefNamesForScanNode(rightScan)
		}
		return append(left, right...)
	case *googlesql.ResolvedTableScan:
		alias, _ := n.Alias()
		if alias != "" {
			return []string{alias}
		}
		return nil
	case *googlesql.ResolvedWithRefScan:
		name, _ := n.WithQueryName()
		return []string{name}
	default:
		p.logger.Debugf("Unsupported type: %T\n", n)
		return nil
	}
	return nil
}

func (p *Project) findInputScan(name string, scanNode googlesql.ResolvedScanNode) (googlesql.ResolvedScanNode, bool) {
	for scanNode != nil {
		switch n := scanNode.(type) {
		case *googlesql.ResolvedProjectScan:
			inputScan, _ := n.InputScan()
			scanNode = inputScan
		case *googlesql.ResolvedWithScan:
			query, _ := n.Query()
			scanNode = query
		case *googlesql.ResolvedOrderByScan:
			inputScan, _ := n.InputScan()
			scanNode = inputScan
		case *googlesql.ResolvedAggregateScan:
			inputScan, _ := n.InputScan()
			scanNode = inputScan
		case *googlesql.ResolvedFilterScan:
			inputScan, _ := n.InputScan()
			scanNode = inputScan
		case *googlesql.ResolvedJoinScan:
			leftScan, _ := n.LeftScan()
			if leftScan != nil {
				left, ok := p.findInputScan(name, leftScan)
				if ok {
					return left, true
				}
			}
			rightScan, _ := n.RightScan()
			if rightScan != nil {
				right, ok := p.findInputScan(name, rightScan)
				if ok {
					return right, true
				}
			}
			return nil, false
		case *googlesql.ResolvedArrayScan:
			inputScan, _ := n.InputScan()
			scanNode = inputScan
		case *googlesql.ResolvedAnalyticScan:
			inputScan, _ := n.InputScan()
			scanNode = inputScan
		case *googlesql.ResolvedTableScan:
			alias, _ := n.Alias()
			if alias == name {
				return n, true
			}
			table, _ := n.Table()
			if table != nil {
				tblName, _ := table.Name()
				if tblName == name {
					return n, true
				}
				if tblName == strings.Join([]string{p.bqClient.GetDefaultProject(), name}, ".") {
					return n, true
				}
			}
			return nil, false
		case *googlesql.ResolvedWithRefScan:
			withQueryName, _ := n.WithQueryName()
			if withQueryName == name {
				return n, true
			}
			return nil, false
		default:
			p.logger.Debugf("Unsupported type: %T\n", n)
			return nil, false
		}
	}
	return nil, false
}

func getMostNarrowScanNode(termOffset int, stmt googlesql.ResolvedStatementNode) (googlesql.ResolvedScanNode, bool) {
	scanNodes := make([]googlesql.ResolvedScanNode, 0)
	file.WalkResolved(stmt, func(n googlesql.ResolvedNode) error { //nolint
		isScan, _ := n.IsScan()
		if !isScan {
			return nil
		}
		t, ok := n.(googlesql.ResolvedScanNode)
		if !ok {
			return nil
		}
		scanNodes = append(scanNodes, t)
		return nil
	})

	mostNarrowWidth := math.MaxInt
	var targetScanNode googlesql.ResolvedScanNode
	for _, node := range scanNodes {
		lrange, _ := node.GetParseLocationRangeOrNULL()
		if lrange == nil {
			continue
		}

		startOffset := file.ParseLocStart(lrange)
		endOffset := file.ParseLocEnd(lrange)
		if startOffset < 0 || endOffset < 0 {
			continue
		}
		if termOffset < startOffset || endOffset < termOffset {
			continue
		}

		width := endOffset - startOffset
		if width < mostNarrowWidth {
			mostNarrowWidth = width
			targetScanNode = node
		}
	}

	return targetScanNode, targetScanNode != nil
}

func getSelectColumnName(targetNode *googlesql.ASTSelectColumn) (string, bool) {
	expr, err := targetNode.Expression()
	if err != nil {
		return "", false
	}
	path, ok := expr.(*googlesql.ASTPathExpression)
	if !ok {
		return "", false
	}

	names, err := path.ToIdentifierVector()
	if err != nil {
		return "", false
	}
	return strings.Join(names, "."), true
}

func createColumnListYamlString(columnList []*googlesql.ResolvedColumn) string {
	markdownBuilder := &strings.Builder{}
	for _, column := range columnList {
		markdownBuilder.WriteString(createColumnYamlString(column))
	}
	return markdownBuilder.String()
}

func createColumnYamlString(column *googlesql.ResolvedColumn) string {
	name, _ := column.Name()
	typ, _ := column.Type()
	typeName := ""
	if typ != nil {
		typeName, _ = typ.DebugString(false)
	}
	return fmt.Sprintf("- name: %s\n  type: %s\n", name, typeName)
}

func createBigQuerySchemaYamlString(schema bigquery.Schema, depth int) string {
	builder := &strings.Builder{}
	for _, f := range schema {
		builder.WriteString(createBigQueryFieldYamlString(f, depth))
	}
	return builder.String()
}

func createBigQueryFieldYamlString(field *bigquery.FieldSchema, depth int) string {
	indent := strings.Repeat("  ", depth)
	builder := &strings.Builder{}
	fmt.Fprintf(builder, "%s- name: %s\n", indent, field.Name)
	fmt.Fprintf(builder, "%s  type: %s\n", indent, field.Type)

	if field.Repeated {
		fmt.Fprintf(builder, "%s  mode: REPEATED\n", indent)
	} else if field.Required {
		fmt.Fprintf(builder, "%s  mode: REQUIRED\n", indent)
	}

	if field.Description != "" {
		fmt.Fprintf(builder, "%s  description: %s\n", indent, field.Description)
	}

	if len(field.Schema) > 0 {
		builder.WriteString(createBigQuerySchemaYamlString(field.Schema, depth+1))
	}

	return builder.String()
}

func buildBigQueryTableMetadataMarkedString(metadata *bigquery.TableMetadata) ([]lsp.MarkedString, error) {
	var sb strings.Builder
	sb.Grow(1024)
	fmt.Fprintf(&sb, "## %s\n", metadata.FullID)

	if len(metadata.Description) > 0 {
		fmt.Fprintf(&sb, "%s\n", metadata.Description)
	}

	sb.WriteString("\n### Table info\n\n")

	fmt.Fprintf(&sb, "* Created: %s\n", metadata.CreationTime.Format("2006-01-02 15:04:05"))
	// If cache the metadata, we should delete last modified time because it is confusing.
	fmt.Fprintf(&sb, "* Last modified: %s\n", metadata.LastModifiedTime.Format("2006-01-02 15:04:05"))

	if !metadata.ExpirationTime.IsZero() {
		fmt.Fprintf(&sb, "* Expired: %s\n", metadata.ExpirationTime.Format("2006-01-02 15:04:05"))
	}

	if len(metadata.Labels) > 0 {
		sb.WriteString("* Labels:\n")
		for k, v := range metadata.Labels {
			fmt.Fprintf(&sb, " * %s: %s\n", k, v)
		}
	}

	if metadata.TableConstraints != nil {
		if len(metadata.TableConstraints.PrimaryKey.Columns) > 0 {
			sb.WriteString("* Primary Key:\n")
			for _, c := range metadata.TableConstraints.PrimaryKey.Columns {
				fmt.Fprintf(&sb, "  * %s\n", c)
			}
		}
	}

	if metadata.TimePartitioning != nil {
		sb.WriteString("* Time Partitioning:\n")
		fmt.Fprintf(&sb, "  * Type: %s\n", metadata.TimePartitioning.Type)
		if metadata.TimePartitioning.Field != "" {
			fmt.Fprintf(&sb, "  * Field: %s\n", metadata.TimePartitioning.Field)
		}
		if metadata.TimePartitioning.RequirePartitionFilter {
			fmt.Fprintf(&sb, "  * Partition Filter: required\n")
		}
	}

	if metadata.RangePartitioning != nil {
		sb.WriteString("* Range Partitioning:\n")
		fmt.Fprintf(&sb, "  * Field: %s\n", metadata.RangePartitioning.Field)
		if metadata.RangePartitioning.Range != nil {
			fmt.Fprintf(&sb, "  * Start: %d\n", metadata.RangePartitioning.Range.Start)
			fmt.Fprintf(&sb, "  * End: %d\n", metadata.RangePartitioning.Range.End)
			fmt.Fprintf(&sb, "  * Interval: %d\n", metadata.RangePartitioning.Range.Interval)
		}
	}

	sb.WriteString("\n### Storage info\n\n")

	p := message.NewPrinter(language.English)
	sb.WriteString(p.Sprintf("* Number of rows: %d\n", metadata.NumRows))
	fmt.Fprintf(&sb, "* Total logical bytes: %s\n", bytesConvert(metadata.NumBytes))

	projectID, datasetID, tableID, ok := extractTableIDsFromMedatada(metadata)
	if ok {
		fmt.Fprintf(&sb, "\n[Docs](https://console.cloud.google.com/bigquery?project=%[1]s&ws=!1m5!1m4!4m3!1s%[1]s!2s%[2]s!3s%[3]s)\n", projectID, datasetID, tableID)
	}

	return []lsp.MarkedString{
		{
			Language: "markdown",
			Value:    sb.String(),
		},
		{
			Language: "yaml",
			Value:    createBigQuerySchemaYamlString(metadata.Schema, 0),
		},
	}, nil
}

func bytesConvert(bytes int64) string {
	if bytes == 0 {
		return "0 bytes"
	}

	base := math.Floor(math.Log(float64(bytes)) / math.Log(1024))
	units := []string{"bytes", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB", "ZiB"}

	stringVal := fmt.Sprintf("%.2f", float64(bytes)/math.Pow(1024, base))
	stringVal = strings.TrimSuffix(stringVal, ".00")
	return fmt.Sprintf("%s %v",
		stringVal,
		units[int(base)],
	)
}

func extractTableIDsFromMedatada(metadata *bigquery.TableMetadata) (projectID, datasetID, tableID string, ok bool) {
	// FullID is projectID:datasetID.tableID
	sep := strings.Split(metadata.FullID, ":")
	if len(sep) != 2 {
		return "", "", "", false
	}

	projectID = sep[0]

	sep = strings.Split(sep[1], ".")
	if len(sep) != 2 {
		return "", "", "", false
	}

	datasetID = sep[0]
	tableID = sep[1]
	return projectID, datasetID, tableID, true
}
