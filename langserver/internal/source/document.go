package source

import (
	"context"
	"fmt"
	"math"
	"strings"

	"cloud.google.com/go/bigquery"
	"github.com/goccy/go-zetasql"
	"github.com/goccy/go-zetasql/ast"
	rast "github.com/goccy/go-zetasql/resolved_ast"
	"github.com/goccy/go-zetasql/types"
	"github.com/kitagry/bqls/langserver/internal/function"
	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/kitagry/bqls/langserver/internal/source/file"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
)

func (p *Project) TermDocument(ctx context.Context, uri lsp.DocumentURI, position lsp.Position) ([]lsp.MarkedString, error) {
	sql := p.cache.Get(uri)
	parsedFile := p.analyzer.ParseFile(uri, sql.RawText)

	termOffset := parsedFile.TermOffset(position)
	targetNode, ok := file.SearchAstNode[*ast.PathExpressionNode](parsedFile.Node, termOffset)
	if !ok {
		p.logger.Debug("not found target node")
		return nil, nil
	}

	output, ok := parsedFile.FindTargetAnalyzeOutput(termOffset)
	if !ok {
		// If not found analyze output, lookup table metadata from ast node.
		if targetNode, ok := file.LookupNode[*ast.TablePathExpressionNode](targetNode); ok {
			result, ok := p.termDocumentFromAstNode(ctx, targetNode)
			if ok {
				return result, nil
			}
		}
		p.logger.Debug("not found target analyze output")
		return nil, nil
	}

	// lookup table metadata
	if targetNode, ok := file.LookupNode[*ast.TablePathExpressionNode](targetNode); ok {
		result, ok := p.termDocumentForInputScan(ctx, termOffset, targetNode, output, parsedFile)
		if ok {
			return result, nil
		}
	}

	if node, ok := file.SearchResolvedAstNode[*rast.FunctionCallNode](output, termOffset); ok {
		builtinFunction, ok := function.FindBuiltInFunction(node.Function().Name())
		if !ok {
			sigs := make([]string, 0, len(node.Function().Signatures()))
			for _, sig := range node.Function().Signatures() {
				sigs = append(sigs, sig.DebugString(node.Function().SQLName(), true))
			}
			return []lsp.MarkedString{
				{
					Language: "markdown",
					Value:    fmt.Sprintf("## %s\n\n%s", node.Function().SQLName(), strings.Join(sigs, "\n")),
				},
			}, nil
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
		return result, nil
	}

	if node, ok := file.SearchResolvedAstNode[*rast.GetStructFieldNode](output, termOffset); ok {
		return []lsp.MarkedString{
			{
				Language: "markdown",
				Value:    node.Type().TypeName(types.ProductExternal),
			},
		}, nil
	}

	if term, ok := file.SearchResolvedAstNode[*rast.ColumnRefNode](output, termOffset); ok {
		column := term.Column()
		if column == nil {
			return nil, fmt.Errorf("failed to find term: %v", term)
		}

		tableMetadata, err := p.analyzer.GetTableMetadataFromPath(ctx, column.TableName())
		if err != nil {
			// cannot find table metadata
			return []lsp.MarkedString{
				{
					Language: "yaml",
					Value:    createColumnYamlString(column),
				},
			}, nil
		}

		for _, f := range tableMetadata.Schema {
			if column.Name() == f.Name {
				return []lsp.MarkedString{
					{
						Language: "yaml",
						Value:    createBigQueryFieldYamlString(f, 0),
					},
				}, nil
			}
		}
	}

	if selectColumnNode, ok := file.LookupNode[*ast.SelectColumnNode](targetNode); ok {
		column, err := p.getSelectColumnNodeToAnalyzedOutputCoumnNode(output, selectColumnNode, termOffset)
		if err != nil {
			return nil, fmt.Errorf("failed to get column info: %w", err)
		}

		tableMetadata, err := p.analyzer.GetTableMetadataFromPath(ctx, column.TableName())
		if err != nil {
			return []lsp.MarkedString{
				{
					Language: "yaml",
					Value:    createColumnYamlString(column),
				},
			}, nil
		}

		for _, f := range tableMetadata.Schema {
			if column.Name() == f.Name {
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

func (p *Project) termDocumentFromAstNode(ctx context.Context, targetNode *ast.TablePathExpressionNode) ([]lsp.MarkedString, bool) {
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

func (p *Project) termDocumentForInputScan(ctx context.Context, termOffset int, targetNode *ast.TablePathExpressionNode, output *zetasql.AnalyzerOutput, parsedFile file.ParsedFile) ([]lsp.MarkedString, bool) {
	targetScanNode, ok := getMostNarrowScanNode(termOffset, output.Statement())
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
	case *rast.TableScanNode:
		result, err := p.createTableMarkedString(ctx, node)
		if err != nil {
			return nil, false
		}
		if len(result) > 0 {
			return result, true
		}
	case *rast.WithRefScanNode:
		withEntries := file.ListResolvedAstNode[*rast.WithEntryNode](output)
		if len(withEntries) == 0 {
			p.logger.Debug("not found with entries")
			return nil, false
		}

		result := []lsp.MarkedString{
			{
				Language: "yaml",
				Value:    createColumnListYamlString(node.ColumnList()),
			},
		}
		for _, entry := range withEntries {
			if entry.WithQueryName() == name {
				subQuery := entry.WithSubquery()
				switch n := subQuery.(type) {
				case *rast.ProjectScanNode:
					sql, ok := parsedFile.ExtractSQL(n.ParseLocationRange())
					if !ok {
						break
					}

					sql = fmt.Sprintf("WITH %s AS (\n%s\n)", entry.WithQueryName(), sql)
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

func (p *Project) createTableMarkedString(ctx context.Context, node *rast.TableScanNode) ([]lsp.MarkedString, error) {
	targetTable, err := p.analyzer.GetTableMetadataFromPath(ctx, node.Table().Name())
	if err != nil {
		return nil, fmt.Errorf("failed to get table metadata: %w", err)
	}

	return buildBigQueryTableMetadataMarkedString(targetTable)
}

func (p *Project) getSelectColumnNodeToAnalyzedOutputCoumnNode(output *zetasql.AnalyzerOutput, column *ast.SelectColumnNode, termOffset int) (*rast.Column, error) {
	targetScanNode, ok := getMostNarrowScanNode(termOffset, output.Statement())
	if !ok {
		return nil, fmt.Errorf("failed to find scand node")
	}

	refNames := p.listRefNamesForScanNode(targetScanNode)

	columnName, ok := getSelectColumnName(column)
	if !ok {
		return nil, fmt.Errorf("failed getSelectColumnName: %s", column.DebugString(0))
	}

	// remove table prefix
	for _, refName := range refNames {
		tablePrefix := fmt.Sprintf("%s.", refName)
		if strings.HasPrefix(columnName, tablePrefix) {
			columnName = strings.TrimPrefix(columnName, tablePrefix)
			break
		}
	}

	for _, c := range targetScanNode.ColumnList() {
		if c.Name() == columnName {
			return c, nil
		}
	}
	return nil, fmt.Errorf("failed to find column info")
}

func (p *Project) listRefNamesForScanNode(scanNode rast.ScanNode) []string {
	switch n := scanNode.(type) {
	case *rast.ProjectScanNode:
		return p.listRefNamesForScanNode(n.InputScan())
	case *rast.WithScanNode:
		return p.listRefNamesForScanNode(n.Query())
	case *rast.OrderByScanNode:
		return p.listRefNamesForScanNode(n.InputScan())
	case *rast.AggregateScanNode:
		return p.listRefNamesForScanNode(n.InputScan())
	case *rast.FilterScanNode:
		return p.listRefNamesForScanNode(n.InputScan())
	case *rast.ArrayScanNode:
		return p.listRefNamesForScanNode(n.InputScan())
	case *rast.AnalyticScanNode:
		return p.listRefNamesForScanNode(n.InputScan())
	case *rast.JoinScanNode:
		return append(p.listRefNamesForScanNode(n.LeftScan()), p.listRefNamesForScanNode(n.RightScan())...)
	case *rast.TableScanNode:
		if n.Alias() != "" {
			return []string{n.Alias()}
		}
		return nil
	case *rast.WithRefScanNode:
		return []string{n.WithQueryName()}
	default:
		p.logger.Debugf("Unsupported type: %T\n%s", n, scanNode.DebugString())
		return nil
	}
}

func (p *Project) findInputScan(name string, scanNode rast.ScanNode) (rast.ScanNode, bool) {
	for scanNode != nil {
		switch n := scanNode.(type) {
		case *rast.ProjectScanNode:
			scanNode = n.InputScan()
		case *rast.WithScanNode:
			scanNode = n.Query()
		case *rast.OrderByScanNode:
			scanNode = n.InputScan()
		case *rast.AggregateScanNode:
			scanNode = n.InputScan()
		case *rast.FilterScanNode:
			scanNode = n.InputScan()
		case *rast.JoinScanNode:
			left, ok := p.findInputScan(name, n.LeftScan())
			if ok {
				return left, true
			}
			right, ok := p.findInputScan(name, n.RightScan())
			if ok {
				return right, true
			}
			return nil, false
		case *rast.ArrayScanNode:
			scanNode = n.InputScan()
		case *rast.AnalyticScanNode:
			scanNode = n.InputScan()
		case *rast.TableScanNode:
			if n.Alias() == name {
				return n, true
			}
			if n.Table().Name() == name {
				return n, true
			}
			if n.Table().Name() == strings.Join([]string{p.bqClient.GetDefaultProject(), name}, ".") {
				return n, true
			}
			return nil, false
		case *rast.WithRefScanNode:
			if n.WithQueryName() == name {
				return n, true
			}
			return nil, false
		default:
			p.logger.Debugf("Unsupported type: %T\n%s", n, n.DebugString())
			return nil, false
		}
	}
	return nil, false
}

func getMostNarrowScanNode(termOffset int, stmt rast.StatementNode) (rast.ScanNode, bool) {
	scanNodes := make([]rast.ScanNode, 0)
	rast.Walk(stmt, func(n rast.Node) error {
		t, ok := n.(rast.ScanNode)
		if !ok {
			return nil
		}
		scanNodes = append(scanNodes, t)
		return nil
	})

	mostNarrowWidth := math.MaxInt
	var targetScanNode rast.ScanNode
	for _, node := range scanNodes {
		lrange := node.ParseLocationRange()
		if lrange == nil {
			continue
		}

		startOffset := lrange.Start().ByteOffset()
		endOffset := lrange.End().ByteOffset()
		if termOffset < startOffset || endOffset < termOffset {
			continue
		}

		width := lrange.End().ByteOffset() - lrange.Start().ByteOffset()
		if width < mostNarrowWidth {
			targetScanNode = node
		}
	}

	return targetScanNode, targetScanNode != nil
}

func getSelectColumnName(targetNode *ast.SelectColumnNode) (string, bool) {
	path, ok := targetNode.Expression().(*ast.PathExpressionNode)
	if !ok {
		return "", false
	}

	names := make([]string, len(path.Names()))
	for i, t := range path.Names() {
		names[i] = t.Name()
	}
	return strings.Join(names, "."), true
}

func createColumnListYamlString(columnLists []*rast.Column) string {
	markdownBuilder := &strings.Builder{}
	for _, column := range columnLists {
		markdownBuilder.WriteString(createColumnYamlString(column))
	}
	return markdownBuilder.String()
}

func createColumnYamlString(column *rast.Column) string {
	return fmt.Sprintf("- name: %s\n  type: %s\n", column.Name(), column.Type().TypeName(types.ProductExternal))
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
