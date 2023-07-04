package source

import (
	"bufio"
	"context"
	"fmt"
	"math"
	"strings"

	"cloud.google.com/go/bigquery"
	"github.com/goccy/go-zetasql"
	"github.com/goccy/go-zetasql/ast"
	rast "github.com/goccy/go-zetasql/resolved_ast"
	"github.com/goccy/go-zetasql/types"
	"github.com/kitagry/bqls/langserver/internal/lsp"
)

func (p *Project) TermDocument(uri string, position lsp.Position) ([]lsp.MarkedString, error) {
	ctx := context.Background()
	sql := p.cache.Get(uri)
	parsedFile := p.ParseFile(uri, sql.RawText)

	termOffset := positionToByteOffset(sql.RawText, position)
	termOffset = parsedFile.fixTermOffsetForNode(termOffset)
	targetNode, ok := searchAstNode[*ast.PathExpressionNode](parsedFile.Node, termOffset)
	if !ok {
		p.logger.Debug("not found target node")
		return nil, nil
	}

	output, ok := parsedFile.FindTargetAnalyzeOutput(termOffset)
	if !ok {
		p.logger.Debug("not found target analyze output")
		return nil, nil
	}

	// lookup table metadata
	if targetNode, ok := lookupNode[*ast.TablePathExpressionNode](targetNode); ok {
		result, ok := p.termDocumentForInputScan(ctx, termOffset, targetNode, output, parsedFile)
		if ok {
			return result, nil
		}
	}

	if node, ok := searchResolvedAstNode[*rast.FunctionCallNode](output, termOffset); ok {
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

	if node, ok := searchResolvedAstNode[*rast.GetStructFieldNode](output, termOffset); ok {
		return []lsp.MarkedString{
			{
				Language: "markdown",
				Value:    node.Type().TypeName(types.ProductExternal),
			},
		}, nil
	}

	if term, ok := searchResolvedAstNode[*rast.ColumnRefNode](output, termOffset); ok {
		column := term.Column()
		if column == nil {
			return nil, fmt.Errorf("failed to find term: %v", term)
		}

		tableMetadata, err := p.getTableMetadataFromPath(ctx, column.TableName())
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

	if selectColumnNode, ok := lookupNode[*ast.SelectColumnNode](targetNode); ok {
		column, err := p.getSelectColumnNodeToAnalyzedOutputCoumnNode(output, selectColumnNode, termOffset)
		if err != nil {
			return nil, fmt.Errorf("failed to get column info: %w", err)
		}

		tableMetadata, err := p.getTableMetadataFromPath(ctx, column.TableName())
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

func (p *Project) termDocumentForInputScan(ctx context.Context, termOffset int, targetNode *ast.TablePathExpressionNode, output *zetasql.AnalyzerOutput, parsedFile ParsedFile) ([]lsp.MarkedString, bool) {
	targetScanNode, ok := getMostNarrowScanNode(termOffset, output.Statement())
	if !ok {
		return nil, false
	}

	name, ok := createTableNameFromTablePathExpressionNode(targetNode)
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
		withEntries := listResolvedAstNode[*rast.WithEntryNode](output)
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
	targetTable, err := p.getTableMetadataFromPath(ctx, node.Table().Name())
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
		p.logger.Debugf("Unsupported type: %T", n)
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
		case *rast.TableScanNode:
			if n.Alias() == name {
				return n, true
			}
			if n.Table().Name() == name {
				return n, true
			}
			return nil, false
		case *rast.WithRefScanNode:
			if n.WithQueryName() == name {
				return n, true
			}
			return nil, false
		default:
			p.logger.Debugf("Unsupported type: %T", n)
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
	builder.WriteString(fmt.Sprintf("%s- name: %s\n", indent, field.Name))
	builder.WriteString(fmt.Sprintf("%s  type: %s\n", indent, field.Type))

	if field.Repeated {
		builder.WriteString(fmt.Sprintf("%s  mode: REPEATED\n", indent))
	} else if field.Required {
		builder.WriteString(fmt.Sprintf("%s  mode: REQUIRED\n", indent))
	}

	if field.Description != "" {
		builder.WriteString(fmt.Sprintf("%s  description: %s\n", indent, field.Description))
	}

	if len(field.Schema) > 0 {
		builder.WriteString(createBigQuerySchemaYamlString(field.Schema, depth+1))
	}

	return builder.String()
}

func buildBigQueryTableMetadataMarkedString(metadata *bigquery.TableMetadata) ([]lsp.MarkedString, error) {
	resultStr := fmt.Sprintf("## %s", metadata.FullID)

	if len(metadata.Description) > 0 {
		resultStr += fmt.Sprintf("\n%s", metadata.Description)
	}

	resultStr += fmt.Sprintf("\ncreated at %s", metadata.CreationTime.Format("2006-01-02 15:04:05"))
	// If cache the metadata, we should delete last modified time because it is confusing.
	resultStr += fmt.Sprintf("\nlast modified at %s", metadata.LastModifiedTime.Format("2006-01-02 15:04:05"))

	return []lsp.MarkedString{
		{
			Language: "markdown",
			Value:    resultStr,
		},
		{
			Language: "yaml",
			Value:    createBigQuerySchemaYamlString(metadata.Schema, 0),
		},
	}, nil
}

func positionToByteOffset(sql string, position lsp.Position) int {
	buf := bufio.NewScanner(strings.NewReader(sql))
	buf.Split(bufio.ScanLines)

	var offset int
	for i := 0; i < position.Line; i++ {
		buf.Scan()
		offset += len([]byte(buf.Text())) + 1
	}
	offset += position.Character
	return offset
}

func byteOffsetToPosition(sql string, offset int) (lsp.Position, bool) {
	lines := strings.Split(sql, "\n")

	line := 0
	for _, l := range lines {
		if offset < len(l)+1 {
			return lsp.Position{
				Line:      line,
				Character: offset,
			}, true
		}

		line++
		offset -= len(l) + 1
	}

	return lsp.Position{}, false
}

type locationRangeNode interface {
	ParseLocationRange() *types.ParseLocationRange
}

func searchAstNode[T locationRangeNode](node ast.Node, termOffset int) (T, bool) {
	var targetNode T
	var found bool
	ast.Walk(node, func(n ast.Node) error {
		node, ok := n.(T)
		if !ok {
			return nil
		}
		lRange := node.ParseLocationRange()
		if lRange == nil {
			return nil
		}
		startOffset := lRange.Start().ByteOffset()
		endOffset := lRange.End().ByteOffset()
		if startOffset <= termOffset && termOffset <= endOffset {
			targetNode = node
			found = true
		}
		return nil
	})
	return targetNode, found
}

func searchResolvedAstNode[T locationRangeNode](output *zetasql.AnalyzerOutput, termOffset int) (T, bool) {
	var targetNode T
	var found bool
	rast.Walk(output.Statement(), func(n rast.Node) error {
		node, ok := n.(T)
		if !ok {
			return nil
		}
		lRange := node.ParseLocationRange()
		if lRange == nil {
			return nil
		}
		startOffset := lRange.Start().ByteOffset()
		endOffset := lRange.End().ByteOffset()
		if startOffset <= termOffset && termOffset <= endOffset {
			targetNode = node
			found = true
		}
		return nil
	})

	if found {
		return targetNode, found
	}
	return targetNode, false
}

func listResolvedAstNode[T locationRangeNode](output *zetasql.AnalyzerOutput) []T {
	result := make([]T, 0)
	rast.Walk(output.Statement(), func(n rast.Node) error {
		node, ok := n.(T)
		if !ok {
			return nil
		}
		result = append(result, node)
		return nil
	})

	return result
}

type astNode interface {
	*ast.TablePathExpressionNode | *ast.PathExpressionNode | *ast.SelectColumnNode
}

func lookupNode[T astNode](n ast.Node) (T, bool) {
	if n == nil {
		return nil, false
	}

	result, ok := n.(T)
	if ok {
		return result, true
	}

	return lookupNode[T](n.Parent())
}
