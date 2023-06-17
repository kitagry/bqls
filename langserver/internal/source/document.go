package source

import (
	"bufio"
	"context"
	"fmt"
	"strings"

	"cloud.google.com/go/bigquery"
	"github.com/goccy/go-zetasql/ast"
	"github.com/kitagry/bqls/langserver/internal/lsp"
)

func (p *Project) TermDocument(uri string, position lsp.Position) ([]lsp.MarkedString, error) {
	sql := p.cache.Get(uri)

	termOffset := positionToByteOffset(sql.RawText, position)
	var targetNode *ast.IdentifierNode
	ast.Walk(sql.Node, func(n ast.Node) error {
		node, ok := n.(*ast.IdentifierNode)
		if !ok {
			return nil
		}
		lRange := node.ParseLocationRange()
		startOffset := lRange.Start().ByteOffset()
		endOffset := lRange.End().ByteOffset()
		if startOffset <= termOffset && termOffset <= endOffset {
			targetNode = node
		}
		return nil
	})

	if targetNode == nil {
		return nil, fmt.Errorf("failed to find node at %s", position)
	}

	if isTableIdentifierNode(targetNode) {
		splitedNode := strings.Split(targetNode.Name(), ".")
		if len(splitedNode) == 3 {
			targetTable, err := p.bqClient.GetTableMetadata(context.Background(), splitedNode[0], splitedNode[1], splitedNode[2])
			if err != nil {
				return nil, fmt.Errorf("failed to get table metadata: %w", err)
			}

			columns := make([]string, len(targetTable.Schema))
			for i, c := range targetTable.Schema {
				columns[i] = fmt.Sprintf("* %s: %s %s", c.Name, string(c.Type), c.Description)
			}

			return buildBigQueryTableMetadataMarkedString(targetTable)
		}
	}

	return nil, nil
}

func buildBigQueryTableMetadataMarkedString(metadata *bigquery.TableMetadata) ([]lsp.MarkedString, error) {
	resultStr := fmt.Sprintf("## %s", metadata.FullID)

	if len(metadata.Description) > 0 {
		resultStr += fmt.Sprintf("\n%s", metadata.Description)
	}

	resultStr += fmt.Sprintf("\ncreated at %s", metadata.CreationTime.Format("2006-01-02 15:04:05"))
	// If cache the metadata, we should delete last modified time because it is confusing.
	resultStr += fmt.Sprintf("\nlast modified at %s", metadata.LastModifiedTime.Format("2006-01-02 15:04:05"))

	schemaJson, err := metadata.Schema.ToJSONFields()
	if err != nil {
		return nil, fmt.Errorf("failed to convert schema to json: %w", err)
	}

	return []lsp.MarkedString{
		{
			Language: "markdown",
			Value:    resultStr,
		},
		{
			Language: "json",
			Value:    string(schemaJson),
		},
	}, nil
}

func buildBigQueryFieldSchemaString(schema *bigquery.FieldSchema) string {
	return buildNestedBigQueryFieldSchemaString(schema, 0)
}

func buildNestedBigQueryFieldSchemaString(schema *bigquery.FieldSchema, depth int) string {
	typ := string(schema.Type)
	if schema.Repeated {
		typ = fmt.Sprintf("ARRAY<%s>", typ)
	}

	space := strings.Repeat(" ", depth*2)
	result := fmt.Sprintf("%s* %s: %s", space, schema.Name, typ)
	if schema.Description != "" {
		result += fmt.Sprintf(" %s", schema.Description)
	}

	if schema.Type == bigquery.RecordFieldType {
		for _, s := range schema.Schema {
			result += fmt.Sprintf("\n%s", buildNestedBigQueryFieldSchemaString(s, depth+1))
		}
	}

	return result
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

func isTableIdentifierNode(n ast.Node) bool {
	if n == nil {
		return false
	}

	_, ok := n.(*ast.FromClauseNode)
	if ok {
		return true
	}

	return isTableIdentifierNode(n.Parent())
}
