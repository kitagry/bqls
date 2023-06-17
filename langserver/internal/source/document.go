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

	if targetNode, ok := lookUpTablePathExpressionNode(targetNode); ok {
		pathNames := make([]string, len(targetNode.PathExpr().Names()))
		for i, n := range targetNode.PathExpr().Names() {
			pathNames[i] = n.Name()
		}
		splitedNode := strings.Split(strings.Join(pathNames, "."), ".")
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

func lookUpTablePathExpressionNode(n ast.Node) (*ast.TablePathExpressionNode, bool) {
	if n == nil {
		return nil, false
	}

	result, ok := n.(*ast.TablePathExpressionNode)
	if ok {
		return result, true
	}

	return lookUpTablePathExpressionNode(n.Parent())
}
