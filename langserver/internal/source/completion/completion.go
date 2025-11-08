package completion

import (
	"context"

	bq "cloud.google.com/go/bigquery"
	"github.com/goccy/go-zetasql"
	rast "github.com/goccy/go-zetasql/resolved_ast"
	"github.com/goccy/go-zetasql/types"
	"github.com/kitagry/bqls/langserver/internal/bigquery"
	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/kitagry/bqls/langserver/internal/source/file"
	"github.com/sirupsen/logrus"
)

type completor struct {
	logger   *logrus.Logger
	analyzer *file.Analyzer
	bqClient bigquery.Client
}

func New(logger *logrus.Logger, analyzer *file.Analyzer, bqClient bigquery.Client) *completor {
	return &completor{
		logger:   logger,
		analyzer: analyzer,
		bqClient: bqClient,
	}
}

func (c *completor) Complete(ctx context.Context, parsedFile file.ParsedFile, position lsp.Position) ([]CompletionItem, error) {
	result, err := c.completeTablePath(ctx, parsedFile, position)
	if err != nil {
		return result, err
	}

	result = append(result, c.completeColumns(ctx, parsedFile, position)...)
	result = append(result, c.completeBuiltinFunction(ctx, parsedFile, position)...)
	result = append(result, c.completeDeclaration(ctx, parsedFile, position)...)
	result = append(result, c.completeKeyword(ctx, parsedFile, position)...)
	return result, nil
}

func findScanNode(output *zetasql.AnalyzerOutput, termOffset int) (node rast.ScanNode, ok bool) {
	node, ok = file.SearchResolvedAstNode[rast.ScanNode](output, termOffset)
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

type columnInterface interface {
	Name() string
	Type() types.Type
}

func createCompletionItemFromColumn(column columnInterface, incompleteColumnName string) CompletionItem {
	return CompletionItem{
		Kind:    lsp.CIKField,
		NewText: column.Name(),
		Documentation: lsp.MarkupContent{
			Kind:  lsp.MKPlainText,
			Value: column.Type().TypeName(types.ProductExternal),
		},
		TypedPrefix: incompleteColumnName,
	}
}

func createCompletionItemFromSchema(schema *bq.FieldSchema, incompleteColumnName string) CompletionItem {
	detail := string(schema.Type)
	if schema.Description != "" {
		detail += "\n" + schema.Description
	}
	return CompletionItem{
		Kind:    lsp.CIKField,
		NewText: schema.Name,
		Documentation: lsp.MarkupContent{
			Kind:  lsp.MKPlainText,
			Value: detail,
		},
		TypedPrefix: incompleteColumnName,
	}
}
