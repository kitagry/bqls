package completion

import (
	"context"

	bq "cloud.google.com/go/bigquery"
	googlesql "github.com/goccy/go-googlesql"
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

func findScanNode(output *googlesql.AnalyzerOutput, termOffset int) (node googlesql.ResolvedScanNode, ok bool) {
	node, ok = file.SearchResolvedAstNode[googlesql.ResolvedScanNode](output, termOffset)
	if ok {
		return node, true
	}

	// In some case, *googlesql.ResolvedProjectScan.ParseLocationRange() returns nil.
	// So, if we cannot find a scan node, we search for one whose ParseLocationRange returns nil.
	stmt, err := output.ResolvedStatement()
	if err != nil || stmt == nil {
		return nil, false
	}
	file.WalkResolved(stmt, func(n googlesql.ResolvedNode) error { //nolint
		isScan, _ := n.IsScan()
		if !isScan {
			return nil
		}

		sNode, ok := n.(googlesql.ResolvedScanNode)
		if !ok {
			return nil
		}

		lRange, _ := n.GetParseLocationRangeOrNULL()
		if lRange == nil {
			node = sNode
			return nil
		}
		endOff := file.ParseLocEnd(lRange)
		if endOff < termOffset+5 {
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

func createCompletionItemFromResolvedColumn(column *googlesql.ResolvedColumn, incompleteColumnName string) CompletionItem {
	name, _ := column.Name()
	typ, _ := column.Type()
	typeName := typeNodeName(typ)
	return CompletionItem{
		Kind:    lsp.CIKField,
		NewText: name,
		Documentation: lsp.MarkupContent{
			Kind:  lsp.MKPlainText,
			Value: typeName,
		},
		TypedPrefix: incompleteColumnName,
	}
}

func createCompletionItemFromStructField(field *googlesql.StructField, incompleteColumnName string) CompletionItem {
	typeName := typeNodeName(field.Type_)
	return CompletionItem{
		Kind:    lsp.CIKField,
		NewText: field.Name,
		Documentation: lsp.MarkupContent{
			Kind:  lsp.MKPlainText,
			Value: typeName,
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

func typeNodeName(typ googlesql.Googlesql_TypeNode) string {
	if typ == nil {
		return ""
	}
	name, _ := typ.DebugString(false)
	return name
}
