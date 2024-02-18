package lsp

import (
	"cloud.google.com/go/bigquery"
)

type VirtualTextDocumentParams struct {
	TextDocument TextDocumentIdentifier `json:"textDocument"`
}

type VirtualTextDocument struct {
	Contents []MarkedString `json:"contents"`
	Result   QueryResult    `json:"result"`
}

type QueryResult struct {
	Columns []string           `json:"columns"`
	Data    [][]bigquery.Value `json:"data"`
}
