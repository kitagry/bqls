package lsp

import (
	"fmt"

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

func NewJobVirtualTextDocumentURI(projectID, jobID, location string) DocumentURI {
	return DocumentURI(fmt.Sprintf("bqls://project/%s/job/%s/location/%s", projectID, jobID, location))
}

func NewTableVirtualTextDocumentURI(projectID, datasetID, tableID string) DocumentURI {
	return DocumentURI(fmt.Sprintf("bqls://project/%s/dataset/%s/table/%s", projectID, datasetID, tableID))
}
