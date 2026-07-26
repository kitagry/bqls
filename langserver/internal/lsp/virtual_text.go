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
	// Schema is additional column type information used to render nested
	// (RECORD/REPEATED) values on the client. Omitted when empty so that
	// older clients ignoring this field keep working unchanged.
	Schema []FieldSchema `json:"schema,omitempty"`
}

type FieldSchema struct {
	Name     string        `json:"name"`
	Type     string        `json:"type"`
	Repeated bool          `json:"repeated,omitempty"`
	Required bool          `json:"required,omitempty"`
	Fields   []FieldSchema `json:"fields,omitempty"`
}

func NewJobVirtualTextDocumentURI(projectID, jobID, location string) DocumentURI {
	return DocumentURI(fmt.Sprintf("bqls://project/%s/job/%s/location/%s", projectID, jobID, location))
}

func NewTableVirtualTextDocumentURI(projectID, datasetID, tableID string) DocumentURI {
	return DocumentURI(fmt.Sprintf("bqls://project/%s/dataset/%s/table/%s", projectID, datasetID, tableID))
}
