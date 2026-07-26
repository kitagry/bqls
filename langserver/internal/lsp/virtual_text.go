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
	// Schema is the table's schema as structured data (name/type/repeated/
	// fields), letting clients render a collapsible tree instead of relying
	// on the Markdown table already embedded in Contents. Only set for
	// table (not job) virtual documents; omitted when empty.
	Schema []FieldSchema `json:"schema,omitempty"`
	// Pending indicates this is a placeholder response; the real contents/
	// result will follow via a bqls/publishVirtualTextDocument notification.
	// Only set when the client declared supports_async_virtual_text_document.
	Pending bool `json:"pending,omitempty"`
}

// VirtualTextDocumentKind distinguishes which part of a virtual text
// document a bqls/publishVirtualTextDocument notification carries. Details
// (table/job metadata) are usually available well before the preview (query
// result rows), so they're published as soon as each becomes ready instead
// of waiting for both.
type VirtualTextDocumentKind string

const (
	VirtualTextDocumentKindDetails VirtualTextDocumentKind = "details"
	VirtualTextDocumentKindPreview VirtualTextDocumentKind = "preview"
)

// PublishVirtualTextDocumentParams is pushed by the server once an async
// bqls/virtualTextDocument fetch completes (or fails) for the given Kind.
// TextDocument.URI correlates it back to the uri the client originally
// requested. Two notifications (one per Kind) are sent per request.
type PublishVirtualTextDocumentParams struct {
	TextDocument TextDocumentIdentifier  `json:"textDocument"`
	Kind         VirtualTextDocumentKind `json:"kind"`
	Contents     []MarkedString          `json:"contents,omitempty"`
	// Schema is set alongside Contents (Kind == details) for table virtual
	// documents; see VirtualTextDocument.Schema.
	Schema []FieldSchema `json:"schema,omitempty"`
	Result *QueryResult  `json:"result,omitempty"`
	Error  string        `json:"error,omitempty"`
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
	Name        string        `json:"name"`
	Type        string        `json:"type"`
	Repeated    bool          `json:"repeated,omitempty"`
	Required    bool          `json:"required,omitempty"`
	Description string        `json:"description,omitempty"`
	Fields      []FieldSchema `json:"fields,omitempty"`
}

func NewJobVirtualTextDocumentURI(projectID, jobID, location string) DocumentURI {
	return DocumentURI(fmt.Sprintf("bqls://project/%s/job/%s/location/%s", projectID, jobID, location))
}

func NewTableVirtualTextDocumentURI(projectID, datasetID, tableID string) DocumentURI {
	return DocumentURI(fmt.Sprintf("bqls://project/%s/dataset/%s/table/%s", projectID, datasetID, tableID))
}
