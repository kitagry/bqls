package source

import (
	"fmt"
	"strings"

	"cloud.google.com/go/bigquery"
	"github.com/kitagry/bqls/langserver/internal/lsp"
)

// A field hover presents its signature and metadata directly; only RECORD
// children need a schema table.
func createBigQueryFieldMarkedStrings(field *bigquery.FieldSchema) []lsp.MarkedString {
	mode := "nullable"
	if field.Repeated {
		mode = "repeated"
	} else if field.Required {
		mode = "required"
	}
	result := []lsp.MarkedString{
		{Language: "sql", Value: fmt.Sprintf("%s: %s (%s)", field.Name, field.Type, mode)},
	}
	if field.Description != "" {
		result = append(result, lsp.RawMarkedString(field.Description))
	}
	if len(field.Schema) > 0 {
		result = append(result, lsp.RawMarkedString(createBigQuerySchemaMarkdownTable(field.Schema)))
	}
	return result
}

// createBigQuerySchemaMarkdownTable renders a BigQuery schema as a markdown
// table (Name/Type/Mode/Description columns). Nested RECORD fields are
// indented with non-breaking spaces since leading spaces inside a markdown
// table cell get stripped by most renderers (including VSCode's hover).
func createBigQuerySchemaMarkdownTable(schema bigquery.Schema) string {
	var sb strings.Builder
	sb.WriteString("| Name | Type | Mode | Description |\n")
	sb.WriteString("| --- | --- | --- | --- |\n")
	writeBigQuerySchemaMarkdownTableRows(&sb, schema, 0)
	return sb.String()
}

func writeBigQuerySchemaMarkdownTableRows(sb *strings.Builder, schema bigquery.Schema, depth int) {
	indent := strings.Repeat("&nbsp;&nbsp;", depth)
	for _, f := range schema {
		mode := "NULLABLE"
		if f.Repeated {
			mode = "REPEATED"
		} else if f.Required {
			mode = "REQUIRED"
		}
		fmt.Fprintf(sb, "| %s%s | %s | %s | %s |\n", indent, f.Name, f.Type, mode, f.Description)
		if len(f.Schema) > 0 {
			writeBigQuerySchemaMarkdownTableRows(sb, f.Schema, depth+1)
		}
	}
}
