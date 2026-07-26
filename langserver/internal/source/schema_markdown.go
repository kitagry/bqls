package source

import (
	"fmt"
	"strings"

	"cloud.google.com/go/bigquery"
)

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
