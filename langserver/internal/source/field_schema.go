package source

import (
	"cloud.google.com/go/bigquery"
	"github.com/kitagry/bqls/langserver/internal/lsp"
)

// BuildFieldSchema converts a BigQuery schema into the client-facing
// FieldSchema tree, used to render nested RECORD/REPEATED values and
// table schemas without exposing the BigQuery SDK types to LSP clients.
func BuildFieldSchema(schema bigquery.Schema) []lsp.FieldSchema {
	if len(schema) == 0 {
		return nil
	}
	result := make([]lsp.FieldSchema, 0, len(schema))
	for _, f := range schema {
		result = append(result, lsp.FieldSchema{
			Name:        f.Name,
			Type:        string(f.Type),
			Repeated:    f.Repeated,
			Required:    f.Required,
			Description: f.Description,
			Fields:      BuildFieldSchema(f.Schema),
		})
	}
	return result
}
