package source_test

import (
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/google/go-cmp/cmp"
	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/kitagry/bqls/langserver/internal/source"
)

func TestBuildFieldSchema(t *testing.T) {
	tests := map[string]struct {
		schema   bigquery.Schema
		expected []lsp.FieldSchema
	}{
		"flat schema": {
			schema: bigquery.Schema{
				{Name: "id", Type: bigquery.IntegerFieldType},
				{Name: "name", Type: bigquery.StringFieldType, Required: true},
			},
			expected: []lsp.FieldSchema{
				{Name: "id", Type: "INTEGER"},
				{Name: "name", Type: "STRING", Required: true},
			},
		},
		"repeated field": {
			schema: bigquery.Schema{
				{Name: "tags", Type: bigquery.StringFieldType, Repeated: true},
			},
			expected: []lsp.FieldSchema{
				{Name: "tags", Type: "STRING", Repeated: true},
			},
		},
		"field with description": {
			schema: bigquery.Schema{
				{Name: "id", Type: bigquery.IntegerFieldType, Description: "primary key"},
			},
			expected: []lsp.FieldSchema{
				{Name: "id", Type: "INTEGER", Description: "primary key"},
			},
		},
		"nested record field": {
			schema: bigquery.Schema{
				{
					Name: "address",
					Type: bigquery.RecordFieldType,
					Schema: bigquery.Schema{
						{Name: "city", Type: bigquery.StringFieldType},
						{Name: "zip", Type: bigquery.StringFieldType},
					},
				},
			},
			expected: []lsp.FieldSchema{
				{
					Name: "address",
					Type: "RECORD",
					Fields: []lsp.FieldSchema{
						{Name: "city", Type: "STRING"},
						{Name: "zip", Type: "STRING"},
					},
				},
			},
		},
	}

	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			got := source.BuildFieldSchema(tt.schema)
			if diff := cmp.Diff(tt.expected, got); diff != "" {
				t.Errorf("BuildFieldSchema() diff (-expect, +got)\n%s", diff)
			}
		})
	}
}
