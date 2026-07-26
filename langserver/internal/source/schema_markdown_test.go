package source

import (
	"testing"

	"cloud.google.com/go/bigquery"
)

func TestCreateBigQuerySchemaMarkdownTable(t *testing.T) {
	tests := map[string]struct {
		schema   bigquery.Schema
		expected string
	}{
		"flat schema": {
			schema: bigquery.Schema{
				{Name: "id", Type: bigquery.IntegerFieldType, Required: true},
				{Name: "name", Type: bigquery.StringFieldType},
			},
			expected: "| Name | Type | Mode | Description |\n" +
				"| --- | --- | --- | --- |\n" +
				"| id | INTEGER | REQUIRED |  |\n" +
				"| name | STRING | NULLABLE |  |\n",
		},
		"repeated field": {
			schema: bigquery.Schema{
				{Name: "tags", Type: bigquery.StringFieldType, Repeated: true},
			},
			expected: "| Name | Type | Mode | Description |\n" +
				"| --- | --- | --- | --- |\n" +
				"| tags | STRING | REPEATED |  |\n",
		},
		"field with description": {
			schema: bigquery.Schema{
				{Name: "id", Type: bigquery.IntegerFieldType, Description: "primary key"},
			},
			expected: "| Name | Type | Mode | Description |\n" +
				"| --- | --- | --- | --- |\n" +
				"| id | INTEGER | NULLABLE | primary key |\n",
		},
		"nested record field is indented": {
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
			expected: "| Name | Type | Mode | Description |\n" +
				"| --- | --- | --- | --- |\n" +
				"| address | RECORD | NULLABLE |  |\n" +
				"| &nbsp;&nbsp;city | STRING | NULLABLE |  |\n" +
				"| &nbsp;&nbsp;zip | STRING | NULLABLE |  |\n",
		},
	}

	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			got := createBigQuerySchemaMarkdownTable(tt.schema)
			if got != tt.expected {
				t.Errorf("createBigQuerySchemaMarkdownTable() =\n%q\nwant\n%q", got, tt.expected)
			}
		})
	}
}
