package source_test

import (
	"errors"
	"testing"
	"time"

	bq "cloud.google.com/go/bigquery"
	"github.com/golang/mock/gomock"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/kitagry/bqls/langserver/internal/bigquery/mock_bigquery"
	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/kitagry/bqls/langserver/internal/source"
	"github.com/kitagry/bqls/langserver/internal/source/helper"
	"github.com/sirupsen/logrus"
)

func TestProject_TermDocument(t *testing.T) {
	tests := map[string]struct {
		// prepare
		files           map[string]string
		bqTableMetadata *bq.TableMetadata

		// output
		expectMarkedStrings []lsp.MarkedString
		expectErr           error
	}{
		"hover table": {
			files: map[string]string{
				"file1.sql": "SELECT * FROM |`project.dataset.table`",
			},
			bqTableMetadata: &bq.TableMetadata{
				FullID:           "project.dataset.table",
				Description:      "table description",
				CreationTime:     time.Date(2023, 6, 17, 0, 0, 0, 0, time.UTC),
				LastModifiedTime: time.Date(2023, 6, 17, 0, 0, 0, 0, time.UTC),
				Schema: bq.Schema{
					{
						Name:        "name",
						Type:        bq.StringFieldType,
						Description: "name description",
					},
				},
			},
			expectMarkedStrings: []lsp.MarkedString{
				{
					Language: "markdown",
					Value: `## project.dataset.table
table description
created at 2023-06-17 00:00:00
last modified at 2023-06-17 00:00:00`,
				},
				{
					Language: "yaml",
					Value: `- name: name
  type: STRING
  description: name description
`,
				},
			},
		},
		"hover joined table": {
			files: map[string]string{
				"file1.sql": "SELECT * FROM `project.dataset.table` table1 JOIN |`project.dataset.table` table2 ON table1.name = table2.name",
			},
			bqTableMetadata: &bq.TableMetadata{
				FullID:           "project.dataset.table",
				CreationTime:     time.Date(2023, 6, 17, 0, 0, 0, 0, time.UTC),
				LastModifiedTime: time.Date(2023, 6, 17, 0, 0, 0, 0, time.UTC),
				Schema: bq.Schema{
					{
						Name:        "name",
						Type:        bq.StringFieldType,
						Description: "name description",
					},
				},
			},
			expectMarkedStrings: []lsp.MarkedString{
				{
					Language: "markdown",
					Value: `## project.dataset.table
created at 2023-06-17 00:00:00
last modified at 2023-06-17 00:00:00`,
				},
				{
					Language: "yaml",
					Value: `- name: name
  type: STRING
  description: name description
`,
				},
			},
		},
		"hover column": {
			files: map[string]string{
				"file1.sql": "SELECT id, |name FROM `project.dataset.table`",
			},
			bqTableMetadata: &bq.TableMetadata{
				FullID:           "project.dataset.table",
				CreationTime:     time.Date(2023, 6, 17, 0, 0, 0, 0, time.UTC),
				LastModifiedTime: time.Date(2023, 6, 17, 0, 0, 0, 0, time.UTC),
				Schema: bq.Schema{
					{
						Name:        "id",
						Type:        bq.IntegerFieldType,
						Description: "id description",
					},
					{
						Name:        "name",
						Type:        bq.StringFieldType,
						Description: "name description",
					},
				},
			},
			expectMarkedStrings: []lsp.MarkedString{
				{
					Language: "yaml",
					Value: `- name: name
  type: STRING
  description: name description
`,
				},
			},
		},
		"hover column with alias": {
			files: map[string]string{
				"file1.sql": "SELECT |name AS alias_name FROM `project.dataset.table`",
			},
			bqTableMetadata: &bq.TableMetadata{
				FullID:           "project.dataset.table",
				CreationTime:     time.Date(2023, 6, 17, 0, 0, 0, 0, time.UTC),
				LastModifiedTime: time.Date(2023, 6, 17, 0, 0, 0, 0, time.UTC),
				Schema: bq.Schema{
					{
						Name:        "name",
						Type:        bq.StringFieldType,
						Description: "name description",
					},
				},
			},
			expectMarkedStrings: []lsp.MarkedString{
				{
					Language: "yaml",
					Value: `- name: name
  type: STRING
  description: name description
`,
				},
			},
		},
		"hover column with table alias": {
			files: map[string]string{
				"file1.sql": "SELECT a.|name FROM `project.dataset.table` AS a",
			},
			bqTableMetadata: &bq.TableMetadata{
				FullID: "project.dataset.table",
				Schema: bq.Schema{
					{
						Name:        "name",
						Type:        bq.StringFieldType,
						Description: "name description",
					},
				},
			},
			expectMarkedStrings: []lsp.MarkedString{
				{
					Language: "yaml",
					Value: `- name: name
  type: STRING
  description: name description
`,
				},
			},
		},
		"hover column unnest record": {
			files: map[string]string{
				"file1.sql": "SELECT param.|key FROM `project.dataset.table`, UNNEST(params) AS param",
			},
			bqTableMetadata: &bq.TableMetadata{
				FullID: "project.dataset.table",
				Schema: bq.Schema{
					{
						Name:     "params",
						Type:     bq.RecordFieldType,
						Repeated: true,
						Schema: bq.Schema{
							{
								Name: "key",
								Type: bq.StringFieldType,
							},
							{
								Name: "value",
								Type: bq.StringFieldType,
							},
						},
					},
				},
			},
			expectMarkedStrings: []lsp.MarkedString{
				{
					Language: "markdown",
					Value:    "STRING",
				},
			},
		},
		"hover column in where clouse": {
			files: map[string]string{
				"file1.sql": "SELECT name AS alias_name FROM `project.dataset.table` WHERE |name = 'test'",
			},
			bqTableMetadata: &bq.TableMetadata{
				FullID:           "project.dataset.table",
				CreationTime:     time.Date(2023, 6, 17, 0, 0, 0, 0, time.UTC),
				LastModifiedTime: time.Date(2023, 6, 17, 0, 0, 0, 0, time.UTC),
				Schema: bq.Schema{
					{
						Name:        "name",
						Type:        bq.StringFieldType,
						Description: "name description",
					},
				},
			},
			expectMarkedStrings: []lsp.MarkedString{
				{
					Language: "yaml",
					Value: `- name: name
  type: STRING
  description: name description
`,
				},
			},
		},
		"hover unnest table": {
			files: map[string]string{
				"file1.sql": "SELECT param.key FROM `project.dataset.table`, UNNEST(|params) AS param",
			},
			bqTableMetadata: &bq.TableMetadata{
				FullID: "project.dataset.table",
				Schema: bq.Schema{
					{
						Name:        "params",
						Description: "params description",
						Type:        bq.RecordFieldType,
						Repeated:    true,
						Schema: bq.Schema{
							{
								Name: "key",
								Type: bq.StringFieldType,
							},
							{
								Name: "value",
								Type: bq.StringFieldType,
							},
						},
					},
				},
			},
			expectMarkedStrings: []lsp.MarkedString{
				{
					Language: "yaml",
					Value: `- name: params
  type: RECORD
  mode: REPEATED
  description: params description
  - name: key
    type: STRING
  - name: value
    type: STRING
`,
				},
			},
		},
		"hover function call": {
			files: map[string]string{
				"file1.sql": "SELECT |JSON_VALUE(json, '$.name') FROM `project.dataset.table`",
			},
			bqTableMetadata: &bq.TableMetadata{
				FullID: "project.dataset.table",
				Schema: bq.Schema{
					{
						Name: "json",
						Type: bq.JSONFieldType,
					},
				},
			},
			expectMarkedStrings: []lsp.MarkedString{
				{
					Language: "markdown",
					Value:    "Extracts a JSON scalar value and converts it to a SQL `STRING` value.\nIn addition, this function:Removes the outermost quotes and unescapes the values.\nReturns a SQL `NULL` if a non-scalar value is selected.\nUses double quotes to escape invalid [JSONPath](#JSONPath_format) characters\nin JSON keys. For example: `\"a.b\"`.Arguments:`json_string_expr`: A JSON-formatted string. For example:\n\n```\n'{\"class\": {\"students\": [{\"name\": \"Jane\"}]}}'\n\n```\n\n`json_expr`: JSON. For example:\n\n```\nJSON '{\"class\": {\"students\": [{\"name\": \"Jane\"}]}}'\n\n```\n\n`json_path`: The [JSONPath](#JSONPath_format). This identifies the data that\nyou want to obtain from the input. If this optional parameter is not\nprovided, then the JSONPath `$` symbol is applied, which means that all of\nthe data is analyzed.\n\nIf `json_path` returns a JSON `null` or a non-scalar value (in other words,\nif `json_path` refers to an object or an array), then a SQL `NULL` is\nreturned.There are differences between the JSON-formatted string and JSON input types.\nFor details, see [Differences between the JSON and JSON-formatted STRING types](#differences_json_and_string).\n\n[bigquery documentation](https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#json_value)",
				},
				{
					Language: "sql",
					Value:    "SELECT JSON_VALUE(JSON '{\"name\": \"Jakob\", \"age\": \"6\" }', '$.age') AS scalar_age;\n\n/*------------*\n | scalar_age |\n +------------+\n | 6          |\n *------------*/",
				},
				{
					Language: "sql",
					Value:    "SELECT JSON_QUERY('{\"name\": \"Jakob\", \"age\": \"6\"}', '$.name') AS json_name,\n  JSON_VALUE('{\"name\": \"Jakob\", \"age\": \"6\"}', '$.name') AS scalar_name,\n  JSON_QUERY('{\"name\": \"Jakob\", \"age\": \"6\"}', '$.age') AS json_age,\n  JSON_VALUE('{\"name\": \"Jakob\", \"age\": \"6\"}', '$.age') AS scalar_age;\n\n/*-----------+-------------+----------+------------*\n | json_name | scalar_name | json_age | scalar_age |\n +-----------+-------------+----------+------------+\n | \"Jakob\"   | Jakob       | \"6\"      | 6          |\n *-----------+-------------+----------+------------*/",
				},
				{
					Language: "sql",
					Value:    "SELECT JSON_QUERY('{\"fruits\": [\"apple\", \"banana\"]}', '$.fruits') AS json_query,\n  JSON_VALUE('{\"fruits\": [\"apple\", \"banana\"]}', '$.fruits') AS json_value;\n\n/*--------------------+------------*\n | json_query         | json_value |\n +--------------------+------------+\n | [\"apple\",\"banana\"] | NULL       |\n *--------------------+------------*/",
				},
				{
					Language: "sql",
					Value:    "SELECT JSON_VALUE('{\"a.b\": {\"c\": \"world\"}}', '$.\"a.b\".c') AS hello;\n\n/*-------*\n | hello |\n +-------+\n | world |\n *-------*/",
				},
			},
		},
		"hover function argument": {
			files: map[string]string{
				"file1.sql": "SELECT JSON_VALUE(|json, '$.name') FROM `project.dataset.table`",
			},
			bqTableMetadata: &bq.TableMetadata{
				FullID: "project.dataset.table",
				Schema: bq.Schema{
					{
						Name:        "json",
						Description: "json description",
						Type:        bq.JSONFieldType,
					},
				},
			},
			expectMarkedStrings: []lsp.MarkedString{
				{
					Language: "yaml",
					Value: `- name: json
  type: JSON
  description: json description
`,
				},
			},
		},
		"hover with WITH clause": {
			files: map[string]string{
				"file1.sql": "WITH data AS (SELECT id FROM `project.dataset.table`)\nSELECT id| FROM data",
			},
			bqTableMetadata: &bq.TableMetadata{
				FullID: "project.dataset.table",
				Schema: bq.Schema{
					{
						Name: "id",
						Type: bq.IntegerFieldType,
					},
				},
			},
			expectMarkedStrings: []lsp.MarkedString{
				{
					Language: "yaml",
					Value: `- name: id
  type: INT64
`,
				},
			},
		},
		"hover in WITH clause": {
			files: map[string]string{
				"file1.sql": "WITH data AS (SELECT id| FROM `project.dataset.table`)\nSELECT id FROM data",
			},
			bqTableMetadata: &bq.TableMetadata{
				FullID: "project.dataset.table",
				Schema: bq.Schema{
					{
						Name:        "id",
						Type:        bq.IntegerFieldType,
						Description: "id description",
					},
				},
			},
			expectMarkedStrings: []lsp.MarkedString{
				{
					Language: "yaml",
					Value: `- name: id
  type: INTEGER
  description: id description
`,
				},
			},
		},
		"hover WITH clause reference name": {
			files: map[string]string{
				"file1.sql": "WITH data AS (SELECT id AS hoge FROM `project.dataset.table`)\nSELECT * FROM data|",
			},
			bqTableMetadata: &bq.TableMetadata{
				FullID: "project.dataset.table",
				Schema: bq.Schema{
					{
						Name:        "id",
						Type:        bq.IntegerFieldType,
						Description: "id description",
					},
				},
			},
			expectMarkedStrings: []lsp.MarkedString{
				{
					Language: "yaml",
					Value: `- name: hoge
  type: INT64
`,
				},
				{
					Language: "sql",
					Value:    "WITH data AS (\nSELECT id AS hoge FROM `project.dataset.table`\n)",
				},
			},
		},
	}

	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			bqClient := mock_bigquery.NewMockClient(ctrl)
			bqClient.EXPECT().GetTableMetadata(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(tt.bqTableMetadata, nil).MinTimes(0)
			logger := logrus.New()
			logger.SetLevel(logrus.DebugLevel)
			p := source.NewProjectWithBQClient("/", bqClient, logger)

			files, path, position, err := helper.GetLspPosition(tt.files)
			if err != nil {
				t.Fatalf("failed to get position: %v", err)
			}

			for uri, content := range files {
				p.UpdateFile(uri, content, 1)
			}

			got, err := p.TermDocument(path, position)
			if !errors.Is(err, tt.expectErr) {
				t.Fatalf("got error %v, but want %v", err, tt.expectErr)
			}

			if diff := cmp.Diff(tt.expectMarkedStrings, got, cmpopts.IgnoreUnexported(lsp.MarkedString{})); diff != "" {
				t.Errorf("project.TermDocument result diff (-expect, +got)\n%s", diff)
			}
		})
	}
}
