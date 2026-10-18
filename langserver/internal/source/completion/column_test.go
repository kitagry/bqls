package completion

import (
	"context"
	"strings"
	"testing"

	bq "cloud.google.com/go/bigquery"
	"github.com/golang/mock/gomock"
	"github.com/google/go-cmp/cmp"
	"github.com/kitagry/bqls/langserver/internal/bigquery/mock_bigquery"
	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/kitagry/bqls/langserver/internal/source/file"
	"github.com/kitagry/bqls/langserver/internal/source/helper"
	"github.com/sirupsen/logrus"
)

func TestProject_CompleteColumns(t *testing.T) {
	tests := map[string]struct {
		files              map[lsp.DocumentURI]string
		bqTableMetadataMap map[string]*bq.TableMetadata

		expectCompletionItems []CompletionItem
	}{
		"Select columns after SET statement": {
			files: map[lsp.DocumentURI]string{
				"file1.sql": "DECLARE x INT64;\nSET x = 1;\nIF x = 1 THEN SELECT 1; END IF;\nSELECT id, | FROM `project.dataset.table`",
			},
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {
					Schema: bq.Schema{
						{Name: "id", Type: bq.IntegerFieldType},
					},
				},
			},
			expectCompletionItems: []CompletionItem{
				{
					Kind:    lsp.CIKField,
					NewText: "id",
					Detail:  "INT64",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "INTEGER",
					},
				},
			},
		},
		"Select columns after statement with unfixable error": {
			files: map[lsp.DocumentURI]string{
				"file1.sql": "SELECT UNKNOWN_FUNC();\nSELECT | FROM `project.dataset.table`",
			},
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {
					Schema: bq.Schema{
						{Name: "id", Type: bq.IntegerFieldType},
					},
				},
			},
			expectCompletionItems: []CompletionItem{
				{
					Kind:    lsp.CIKField,
					NewText: "id",
					Detail:  "INT64",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "INTEGER",
					},
				},
			},
		},
		"Select columns between statement with unfixable error and following statement": {
			files: map[lsp.DocumentURI]string{
				"file1.sql": "SELECT UNKNOWN_FUNC();\nSELECT | FROM `project.dataset.table`;\nSELECT name FROM `project.dataset.table2`",
			},
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {
					Schema: bq.Schema{
						{Name: "id", Type: bq.IntegerFieldType},
					},
				},
				"project.dataset.table2": {
					Schema: bq.Schema{
						{Name: "name", Type: bq.StringFieldType},
					},
				},
			},
			expectCompletionItems: []CompletionItem{
				{
					Kind:    lsp.CIKField,
					NewText: "id",
					Detail:  "INT64",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "INTEGER",
					},
				},
			},
		},
		"Select columns after CREATE TEMP FUNCTION without RETURNS": {
			files: map[lsp.DocumentURI]string{
				"file1.sql": "CREATE TEMP FUNCTION f(x INT64) AS (x);\nSELECT | FROM `project.dataset.table`",
			},
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {
					Schema: bq.Schema{
						{Name: "id", Type: bq.IntegerFieldType},
					},
				},
			},
			expectCompletionItems: []CompletionItem{
				{
					Kind:    lsp.CIKField,
					NewText: "id",
					Detail:  "INT64",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "INTEGER",
					},
				},
			},
		},
		"Select columns in pipe WHERE": {
			files: map[lsp.DocumentURI]string{
				"file1.sql": "FROM `project.dataset.table` |> WHERE |",
			},
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {
					Schema: bq.Schema{
						{Name: "id", Type: bq.IntegerFieldType},
					},
				},
			},
			expectCompletionItems: []CompletionItem{
				{
					Kind:    lsp.CIKField,
					NewText: "id",
					Detail:  "INT64",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "INTEGER",
					},
				},
			},
		},
		"Select columns in pipe SELECT": {
			files: map[lsp.DocumentURI]string{
				"file1.sql": "FROM `project.dataset.table` |> SELECT |",
			},
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {
					Schema: bq.Schema{
						{Name: "id", Type: bq.IntegerFieldType},
					},
				},
			},
			expectCompletionItems: []CompletionItem{
				{
					Kind:    lsp.CIKField,
					NewText: "id",
					Detail:  "INT64",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "INTEGER",
					},
				},
			},
		},
		"Select columns in pipe ORDER BY": {
			files: map[lsp.DocumentURI]string{
				"file1.sql": "FROM `project.dataset.table` |> WHERE id = 1 |> ORDER BY |",
			},
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {
					Schema: bq.Schema{
						{Name: "id", Type: bq.IntegerFieldType},
					},
				},
			},
			expectCompletionItems: []CompletionItem{
				{
					Kind:    lsp.CIKField,
					NewText: "id",
					Detail:  "INT64",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "INTEGER",
					},
				},
			},
		},
		"Select columns in pipe AGGREGATE": {
			files: map[lsp.DocumentURI]string{
				"file1.sql": "FROM `project.dataset.table` |> AGGREGATE |",
			},
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {
					Schema: bq.Schema{
						{Name: "id", Type: bq.IntegerFieldType},
					},
				},
			},
			expectCompletionItems: []CompletionItem{
				{
					Kind:    lsp.CIKField,
					NewText: "id",
					Detail:  "INT64",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "INTEGER",
					},
				},
			},
		},
		"Select columns in pipe EXTEND": {
			files: map[lsp.DocumentURI]string{
				"file1.sql": "FROM `project.dataset.table` |> EXTEND |",
			},
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {
					Schema: bq.Schema{
						{Name: "id", Type: bq.IntegerFieldType},
					},
				},
			},
			expectCompletionItems: []CompletionItem{
				{
					Kind:    lsp.CIKField,
					NewText: "id",
					Detail:  "INT64",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "INTEGER",
					},
				},
			},
		},
		"Select columns with supportSunippet is true": {
			files: map[lsp.DocumentURI]string{
				"file1.sql": "SELECT id, | FROM `project.dataset.table`",
			},
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {
					Schema: bq.Schema{
						{
							Name:        "id",
							Type:        bq.IntegerFieldType,
							Description: "id description",
						},
						{
							Name: "name",
							Type: bq.StringFieldType,
						},
						{
							Name:     "packages",
							Type:     bq.RecordFieldType,
							Repeated: true,
							Schema: bq.Schema{
								{Name: "carrier", Type: bq.StringFieldType},
							},
						},
					},
				},
			},
			expectCompletionItems: []CompletionItem{
				{
					Kind:    lsp.CIKField,
					NewText: "id",
					Detail:  "INT64",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "INTEGER\nid description",
					},
				},
				{
					Kind:    lsp.CIKField,
					NewText: "name",
					Detail:  "STRING",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "STRING",
					},
				},
				{
					Kind:    lsp.CIKField,
					NewText: "packages",
					Detail:  "ARRAY<STRUCT>",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "RECORD",
					},
				},
			},
		},
		"When file cannot be parsed": {
			files: map[lsp.DocumentURI]string{
				"file1.sql": "SELECT | FROM `project.dataset.table`",
			},
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {
					Schema: bq.Schema{
						{
							Name:        "id",
							Type:        bq.IntegerFieldType,
							Description: "id description",
						},
					},
				},
			},
			expectCompletionItems: []CompletionItem{
				{
					Kind:    lsp.CIKField,
					NewText: "id",
					Detail:  "INT64",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "INTEGER\nid description",
					},
				},
			},
		},
		"Consider selectable table": {
			files: map[lsp.DocumentURI]string{
				"file1.sql": "SELECT * FROM `project.dataset.table`;\n" +
					"SELECT | FROM `project.dataset.table2`;",
			},
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {
					Schema: bq.Schema{
						{
							Name:        "id",
							Type:        bq.IntegerFieldType,
							Description: "id description",
						},
					},
				},
				"project.dataset.table2": {
					Schema: bq.Schema{
						{
							Name: "name",
							Type: bq.StringFieldType,
						},
					},
				},
			},
			expectCompletionItems: []CompletionItem{
				{
					Kind:    lsp.CIKField,
					NewText: "name",
					Detail:  "STRING",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "STRING",
					},
				},
			},
		},
		"Select WITH table": {
			files: map[lsp.DocumentURI]string{
				"file1.sql": "WITH data AS (SELECT id FROM `project.dataset.table`)\n" +
					"SELECT | FROM data;",
			},
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {
					Schema: bq.Schema{
						{
							Name:        "id",
							Type:        bq.IntegerFieldType,
							Description: "id description",
						},
					},
				},
			},
			expectCompletionItems: []CompletionItem{
				{
					Kind:    lsp.CIKField,
					NewText: "id",
					Detail:  "INT64",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "INT64",
					},
				},
				{ // TODO: Refactoring test
					Kind:    lsp.CIKModule,
					NewText: "data",
					Detail:  "CTE",
				},
			},
		},
		"Complete incomplete column": {
			files: map[lsp.DocumentURI]string{
				"file1.sql": "SELECT i| FROM `project.dataset.table`",
			},
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {
					Schema: bq.Schema{
						{
							Name:        "id",
							Type:        bq.IntegerFieldType,
							Description: "id description",
						},
					},
				},
			},
			expectCompletionItems: []CompletionItem{
				{
					Kind:    lsp.CIKField,
					NewText: "id",
					Detail:  "INT64",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "INTEGER\nid description",
					},
					TypedPrefix: "i",
				},
			},
		},
		"Complete incomplete column of dataset-qualified table": {
			files: map[lsp.DocumentURI]string{
				"file1.sql": "SELECT i| FROM dataset.table",
			},
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {
					Schema: bq.Schema{
						{
							Name:        "id",
							Type:        bq.IntegerFieldType,
							Description: "id description",
						},
					},
				},
			},
			expectCompletionItems: []CompletionItem{
				{
					Kind:    lsp.CIKField,
					NewText: "id",
					Detail:  "INT64",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "INTEGER\nid description",
					},
					TypedPrefix: "i",
				},
			},
		},
		"Complete column with alias of dataset-qualified table": {
			files: map[lsp.DocumentURI]string{
				"file1.sql": "SELECT t.| FROM dataset.table AS t",
			},
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {
					Schema: bq.Schema{
						{
							Name:        "id",
							Description: "id description",
							Type:        bq.IntegerFieldType,
						},
					},
				},
			},
			expectCompletionItems: []CompletionItem{
				{
					Kind:    lsp.CIKField,
					NewText: "id",
					Detail:  "INT64",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "INTEGER\nid description",
					},
				},
			},
		},
		"Complete incomplete column2": {
			files: map[lsp.DocumentURI]string{
				"file1.sql": "SELECT id, i| id FROM `project.dataset.table`",
			},
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {
					Schema: bq.Schema{
						{
							Name:        "id",
							Type:        bq.IntegerFieldType,
							Description: "id description",
						},
					},
				},
			},
			expectCompletionItems: []CompletionItem{
				{
					Kind:    lsp.CIKField,
					NewText: "id",
					Detail:  "INT64",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "INTEGER\nid description",
					},
					TypedPrefix: "i",
				},
			},
		},
		"Complete record column": {
			files: map[lsp.DocumentURI]string{
				"file1.sql": "SELECT record.| FROM `project.dataset.table`",
			},
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {
					Schema: bq.Schema{
						{
							Name: "record",
							Type: bq.RecordFieldType,
							Schema: bq.Schema{
								{
									Name:        "id",
									Description: "id description",
									Type:        bq.IntegerFieldType,
								},
							},
						},
					},
				},
			},
			expectCompletionItems: []CompletionItem{
				{
					Kind:    lsp.CIKField,
					NewText: "id",
					Detail:  "INT64",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "INTEGER\nid description",
					},
				},
			},
		},
		"Complete nested record column": {
			files: map[lsp.DocumentURI]string{
				"file1.sql": "SELECT record.record.| FROM `project.dataset.table`",
			},
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {
					Schema: bq.Schema{
						{
							Name: "record",
							Type: bq.RecordFieldType,
							Schema: bq.Schema{
								{
									Name: "record",
									Type: bq.RecordFieldType,
									Schema: bq.Schema{
										{
											Name:        "id",
											Description: "id description",
											Type:        bq.IntegerFieldType,
										},
									},
								},
							},
						},
					},
				},
			},
			expectCompletionItems: []CompletionItem{
				{
					Kind:    lsp.CIKField,
					NewText: "id",
					Detail:  "INT64",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "INTEGER\nid description",
					},
				},
			},
		},
		"Complete record column with incomplete word": {
			files: map[lsp.DocumentURI]string{
				"file1.sql": "SELECT record.i| FROM `project.dataset.table`",
			},
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {
					Schema: bq.Schema{
						{
							Name: "record",
							Type: bq.RecordFieldType,
							Schema: bq.Schema{
								{
									Name:        "id",
									Description: "id description",
									Type:        bq.IntegerFieldType,
								},
							},
						},
					},
				},
			},
			expectCompletionItems: []CompletionItem{
				{
					Kind:    lsp.CIKField,
					NewText: "id",
					Detail:  "INT64",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "INTEGER\nid description",
					},
					TypedPrefix: "i",
				},
			},
		},
		"Complete record column with incomplete word and zetasql odd error": {
			files: map[lsp.DocumentURI]string{
				"file1.sql": "SELECT record.id, record.| FROM `project.dataset.table`",
			},
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {
					Schema: bq.Schema{
						{
							Name: "record",
							Type: bq.RecordFieldType,
							Schema: bq.Schema{
								{
									Name:        "id",
									Description: "id description",
									Type:        bq.IntegerFieldType,
								},
							},
						},
					},
				},
			},
			expectCompletionItems: []CompletionItem{
				{
					Kind:    lsp.CIKField,
					NewText: "id",
					Detail:  "INT64",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "INTEGER\nid description",
					},
				},
			},
		},
		"Complete record column with WITH clause": {
			files: map[lsp.DocumentURI]string{
				"file1.sql": "WITH data AS (SELECT * FROM `project.dataset.table`)\n" + "SELECT record.| FROM data",
			},
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {
					Schema: bq.Schema{
						{
							Name: "record",
							Type: bq.RecordFieldType,
							Schema: bq.Schema{
								{
									Name:        "id",
									Description: "id description",
									Type:        bq.IntegerFieldType,
								},
							},
						},
					},
				},
			},
			expectCompletionItems: []CompletionItem{
				{
					Kind:    lsp.CIKField,
					NewText: "id",
					Detail:  "INT64",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "INT64",
					},
				},
			},
		},
		"Complete column with table alias": {
			files: map[lsp.DocumentURI]string{
				"file1.sql": "SELECT t.| FROM `project.dataset.table` AS t",
			},
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {
					Schema: bq.Schema{
						{
							Name:        "id",
							Description: "id description",
							Type:        bq.IntegerFieldType,
						},
					},
				},
			},
			expectCompletionItems: []CompletionItem{
				{
					Kind:    lsp.CIKField,
					NewText: "id",
					Detail:  "INT64",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "INTEGER\nid description",
					},
				},
			},
		},
		"Complete column with table alias with join": {
			files: map[lsp.DocumentURI]string{
				"file1.sql": "SELECT t1.| FROM `project.dataset.table` AS t1 JOIN `project.dataset.table` AS t2 ON t1.id = t2.id",
			},
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {
					Schema: bq.Schema{
						{
							Name:        "id",
							Description: "id description",
							Type:        bq.IntegerFieldType,
						},
					},
				},
			},
			expectCompletionItems: []CompletionItem{
				{
					Kind:    lsp.CIKField,
					NewText: "id",
					Detail:  "INT64",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "INTEGER\nid description",
					},
				},
			},
		},
		"Complete incomplete column with table alias with join": {
			files: map[lsp.DocumentURI]string{
				"file1.sql": "SELECT t1.i| FROM `project.dataset.table` AS t1 JOIN `project.dataset.table` AS t2 ON t1.id = t2.id WHERE t1.id = 1",
			},
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {
					Schema: bq.Schema{
						{
							Name:        "id",
							Description: "id description",
							Type:        bq.IntegerFieldType,
						},
					},
				},
			},
			expectCompletionItems: []CompletionItem{
				{
					Kind:    lsp.CIKField,
					NewText: "id",
					Detail:  "INT64",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "INTEGER\nid description",
					},
					TypedPrefix: "i",
				},
			},
		},
		"Complete column with with table": {
			files: map[lsp.DocumentURI]string{
				"file1.sql": "WITH data AS (SELECT * FROM `project.dataset.table`)\nSELECT data.| FROM data",
			},
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {
					Schema: bq.Schema{
						{
							Name:        "id",
							Description: "id description",
							Type:        bq.IntegerFieldType,
						},
					},
				},
			},
			expectCompletionItems: []CompletionItem{
				{
					Kind:    lsp.CIKField,
					NewText: "id",
					Detail:  "INT64",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "INT64",
					},
				},
			},
		},
		"Complete column with with table and join": {
			files: map[lsp.DocumentURI]string{
				"file1.sql": "WITH data1 AS (SELECT id AS hoge FROM `project.dataset.table`), data2 AS (SELECT id AS fuga FROM `project.dataset.table`)\nSELECT d| FROM data1 INNER JOIN data2 ON data1.hoge = data2.fuga",
			},
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {
					Schema: bq.Schema{
						{
							Name:        "id",
							Description: "id description",
							Type:        bq.IntegerFieldType,
						},
					},
				},
			},
			expectCompletionItems: []CompletionItem{
				{
					Kind:          lsp.CIKModule,
					NewText:       "data1",
					Detail:        "CTE",
					Documentation: lsp.MarkupContent{},
					TypedPrefix:   "d",
				},
				{
					Kind:          lsp.CIKModule,
					NewText:       "data2",
					Detail:        "CTE",
					Documentation: lsp.MarkupContent{},
					TypedPrefix:   "d",
				},
			},
		},
		"Complete table alias": {
			files: map[lsp.DocumentURI]string{
				"file1.sql": "SELECT t| FROM `project.dataset.table` AS table",
			},
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {
					Schema: bq.Schema{
						{
							Name: "id",
							Type: bq.IntegerFieldType,
						},
					},
				},
			},
			expectCompletionItems: []CompletionItem{
				{
					Kind:    lsp.CIKModule,
					NewText: "table",
					Detail:  "TABLE ALIAS",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "project.dataset.table",
					},
					TypedPrefix: "t",
				},
			},
		},
		"Complete with scan alias": {
			files: map[lsp.DocumentURI]string{
				"file1.sql": "WITH data AS (SELECT * FROM `project.dataset.table`)\nSELECT d| FROM data",
			},
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {
					Schema: bq.Schema{
						{
							Name: "id",
							Type: bq.IntegerFieldType,
						},
					},
				},
			},
			expectCompletionItems: []CompletionItem{
				{
					Kind:        lsp.CIKModule,
					NewText:     "data",
					Detail:      "CTE",
					TypedPrefix: "d",
				},
			},
		},
		"Complete column in where clause": {
			files: map[lsp.DocumentURI]string{
				"file1.sql": "SELECT * FROM `project.dataset.table` WHERE |",
			},
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {
					Schema: bq.Schema{
						{
							Name:        "id",
							Description: "id description",
							Type:        bq.IntegerFieldType,
						},
					},
				},
			},
			expectCompletionItems: []CompletionItem{
				{
					Kind:    lsp.CIKField,
					NewText: "id",
					Detail:  "INT64",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "INTEGER\nid description",
					},
				},
			},
		},
		"Complete column in where clause after AND": {
			files: map[lsp.DocumentURI]string{
				"file1.sql": "SELECT * FROM `project.dataset.table` WHERE id = 1 AND |",
			},
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {
					Schema: bq.Schema{
						{
							Name:        "id",
							Description: "id description",
							Type:        bq.IntegerFieldType,
						},
					},
				},
			},
			expectCompletionItems: []CompletionItem{
				{
					Kind:    lsp.CIKField,
					NewText: "id",
					Detail:  "INT64",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "INTEGER\nid description",
					},
				},
			},
		},
		"Complete column with table alias in where clause": {
			files: map[lsp.DocumentURI]string{
				"file1.sql": "SELECT * FROM `project.dataset.table` t WHERE t.|",
			},
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {
					Schema: bq.Schema{
						{
							Name:        "id",
							Description: "id description",
							Type:        bq.IntegerFieldType,
						},
					},
				},
			},
			expectCompletionItems: []CompletionItem{
				{
					Kind:    lsp.CIKField,
					NewText: "id",
					Detail:  "INT64",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "INTEGER\nid description",
					},
				},
			},
		},
		"Complete column in group by clause": {
			files: map[lsp.DocumentURI]string{
				"file1.sql": "SELECT * FROM `project.dataset.table` GROUP BY |",
			},
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {
					Schema: bq.Schema{
						{
							Name:        "id",
							Description: "id description",
							Type:        bq.IntegerFieldType,
						},
					},
				},
			},
			expectCompletionItems: []CompletionItem{
				{
					Kind:    lsp.CIKField,
					NewText: "id",
					Detail:  "INT64",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "INTEGER\nid description",
					},
				},
			},
		},
		"Complete incomplete column in group by clause": {
			files: map[lsp.DocumentURI]string{
				"file1.sql": "SELECT * FROM `project.dataset.table` GROUP BY i|",
			},
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {
					Schema: bq.Schema{
						{
							Name:        "id",
							Description: "id description",
							Type:        bq.IntegerFieldType,
						},
					},
				},
			},
			expectCompletionItems: []CompletionItem{
				{
					Kind:    lsp.CIKField,
					NewText: "id",
					Detail:  "INT64",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "INT64",
					},
					TypedPrefix: "i",
				},
			},
		},
		"Complete column in where clause with group by clause": {
			files: map[lsp.DocumentURI]string{
				"file1.sql": "SELECT id, COUNT(name) FROM `project.dataset.table` WHERE n| GROUP BY id",
			},
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {
					Schema: bq.Schema{
						{
							Name:        "id",
							Description: "id description",
							Type:        bq.IntegerFieldType,
						},
						{
							Name:        "name",
							Description: "name description",
							Type:        bq.StringFieldType,
						},
					},
				},
			},
			expectCompletionItems: []CompletionItem{
				{
					Kind:    lsp.CIKField,
					NewText: "name",
					Detail:  "STRING",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "STRING\nname description",
					},
					TypedPrefix: "n",
				},
			},
		},
		"Complete column in where clause while typing an existing column name": {
			files: map[lsp.DocumentURI]string{
				"file1.sql": "SELECT * FROM `project.dataset.table` WHERE id|",
			},
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {
					Schema: bq.Schema{
						{
							Name: "id",
							Type: bq.IntegerFieldType,
						},
						{
							Name: "id_alias",
							Type: bq.IntegerFieldType,
						},
					},
				},
			},
			expectCompletionItems: []CompletionItem{
				{
					Kind:    lsp.CIKField,
					NewText: "id",
					Detail:  "INT64",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "INTEGER",
					},
					TypedPrefix: "id",
				},
				{
					Kind:    lsp.CIKField,
					NewText: "id_alias",
					Detail:  "INT64",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "INTEGER",
					},
					TypedPrefix: "id",
				},
			},
		},
		"Complete column in where clause while typing a column name prefix at end of script": {
			files: map[lsp.DocumentURI]string{
				"file1.sql": "SELECT * FROM `project.dataset.table` WHERE i|",
			},
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {
					Schema: bq.Schema{
						{
							Name: "id",
							Type: bq.IntegerFieldType,
						},
						{
							Name: "id_alias",
							Type: bq.IntegerFieldType,
						},
					},
				},
			},
			expectCompletionItems: []CompletionItem{
				{
					Kind:    lsp.CIKField,
					NewText: "id",
					Detail:  "INT64",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "INTEGER",
					},
					TypedPrefix: "i",
				},
				{
					Kind:    lsp.CIKField,
					NewText: "id_alias",
					Detail:  "INT64",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "INTEGER",
					},
					TypedPrefix: "i",
				},
			},
		},
		"Complete column in where clause of a subquery compared by outer binary expression": {
			files: map[lsp.DocumentURI]string{
				"file1.sql": "SELECT * FROM `project.dataset.outer_table` WHERE id = (SELECT 1 FROM `project.dataset.table` WHERE i|)",
			},
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.outer_table": {
					Schema: bq.Schema{
						{
							Name: "id",
							Type: bq.IntegerFieldType,
						},
						{
							Name: "id_outer",
							Type: bq.IntegerFieldType,
						},
					},
				},
				"project.dataset.table": {
					Schema: bq.Schema{
						{
							Name: "id",
							Type: bq.IntegerFieldType,
						},
						{
							Name: "id_alias",
							Type: bq.IntegerFieldType,
						},
					},
				},
			},
			expectCompletionItems: []CompletionItem{
				{
					Kind:    lsp.CIKField,
					NewText: "id",
					Detail:  "INT64",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "INTEGER",
					},
					TypedPrefix: "i",
				},
				{
					Kind:    lsp.CIKField,
					NewText: "id_alias",
					Detail:  "INT64",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "INTEGER",
					},
					TypedPrefix: "i",
				},
			},
		},
		"Complete incomplete column in order by clause": {
			files: map[lsp.DocumentURI]string{
				"file1.sql": "SELECT * FROM `project.dataset.table` ORDER BY i|",
			},
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {
					Schema: bq.Schema{
						{
							Name:        "id",
							Description: "id description",
							Type:        bq.IntegerFieldType,
						},
					},
				},
			},
			expectCompletionItems: []CompletionItem{
				{
					Kind:    lsp.CIKField,
					NewText: "id",
					Detail:  "INT64",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "INTEGER\nid description",
					},
					TypedPrefix: "i",
				},
			},
		},
		"Complete column on join clause": {
			files: map[lsp.DocumentURI]string{
				"file1.sql": "SELECT * FROM `project.dataset.table` AS t1\nJOIN `project.dataset.table2` AS t2\nON |",
			},
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {
					Schema: bq.Schema{
						{
							Name:        "id",
							Description: "id description",
							Type:        bq.IntegerFieldType,
						},
					},
				},
				"project.dataset.table2": {
					Schema: bq.Schema{
						{
							Name:        "id",
							Description: "id description2",
							Type:        bq.IntegerFieldType,
						},
					},
				},
			},
			expectCompletionItems: []CompletionItem{
				{
					Kind:    lsp.CIKField,
					NewText: "id",
					Detail:  "INT64",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "INTEGER\nid description",
					},
					TypedPrefix: "",
				},
				{
					Kind:    lsp.CIKField,
					NewText: "id",
					Detail:  "INT64",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "INTEGER\nid description2",
					},
					TypedPrefix: "",
				},
				{
					Kind:    lsp.CIKModule,
					NewText: "t1",
					Detail:  "TABLE ALIAS",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "project.dataset.table",
					},
					TypedPrefix: "",
				},
				{
					Kind:    lsp.CIKModule,
					NewText: "t2",
					Detail:  "TABLE ALIAS",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "project.dataset.table2",
					},
					TypedPrefix: "",
				},
			},
		},
		"Complete column on limit clause": {
			files: map[lsp.DocumentURI]string{
				"file1.sql": "SELECT | FROM `project.dataset.table` LIMIT 10",
			},
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {
					Schema: bq.Schema{
						{
							Name:        "id",
							Description: "id description",
							Type:        bq.IntegerFieldType,
						},
					},
				},
			},
			expectCompletionItems: []CompletionItem{
				{
					Kind:    lsp.CIKField,
					NewText: "id",
					Detail:  "INT64",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "INTEGER\nid description",
					},
					TypedPrefix: "",
				},
			},
		},
		"Complete partition pseudo columns of daily ingestion-time partitioned table": {
			files: map[lsp.DocumentURI]string{
				"file1.sql": "SELECT | FROM `project.dataset.table`",
			},
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {
					Schema: bq.Schema{
						{Name: "id", Type: bq.IntegerFieldType},
					},
					TimePartitioning: &bq.TimePartitioning{Type: bq.DayPartitioningType},
				},
			},
			expectCompletionItems: []CompletionItem{
				{
					Kind:    lsp.CIKField,
					NewText: "id",
					Detail:  "INT64",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "INTEGER",
					},
				},
				{
					Kind:    lsp.CIKField,
					NewText: "_PARTITIONTIME",
					Detail:  "TIMESTAMP",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "TIMESTAMP",
					},
				},
				{
					Kind:    lsp.CIKField,
					NewText: "_PARTITIONDATE",
					Detail:  "DATE",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "DATE",
					},
				},
			},
		},
		"Complete only _PARTITIONTIME of hourly ingestion-time partitioned table": {
			files: map[lsp.DocumentURI]string{
				"file1.sql": "SELECT | FROM `project.dataset.table`",
			},
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {
					Schema: bq.Schema{
						{Name: "id", Type: bq.IntegerFieldType},
					},
					TimePartitioning: &bq.TimePartitioning{Type: bq.HourPartitioningType},
				},
			},
			expectCompletionItems: []CompletionItem{
				{
					Kind:    lsp.CIKField,
					NewText: "id",
					Detail:  "INT64",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "INTEGER",
					},
				},
				{
					Kind:    lsp.CIKField,
					NewText: "_PARTITIONTIME",
					Detail:  "TIMESTAMP",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "TIMESTAMP",
					},
				},
			},
		},
		"Complete partition pseudo columns of ingestion-time partitioned table without type": {
			files: map[lsp.DocumentURI]string{
				"file1.sql": "SELECT | FROM `project.dataset.table`",
			},
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {
					Schema: bq.Schema{
						{Name: "id", Type: bq.IntegerFieldType},
					},
					TimePartitioning: &bq.TimePartitioning{},
				},
			},
			expectCompletionItems: []CompletionItem{
				{
					Kind:    lsp.CIKField,
					NewText: "id",
					Detail:  "INT64",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "INTEGER",
					},
				},
				{
					Kind:    lsp.CIKField,
					NewText: "_PARTITIONTIME",
					Detail:  "TIMESTAMP",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "TIMESTAMP",
					},
				},
				{
					Kind:    lsp.CIKField,
					NewText: "_PARTITIONDATE",
					Detail:  "DATE",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "DATE",
					},
				},
			},
		},
		"Complete only _PARTITIONTIME of monthly ingestion-time partitioned table": {
			files: map[lsp.DocumentURI]string{
				"file1.sql": "SELECT | FROM `project.dataset.table`",
			},
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {
					Schema: bq.Schema{
						{Name: "id", Type: bq.IntegerFieldType},
					},
					TimePartitioning: &bq.TimePartitioning{Type: bq.MonthPartitioningType},
				},
			},
			expectCompletionItems: []CompletionItem{
				{
					Kind:    lsp.CIKField,
					NewText: "id",
					Detail:  "INT64",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "INTEGER",
					},
				},
				{
					Kind:    lsp.CIKField,
					NewText: "_PARTITIONTIME",
					Detail:  "TIMESTAMP",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "TIMESTAMP",
					},
				},
			},
		},
		"Do not complete partition pseudo columns of column-partitioned table": {
			files: map[lsp.DocumentURI]string{
				"file1.sql": "SELECT | FROM `project.dataset.table`",
			},
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {
					Schema: bq.Schema{
						{Name: "dt", Type: bq.DateFieldType},
					},
					TimePartitioning: &bq.TimePartitioning{Type: bq.DayPartitioningType, Field: "dt"},
				},
			},
			expectCompletionItems: []CompletionItem{
				{
					Kind:    lsp.CIKField,
					NewText: "dt",
					Detail:  "DATE",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "DATE",
					},
				},
			},
		},
		"Complete incomplete partition pseudo column": {
			files: map[lsp.DocumentURI]string{
				"file1.sql": "SELECT _PART| FROM `project.dataset.table`",
			},
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {
					Schema: bq.Schema{
						{Name: "id", Type: bq.IntegerFieldType},
					},
					TimePartitioning: &bq.TimePartitioning{Type: bq.DayPartitioningType},
				},
			},
			expectCompletionItems: []CompletionItem{
				{
					Kind:    lsp.CIKField,
					NewText: "_PARTITIONTIME",
					Detail:  "TIMESTAMP",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "TIMESTAMP",
					},
					TypedPrefix: "_PART",
				},
				{
					Kind:    lsp.CIKField,
					NewText: "_PARTITIONDATE",
					Detail:  "DATE",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "DATE",
					},
					TypedPrefix: "_PART",
				},
			},
		},
		"Complete partition pseudo columns with table alias": {
			files: map[lsp.DocumentURI]string{
				"file1.sql": "SELECT t.| FROM `project.dataset.table` AS t",
			},
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {
					Schema: bq.Schema{
						{Name: "id", Type: bq.IntegerFieldType},
					},
					TimePartitioning: &bq.TimePartitioning{Type: bq.DayPartitioningType},
				},
			},
			expectCompletionItems: []CompletionItem{
				{
					Kind:    lsp.CIKField,
					NewText: "id",
					Detail:  "INT64",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "INTEGER",
					},
				},
				{
					Kind:    lsp.CIKField,
					NewText: "_PARTITIONTIME",
					Detail:  "TIMESTAMP",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "TIMESTAMP",
					},
				},
				{
					Kind:    lsp.CIKField,
					NewText: "_PARTITIONDATE",
					Detail:  "DATE",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "DATE",
					},
				},
			},
		},
		"Complete partition pseudo columns in where clause": {
			files: map[lsp.DocumentURI]string{
				"file1.sql": "SELECT id FROM `project.dataset.table` WHERE |",
			},
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {
					Schema: bq.Schema{
						{Name: "id", Type: bq.IntegerFieldType},
					},
					TimePartitioning: &bq.TimePartitioning{Type: bq.DayPartitioningType},
				},
			},
			expectCompletionItems: []CompletionItem{
				{
					Kind:    lsp.CIKField,
					NewText: "id",
					Detail:  "INT64",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "INTEGER",
					},
				},
				{
					Kind:    lsp.CIKField,
					NewText: "_PARTITIONTIME",
					Detail:  "TIMESTAMP",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "TIMESTAMP",
					},
				},
				{
					Kind:    lsp.CIKField,
					NewText: "_PARTITIONDATE",
					Detail:  "DATE",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "DATE",
					},
				},
			},
		},
		"Complete _TABLE_SUFFIX of wildcard table": {
			files: map[lsp.DocumentURI]string{
				"file1.sql": "SELECT | FROM `project.dataset.table_*`",
			},
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table_*": {
					Schema: bq.Schema{
						{Name: "id", Type: bq.IntegerFieldType},
					},
				},
			},
			expectCompletionItems: []CompletionItem{
				{
					Kind:    lsp.CIKField,
					NewText: "id",
					Detail:  "INT64",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "INTEGER",
					},
				},
				{
					Kind:    lsp.CIKField,
					NewText: "_TABLE_SUFFIX",
					Detail:  "STRING",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "STRING",
					},
				},
			},
		},
		"Complete incomplete _TABLE_SUFFIX of wildcard table": {
			files: map[lsp.DocumentURI]string{
				"file1.sql": "SELECT _TABLE| FROM `project.dataset.table_*`",
			},
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table_*": {
					Schema: bq.Schema{
						{Name: "id", Type: bq.IntegerFieldType},
					},
				},
			},
			expectCompletionItems: []CompletionItem{
				{
					Kind:    lsp.CIKField,
					NewText: "_TABLE_SUFFIX",
					Detail:  "STRING",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "STRING",
					},
					TypedPrefix: "_TABLE",
				},
			},
		},
		"Complete incomplete column case-insensitively": {
			files: map[lsp.DocumentURI]string{
				"file1.sql": "SELECT I| FROM `project.dataset.table`",
			},
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {
					Schema: bq.Schema{
						{Name: "id", Type: bq.IntegerFieldType},
						{Name: "name", Type: bq.StringFieldType},
					},
				},
			},
			expectCompletionItems: []CompletionItem{
				{
					Kind:    lsp.CIKField,
					NewText: "id",
					Detail:  "INT64",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "INTEGER",
					},
					TypedPrefix: "I",
				},
			},
		},
		"Complete incomplete partition pseudo column case-insensitively": {
			files: map[lsp.DocumentURI]string{
				"file1.sql": "SELECT _part| FROM `project.dataset.table`",
			},
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {
					Schema: bq.Schema{
						{Name: "id", Type: bq.IntegerFieldType},
					},
					TimePartitioning: &bq.TimePartitioning{Type: bq.DayPartitioningType},
				},
			},
			expectCompletionItems: []CompletionItem{
				{
					Kind:    lsp.CIKField,
					NewText: "_PARTITIONTIME",
					Detail:  "TIMESTAMP",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "TIMESTAMP",
					},
					TypedPrefix: "_part",
				},
				{
					Kind:    lsp.CIKField,
					NewText: "_PARTITIONDATE",
					Detail:  "DATE",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "DATE",
					},
					TypedPrefix: "_part",
				},
			},
		},
		"Complete incomplete column with table alias case-insensitively": {
			files: map[lsp.DocumentURI]string{
				"file1.sql": "SELECT t.I| FROM `project.dataset.table` AS t",
			},
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {
					Schema: bq.Schema{
						{Name: "id", Type: bq.IntegerFieldType},
						{Name: "name", Type: bq.StringFieldType},
					},
				},
			},
			expectCompletionItems: []CompletionItem{
				{
					Kind:    lsp.CIKField,
					NewText: "id",
					Detail:  "INT64",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "INTEGER",
					},
					TypedPrefix: "I",
				},
			},
		},
		"Complete incomplete record column case-insensitively": {
			files: map[lsp.DocumentURI]string{
				"file1.sql": "SELECT record.I| FROM `project.dataset.table`",
			},
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {
					Schema: bq.Schema{
						{
							Name: "record",
							Type: bq.RecordFieldType,
							Schema: bq.Schema{
								{Name: "id", Type: bq.IntegerFieldType},
								{Name: "name", Type: bq.StringFieldType},
							},
						},
					},
				},
			},
			expectCompletionItems: []CompletionItem{
				{
					Kind:    lsp.CIKField,
					NewText: "id",
					Detail:  "INT64",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "INTEGER",
					},
					TypedPrefix: "I",
				},
			},
		},
		"Complete table alias case-insensitively": {
			files: map[lsp.DocumentURI]string{
				"file1.sql": "SELECT T| FROM `project.dataset.table` AS table",
			},
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {
					Schema: bq.Schema{
						{Name: "id", Type: bq.IntegerFieldType},
					},
				},
			},
			expectCompletionItems: []CompletionItem{
				{
					Kind:    lsp.CIKModule,
					NewText: "table",
					Detail:  "TABLE ALIAS",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "project.dataset.table",
					},
					TypedPrefix: "T",
				},
			},
		},
		"Complete with scan alias case-insensitively": {
			files: map[lsp.DocumentURI]string{
				"file1.sql": "WITH data AS (SELECT * FROM `project.dataset.table`)\nSELECT D| FROM data",
			},
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {
					Schema: bq.Schema{
						{Name: "id", Type: bq.IntegerFieldType},
					},
				},
			},
			expectCompletionItems: []CompletionItem{
				{
					Kind:        lsp.CIKModule,
					NewText:     "data",
					Detail:      "CTE",
					TypedPrefix: "D",
				},
			},
		},
		"Complete column with table alias qualifier case-insensitively": {
			files: map[lsp.DocumentURI]string{
				"file1.sql": "SELECT T.I| FROM `project.dataset.table` AS t",
			},
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {
					Schema: bq.Schema{
						{Name: "id", Type: bq.IntegerFieldType},
						{Name: "name", Type: bq.StringFieldType},
					},
				},
			},
			expectCompletionItems: []CompletionItem{
				{
					Kind:    lsp.CIKField,
					NewText: "id",
					Detail:  "INT64",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "INTEGER",
					},
					TypedPrefix: "I",
				},
			},
		},
		"Complete column with with table qualifier case-insensitively": {
			files: map[lsp.DocumentURI]string{
				"file1.sql": "WITH data AS (SELECT * FROM `project.dataset.table`)\nSELECT DATA.I| FROM data",
			},
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {
					Schema: bq.Schema{
						{Name: "id", Type: bq.IntegerFieldType},
						{Name: "name", Type: bq.StringFieldType},
					},
				},
			},
			expectCompletionItems: []CompletionItem{
				{
					Kind:    lsp.CIKField,
					NewText: "id",
					Detail:  "INT64",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "INT64",
					},
					TypedPrefix: "I",
				},
			},
		},
		"Complete nested record column with record qualifier case-insensitively": {
			files: map[lsp.DocumentURI]string{
				"file1.sql": "SELECT RECORD.Record.| FROM `project.dataset.table`",
			},
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {
					Schema: bq.Schema{
						{
							Name: "record",
							Type: bq.RecordFieldType,
							Schema: bq.Schema{
								{
									Name: "record",
									Type: bq.RecordFieldType,
									Schema: bq.Schema{
										{Name: "id", Type: bq.IntegerFieldType},
									},
								},
							},
						},
					},
				},
			},
			expectCompletionItems: []CompletionItem{
				{
					Kind:    lsp.CIKField,
					NewText: "id",
					Detail:  "INT64",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "INTEGER",
					},
				},
			},
		},
		"Complete nested record column of with table with record qualifier case-insensitively": {
			files: map[lsp.DocumentURI]string{
				"file1.sql": "WITH data AS (SELECT * FROM `project.dataset.table`)\nSELECT RECORD.Record.| FROM data",
			},
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {
					Schema: bq.Schema{
						{
							Name: "record",
							Type: bq.RecordFieldType,
							Schema: bq.Schema{
								{
									Name: "record",
									Type: bq.RecordFieldType,
									Schema: bq.Schema{
										{Name: "id", Type: bq.IntegerFieldType},
									},
								},
							},
						},
					},
				},
			},
			expectCompletionItems: []CompletionItem{
				{
					Kind:    lsp.CIKField,
					NewText: "id",
					Detail:  "INT64",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "INT64",
					},
				},
			},
		},
	}

	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			bqClient := mock_bigquery.NewMockClient(ctrl)
			for tablePath, schema := range tt.bqTableMetadataMap {
				tablePathSplitted := strings.Split(tablePath, ".")
				if len(tablePathSplitted) != 3 {
					t.Fatalf("table path length should be 3, got %s", tablePath)
				}
				bqClient.EXPECT().GetTableMetadata(gomock.Any(), tablePathSplitted[0], tablePathSplitted[1], tablePathSplitted[2]).Return(schema, nil).MinTimes(0)
			}
			bqClient.EXPECT().ListTables(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).MinTimes(0)
			bqClient.EXPECT().GetDefaultProject().Return("project").AnyTimes()
			logger := logrus.New()
			logger.SetLevel(logrus.DebugLevel)

			analyzer := file.NewAnalyzer(logger, bqClient)
			completor := New(logger, analyzer, bqClient)

			files, path, position, err := helper.GetLspPosition(tt.files)
			if err != nil {
				t.Fatal(err)
			}

			parsedFile := analyzer.ParseFile(path, files[path])
			defer parsedFile.Close()

			got := completor.completeColumns(context.Background(), parsedFile, position)
			if diff := cmp.Diff(got, tt.expectCompletionItems); diff != "" {
				t.Errorf("(-got, +want)\n%s", diff)
			}
		})
	}
}
