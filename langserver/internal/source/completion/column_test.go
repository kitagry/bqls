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
					},
				},
			},
			expectCompletionItems: []CompletionItem{
				{
					Kind:    lsp.CIKField,
					NewText: "id",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "INTEGER\nid description",
					},
				},
				{
					Kind:    lsp.CIKField,
					NewText: "name",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "STRING",
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
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "INT64",
					},
				},
				{ // TODO: Refactoring test
					Kind:    lsp.CIKField,
					NewText: "data",
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
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "INTEGER\nid description",
					},
					TypedPrefix: "i",
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
					Kind:          lsp.CIKField,
					NewText:       "data1",
					Documentation: lsp.MarkupContent{},
					TypedPrefix:   "d",
				},
				{
					Kind:          lsp.CIKField,
					NewText:       "data2",
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
					Kind:    lsp.CIKField,
					NewText: "table",
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
					Kind:        lsp.CIKField,
					NewText:     "data",
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
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "STRING\nname description",
					},
					TypedPrefix: "n",
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
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "INTEGER\nid description",
					},
					TypedPrefix: "",
				},
				{
					Kind:    lsp.CIKField,
					NewText: "id",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "INTEGER\nid description2",
					},
					TypedPrefix: "",
				},
				{
					Kind:    lsp.CIKField,
					NewText: "t1",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "project.dataset.table",
					},
					TypedPrefix: "",
				},
				{
					Kind:    lsp.CIKField,
					NewText: "t2",
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
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "INTEGER\nid description",
					},
					TypedPrefix: "",
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
			logger := logrus.New()
			logger.SetLevel(logrus.DebugLevel)

			analyzer := file.NewAnalyzer(logger, bqClient)
			completor := New(logger, analyzer, bqClient)

			files, path, position, err := helper.GetLspPosition(tt.files)
			if err != nil {
				t.Fatal(err)
			}

			parsedFile := analyzer.ParseFile(path, files[path])

			got := completor.completeColumns(context.Background(), parsedFile, position)
			if diff := cmp.Diff(got, tt.expectCompletionItems); diff != "" {
				t.Errorf("(-got, +want)\n%s", diff)
			}
		})
	}
}
