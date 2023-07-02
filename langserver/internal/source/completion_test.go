package source_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	bq "cloud.google.com/go/bigquery"
	"github.com/golang/mock/gomock"
	"github.com/google/go-cmp/cmp"
	"github.com/kitagry/bqls/langserver/internal/bigquery"
	"github.com/kitagry/bqls/langserver/internal/bigquery/mock_bigquery"
	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/kitagry/bqls/langserver/internal/source"
	"github.com/kitagry/bqls/langserver/internal/source/helper"
	"github.com/sirupsen/logrus"
	"google.golang.org/api/cloudresourcemanager/v1"
)

func TestProject_CompleteColumn(t *testing.T) {
	tests := map[string]struct {
		files              map[string]string
		bqTableMetadataMap map[string]*bq.TableMetadata

		expectCompletionItems []source.CompletionItem
		expectErr             error
	}{
		"Select columns with supportSunippet is true": {
			files: map[string]string{
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
			expectCompletionItems: []source.CompletionItem{
				{
					Kind:    lsp.CIKField,
					NewText: "id",
					Detail:  "INTEGER\nid description",
				},
				{
					Kind:    lsp.CIKField,
					NewText: "name",
					Detail:  "STRING",
				},
			},
		},
		"When file cannot be parsed": {
			files: map[string]string{
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
			expectCompletionItems: []source.CompletionItem{
				{
					Kind:    lsp.CIKField,
					NewText: "id",
					Detail:  "INTEGER\nid description",
				},
			},
		},
		"Consider selectable table": {
			files: map[string]string{
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
			expectCompletionItems: []source.CompletionItem{
				{
					Kind:    lsp.CIKField,
					NewText: "name",
					Detail:  "STRING",
				},
			},
		},
		"Select WITH table": {
			files: map[string]string{
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
			expectCompletionItems: []source.CompletionItem{
				{
					Kind:    lsp.CIKField,
					NewText: "id",
					Detail:  "INT64",
				},
				{ // TODO: Refactoring test
					Kind:    lsp.CIKField,
					NewText: "data",
				},
			},
		},
		"Complete incomplete column": {
			files: map[string]string{
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
			expectCompletionItems: []source.CompletionItem{
				{
					Kind:        lsp.CIKField,
					NewText:     "id",
					Detail:      "INTEGER\nid description",
					TypedPrefix: "i",
				},
			},
		},
		"Complete incomplete column2": {
			files: map[string]string{
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
			expectCompletionItems: []source.CompletionItem{
				{
					Kind:        lsp.CIKField,
					NewText:     "id",
					Detail:      "INTEGER\nid description",
					TypedPrefix: "i",
				},
			},
		},
		"Complete record column": {
			files: map[string]string{
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
			expectCompletionItems: []source.CompletionItem{
				{
					Kind:    lsp.CIKField,
					NewText: "id",
					Detail:  "INTEGER\nid description",
				},
			},
		},
		"Complete record column with incomplete word": {
			files: map[string]string{
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
			expectCompletionItems: []source.CompletionItem{
				{
					Kind:        lsp.CIKField,
					NewText:     "id",
					Detail:      "INTEGER\nid description",
					TypedPrefix: "i",
				},
			},
		},
		"Complete record column with incomplete word and zetasql odd error": {
			files: map[string]string{
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
			expectCompletionItems: []source.CompletionItem{
				{
					Kind:    lsp.CIKField,
					NewText: "id",
					Detail:  "INTEGER\nid description",
				},
			},
		},
		"Complete record column with WITH clause": {
			files: map[string]string{
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
			expectCompletionItems: []source.CompletionItem{
				{
					Kind:    lsp.CIKField,
					NewText: "id",
					Detail:  "INT64",
				},
			},
		},
		"Complete column with table alias": {
			files: map[string]string{
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
			expectCompletionItems: []source.CompletionItem{
				{
					Kind:    lsp.CIKField,
					NewText: "id",
					Detail:  "INTEGER\nid description",
				},
			},
		},
		"Complete column with table alias with join": {
			files: map[string]string{
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
			expectCompletionItems: []source.CompletionItem{
				{
					Kind:    lsp.CIKField,
					NewText: "id",
					Detail:  "INTEGER\nid description",
				},
			},
		},
		"Complete incomplete column with table alias with join": {
			files: map[string]string{
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
			expectCompletionItems: []source.CompletionItem{
				{
					Kind:        lsp.CIKField,
					NewText:     "id",
					Detail:      "INTEGER\nid description",
					TypedPrefix: "i",
				},
			},
		},
		"Complete column with with table": {
			files: map[string]string{
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
			expectCompletionItems: []source.CompletionItem{
				{
					Kind:    lsp.CIKField,
					NewText: "id",
					Detail:  "INT64",
				},
			},
		},
		"Complete table alias": {
			files: map[string]string{
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
			expectCompletionItems: []source.CompletionItem{
				{
					Kind:        lsp.CIKField,
					NewText:     "table",
					Detail:      "project.dataset.table",
					TypedPrefix: "t",
				},
			},
		},
		"Complete with scan alias": {
			files: map[string]string{
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
			expectCompletionItems: []source.CompletionItem{
				{
					Kind:        lsp.CIKField,
					NewText:     "data",
					TypedPrefix: "d",
				},
			},
		},
		"Complete column in where clause": {
			files: map[string]string{
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
			expectCompletionItems: []source.CompletionItem{
				{
					Kind:    lsp.CIKField,
					NewText: "id",
					Detail:  "INTEGER\nid description",
				},
			},
		},
		"Complete column in group by clause": {
			files: map[string]string{
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
			expectCompletionItems: []source.CompletionItem{
				{
					Kind:    lsp.CIKField,
					NewText: "id",
					Detail:  "INTEGER\nid description",
				},
			},
		},
		"Complete incomplete column in group by clause": {
			files: map[string]string{
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
			expectCompletionItems: []source.CompletionItem{
				{
					Kind:        lsp.CIKField,
					NewText:     "id",
					Detail:      "INTEGER\nid description",
					TypedPrefix: "i",
				},
			},
		},
		"Complete incomplete column in order by clause": {
			files: map[string]string{
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
			expectCompletionItems: []source.CompletionItem{
				{
					Kind:        lsp.CIKField,
					NewText:     "id",
					Detail:      "INTEGER\nid description",
					TypedPrefix: "i",
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
			p := source.NewProjectWithBQClient("/", bqClient, logger)

			files, path, position, err := helper.GetLspPosition(tt.files)
			if err != nil {
				t.Fatal(err)
			}

			for uri, content := range files {
				p.UpdateFile(uri, content, 1)
			}

			got, err := p.Complete(context.Background(), path, position)
			if !errors.Is(err, tt.expectErr) {
				t.Fatalf("got error %v, but want %v", err, tt.expectErr)
			}

			if diff := cmp.Diff(got, tt.expectCompletionItems); diff != "" {
				t.Errorf("(-got, +want)\n%s", diff)
			}
		})
	}
}

func TestProject_CompleteFromClause(t *testing.T) {
	tests := map[string]struct {
		files                  map[string]string
		bigqueryClientMockFunc func(t *testing.T) bigquery.Client

		expectCompletionItems []source.CompletionItem
		expectErr             error
	}{
		"list table": {
			files: map[string]string{
				"file1.sql": "SELECT * FROM `project.dataset.|`",
			},
			bigqueryClientMockFunc: func(t *testing.T) bigquery.Client {
				ctrl := gomock.NewController(t)
				bqClient := mock_bigquery.NewMockClient(ctrl)

				bqClient.EXPECT().ListTables(gomock.Any(), "project", "dataset").Return([]*bq.Table{
					{
						ProjectID: "project",
						DatasetID: "dataset",
						TableID:   "1table",
					},
					{
						ProjectID: "project",
						DatasetID: "dataset",
						TableID:   "2table",
					},
				}, nil)
				bqClient.EXPECT().GetTableMetadata(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, fmt.Errorf("not found")).MinTimes(0)
				return bqClient
			},
			expectCompletionItems: []source.CompletionItem{
				{
					Kind:    lsp.CIKModule,
					NewText: "1table",
					Detail:  "project.dataset.1table",
				},
				{
					Kind:    lsp.CIKModule,
					NewText: "2table",
					Detail:  "project.dataset.2table",
				},
			},
		},
		"select latest suffix table": {
			files: map[string]string{
				"file1.sql": "SELECT * FROM `project.dataset.|`",
			},
			bigqueryClientMockFunc: func(t *testing.T) bigquery.Client {
				ctrl := gomock.NewController(t)
				bqClient := mock_bigquery.NewMockClient(ctrl)

				bqClient.EXPECT().ListTables(gomock.Any(), "project", "dataset").Return([]*bq.Table{
					{
						ProjectID: "project",
						DatasetID: "dataset",
						TableID:   "table20230621",
					},
					{
						ProjectID: "project",
						DatasetID: "dataset",
						TableID:   "table20230622",
					},
				}, nil)
				bqClient.EXPECT().GetDefaultProject().Return("").MinTimes(0)
				bqClient.EXPECT().GetTableMetadata(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, fmt.Errorf("not found")).MinTimes(0)
				return bqClient
			},
			expectCompletionItems: []source.CompletionItem{
				{
					Kind:    lsp.CIKModule,
					NewText: "table20230622",
					Detail:  "project.dataset.table20230622",
				},
			},
		},
		"complete datasetID": {
			files: map[string]string{
				"file1.sql": "SELECT * FROM `project.|`",
			},
			bigqueryClientMockFunc: func(t *testing.T) bigquery.Client {
				ctrl := gomock.NewController(t)
				bqClient := mock_bigquery.NewMockClient(ctrl)

				bqClient.EXPECT().ListDatasets(gomock.Any(), "project").Return([]*bq.Dataset{
					{
						ProjectID: "project",
						DatasetID: "dataset1",
					},
					{
						ProjectID: "project",
						DatasetID: "dataset2",
					},
				}, nil)
				bqClient.EXPECT().GetDefaultProject().Return("").MinTimes(0)
				bqClient.EXPECT().GetTableMetadata(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, fmt.Errorf("not found")).MinTimes(0)
				return bqClient
			},
			expectCompletionItems: []source.CompletionItem{
				{
					Kind:    lsp.CIKModule,
					NewText: "dataset1",
					Detail:  "project.dataset1",
				},
				{
					Kind:    lsp.CIKModule,
					NewText: "dataset2",
					Detail:  "project.dataset2",
				},
			},
		},
		"complete projectID": {
			files: map[string]string{
				"file1.sql": "SELECT * FROM `p|`",
			},
			bigqueryClientMockFunc: func(t *testing.T) bigquery.Client {
				ctrl := gomock.NewController(t)
				bqClient := mock_bigquery.NewMockClient(ctrl)

				bqClient.EXPECT().ListProjects(gomock.Any()).Return([]*cloudresourcemanager.Project{
					{
						ProjectId: "project1",
						Name:      "project name",
					},
					{
						ProjectId: "project2",
						Name:      "project name",
					},
				}, nil)
				bqClient.EXPECT().GetTableMetadata(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, fmt.Errorf("not found")).MinTimes(0)
				return bqClient
			},
			expectCompletionItems: []source.CompletionItem{
				{
					Kind:        lsp.CIKModule,
					NewText:     "project1",
					Detail:      "project name",
					TypedPrefix: "p",
				},
				{
					Kind:        lsp.CIKModule,
					NewText:     "project2",
					Detail:      "project name",
					TypedPrefix: "p",
				},
			},
		},
	}

	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			bqClient := tt.bigqueryClientMockFunc(t)
			p := source.NewProjectWithBQClient("/", bqClient, logrus.New())

			files, path, position, err := helper.GetLspPosition(tt.files)
			if err != nil {
				t.Fatal(err)
			}

			for uri, content := range files {
				p.UpdateFile(uri, content, 1)
			}

			got, err := p.Complete(context.Background(), path, position)
			if !errors.Is(err, tt.expectErr) {
				t.Fatalf("got error %v, but want %v", err, tt.expectErr)
			}

			if diff := cmp.Diff(got, tt.expectCompletionItems); diff != "" {
				t.Errorf("(-got, +want)\n%s", diff)
			}
		})
	}
}
