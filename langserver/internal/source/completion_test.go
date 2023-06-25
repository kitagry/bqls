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
	"google.golang.org/api/cloudresourcemanager/v1"
)

func TestProject_CompleteColumn(t *testing.T) {
	tests := map[string]struct {
		files              map[string]string
		supportSunippet    bool
		bqTableMetadataMap map[string]*bq.TableMetadata

		expectCompletionItems []lsp.CompletionItem
		expectErr             error
	}{
		"Select columns with supportSunippet is false": {
			files: map[string]string{
				"file1.sql": "SELECT id, | FROM `project.dataset.table`",
			},
			supportSunippet: false,
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
			expectCompletionItems: []lsp.CompletionItem{
				{
					InsertTextFormat: lsp.ITFPlainText,
					Kind:             lsp.CIKField,
					Label:            "id",
					Detail:           "INTEGER\nid description",
				},
				{
					InsertTextFormat: lsp.ITFPlainText,
					Kind:             lsp.CIKField,
					Label:            "name",
					Detail:           "STRING",
				},
			},
		},
		"Select columns with supportSunippet is true": {
			files: map[string]string{
				"file1.sql": "SELECT id, | FROM `project.dataset.table`",
			},
			supportSunippet: true,
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
			expectCompletionItems: []lsp.CompletionItem{
				{
					InsertTextFormat: lsp.ITFSnippet,
					Kind:             lsp.CIKField,
					Label:            "id",
					Detail:           "INTEGER\nid description",
					TextEdit: &lsp.TextEdit{
						NewText: "id",
						Range: lsp.Range{
							Start: lsp.Position{Line: 0, Character: 11},
							End:   lsp.Position{Line: 0, Character: 11},
						},
					},
				},
				{
					InsertTextFormat: lsp.ITFSnippet,
					Kind:             lsp.CIKField,
					Label:            "name",
					Detail:           "STRING",
					TextEdit: &lsp.TextEdit{
						NewText: "name",
						Range: lsp.Range{
							Start: lsp.Position{Line: 0, Character: 11},
							End:   lsp.Position{Line: 0, Character: 11},
						},
					},
				},
			},
		},
		"When file cannot be parsed": {
			files: map[string]string{
				"file1.sql": "SELECT | FROM `project.dataset.table`",
			},
			supportSunippet: true,
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
			expectCompletionItems: []lsp.CompletionItem{
				{
					InsertTextFormat: lsp.ITFSnippet,
					Kind:             lsp.CIKField,
					Label:            "id",
					Detail:           "INTEGER\nid description",
					TextEdit: &lsp.TextEdit{
						NewText: "id",
						Range: lsp.Range{
							Start: lsp.Position{Line: 0, Character: 7},
							End:   lsp.Position{Line: 0, Character: 7},
						},
					},
				},
			},
		},
		"Consider selectable table": {
			files: map[string]string{
				"file1.sql": "SELECT * FROM `project.dataset.table`;\n" +
					"SELECT *| FROM `project.dataset.table2`;",
			},
			supportSunippet: true,
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
			expectCompletionItems: []lsp.CompletionItem{
				{
					InsertTextFormat: lsp.ITFSnippet,
					Kind:             lsp.CIKField,
					Label:            "name",
					Detail:           "STRING",
					TextEdit: &lsp.TextEdit{
						NewText: "name",
						Range: lsp.Range{
							Start: lsp.Position{Line: 1, Character: 8},
							End:   lsp.Position{Line: 1, Character: 8},
						},
					},
				},
			},
		},
		"Select WITH table": {
			files: map[string]string{
				"file1.sql": "WITH data AS (SELECT id FROM `project.dataset.table`)\n" +
					"SELECT | FROM data;",
			},
			supportSunippet: true,
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
			expectCompletionItems: []lsp.CompletionItem{
				{
					InsertTextFormat: lsp.ITFSnippet,
					Kind:             lsp.CIKField,
					Label:            "id",
					Detail:           "INT64",
					TextEdit: &lsp.TextEdit{
						NewText: "id",
						Range: lsp.Range{
							Start: lsp.Position{Line: 1, Character: 7},
							End:   lsp.Position{Line: 1, Character: 7},
						},
					},
				},
				{ // TODO: Refactoring test
					InsertTextFormat: lsp.ITFSnippet,
					Kind:             lsp.CIKField,
					Label:            "data",
					TextEdit: &lsp.TextEdit{
						NewText: "data",
						Range: lsp.Range{
							Start: lsp.Position{Line: 1, Character: 7},
							End:   lsp.Position{Line: 1, Character: 7},
						},
					},
				},
			},
		},
		"Complete incomplete column": {
			files: map[string]string{
				"file1.sql": "SELECT i| FROM `project.dataset.table`",
			},
			supportSunippet: true,
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
			expectCompletionItems: []lsp.CompletionItem{
				{
					InsertTextFormat: lsp.ITFSnippet,
					Kind:             lsp.CIKField,
					Label:            "id",
					Detail:           "INTEGER\nid description",
					TextEdit: &lsp.TextEdit{
						NewText: "id",
						Range: lsp.Range{
							Start: lsp.Position{Line: 0, Character: 7},
							End:   lsp.Position{Line: 0, Character: 8},
						},
					},
				},
			},
		},
		"Complete incomplete column2": {
			files: map[string]string{
				"file1.sql": "SELECT id, i| id FROM `project.dataset.table`",
			},
			supportSunippet: true,
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
			expectCompletionItems: []lsp.CompletionItem{
				{
					InsertTextFormat: lsp.ITFSnippet,
					Kind:             lsp.CIKField,
					Label:            "id",
					Detail:           "INTEGER\nid description",
					TextEdit: &lsp.TextEdit{
						NewText: "id",
						Range: lsp.Range{
							Start: lsp.Position{Line: 0, Character: 11},
							End:   lsp.Position{Line: 0, Character: 12},
						},
					},
				},
			},
		},
		"Complete record column": {
			files: map[string]string{
				"file1.sql": "SELECT record.| FROM `project.dataset.table`",
			},
			supportSunippet: true,
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
			expectCompletionItems: []lsp.CompletionItem{
				{
					InsertTextFormat: lsp.ITFSnippet,
					Kind:             lsp.CIKField,
					Label:            "id",
					Detail:           "INTEGER\nid description",
					TextEdit: &lsp.TextEdit{
						NewText: "id",
						Range: lsp.Range{
							Start: lsp.Position{Line: 0, Character: 14},
							End:   lsp.Position{Line: 0, Character: 14},
						},
					},
				},
			},
		},
		"Complete record column with incomplete word": {
			files: map[string]string{
				"file1.sql": "SELECT record.i| FROM `project.dataset.table`",
			},
			supportSunippet: true,
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
			expectCompletionItems: []lsp.CompletionItem{
				{
					InsertTextFormat: lsp.ITFSnippet,
					Kind:             lsp.CIKField,
					Label:            "id",
					Detail:           "INTEGER\nid description",
					TextEdit: &lsp.TextEdit{
						NewText: "id",
						Range: lsp.Range{
							Start: lsp.Position{Line: 0, Character: 14},
							End:   lsp.Position{Line: 0, Character: 15},
						},
					},
				},
			},
		},
		"Complete record column with incomplete word and zetasql odd error": {
			files: map[string]string{
				"file1.sql": "SELECT record.id, record.| FROM `project.dataset.table`",
			},
			supportSunippet: true,
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
			expectCompletionItems: []lsp.CompletionItem{
				{
					InsertTextFormat: lsp.ITFSnippet,
					Kind:             lsp.CIKField,
					Label:            "id",
					Detail:           "INTEGER\nid description",
					TextEdit: &lsp.TextEdit{
						NewText: "id",
						Range: lsp.Range{
							Start: lsp.Position{Line: 0, Character: 25},
							End:   lsp.Position{Line: 0, Character: 25},
						},
					},
				},
			},
		},
		"Complete record column with WITH clause": {
			files: map[string]string{
				"file1.sql": "WITH data AS (SELECT * FROM `project.dataset.table`)\n" + "SELECT record.| FROM data",
			},
			supportSunippet: true,
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
			expectCompletionItems: []lsp.CompletionItem{
				{
					InsertTextFormat: lsp.ITFSnippet,
					Kind:             lsp.CIKField,
					Label:            "id",
					Detail:           "INT64",
					TextEdit: &lsp.TextEdit{
						NewText: "id",
						Range: lsp.Range{
							Start: lsp.Position{Line: 1, Character: 14},
							End:   lsp.Position{Line: 1, Character: 14},
						},
					},
				},
			},
		},
		"Complete column with table alias": {
			files: map[string]string{
				"file1.sql": "SELECT t.| FROM `project.dataset.table` AS t",
			},
			supportSunippet: true,
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
			expectCompletionItems: []lsp.CompletionItem{
				{
					InsertTextFormat: lsp.ITFSnippet,
					Kind:             lsp.CIKField,
					Label:            "id",
					Detail:           "INTEGER\nid description",
					TextEdit: &lsp.TextEdit{
						NewText: "id",
						Range: lsp.Range{
							Start: lsp.Position{Line: 0, Character: 9},
							End:   lsp.Position{Line: 0, Character: 9},
						},
					},
				},
			},
		},
		"Complete column with with table": {
			files: map[string]string{
				"file1.sql": "WITH data AS (SELECT * FROM `project.dataset.table`)\nSELECT data.| FROM data",
			},
			supportSunippet: true,
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
			expectCompletionItems: []lsp.CompletionItem{
				{
					InsertTextFormat: lsp.ITFSnippet,
					Kind:             lsp.CIKField,
					Label:            "id",
					Detail:           "INT64",
					TextEdit: &lsp.TextEdit{
						NewText: "id",
						Range: lsp.Range{
							Start: lsp.Position{Line: 1, Character: 12},
							End:   lsp.Position{Line: 1, Character: 12},
						},
					},
				},
			},
		},
		"Complete table alias": {
			files: map[string]string{
				"file1.sql": "SELECT t| FROM `project.dataset.table` AS table",
			},
			supportSunippet: true,
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
			expectCompletionItems: []lsp.CompletionItem{
				{
					InsertTextFormat: lsp.ITFSnippet,
					Kind:             lsp.CIKField,
					Label:            "table",
					Detail:           "project.dataset.table",
					TextEdit: &lsp.TextEdit{
						NewText: "table",
						Range: lsp.Range{
							Start: lsp.Position{Line: 0, Character: 7},
							End:   lsp.Position{Line: 0, Character: 8},
						},
					},
				},
			},
		},
		"Complete with scan alias": {
			files: map[string]string{
				"file1.sql": "WITH data AS (SELECT * FROM `project.dataset.table`)\nSELECT d| FROM data",
			},
			supportSunippet: true,
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
			expectCompletionItems: []lsp.CompletionItem{
				{
					InsertTextFormat: lsp.ITFSnippet,
					Kind:             lsp.CIKField,
					Label:            "data",
					TextEdit: &lsp.TextEdit{
						NewText: "data",
						Range: lsp.Range{
							Start: lsp.Position{Line: 1, Character: 7},
							End:   lsp.Position{Line: 1, Character: 8},
						},
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
			p := source.NewProjectWithBQClient("/", bqClient)

			files, path, position, err := helper.GetLspPosition(tt.files)
			if err != nil {
				t.Fatal(err)
			}

			for uri, content := range files {
				p.UpdateFile(uri, content, 1)
			}

			got, err := p.Complete(context.Background(), path, position, tt.supportSunippet)
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
		supportSnippet         bool
		bigqueryClientMockFunc func(t *testing.T) bigquery.Client

		expectCompletionItems []lsp.CompletionItem
		expectErr             error
	}{
		"list table": {
			files: map[string]string{
				"file1.sql": "SELECT * FROM `project.dataset.|`",
			},
			supportSnippet: true,
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
			expectCompletionItems: []lsp.CompletionItem{
				{
					InsertTextFormat: lsp.ITFSnippet,
					Kind:             lsp.CIKFile,
					Label:            "1table",
					Detail:           "project.dataset.1table",
					TextEdit: &lsp.TextEdit{
						NewText: "1table",
						Range: lsp.Range{
							Start: lsp.Position{Line: 0, Character: 31},
							End:   lsp.Position{Line: 0, Character: 31},
						},
					},
				},
				{
					InsertTextFormat: lsp.ITFSnippet,
					Kind:             lsp.CIKFile,
					Label:            "2table",
					Detail:           "project.dataset.2table",
					TextEdit: &lsp.TextEdit{
						NewText: "2table",
						Range: lsp.Range{
							Start: lsp.Position{Line: 0, Character: 31},
							End:   lsp.Position{Line: 0, Character: 31},
						},
					},
				},
			},
		},
		"select latest suffix table": {
			files: map[string]string{
				"file1.sql": "SELECT * FROM `project.dataset.|`",
			},
			supportSnippet: true,
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
			expectCompletionItems: []lsp.CompletionItem{
				{
					InsertTextFormat: lsp.ITFSnippet,
					Kind:             lsp.CIKFile,
					Label:            "table20230622",
					Detail:           "project.dataset.table20230622",
					TextEdit: &lsp.TextEdit{
						NewText: "table20230622",
						Range: lsp.Range{
							Start: lsp.Position{Line: 0, Character: 31},
							End:   lsp.Position{Line: 0, Character: 31},
						},
					},
				},
			},
		},
		"complete datasetID": {
			files: map[string]string{
				"file1.sql": "SELECT * FROM `project.|`",
			},
			supportSnippet: true,
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
			expectCompletionItems: []lsp.CompletionItem{
				{
					InsertTextFormat: lsp.ITFSnippet,
					Kind:             lsp.CIKFile,
					Label:            "dataset1",
					Detail:           "project.dataset1",
					TextEdit: &lsp.TextEdit{
						NewText: "dataset1",
						Range: lsp.Range{
							Start: lsp.Position{Line: 0, Character: 23},
							End:   lsp.Position{Line: 0, Character: 23},
						},
					},
				},
				{
					InsertTextFormat: lsp.ITFSnippet,
					Kind:             lsp.CIKFile,
					Label:            "dataset2",
					Detail:           "project.dataset2",
					TextEdit: &lsp.TextEdit{
						NewText: "dataset2",
						Range: lsp.Range{
							Start: lsp.Position{Line: 0, Character: 23},
							End:   lsp.Position{Line: 0, Character: 23},
						},
					},
				},
			},
		},
		"complete projectID": {
			files: map[string]string{
				"file1.sql": "SELECT * FROM `p|`",
			},
			supportSnippet: true,
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
			expectCompletionItems: []lsp.CompletionItem{
				{
					InsertTextFormat: lsp.ITFSnippet,
					Kind:             lsp.CIKFile,
					Label:            "project1",
					Detail:           "project name",
					TextEdit: &lsp.TextEdit{
						NewText: "project1",
						Range: lsp.Range{
							Start: lsp.Position{Line: 0, Character: 15},
							End:   lsp.Position{Line: 0, Character: 16},
						},
					},
				},
				{
					InsertTextFormat: lsp.ITFSnippet,
					Kind:             lsp.CIKFile,
					Label:            "project2",
					Detail:           "project name",
					TextEdit: &lsp.TextEdit{
						NewText: "project2",
						Range: lsp.Range{
							Start: lsp.Position{Line: 0, Character: 15},
							End:   lsp.Position{Line: 0, Character: 16},
						},
					},
				},
			},
		},
	}

	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			bqClient := tt.bigqueryClientMockFunc(t)
			p := source.NewProjectWithBQClient("/", bqClient)

			files, path, position, err := helper.GetLspPosition(tt.files)
			if err != nil {
				t.Fatal(err)
			}

			for uri, content := range files {
				p.UpdateFile(uri, content, 1)
			}

			got, err := p.Complete(context.Background(), path, position, tt.supportSnippet)
			if !errors.Is(err, tt.expectErr) {
				t.Fatalf("got error %v, but want %v", err, tt.expectErr)
			}

			if diff := cmp.Diff(got, tt.expectCompletionItems); diff != "" {
				t.Errorf("(-got, +want)\n%s", diff)
			}
		})
	}
}
