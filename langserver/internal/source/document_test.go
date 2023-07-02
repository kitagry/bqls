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
		"hover separated into project, dataset and table": {
			files: map[string]string{
				"file1.sql": "SELECT * FROM |`project`.`dataset`.`table`",
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
				"file1.sql": "SELECT * FROM `project.dataset.dummy_table` JOIN |`project.dataset.table`",
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
					Language: "markdown",
					Value:    "name: STRING\nname description",
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
					Language: "markdown",
					Value:    "name: STRING\nname description",
				},
			},
		},
		"hover column with table_alias": {
			files: map[string]string{
				"file1.sql": "SELECT a.|name FROM `project.dataset.table` AS a",
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
					Value:    "name: STRING\nname description",
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
		"hover column in where coluse": {
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
					Language: "markdown",
					Value:    "name: STRING\nname description",
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
					Language: "markdown",
					Value: `params: RECORD
params description`,
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
					Value: `## JSON_VALUE

JSON_VALUE(STRING, optional STRING {default_value: "$"}) -> STRING rejects_collation=TRUE
JSON_VALUE(JSON, optional STRING {default_value: "$"}) -> STRING`,
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
					Language: "markdown",
					Value: `json: JSON
json description`,
				},
			},
		},
	}

	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			bqClient := mock_bigquery.NewMockClient(ctrl)
			bqClient.EXPECT().GetTableMetadata(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(tt.bqTableMetadata, nil).MinTimes(0)
			p := source.NewProjectWithBQClient("/", bqClient, logrus.New())

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
