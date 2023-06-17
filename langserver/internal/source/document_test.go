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
)

func TestProject_TermDocument(t *testing.T) {
	tests := map[string]struct {
		// prepare
		files           map[string]string
		bqTableMetadata *bq.TableMetadata

		// input
		uri      string
		position lsp.Position

		// output
		expectMarkedStrings []lsp.MarkedString
		expectErr           error
	}{
		"hover table": {
			files: map[string]string{
				"file1.sql": "SELECT * FROM `project.dataset.table`",
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
			uri: "file1.sql",
			position: lsp.Position{
				Line:      0,
				Character: 14,
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
					Language: "json",
					Value: `[
 {
  "description": "name description",
  "name": "name",
  "type": "STRING"
 }
]`,
				},
			},
		},
		"hover separated into project, dataset and table": {
			files: map[string]string{
				"file1.sql": "SELECT * FROM `project`.`dataset`.`table`",
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
			uri: "file1.sql",
			position: lsp.Position{
				Line:      0,
				Character: 14,
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
					Language: "json",
					Value: `[
 {
  "description": "name description",
  "name": "name",
  "type": "STRING"
 }
]`,
				},
			},
		},
		"hover joined table": {
			files: map[string]string{
				"file1.sql": "SELECT * FROM `project.dataset.dummy_table` JOIN `project.dataset.table`",
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
			uri: "file1.sql",
			position: lsp.Position{
				Line:      0,
				Character: 49,
			},
			expectMarkedStrings: []lsp.MarkedString{
				{
					Language: "markdown",
					Value: `## project.dataset.table
created at 2023-06-17 00:00:00
last modified at 2023-06-17 00:00:00`,
				},
				{
					Language: "json",
					Value: `[
 {
  "description": "name description",
  "name": "name",
  "type": "STRING"
 }
]`,
				},
			},
		},
		"hover column": {
			files: map[string]string{
				"file1.sql": "SELECT id, name FROM `project.dataset.table`",
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
			uri: "file1.sql",
			position: lsp.Position{
				Line:      0,
				Character: 11,
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
				"file1.sql": "SELECT name AS alias_name FROM `project.dataset.table`",
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
			uri: "file1.sql",
			position: lsp.Position{
				Line:      0,
				Character: 7,
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
				"file1.sql": "SELECT a.name FROM `project.dataset.table` AS a",
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
			uri: "file1.sql",
			position: lsp.Position{
				Line:      0,
				Character: 9,
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
				"file1.sql": "SELECT param.key FROM `project.dataset.table`, UNNEST(params) AS param",
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
			uri: "file1.sql",
			position: lsp.Position{
				Line:      0,
				Character: 13,
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
				"file1.sql": "SELECT name AS alias_name FROM `project.dataset.table` WHERE name = 'test'",
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
			uri: "file1.sql",
			position: lsp.Position{
				Line:      0,
				Character: 61,
			},
			expectMarkedStrings: []lsp.MarkedString{
				{
					Language: "markdown",
					Value:    "name: STRING\nname description",
				},
			},
		},
	}

	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			bqClient := mock_bigquery.NewMockClient(ctrl)
			bqClient.EXPECT().GetTableMetadata(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(tt.bqTableMetadata, nil).MinTimes(0)
			p := source.NewProjectWithBQClient("/", bqClient)

			for uri, content := range tt.files {
				p.UpdateFile(uri, content, 1)
			}

			got, err := p.TermDocument(tt.uri, tt.position)
			if !errors.Is(err, tt.expectErr) {
				t.Fatalf("got error %v, but want %v", err, tt.expectErr)
			}

			if diff := cmp.Diff(tt.expectMarkedStrings, got, cmpopts.IgnoreUnexported(lsp.MarkedString{})); diff != "" {
				t.Errorf("project.TermDocument result diff (-expect, +got)\n%s", diff)
			}
		})
	}
}
