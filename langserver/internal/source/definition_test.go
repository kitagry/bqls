package source_test

import (
	"context"
	"errors"
	"testing"

	bq "cloud.google.com/go/bigquery"
	"github.com/golang/mock/gomock"
	"github.com/google/go-cmp/cmp"
	"github.com/kitagry/bqls/langserver/internal/bigquery/mock_bigquery"
	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/kitagry/bqls/langserver/internal/source"
	"github.com/kitagry/bqls/langserver/internal/source/helper"
	"github.com/sirupsen/logrus"
)

func TestProject_LookupIdent(t *testing.T) {
	tests := map[string]struct {
		// prepare
		files           map[lsp.DocumentURI]string
		bqTableMetadata *bq.TableMetadata
		defaultProject  string

		// output
		expectLocations []lsp.Location
		expectErr       error
	}{
		"definition to with clause": {
			files: map[lsp.DocumentURI]string{
				"a.sql": `WITH data AS ( SELECT 1 AS a )
SELECT a FROM data|`,
			},
			bqTableMetadata: &bq.TableMetadata{
				FullID: "project:dataset.table",
				Schema: bq.Schema{},
			},
			expectLocations: []lsp.Location{
				{
					URI: "a.sql",
					Range: lsp.Range{
						Start: lsp.Position{
							Line:      0,
							Character: 5,
						},
						End: lsp.Position{
							Line:      0,
							Character: 9,
						},
					},
				},
			},
		},
		"definition to 2 with clause": {
			files: map[lsp.DocumentURI]string{
				"a.sql": `WITH data AS ( SELECT 1 AS a ),
data2 AS (SELECT * FROM data )
SELECT a FROM data2|`,
			},
			bqTableMetadata: &bq.TableMetadata{
				FullID: "project:dataset.table",
				Schema: bq.Schema{},
			},
			expectLocations: []lsp.Location{
				{
					URI: "a.sql",
					Range: lsp.Range{
						Start: lsp.Position{
							Line:      1,
							Character: 0,
						},
						End: lsp.Position{
							Line:      1,
							Character: 5,
						},
					},
				},
			},
		},
		"definition to bq table": {
			files: map[lsp.DocumentURI]string{
				"a.sql": "SELECT * FROM `project.dataset.table|`",
			},
			bqTableMetadata: &bq.TableMetadata{
				FullID: "project:dataset.table",
				Schema: bq.Schema{
					{
						Name:        "id",
						Type:        bq.IntegerFieldType,
						Description: "id description",
					},
				},
			},
			expectLocations: []lsp.Location{
				{
					URI: lsp.NewTableVirtualTextDocumentURI("project", "dataset", "table"),
					Range: lsp.Range{
						Start: lsp.Position{
							Line:      0,
							Character: 0,
						},
						End: lsp.Position{
							Line:      0,
							Character: 0,
						},
					},
				},
			},
		},
		"definition to INSERT target bq table": {
			files: map[lsp.DocumentURI]string{
				"a.sql": "INSERT `project.dataset.table|` (id) SELECT id FROM `project.dataset.table`",
			},
			bqTableMetadata: &bq.TableMetadata{
				FullID: "project:dataset.table",
				Schema: bq.Schema{
					{
						Name: "id",
						Type: bq.IntegerFieldType,
					},
				},
			},
			expectLocations: []lsp.Location{
				{
					URI: lsp.NewTableVirtualTextDocumentURI("project", "dataset", "table"),
				},
			},
		},
		"definition to dataset-qualified INSERT target bq table": {
			files: map[lsp.DocumentURI]string{
				"a.sql": "INSERT INTO dataset.table| (id) VALUES (1)",
			},
			bqTableMetadata: &bq.TableMetadata{
				FullID: "project:dataset.table",
				Schema: bq.Schema{
					{
						Name: "id",
						Type: bq.IntegerFieldType,
					},
				},
			},
			defaultProject: "project",
			expectLocations: []lsp.Location{
				{
					URI: lsp.NewTableVirtualTextDocumentURI("project", "dataset", "table"),
				},
			},
		},
		"definition to UPDATE target bq table": {
			files: map[lsp.DocumentURI]string{
				"a.sql": "UPDATE `project.dataset.table|` SET id = 1 WHERE true",
			},
			bqTableMetadata: &bq.TableMetadata{
				FullID: "project:dataset.table",
				Schema: bq.Schema{
					{
						Name: "id",
						Type: bq.IntegerFieldType,
					},
				},
			},
			expectLocations: []lsp.Location{
				{
					URI: lsp.NewTableVirtualTextDocumentURI("project", "dataset", "table"),
				},
			},
		},
		"definition to DELETE target bq table": {
			files: map[lsp.DocumentURI]string{
				"a.sql": "DELETE FROM `project.dataset.table|` WHERE id = 1",
			},
			bqTableMetadata: &bq.TableMetadata{
				FullID: "project:dataset.table",
				Schema: bq.Schema{
					{
						Name: "id",
						Type: bq.IntegerFieldType,
					},
				},
			},
			expectLocations: []lsp.Location{
				{
					URI: lsp.NewTableVirtualTextDocumentURI("project", "dataset", "table"),
				},
			},
		},
		"definition to MERGE target bq table": {
			files: map[lsp.DocumentURI]string{
				"a.sql": "MERGE `project.dataset.table|` T USING `project.dataset.table` S ON T.id = S.id WHEN MATCHED THEN DELETE",
			},
			bqTableMetadata: &bq.TableMetadata{
				FullID: "project:dataset.table",
				Schema: bq.Schema{
					{
						Name: "id",
						Type: bq.IntegerFieldType,
					},
				},
			},
			expectLocations: []lsp.Location{
				{
					URI: lsp.NewTableVirtualTextDocumentURI("project", "dataset", "table"),
				},
			},
		},
		"definition to dataset-qualified bq table": {
			files: map[lsp.DocumentURI]string{
				"a.sql": "SELECT * FROM dataset.table|",
			},
			bqTableMetadata: &bq.TableMetadata{
				FullID: "project:dataset.table",
				Schema: bq.Schema{
					{
						Name: "id",
						Type: bq.IntegerFieldType,
					},
				},
			},
			defaultProject: "project",
			expectLocations: []lsp.Location{
				{
					URI: lsp.NewTableVirtualTextDocumentURI("project", "dataset", "table"),
					Range: lsp.Range{
						Start: lsp.Position{
							Line:      0,
							Character: 0,
						},
						End: lsp.Position{
							Line:      0,
							Character: 0,
						},
					},
				},
			},
		},
		"definition to backticked dataset-qualified bq table": {
			files: map[lsp.DocumentURI]string{
				"a.sql": "SELECT * FROM `dataset.table|`",
			},
			bqTableMetadata: &bq.TableMetadata{
				FullID: "project:dataset.table",
				Schema: bq.Schema{
					{
						Name: "id",
						Type: bq.IntegerFieldType,
					},
				},
			},
			defaultProject: "project",
			expectLocations: []lsp.Location{
				{
					URI: lsp.NewTableVirtualTextDocumentURI("project", "dataset", "table"),
					Range: lsp.Range{
						Start: lsp.Position{
							Line:      0,
							Character: 0,
						},
						End: lsp.Position{
							Line:      0,
							Character: 0,
						},
					},
				},
			},
		},
		"definition to create temp function": {
			files: map[lsp.DocumentURI]string{
				"a.sql": "CREATE TEMP FUNCTION target_func(x INT64) RETURNS INT64 AS (x * 2);\nSELECT target_func|(123) FROM `project.dataset.table`",
			},
			bqTableMetadata: &bq.TableMetadata{
				FullID: "project:dataset.table",
				Schema: bq.Schema{},
			},
			expectLocations: []lsp.Location{
				{
					URI: "a.sql",
					Range: lsp.Range{
						Start: lsp.Position{
							Line:      0,
							Character: 21,
						},
						End: lsp.Position{
							Line:      0,
							Character: 32,
						},
					},
				},
			},
		},
		"definition to declare variable": {
			files: map[lsp.DocumentURI]string{
				"a.sql": "DECLARE target_id INT64;\nSELECT id FROM `bigquery-public-data.samples.wikipedia` WHERE id = target_id|",
			},
			bqTableMetadata: &bq.TableMetadata{
				FullID: "bigquery-public-data:samples.wikipedia",
				Schema: bq.Schema{},
			},
			expectLocations: []lsp.Location{
				{
					URI: "a.sql",
					Range: lsp.Range{
						Start: lsp.Position{
							Line:      0,
							Character: 8,
						},
						End: lsp.Position{
							Line:      0,
							Character: 17,
						},
					},
				},
			},
		},
		"definition to declare variable after SET statement": {
			files: map[lsp.DocumentURI]string{
				"a.sql": "DECLARE target_id INT64;\nSET target_id = 1;\nSELECT id FROM `bigquery-public-data.samples.wikipedia` WHERE id = target_id|",
			},
			bqTableMetadata: &bq.TableMetadata{
				FullID: "bigquery-public-data:samples.wikipedia",
				Schema: bq.Schema{},
			},
			expectLocations: []lsp.Location{
				{
					URI: "a.sql",
					Range: lsp.Range{
						Start: lsp.Position{Line: 0, Character: 8},
						End:   lsp.Position{Line: 0, Character: 17},
					},
				},
			},
		},
		"definition to declare variable from SET expression": {
			files: map[lsp.DocumentURI]string{
				"a.sql": "DECLARE a INT64;\nDECLARE b INT64;\nSET b = a|;",
			},
			bqTableMetadata: &bq.TableMetadata{},
			expectLocations: []lsp.Location{
				{
					URI: "a.sql",
					Range: lsp.Range{
						Start: lsp.Position{Line: 0, Character: 8},
						End:   lsp.Position{Line: 0, Character: 9},
					},
				},
			},
		},
		// Both the cursor and the returned range are in UTF-16 units.
		"definition to declare variable after multibyte text": {
			files: map[lsp.DocumentURI]string{
				"a.sql": "DECLARE /* あ */ target_id INT64;\nSELECT '😀', target_id|",
			},
			bqTableMetadata: &bq.TableMetadata{},
			expectLocations: []lsp.Location{
				{
					URI: "a.sql",
					Range: lsp.Range{
						Start: lsp.Position{Line: 0, Character: 16},
						End:   lsp.Position{Line: 0, Character: 25},
					},
				},
			},
		},
		"definition to create temp function after multibyte text": {
			files: map[lsp.DocumentURI]string{
				"a.sql": "CREATE TEMP FUNCTION /* あ */ target_func(x INT64) RETURNS INT64 AS (x * 2);\nSELECT '😀', target_func|(1)",
			},
			bqTableMetadata: &bq.TableMetadata{},
			expectLocations: []lsp.Location{
				{
					URI: "a.sql",
					Range: lsp.Range{
						Start: lsp.Position{Line: 0, Character: 29},
						End:   lsp.Position{Line: 0, Character: 40},
					},
				},
			},
		},
		"definition to with clause after multibyte text": {
			files: map[lsp.DocumentURI]string{
				"a.sql": "WITH /* あ */ data AS (SELECT 1 AS a)\nSELECT '😀', a FROM data|",
			},
			bqTableMetadata: &bq.TableMetadata{},
			expectLocations: []lsp.Location{
				{
					URI: "a.sql",
					Range: lsp.Range{
						Start: lsp.Position{Line: 0, Character: 13},
						End:   lsp.Position{Line: 0, Character: 17},
					},
				},
			},
		},
		"definition to bq table in other scan node": {
			files: map[lsp.DocumentURI]string{
				"a.sql": "WITH data AS (SELECT * FROM `project.dataset.table|`) SELECT * FROM data",
			},
			bqTableMetadata: &bq.TableMetadata{
				FullID: "project:dataset.table",
				Schema: bq.Schema{
					{
						Name:        "id",
						Type:        bq.IntegerFieldType,
						Description: "id description",
					},
				},
			},
			expectLocations: []lsp.Location{
				{
					URI: lsp.NewTableVirtualTextDocumentURI("project", "dataset", "table"),
					Range: lsp.Range{
						Start: lsp.Position{
							Line:      0,
							Character: 0,
						},
						End: lsp.Position{
							Line:      0,
							Character: 0,
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
			bqClient.EXPECT().GetTableMetadata(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(tt.bqTableMetadata, nil).MinTimes(0)
			if tt.defaultProject != "" {
				bqClient.EXPECT().GetDefaultProject().Return(tt.defaultProject).AnyTimes()
			}
			logger := logrus.New()
			logger.SetLevel(logrus.DebugLevel)
			p := source.NewProjectWithBQClient("/", bqClient, logger)

			files, path, position, err := helper.GetUTF16LspPosition(tt.files)
			if err != nil {
				t.Fatalf("failed to get position: %v", err)
			}

			for uri, content := range files {
				p.UpdateFile(uri, content, 1)
			}

			got, err := p.LookupIdent(context.Background(), path, position)
			if !errors.Is(err, tt.expectErr) {
				t.Fatalf("got error %v, but want %v", err, tt.expectErr)
			}

			if diff := cmp.Diff(tt.expectLocations, got); diff != "" {
				t.Errorf("project.TermDocument result diff (-expect, +got)\n%s", diff)
			}
		})
	}
}
