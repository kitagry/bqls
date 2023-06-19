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
)

func TestProject_Complete(t *testing.T) {
	tests := map[string]struct {
		files           map[string]string
		supportSunippet bool
		bqTableMetadata *bq.TableMetadata

		expectCompletionItems []lsp.CompletionItem
		expectErr             error
	}{
		"Select columns with supportSunippet is false": {
			files: map[string]string{
				"file1.sql": "SELECT id, | FROM `project.dataset.table`",
			},
			supportSunippet: false,
			bqTableMetadata: &bq.TableMetadata{
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
			bqTableMetadata: &bq.TableMetadata{
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
	}

	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			bqClient := mock_bigquery.NewMockClient(ctrl)
			bqClient.EXPECT().GetTableMetadata(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(tt.bqTableMetadata, nil).MinTimes(0)
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
