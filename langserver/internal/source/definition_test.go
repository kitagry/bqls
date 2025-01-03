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
				FullID: "project.dataset.table",
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
				FullID: "project.dataset.table",
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
