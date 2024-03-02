package completion

import (
	"context"
	"errors"
	"fmt"
	"testing"

	bq "cloud.google.com/go/bigquery"
	"github.com/golang/mock/gomock"
	"github.com/google/go-cmp/cmp"
	"github.com/kitagry/bqls/langserver/internal/bigquery"
	"github.com/kitagry/bqls/langserver/internal/bigquery/mock_bigquery"
	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/kitagry/bqls/langserver/internal/source/file"
	"github.com/kitagry/bqls/langserver/internal/source/helper"
	"github.com/sirupsen/logrus"
	"google.golang.org/api/cloudresourcemanager/v1"
)

func TestProject_CompleteTablePath(t *testing.T) {
	tests := map[string]struct {
		files                  map[string]string
		bigqueryClientMockFunc func(t *testing.T) bigquery.Client

		expectCompletionItems []CompletionItem
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
			expectCompletionItems: []CompletionItem{
				{
					Kind:    lsp.CIKModule,
					NewText: "1table",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "project.dataset.1table",
					},
				},
				{
					Kind:    lsp.CIKModule,
					NewText: "2table",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "project.dataset.2table",
					},
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
						TableID:   "table20230622",
					},
				}, nil)
				bqClient.EXPECT().GetDefaultProject().Return("").MinTimes(0)
				bqClient.EXPECT().GetTableMetadata(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, fmt.Errorf("not found")).MinTimes(0)
				return bqClient
			},
			expectCompletionItems: []CompletionItem{
				{
					Kind:    lsp.CIKModule,
					NewText: "table20230622",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "project.dataset.table20230622",
					},
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
			expectCompletionItems: []CompletionItem{
				{
					Kind:    lsp.CIKModule,
					NewText: "dataset1",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "project.dataset1",
					},
				},
				{
					Kind:    lsp.CIKModule,
					NewText: "dataset2",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "project.dataset2",
					},
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
			expectCompletionItems: []CompletionItem{
				{
					Kind:    lsp.CIKModule,
					NewText: "project1",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "project name",
					},
					TypedPrefix: "p",
				},
				{
					Kind:    lsp.CIKModule,
					NewText: "project2",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "project name",
					},
					TypedPrefix: "p",
				},
			},
		},
	}

	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			bqClient := tt.bigqueryClientMockFunc(t)
			logger := logrus.New()
			logger.SetLevel(logrus.DebugLevel)

			analyzer := file.NewAnalyzer(logger, bqClient)
			completor := New(logrus.New(), analyzer, bqClient)

			files, path, position, err := helper.GetLspPosition(tt.files)
			if err != nil {
				t.Fatal(err)
			}

			parsedFile := analyzer.ParseFile(path, files[path])

			got, err := completor.completeTablePath(context.Background(), parsedFile, position)
			if !errors.Is(err, tt.expectErr) {
				t.Fatalf("got error %v, but want %v", err, tt.expectErr)
			}

			if diff := cmp.Diff(got, tt.expectCompletionItems); diff != "" {
				t.Errorf("(-got, +want)\n%s", diff)
			}
		})
	}
}
