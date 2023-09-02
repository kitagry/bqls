package completion

import (
	"context"
	"testing"

	bq "cloud.google.com/go/bigquery"
	"github.com/golang/mock/gomock"
	"github.com/google/go-cmp/cmp"
	"github.com/kitagry/bqls/langserver/internal/bigquery"
	"github.com/kitagry/bqls/langserver/internal/bigquery/mock_bigquery"
	"github.com/kitagry/bqls/langserver/internal/source/file"
	"github.com/kitagry/bqls/langserver/internal/source/helper"
	"github.com/sirupsen/logrus"
)

func TestCompletor_CompleteBuiltinFunction(t *testing.T) {
	tests := map[string]struct {
		files                  map[string]string
		bigqueryClientMockFunc func(t *testing.T) bigquery.Client

		expectCompletionItems []CompletionItem
	}{
		"Don't complete in table completion": {
			files: map[string]string{
				"file1.sql": "SELECT FROM `bigquery-public-data.samples.|`\n",
			},
			bigqueryClientMockFunc: func(t *testing.T) bigquery.Client {
				ctrl := gomock.NewController(t)
				bqClient := mock_bigquery.NewMockClient(ctrl)

				bqClient.EXPECT().GetTableMetadata(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(&bq.TableMetadata{}, nil).MinTimes(0)
				return bqClient
			},
			expectCompletionItems: []CompletionItem{}, // empty
		},
	}

	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			bqClient := tt.bigqueryClientMockFunc(t)
			logger := logrus.New()
			logger.SetLevel(logrus.DebugLevel)

			analyzer := file.NewAnalyzer(logger, bqClient)
			completor := New(logger, analyzer, bqClient)

			files, path, position, err := helper.GetLspPosition(tt.files)
			if err != nil {
				t.Fatal(err)
			}

			parsedFile := analyzer.ParseFile(path, files[path])

			got := completor.completeBuiltinFunction(context.Background(), parsedFile, position)
			if diff := cmp.Diff(got, tt.expectCompletionItems); diff != "" {
				t.Errorf("(-got, +want)\n%s", diff)
			}
		})
	}
}
