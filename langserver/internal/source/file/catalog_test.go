package file_test

import (
	"errors"
	"fmt"
	"testing"

	bq "cloud.google.com/go/bigquery"
	"github.com/golang/mock/gomock"
	"github.com/kitagry/bqls/langserver/internal/bigquery"
	"github.com/kitagry/bqls/langserver/internal/bigquery/mock_bigquery"
	"github.com/kitagry/bqls/langserver/internal/source/file"
)

func TestCatalog_AddTable(t *testing.T) {
	dummyErr := fmt.Errorf("dummy error")
	tests := map[string]struct {
		path               []string
		createMockBigQuery func(ctrl *gomock.Controller) bigquery.Client

		expectError       error
		expectTableName   string
		expectColumnNames []string
	}{
		"Normal case": {
			path: []string{"project.dataset.table"},
			createMockBigQuery: func(ctrl *gomock.Controller) bigquery.Client {
				bqClient := mock_bigquery.NewMockClient(ctrl)
				bqClient.EXPECT().GetTableMetadata(gomock.Any(), "project", "dataset", "table").Return(&bq.TableMetadata{
					Schema: bq.Schema{
						{
							Name: "name",
							Type: bq.StringFieldType,
						},
					},
				}, nil)
				return bqClient
			},

			expectTableName:   "project.dataset.table",
			expectColumnNames: []string{"name"},
		},
		"Expect error": {
			path: []string{"project.dataset.table"},
			createMockBigQuery: func(ctrl *gomock.Controller) bigquery.Client {
				bqClient := mock_bigquery.NewMockClient(ctrl)
				bqClient.EXPECT().GetTableMetadata(gomock.Any(), "project", "dataset", "table").Return(nil, dummyErr)
				return bqClient
			},

			expectError: dummyErr,
		},
		"Wildcard table": {
			path: []string{"project.dataset.table*"},
			createMockBigQuery: func(ctrl *gomock.Controller) bigquery.Client {
				bqClient := mock_bigquery.NewMockClient(ctrl)
				bqClient.EXPECT().GetTableMetadata(gomock.Any(), "project", "dataset", "table*").Return(&bq.TableMetadata{
					Schema: bq.Schema{
						{
							Name: "name",
							Type: bq.StringFieldType,
						},
					},
				}, nil)
				return bqClient
			},
			expectTableName:   "project.dataset.table*",
			expectColumnNames: []string{"name", "_TABLE_SUFFIX"},
		},
	}

	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			mockCtrl := gomock.NewController(t)
			defer mockCtrl.Finish()

			bqClient := tt.createMockBigQuery(mockCtrl)
			catalog := file.NewCatalog(bqClient)

			got, err := catalog.FindTable(tt.path)
			if !errors.Is(err, tt.expectError) {
				t.Fatalf("error: got %v, want %v", err, tt.expectError)
			}
			if err != nil {
				return
			}

			if got.Name() != tt.expectTableName {
				t.Errorf("tableName: got %s, want %s", got.Name(), tt.expectTableName)
			}

			for i, name := range tt.expectColumnNames {
				got := got.Column(i).Name()
				if got != name {
					t.Errorf("columnName(%d): got %s, want %s", i, got, name)
				}
			}
		})
	}
}

func TestCatalog_shouldNotGetSchemaTwice(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	bqClient := mock_bigquery.NewMockClient(ctrl)
	bqClient.EXPECT().GetTableMetadata(gomock.Any(), "project", "dataset", "table").Return(&bq.TableMetadata{
		Schema: bq.Schema{
			{
				Name: "name",
				Type: bq.StringFieldType,
			},
		},
	}, nil).Times(1)
	catalog := file.NewCatalog(bqClient)

	_, err := catalog.FindTable([]string{"project.dataset.table"})
	if err != nil {
		t.Fatal(err)
	}

	_, err = catalog.FindTable([]string{"project.dataset.table"})
	if err != nil {
		t.Fatal(err)
	}
}
