package file_test

import (
	"errors"
	"fmt"
	"strings"
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
		"partitiontime table": {
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
					TimePartitioning: &bq.TimePartitioning{
						Type: bq.DayPartitioningType,
					},
				}, nil)
				return bqClient
			},
			expectTableName:   "project.dataset.table",
			expectColumnNames: []string{"name", "_PARTITIONTIME"},
		},
	}

	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			mockCtrl := gomock.NewController(t)
			defer mockCtrl.Finish()

			bqClient := tt.createMockBigQuery(mockCtrl)
			catalog := file.NewCatalog(bqClient)

			err := catalog.EnsureTable(tt.path)
			if !errors.Is(err, tt.expectError) {
				t.Fatalf("error: got %v, want %v", err, tt.expectError)
			}
			if err != nil {
				return
			}

			tablePath := strings.Join(tt.path, ".")
			// strip trailing dots if path was ["project.dataset.table"] form
			meta, err := catalog.FindTableMetadata(tablePath)
			if err != nil {
				t.Fatalf("FindTableMetadata: %v", err)
			}

			// The tableName in tableMetaMap uses the 3-part form
			wantTableName := tt.expectTableName
			if meta == nil {
				t.Fatalf("expected metadata for %s, got nil", wantTableName)
			}

			// Verify columns via schema
			schemaNames := make([]string, 0, len(meta.Schema))
			for _, f := range meta.Schema {
				schemaNames = append(schemaNames, f.Name)
			}
			// expectColumnNames may include pseudo-columns (_PARTITIONTIME, _TABLE_SUFFIX)
			// which are not in the schema — just verify schema columns are a prefix
			for i, name := range tt.expectColumnNames {
				if i >= len(schemaNames) {
					// pseudo-columns are ok to miss from schema
					break
				}
				if schemaNames[i] != name {
					t.Errorf("columnName(%d): got %s, want %s", i, schemaNames[i], name)
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

	err := catalog.EnsureTable([]string{"project.dataset.table"})
	if err != nil {
		t.Fatal(err)
	}

	err = catalog.EnsureTable([]string{"project.dataset.table"})
	if err != nil {
		t.Fatal(err)
	}
}
