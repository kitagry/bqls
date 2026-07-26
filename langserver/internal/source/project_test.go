package source_test

import (
	"testing"

	bq "cloud.google.com/go/bigquery"
	"github.com/golang/mock/gomock"
	"github.com/kitagry/bqls/langserver/internal/bigquery/mock_bigquery"
	"github.com/kitagry/bqls/langserver/internal/source"
	"github.com/sirupsen/logrus"
)

func TestProject_GetTableDetails(t *testing.T) {
	tests := map[string]struct {
		tableMetadata *bq.TableMetadata
		expectPreview bool
	}{
		"regular table has a preview": {
			tableMetadata: &bq.TableMetadata{
				FullID: "p:d.t",
				Type:   bq.RegularTable,
				Schema: bq.Schema{{Name: "id", Type: bq.IntegerFieldType}},
			},
			expectPreview: true,
		},
		"view table has no preview": {
			tableMetadata: &bq.TableMetadata{
				FullID: "p:d.t",
				Type:   bq.ViewTable,
				Schema: bq.Schema{{Name: "id", Type: bq.IntegerFieldType}},
			},
			expectPreview: false,
		},
	}

	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			bqClient := mock_bigquery.NewMockClient(ctrl)
			bqClient.EXPECT().GetTableMetadata(gomock.Any(), "p", "d", "t").Return(tt.tableMetadata, nil)
			if tt.expectPreview {
				bqClient.EXPECT().GetTableRecord(gomock.Any(), "p", "d", "t").Return(&bq.RowIterator{}, nil)
			}

			logger := logrus.New()
			p := source.NewProjectWithBQClient("/", bqClient, logger)

			contents, schema, fetchPreview, err := p.GetTableDetails(t.Context(), "p", "d", "t")
			if err != nil {
				t.Fatalf("GetTableDetails() error = %v", err)
			}
			if len(contents) == 0 {
				t.Errorf("GetTableDetails() contents is empty, want table metadata markdown")
			}
			if len(schema) != 1 || schema[0].Name != "id" {
				t.Errorf("GetTableDetails() schema = %+v, want a single FieldSchema for column id", schema)
			}

			it, err := fetchPreview(t.Context())
			if err != nil {
				t.Fatalf("fetchPreview() error = %v", err)
			}
			if tt.expectPreview && it == nil {
				t.Errorf("fetchPreview() = nil, want a non-nil iterator for a regular table")
			}
			if !tt.expectPreview && it != nil {
				t.Errorf("fetchPreview() = non-nil, want nil (not a regular table)")
			}
		})
	}
}

func TestProject_GetJobDetails(t *testing.T) {
	ctrl := gomock.NewController(t)
	bqClient := mock_bigquery.NewMockClient(ctrl)
	job := mock_bigquery.NewMockBigqueryJob(ctrl)

	bqClient.EXPECT().JobFromProject(gomock.Any(), "p", "j", "US").Return(job, nil)
	job.EXPECT().LastStatus().Return(&bq.JobStatus{
		State:      bq.Done,
		Statistics: &bq.JobStatistics{},
	}).AnyTimes()
	job.EXPECT().ID().Return("j").AnyTimes()
	job.EXPECT().URL().Return("https://console.cloud.google.com/bigquery").AnyTimes()
	job.EXPECT().Config().Return(&bq.QueryConfig{Q: "SELECT 1"}, nil).AnyTimes()
	job.EXPECT().Read(gomock.Any()).Return(&bq.RowIterator{}, nil)

	logger := logrus.New()
	p := source.NewProjectWithBQClient("/", bqClient, logger)

	contents, fetchPreview, err := p.GetJobDetails(t.Context(), "p", "j", "US")
	if err != nil {
		t.Fatalf("GetJobDetails() error = %v", err)
	}
	if len(contents) == 0 {
		t.Errorf("GetJobDetails() contents is empty, want job info markdown")
	}

	it, err := fetchPreview(t.Context())
	if err != nil {
		t.Fatalf("fetchPreview() error = %v", err)
	}
	if it == nil {
		t.Errorf("fetchPreview() = nil, want a non-nil iterator")
	}
}
