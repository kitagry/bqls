package source_test

import (
	"strings"
	"testing"

	bq "cloud.google.com/go/bigquery"
	"github.com/golang/mock/gomock"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/kitagry/bqls/langserver/internal/bigquery/mock_bigquery"
	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/kitagry/bqls/langserver/internal/source"
)

func TestProject_ParseFile(t *testing.T) {
	tests := map[string]struct {
		file               string
		bqTableMetadataMap map[string]*bq.TableMetadata

		expectedErrs []source.Error
	}{
		"parse SELECT list must not be empty error file": {
			file: "SELECT FROM `project.dataset.table`",
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {
					Schema: bq.Schema{
						{
							Name: "id",
							Type: bq.IntegerFieldType,
						},
					},
				},
			},
			expectedErrs: []source.Error{
				{
					Msg: "INVALID_ARGUMENT: Syntax error: SELECT list must not be empty",
					Position: lsp.Position{
						Line:      0,
						Character: 7,
					},
					TermLength: 0,
				},
			},
		},
		"parse unrecognized file": {
			file: "SELECT unexist_column FROM `project.dataset.table`",
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {
					Schema: bq.Schema{
						{
							Name: "id",
							Type: bq.IntegerFieldType,
						},
					},
				},
			},
			expectedErrs: []source.Error{
				{
					Msg: "INVALID_ARGUMENT: Unrecognized name: unexist_column",
					Position: lsp.Position{
						Line:      0,
						Character: 7,
					},
					TermLength: 14,
				},
			},
		},
		"parse struct does not exist file": {
			file: "SELECT param.unexist_column FROM `project.dataset.table`",
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {
					Schema: bq.Schema{
						{
							Name: "param",
							Type: bq.RecordFieldType,
							Schema: bq.Schema{
								{
									Name: "id",
									Type: bq.IntegerFieldType,
								},
							},
						},
					},
				},
			},
			expectedErrs: []source.Error{
				{
					Msg: "INVALID_ARGUMENT: Field name unexist_column does not exist in STRUCT<id INT64>",
					Position: lsp.Position{
						Line:      0,
						Character: 13,
					},
					TermLength: 14,
				},
			},
		},
		"parse struct does not exist file with other column": {
			file: "SELECT param.unexist_column, param.id FROM `project.dataset.table`",
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {
					Schema: bq.Schema{
						{
							Name: "param",
							Type: bq.RecordFieldType,
							Schema: bq.Schema{
								{
									Name: "id",
									Type: bq.IntegerFieldType,
								},
							},
						},
					},
				},
			},
			expectedErrs: []source.Error{
				{
					Msg: "INVALID_ARGUMENT: Field name unexist_column does not exist in STRUCT<id INT64>",
					Position: lsp.Position{
						Line:      0,
						Character: 13,
					},
					TermLength: 14,
				},
			},
		},
		"parse not found inside table error file": {
			file: "SELECT t.unexist_column FROM `project.dataset.table` t",
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {
					Schema: bq.Schema{
						{
							Name: "id",
							Type: bq.IntegerFieldType,
						},
					},
				},
			},
			expectedErrs: []source.Error{
				{
					Msg: "INVALID_ARGUMENT: Name unexist_column not found inside t",
					Position: lsp.Position{
						Line:      0,
						Character: 9,
					},
					TermLength: 14,
				},
			},
		},
		"parse not found inside table error file with other correct column": {
			file: "SELECT t.unexist_column,\nt.id FROM `project.dataset.table` t",
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {
					Schema: bq.Schema{
						{
							Name: "id",
							Type: bq.IntegerFieldType,
						},
					},
				},
			},
			expectedErrs: []source.Error{
				{
					Msg: "INVALID_ARGUMENT: Name unexist_column not found inside t",
					Position: lsp.Position{
						Line:      0,
						Character: 9,
					},
					TermLength: 14,
				},
			},
		},
		"parse dot file": {
			file: "SELECT t. FROM `project.dataset.table` t",
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {
					Schema: bq.Schema{
						{
							Name: "id",
							Type: bq.IntegerFieldType,
						},
					},
				},
			},
			expectedErrs: []source.Error{
				{
					Msg: "INVALID_ARGUMENT: Unrecognized name: t.",
					Position: lsp.Position{
						Line:      0,
						Character: 7,
					},
					TermLength: 2,
				},
			},
		},
	}

	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			bqClient := mock_bigquery.NewMockClient(ctrl)
			for tablePath, schema := range tt.bqTableMetadataMap {
				tablePathSplitted := strings.Split(tablePath, ".")
				if len(tablePathSplitted) != 3 {
					t.Fatalf("table path length should be 3, got %s", tablePath)
				}
				bqClient.EXPECT().GetTableMetadata(gomock.Any(), tablePathSplitted[0], tablePathSplitted[1], tablePathSplitted[2]).Return(schema, nil).MinTimes(0)
			}
			p := source.NewProjectWithBQClient("/", bqClient)

			got := p.ParseFile("uri", tt.file)
			if diff := cmp.Diff(tt.expectedErrs, got.Errors, cmpopts.IgnoreUnexported()); diff != "" {
				t.Errorf("ParseFile result diff (-expect, +got)\n%s", diff)
			}

			if len(got.RNode) == 0 {
				t.Errorf("failed to parse")
			}
		})
	}
}
