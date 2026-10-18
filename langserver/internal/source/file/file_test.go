package file_test

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
	"testing"

	bq "cloud.google.com/go/bigquery"
	googlesql "github.com/goccy/go-googlesql"
	"github.com/golang/mock/gomock"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/kitagry/bqls/langserver/internal/bigquery"
	"github.com/kitagry/bqls/langserver/internal/bigquery/mock_bigquery"
	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/kitagry/bqls/langserver/internal/source/file"
	"github.com/sirupsen/logrus"
	"google.golang.org/api/googleapi"
)

func TestProject_ParseFile(t *testing.T) {
	tests := map[string]struct {
		file               string
		bqTableMetadataMap map[string]*bq.TableMetadata

		expectedErrs []file.Error
	}{
		"snapshot timestamp parameter": {
			file: "SELECT @cut AS source_cut, t.id FROM `project.dataset.table` t FOR SYSTEM_TIME AS OF @cut " +
				"JOIN `project.dataset.table` u FOR SYSTEM_TIME AS OF @CUT ON t.id = u.id " +
				"JOIN `project.dataset.table` v FOR SYSTEM_TIME AS OF @`É` ON t.id = v.id " +
				"JOIN `project.dataset.table` w FOR SYSTEM_TIME AS OF @`é` ON t.id = w.id",
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {
					Schema: bq.Schema{{Name: "id", Type: bq.IntegerFieldType}},
				},
			},
			expectedErrs: []file.Error{},
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
			expectedErrs: []file.Error{
				{
					Msg: "Unrecognized name: t.",
					Position: lsp.Position{
						Line:      0,
						Character: 7,
					},
					TermLength:           2,
					IncompleteColumnName: "t.",
				},
			},
		},
		"parse 2 dot file": {
			file: "SELECT t.record. FROM `project.dataset.table` t",
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {
					Schema: bq.Schema{
						{
							Name: "record",
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
			expectedErrs: []file.Error{
				{
					Msg: "Unrecognized name: t.record.",
					Position: lsp.Position{
						Line:      0,
						Character: 7,
					},
					TermLength:           9,
					IncompleteColumnName: "t.record.",
				},
			},
		},
		"ignore dot comment": {
			file:               "SELECT 1; -- last dot comment t.",
			bqTableMetadataMap: map[string]*bq.TableMetadata{},
			expectedErrs:       []file.Error{},
		},
		"parse dot in where clause": {
			file: "SELECT * FROM `project.dataset.table` t\nWHERE t.",
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
			expectedErrs: []file.Error{
				{
					Msg: "Unrecognized name: t.",
					Position: lsp.Position{
						Line:      1,
						Character: 6,
					},
					TermLength:           2,
					IncompleteColumnName: "t.",
				},
			},
		},
		"report error after a rewritten WHERE condition in the original source": {
			file: "SELECT * FROM `project.dataset.table` WHERE idx; SELECT idy FROM `project.dataset.table`",
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {
					Schema: bq.Schema{{Name: "id", Type: bq.IntegerFieldType}},
				},
			},
			expectedErrs: []file.Error{
				{
					Msg: "Unrecognized name: idx; Did you mean id?",
					Position: lsp.Position{
						Line:      0,
						Character: 44,
					},
					TermLength:           3,
					IncompleteColumnName: "idx",
				},
				{
					Msg: "Unrecognized name: idy; Did you mean id?",
					Position: lsp.Position{
						Line:      0,
						Character: 56,
					},
					TermLength:           3,
					IncompleteColumnName: "idy",
				},
			},
		},
		"parse non-bool column in where clause": {
			file: "SELECT * FROM `project.dataset.table` WHERE id",
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
			expectedErrs: []file.Error{
				{
					Msg: "WHERE clause should return type BOOL, but returns INT64",
					Position: lsp.Position{
						Line:      0,
						Character: 44,
					},
					TermLength:           2,
					IncompleteColumnName: "id",
				},
			},
		},
		"parse non-bool column in join on clause": {
			file: "SELECT * FROM `project.dataset.table` t1 JOIN `project.dataset.table` t2 ON t1.id",
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
			expectedErrs: []file.Error{
				{
					Msg: "JOIN ON clause should return type BOOL, but returns INT64",
					Position: lsp.Position{
						Line:      0,
						Character: 76,
					},
					TermLength:           5,
					IncompleteColumnName: "t1.id",
				},
			},
		},
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
			expectedErrs: []file.Error{
				{
					Msg: "Syntax error: SELECT list must not be empty",
					Position: lsp.Position{
						Line:      0,
						Character: 7,
					},
					TermLength: 0,
				},
			},
		},
		"parse Unexpected end of script error with WHERE file": {
			file: "SELECT * FROM `project.dataset.table` WHERE ",
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
			expectedErrs: []file.Error{
				{
					Msg: "Syntax error: Unexpected end of script",
					Position: lsp.Position{
						Line:      0,
						Character: 43,
					},
					TermLength: 0,
				},
			},
		},
		"parse Unexpected end of script error with GROUP BY file": {
			file: "SELECT * FROM `project.dataset.table` GROUP BY",
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
			expectedErrs: []file.Error{
				{
					Msg: "Syntax error: Unexpected end of script",
					Position: lsp.Position{
						Line:      0,
						Character: 46,
					},
					TermLength: 0,
				},
			},
		},
		"parse unrecognized name in select clause": {
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
			expectedErrs: []file.Error{
				{
					Msg: "Unrecognized name: unexist_column",
					Position: lsp.Position{
						Line:      0,
						Character: 7,
					},
					TermLength:           14,
					IncompleteColumnName: "unexist_column",
				},
			},
		},
		"parse only unrecognized name in where clause": {
			file: "SELECT id FROM `project.dataset.table` WHERE unexist_column",
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
			expectedErrs: []file.Error{
				{
					Msg: "Unrecognized name: unexist_column",
					Position: lsp.Position{
						Line:      0,
						Character: 45,
					},
					TermLength:           14,
					IncompleteColumnName: "unexist_column",
				},
			},
		},
		"parse unrecognized name with binary expression in where clause": {
			file: "SELECT id FROM `project.dataset.table` WHERE unexist_column = 1",
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
			expectedErrs: []file.Error{
				{
					Msg: "Unrecognized name: unexist_column",
					Position: lsp.Position{
						Line:      0,
						Character: 45,
					},
					TermLength:           14,
					IncompleteColumnName: "unexist_column",
				},
			},
		},
		"parse unrecognized name in GROUP BY clause": {
			file: "SELECT * FROM `project.dataset.table` GROUP BY unexist_column",
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
			expectedErrs: []file.Error{
				{
					Msg: "Unrecognized name: unexist_column",
					Position: lsp.Position{
						Line:      0,
						Character: 47,
					},
					TermLength:           14,
					IncompleteColumnName: "unexist_column",
				},
			},
		},
		"parse unrecognized file with recommend": {
			file: "SELECT timestam FROM `project.dataset.table`",
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {
					Schema: bq.Schema{
						{
							Name: "timestamp",
							Type: bq.TimestampFieldType,
						},
					},
				},
			},
			expectedErrs: []file.Error{
				{
					Msg: "Unrecognized name: timestam; Did you mean timestamp?",
					Position: lsp.Position{
						Line:      0,
						Character: 7,
					},
					TermLength:           8,
					IncompleteColumnName: "timestam",
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
			expectedErrs: []file.Error{
				{
					Msg: "Field name unexist_column does not exist in STRUCT<id INT64>",
					Position: lsp.Position{
						Line:      0,
						Character: 13,
					},
					TermLength:           14,
					IncompleteColumnName: "param.unexist_column",
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
			expectedErrs: []file.Error{
				{
					Msg: "Field name unexist_column does not exist in STRUCT<id INT64>",
					Position: lsp.Position{
						Line:      0,
						Character: 13,
					},
					TermLength:           14,
					IncompleteColumnName: "param.unexist_column",
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
			expectedErrs: []file.Error{
				{
					Msg: "Name unexist_column not found inside t",
					Position: lsp.Position{
						Line:      0,
						Character: 9,
					},
					TermLength:           14,
					IncompleteColumnName: "t.unexist_column",
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
			expectedErrs: []file.Error{
				{
					Msg: "Name unexist_column not found inside t",
					Position: lsp.Position{
						Line:      0,
						Character: 9,
					},
					TermLength:           14,
					IncompleteColumnName: "t.unexist_column",
				},
			},
		},
		"parse not found inside table error file after tab": {
			file: "SELECT 'a',\tt.unexist_column, id FROM `project.dataset.table` t",
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
			expectedErrs: []file.Error{
				{
					Msg: "Name unexist_column not found inside t",
					Position: lsp.Position{
						Line:      0,
						Character: 14,
					},
					TermLength:           14,
					IncompleteColumnName: "t.unexist_column",
				},
			},
		},
		"parse not found inside table error file after newline": {
			file: "SELECT id,\n  'a',\nt.unexist_column FROM `project.dataset.table` t",
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
			expectedErrs: []file.Error{
				{
					Msg: "Name unexist_column not found inside t",
					Position: lsp.Position{
						Line:      2,
						Character: 2,
					},
					TermLength:           14,
					IncompleteColumnName: "t.unexist_column",
				},
			},
		},
		"parse not found inside table error file in where clause": {
			file: "SELECT * FROM `project.dataset.table` t\nWHERE t.unexist_column",
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
			expectedErrs: []file.Error{
				{
					Msg: "Name unexist_column not found inside t",
					Position: lsp.Position{
						Line:      1,
						Character: 8,
					},
					TermLength:           14,
					IncompleteColumnName: "t.unexist_column",
				},
			},
		},
		"parse not found inside table error file in where clause2": {
			file: "SELECT * FROM `project.dataset.table` t\nWHERE t.unexist_column",
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
			expectedErrs: []file.Error{
				{
					Msg: "Name unexist_column not found inside t",
					Position: lsp.Position{
						Line:      1,
						Character: 8,
					},
					TermLength:           14,
					IncompleteColumnName: "t.unexist_column",
				},
			},
		},
		"parse incomplete join columns": {
			file: "SELECT * FROM `project.dataset.table` t1\nJOIN `project.dataset.table` t2 ON t",
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
			expectedErrs: []file.Error{
				{
					Msg: "Unrecognized name: t",
					Position: lsp.Position{
						Line:      1,
						Character: 35,
					},
					TermLength:           1,
					IncompleteColumnName: "t",
				},
			},
		},
		"parse incomplete join columns2": {
			file: "SELECT * FROM `project.dataset.table` t1\nJOIN `project.dataset.table` t2 ON t1.i",
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
			expectedErrs: []file.Error{
				{
					Msg: "Name i not found inside t1",
					Position: lsp.Position{
						Line:      1,
						Character: 38,
					},
					TermLength:           1,
					IncompleteColumnName: "t1.i",
				},
			},
		},
		"parse incomplete join columns3": {
			file: "SELECT * FROM `project.dataset.table` t1\nJOIN `project.dataset.table` t2 ON t1.id = t",
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
			expectedErrs: []file.Error{
				{
					Msg: "Unrecognized name: t",
					Position: lsp.Position{
						Line:      1,
						Character: 43,
					},
					TermLength:           1,
					IncompleteColumnName: "t",
				},
			},
		},
		"parse incomplete join columns4": {
			file: "SELECT * FROM `project.dataset.table` t1\nJOIN `project.dataset.table` t2 ON t1.id = t2.i",
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
			expectedErrs: []file.Error{
				{
					Msg: "Name i not found inside t2",
					Position: lsp.Position{
						Line:      1,
						Character: 46,
					},
					TermLength:           1,
					IncompleteColumnName: "t2.i",
				},
			},
		},
		"parse incomplete join columns5": {
			file: "SELECT * FROM `project.dataset.table` t1\nJOIN `project.dataset.table` t2 ON",
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
			expectedErrs: []file.Error{
				{
					Msg: "Syntax error: Unexpected end of script",
					Position: lsp.Position{
						Line:      1,
						Character: 34,
					},
					TermLength:           0,
					IncompleteColumnName: "",
				},
			},
		},
		"parse QUALIFY statement": {
			file: "SELECT * FROM `project.dataset.table` t1\nQUALIFY RANK() OVER (PARTITION BY city ORDER BY temperature) = 1",
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {
					Schema: bq.Schema{
						{
							Name: "city",
							Type: bq.StringFieldType,
						},
						{
							Name: "temperature",
							Type: bq.IntegerFieldType,
						},
					},
				},
			},
			expectedErrs: []file.Error{},
		},
		"parse without backtick": {
			file: "SELECT * FROM project.dataset.table",
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
			expectedErrs: []file.Error{},
		},
		"pipe operator without operation": {
			file: "FROM `project.dataset.table` |> ",
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {
					Schema: bq.Schema{{Name: "x", Type: bq.IntegerFieldType}},
				},
			},
			expectedErrs: []file.Error{
				{
					Msg:      "Syntax error: Expected keyword JOIN but got end of script",
					Position: lsp.Position{Line: 0, Character: 31},
				},
			},
		},
		"pipe operator without operation on a new line": {
			file: "FROM `project.dataset.table`\n|> ",
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {
					Schema: bq.Schema{{Name: "x", Type: bq.IntegerFieldType}},
				},
			},
			expectedErrs: []file.Error{
				{
					Msg:      "Syntax error: Expected keyword JOIN but got end of script",
					Position: lsp.Position{Line: 1, Character: 2},
				},
			},
		},
		"pipe operator after another pipe operator": {
			file: "FROM `project.dataset.table` |> SELECT x |> ",
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {
					Schema: bq.Schema{{Name: "x", Type: bq.IntegerFieldType}},
				},
			},
			expectedErrs: []file.Error{
				{
					Msg:      "Syntax error: Expected keyword JOIN but got end of script",
					Position: lsp.Position{Line: 0, Character: 43},
				},
			},
		},
		"pipe SELECT without columns": {
			file: "FROM `project.dataset.table` |> SELECT ",
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {
					Schema: bq.Schema{{Name: "x", Type: bq.IntegerFieldType}},
				},
			},
			expectedErrs: []file.Error{
				{
					Msg:      "Syntax error: Unexpected end of script",
					Position: lsp.Position{Line: 0, Character: 38},
				},
			},
		},
		"pipe EXTEND without columns": {
			file: "FROM `project.dataset.table` |> EXTEND ",
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {
					Schema: bq.Schema{{Name: "x", Type: bq.IntegerFieldType}},
				},
			},
			expectedErrs: []file.Error{
				{
					Msg:      "Syntax error: Unexpected end of script",
					Position: lsp.Position{Line: 0, Character: 38},
				},
			},
		},
		"pipe JOIN without table": {
			file: "FROM `project.dataset.table` |> JOIN ",
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {
					Schema: bq.Schema{{Name: "x", Type: bq.IntegerFieldType}},
				},
			},
			expectedErrs: []file.Error{
				{
					Msg:      "Syntax error: Unexpected end of script",
					Position: lsp.Position{Line: 0, Character: 36},
				},
			},
		},
		"pipe ORDER BY without columns": {
			file: "FROM `project.dataset.table` |> WHERE x = 1 |> ORDER BY ",
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {
					Schema: bq.Schema{{Name: "x", Type: bq.IntegerFieldType}},
				},
			},
			expectedErrs: []file.Error{
				{
					Msg:      "Syntax error: Unexpected end of script",
					Position: lsp.Position{Line: 0, Character: 55},
				},
			},
		},
		"pipe GROUP BY without AGGREGATE": {
			file: "FROM `project.dataset.table` |> GROUP BY ",
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {
					Schema: bq.Schema{{Name: "x", Type: bq.IntegerFieldType}},
				},
			},
			expectedErrs: []file.Error{
				{
					Msg:      "Syntax error: GROUP BY should be part of a pipe AGGREGATE operator, without a leading pipe symbol",
					Position: lsp.Position{Line: 0, Character: 32},
				},
			},
		},
		"pipe AGGREGATE without aggregations": {
			file: "FROM `project.dataset.table` |> AGGREGATE ",
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {
					Schema: bq.Schema{{Name: "x", Type: bq.IntegerFieldType}},
				},
			},
			expectedErrs: []file.Error{
				{
					Msg:      "Pipe AGGREGATE cannot have both an empty aggregate list and an empty GROUP BY",
					Position: lsp.Position{Line: 0, Character: 32},
				},
			},
		},
		"unknown column in GROUP BY of pipe AGGREGATE": {
			file: "FROM `project.dataset.table` |> AGGREGATE COUNT(*) GROUP BY unknown",
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {
					Schema: bq.Schema{{Name: "x", Type: bq.IntegerFieldType}},
				},
			},
			expectedErrs: []file.Error{
				{
					Msg:                  "Unrecognized name: unknown",
					Position:             lsp.Position{Line: 0, Character: 60},
					TermLength:           7,
					IncompleteColumnName: "unknown",
				},
			},
		},
		"unknown column in pipe ORDER BY": {
			file: "FROM `project.dataset.table` |> ORDER BY unknown",
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {
					Schema: bq.Schema{{Name: "x", Type: bq.IntegerFieldType}},
				},
			},
			expectedErrs: []file.Error{
				{
					Msg:                  "Unrecognized name: unknown",
					Position:             lsp.Position{Line: 0, Character: 41},
					TermLength:           7,
					IncompleteColumnName: "unknown",
				},
			},
		},
		"pipe WHERE without condition followed by a semicolon": {
			file: "FROM `project.dataset.table` |> WHERE;",
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {
					Schema: bq.Schema{{Name: "x", Type: bq.IntegerFieldType}},
				},
			},
			expectedErrs: []file.Error{
				{
					Msg:      "Syntax error: Unexpected \";\"",
					Position: lsp.Position{Line: 0, Character: 37},
				},
			},
		},
		"pipe AGGREGATE without aggregations followed by a semicolon": {
			file: "SELECT x FROM `project.dataset.table` |> AGGREGATE;",
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {
					Schema: bq.Schema{{Name: "x", Type: bq.IntegerFieldType}},
				},
			},
			expectedErrs: []file.Error{
				{
					Msg:      "Pipe AGGREGATE cannot have both an empty aggregate list and an empty GROUP BY",
					Position: lsp.Position{Line: 0, Character: 41},
				},
			},
		},
		"pipe SELECT without columns followed by another statement": {
			file: "FROM `project.dataset.table` |> SELECT\n;\nSELECT 1",
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {
					Schema: bq.Schema{{Name: "x", Type: bq.IntegerFieldType}},
				},
			},
			expectedErrs: []file.Error{
				{
					Msg:      "Syntax error: Unexpected \";\"",
					Position: lsp.Position{Line: 1, Character: 0},
				},
			},
		},
		"pipe SELECT without columns followed by a comment": {
			file: "FROM `project.dataset.table` |> SELECT -- comment",
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {
					Schema: bq.Schema{{Name: "x", Type: bq.IntegerFieldType}},
				},
			},
			expectedErrs: []file.Error{
				{
					Msg:      "Syntax error: Unexpected end of script",
					Position: lsp.Position{Line: 0, Character: 49},
				},
			},
		},
		"pipe WHERE without condition followed by another pipe operator": {
			file: "FROM `project.dataset.table` |> WHERE |> SELECT x",
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {
					Schema: bq.Schema{{Name: "x", Type: bq.IntegerFieldType}},
				},
			},
			expectedErrs: []file.Error{
				{
					Msg:      "Syntax error: Unexpected \"|>\"",
					Position: lsp.Position{Line: 0, Character: 38},
				},
			},
		},
		"pipe SELECT without columns in a subquery": {
			file: "SELECT * FROM (FROM `project.dataset.table` |> SELECT)",
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {
					Schema: bq.Schema{{Name: "x", Type: bq.IntegerFieldType}},
				},
			},
			expectedErrs: []file.Error{
				{
					Msg:      "Syntax error: Unexpected \")\"",
					Position: lsp.Position{Line: 0, Character: 53},
				},
			},
		},
		"parse with clause only": {
			file: "WITH t1 AS (SELECT * FROM `project.dataset.table` t1)\n",
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
			expectedErrs: []file.Error{
				{
					Msg: "Syntax error: Unexpected end of script",
					Position: lsp.Position{
						Line:      0,
						Character: 53,
					},
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
			logger := logrus.New()
			logger.SetLevel(logrus.DebugLevel)

			analyzer := file.NewAnalyzer(logger, bqClient)
			got := analyzer.ParseFile("uri", tt.file)
			defer got.Close()
			if diff := cmp.Diff(tt.expectedErrs, got.Errors, cmpopts.IgnoreUnexported()); diff != "" {
				t.Errorf("ParseFile result diff (-expect, +got)\n%s", diff)
			}

			if len(got.RNode) == 0 {
				t.Errorf("failed to parse")
			}
		})
	}
}

func TestAnalyzer_ParseFileWithPipeOperatorUnsupportedByBigQuery(t *testing.T) {
	ctrl := gomock.NewController(t)
	bqClient := mock_bigquery.NewMockClient(ctrl)
	analyzer := file.NewAnalyzer(logrus.New(), bqClient)

	got := analyzer.ParseFile("uri", "SELECT 1 AS x |> ASSERT x > 0")
	defer got.Close()

	want := []file.Error{
		{
			Msg:      "Pipe ASSERT not supported",
			Position: lsp.Position{Line: 0, Character: 14},
		},
	}
	if diff := cmp.Diff(want, got.Errors, cmpopts.IgnoreUnexported()); diff != "" {
		t.Errorf("ParseFile errors diff (-want, +got)\n%s", diff)
	}
}

func TestAnalyzer_ParseFileWithUnknownFieldOfDeclaredStruct(t *testing.T) {
	ctrl := gomock.NewController(t)
	bqClient := mock_bigquery.NewMockClient(ctrl)
	analyzer := file.NewAnalyzer(logrus.New(), bqClient)

	// Only the message is checked: positions after a substituted variable on
	// the same line are not remapped yet.
	parsed := analyzer.ParseFile("uri", "DECLARE s STRUCT<a INT64>;\nSELECT s.b")
	defer parsed.Close()
	if len(parsed.Errors) != 1 || parsed.Errors[0].Msg != "Field name b does not exist in STRUCT<a INT64>" {
		t.Fatalf("expected an unknown struct field error, got %v", parsed.Errors)
	}
}

func TestAnalyzer_SnapshotParameterPreservesTypeErrors(t *testing.T) {
	ctrl := gomock.NewController(t)
	bqClient := mock_bigquery.NewMockClient(ctrl)
	bqClient.EXPECT().GetTableMetadata(gomock.Any(), "project", "dataset", "table").
		Return(&bq.TableMetadata{Schema: bq.Schema{{Name: "id", Type: bq.IntegerFieldType}}}, nil).
		AnyTimes()
	analyzer := file.NewAnalyzer(logrus.New(), bqClient)

	// A snapshot parameter is a timestamp everywhere in the same statement;
	// using it as an integer must still fail.
	parsed := analyzer.ParseFile("uri", "SELECT * FROM `project.dataset.table` FOR SYSTEM_TIME AS OF @cut WHERE id = @cut")
	defer parsed.Close()
	if len(parsed.Errors) != 1 || !strings.Contains(parsed.Errors[0].Msg, "No matching signature for operator =") {
		t.Fatalf("expected incompatible parameter use to fail, got %v", parsed.Errors)
	}

	// A literal is not an unknown runtime parameter and must not be coerced.
	literal := analyzer.ParseFile("uri", "SELECT * FROM `project.dataset.table` FOR SYSTEM_TIME AS OF 1")
	defer literal.Close()
	if len(literal.Errors) != 1 || literal.Errors[0].Msg != "FOR SYSTEM_TIME AS OF must be of type TIMESTAMP but was of type INT64" {
		t.Fatalf("expected an invalid snapshot literal to fail, got %v", literal.Errors)
	}
}

func TestAnalyzer_NotBoolWhereClauseWithNonPathExpression(t *testing.T) {
	ctrl := gomock.NewController(t)
	bqClient := mock_bigquery.NewMockClient(ctrl)
	bqClient.EXPECT().GetTableMetadata(gomock.Any(), "project", "dataset", "table").
		Return(&bq.TableMetadata{Schema: bq.Schema{{Name: "id", Type: bq.IntegerFieldType}}}, nil).
		AnyTimes()
	analyzer := file.NewAnalyzer(logrus.New(), bqClient)

	got := analyzer.ParseFile("uri", "SELECT * FROM `project.dataset.table` WHERE id + 1")
	defer got.Close()

	expected := []file.Error{
		{
			Msg:      "WHERE clause should return type BOOL, but returns INT64",
			Position: lsp.Position{Line: 0, Character: 44},
		},
	}
	if diff := cmp.Diff(expected, got.Errors, cmpopts.IgnoreUnexported()); diff != "" {
		t.Errorf("ParseFile result diff (-expect, +got)\n%s", diff)
	}
}

func TestAnalyzer_NotBoolClauseRewritesOnlyBarePath(t *testing.T) {
	tests := map[string]struct {
		file string

		expectedFixedSrc   string
		expectedFixOffsets []file.FixOffset
	}{
		"where clause": {
			file:               "SELECT * FROM `project.dataset.table` WHERE id",
			expectedFixedSrc:   "SELECT * FROM `project.dataset.table` WHERE true",
			expectedFixOffsets: []file.FixOffset{{Offset: 44, Length: 2}},
		},
		"where clause in subquery compared by outer binary expression": {
			file:               "SELECT * FROM `project.dataset.table` WHERE id = (SELECT 1 FROM `project.dataset.table` WHERE id)",
			expectedFixedSrc:   "SELECT * FROM `project.dataset.table` WHERE id = (SELECT 1 FROM `project.dataset.table` WHERE true)",
			expectedFixOffsets: []file.FixOffset{{Offset: 94, Length: 2}},
		},
		"join on clause in where subquery": {
			file:               "SELECT * FROM `project.dataset.table` WHERE EXISTS(SELECT 1 FROM `project.dataset.table` a JOIN `project.dataset.table` b ON a.id)",
			expectedFixedSrc:   "SELECT * FROM `project.dataset.table` WHERE EXISTS(SELECT 1 FROM `project.dataset.table` a JOIN `project.dataset.table` b ON true)",
			expectedFixOffsets: []file.FixOffset{{Offset: 125, Length: 0}},
		},
	}

	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			bqClient := mock_bigquery.NewMockClient(ctrl)
			bqClient.EXPECT().GetTableMetadata(gomock.Any(), "project", "dataset", "table").
				Return(&bq.TableMetadata{Schema: bq.Schema{{Name: "id", Type: bq.IntegerFieldType}}}, nil).
				AnyTimes()
			analyzer := file.NewAnalyzer(logrus.New(), bqClient)

			got := analyzer.ParseFile("uri", tt.file)
			defer got.Close()

			if got.FixedSrc != tt.expectedFixedSrc {
				t.Errorf("FixedSrc = %q, want %q", got.FixedSrc, tt.expectedFixedSrc)
			}
			if diff := cmp.Diff(tt.expectedFixOffsets, got.FixOffsets, cmpopts.IgnoreUnexported(file.FixOffset{})); diff != "" {
				t.Errorf("FixOffsets diff (-expect, +got)\n%s", diff)
			}
			if len(got.RNode) == 0 {
				t.Errorf("failed to analyze")
			}
		})
	}
}

func TestAnalyzer_ParseFileKeepsTermBeforeDeletedTrailingClause(t *testing.T) {
	tests := map[string]struct {
		file         string
		wantMsg      string
		wantPosition lsp.Position
		wantLength   int
	}{
		"WHERE condition before a trailing AND": {
			file:         "SELECT * FROM `project.dataset.table` WHERE idq AND",
			wantMsg:      "Unrecognized name: idq; Did you mean id?",
			wantPosition: lsp.Position{Line: 0, Character: 44},
			wantLength:   3,
		},
		"table before a trailing GROUP BY": {
			file:         "SELECT id FROM unknown_table GROUP BY",
			wantMsg:      "Table not found: unknown_table",
			wantPosition: lsp.Position{Line: 0, Character: 15},
			wantLength:   len("unknown_table"),
		},
		"pipe WHERE condition before a trailing pipe ORDER BY": {
			file:         "FROM `project.dataset.table` |> WHERE idx |> ORDER BY",
			wantMsg:      "Unrecognized name: idx; Did you mean id?",
			wantPosition: lsp.Position{Line: 0, Character: 38},
			wantLength:   3,
		},
	}

	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			bqClient := mock_bigquery.NewMockClient(ctrl)
			bqClient.EXPECT().GetDefaultProject().Return("project").AnyTimes()
			bqClient.EXPECT().GetTableMetadata(gomock.Any(), "project", "dataset", "table").Return(&bq.TableMetadata{
				Schema: bq.Schema{{Name: "id", Type: bq.IntegerFieldType}},
			}, nil).AnyTimes()
			bqClient.EXPECT().GetTableMetadata(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, fmt.Errorf("not found")).AnyTimes()
			analyzer := file.NewAnalyzer(logrus.New(), bqClient)

			got := analyzer.ParseFile("uri", tt.file)
			defer got.Close()
			for _, e := range got.Errors {
				if e.Msg != tt.wantMsg {
					continue
				}
				if e.Position != tt.wantPosition || e.TermLength != tt.wantLength {
					t.Errorf("error at %+v with length %d, want %+v with length %d", e.Position, e.TermLength, tt.wantPosition, tt.wantLength)
				}
				return
			}
			t.Errorf("no error %q in %+v", tt.wantMsg, got.Errors)
		})
	}
}

func TestProject_ParseFileWithIncompleteTable(t *testing.T) {
	tests := map[string]struct {
		file                   string
		bigqueryClientMockFunc func(t *testing.T) bigquery.Client

		expectedErrs []file.Error
	}{
		"Parse with incomplete table name": {
			file: "SELECT * FROM `project.dataset.`",
			bigqueryClientMockFunc: func(t *testing.T) bigquery.Client {
				ctrl := gomock.NewController(t)
				bqClient := mock_bigquery.NewMockClient(ctrl)
				bqClient.EXPECT().GetTableMetadata(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, &googleapi.Error{Code: 404, Message: "Not found: Table project:dataset."})
				return bqClient
			},
			expectedErrs: []file.Error{
				{
					Msg: "Table not found: `project.dataset.`",
					Position: lsp.Position{
						Line:      0,
						Character: 14,
					},
					TermLength:           18,
					IncompleteColumnName: "`project.dataset.`",
				},
			},
		},
	}

	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			logger := logrus.New()
			logger.SetLevel(logrus.DebugLevel)
			bqClient := tt.bigqueryClientMockFunc(t)
			analyzer := file.NewAnalyzer(logger, bqClient)

			got := analyzer.ParseFile("uri", tt.file)
			defer got.Close()
			if diff := cmp.Diff(tt.expectedErrs, got.Errors, cmpopts.IgnoreUnexported()); diff != "" {
				t.Errorf("ParseFile result diff (-expect, +got)\n%s", diff)
			}
		})
	}
}

func TestAnalyzer_ParseFileReportsTableLoadFailures(t *testing.T) {
	tests := map[string]struct {
		file    string
		err     error
		wantMsg string
	}{
		"not found stays a plain finding": {
			file:    "SELECT x FROM `p.d.t`",
			err:     &googleapi.Error{Code: 404, Message: "Not found: Table p:d.t"},
			wantMsg: "Table not found: `p.d.t`",
		},
		"permission denied": {
			file:    "SELECT x FROM `p.d.t`",
			err:     &googleapi.Error{Code: 403, Message: "Access Denied: Table p:d.t"},
			wantMsg: "Table not found: `p.d.t` (failed to load table metadata: googleapi: Error 403: Access Denied: Table p:d.t)",
		},
		"transport failure": {
			file:    "SELECT x FROM `p.d.t`",
			err:     &url.Error{Op: "Get", URL: "https://bigquery.googleapis.com", Err: errors.New("connection refused")},
			wantMsg: `Table not found: ` + "`p.d.t`" + ` (failed to load table metadata: Get "https://bigquery.googleapis.com": connection refused)`,
		},
		"dataset-qualified path": {
			file:    "SELECT x FROM d.t",
			err:     &googleapi.Error{Code: 401, Message: "Request had invalid authentication credentials."},
			wantMsg: "Table not found: d.t (failed to load table metadata: googleapi: Error 401: Request had invalid authentication credentials.)",
		},
	}
	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			bqClient := mock_bigquery.NewMockClient(ctrl)
			bqClient.EXPECT().GetDefaultProject().Return("p").AnyTimes()
			bqClient.EXPECT().GetTableMetadata(gomock.Any(), "p", "d", "t").Return(nil, tt.err).AnyTimes()
			analyzer := file.NewAnalyzer(logrus.New(), bqClient)

			got := analyzer.ParseFile("uri", tt.file)
			defer got.Close()
			want := []file.Error{{
				Msg:                  tt.wantMsg,
				Position:             lsp.Position{Line: 0, Character: 14},
				TermLength:           len(tt.file) - 14,
				IncompleteColumnName: tt.file[14:],
			}}
			if diff := cmp.Diff(want, got.Errors); diff != "" {
				t.Errorf("errors diff (-want +got)\n%s", diff)
			}
		})
	}
}

func TestAnalyzer_ParseFileWithDeclareStatement(t *testing.T) {
	tests := map[string]struct {
		file               string
		bqTableMetadataMap map[string]*bq.TableMetadata

		expectedErrs []file.Error
	}{
		"Parse with default value": {
			file: "DECLARE target_id INT64 DEFAULT 10;\n" +
				"SELECT * FROM `project.dataset.table` WHERE id = target_id",
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
			expectedErrs: []file.Error{},
		},
		"Parse without default value": {
			file: "DECLARE target_id INT64;\n" +
				"SELECT * FROM `project.dataset.table` WHERE id = target_id",
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
			expectedErrs: []file.Error{},
		},
		"Parse without type": {
			file: "DECLARE target_id DEFAULT 10;\n" +
				"SELECT * FROM `project.dataset.table` WHERE id = target_id",
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
			expectedErrs: []file.Error{},
		},
		"Parse typed declaration with NULL default": {
			file: "DECLARE target_id INT64 DEFAULT NULL;\n" +
				"SELECT * FROM `project.dataset.table` WHERE id = target_id",
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
			expectedErrs: []file.Error{},
		},
		"Parse untyped declaration with NULL default": {
			file: "DECLARE target_id DEFAULT NULL;\n" +
				"SELECT * FROM `project.dataset.table` WHERE id = target_id",
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
			expectedErrs: []file.Error{},
		},
		"Parse lowercase type declaration with NULL default": {
			file: "DECLARE target_id int64 DEFAULT NULL;\n" +
				"SELECT * FROM `project.dataset.table` WHERE id = target_id",
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
			expectedErrs: []file.Error{},
		},
		"Parse type alias declaration with NULL default": {
			file: "DECLARE target_id INTEGER DEFAULT NULL;\n" +
				"SELECT * FROM `project.dataset.table` WHERE id = target_id",
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
			expectedErrs: []file.Error{},
		},
		"Parse numeric declaration with NULL default": {
			file: "DECLARE target_id NUMERIC DEFAULT NULL;\n" +
				"SELECT * FROM `project.dataset.table` WHERE id = target_id",
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {
					Schema: bq.Schema{
						{
							Name: "id",
							Type: bq.NumericFieldType,
						},
					},
				},
			},
			expectedErrs: []file.Error{},
		},
		"Parse json declaration with NULL default": {
			file: "DECLARE v JSON DEFAULT NULL;\n" +
				"SELECT JSON_VALUE(v, '$.a')",
			bqTableMetadataMap: map[string]*bq.TableMetadata{},
			expectedErrs:       []file.Error{},
		},
		"Parse declaration with function call default": {
			file: "DECLARE target_date DEFAULT CURRENT_DATE();\n" +
				"SELECT DATE_ADD(target_date, INTERVAL 1 DAY)",
			bqTableMetadataMap: map[string]*bq.TableMetadata{},
			expectedErrs:       []file.Error{},
		},
		"Parse declaration with subquery default": {
			file: "DECLARE max_id DEFAULT (SELECT MAX(id) FROM `project.dataset.table`);\n" +
				"SELECT * FROM `project.dataset.table` WHERE id = max_id",
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
			expectedErrs: []file.Error{},
		},
		"Parse declaration with timestamp literal default": {
			file: "DECLARE ts DEFAULT TIMESTAMP '2020-01-01 00:00:00';\n" +
				"SELECT TIMESTAMP_ADD(ts, INTERVAL 1 HOUR)",
			bqTableMetadataMap: map[string]*bq.TableMetadata{},
			expectedErrs:       []file.Error{},
		},
		"Parse declaration with default referencing another variable": {
			file: "DECLARE a INT64;\n" +
				"DECLARE b DEFAULT a + 1;\n" +
				"SELECT b + 1",
			bqTableMetadataMap: map[string]*bq.TableMetadata{},
			expectedErrs:       []file.Error{},
		},
		"Parse struct declaration": {
			file: "DECLARE s STRUCT<a INT64, b STRING>;\n" +
				"SELECT s.a + 1, LENGTH(s.b)",
			bqTableMetadataMap: map[string]*bq.TableMetadata{},
			expectedErrs:       []file.Error{},
		},
		"Parse array of struct declaration": {
			file: "DECLARE arr ARRAY<STRUCT<x INT64>>;\n" +
				"SELECT arr[OFFSET(0)].x + 1",
			bqTableMetadataMap: map[string]*bq.TableMetadata{},
			expectedErrs:       []file.Error{},
		},
		"Parse range declaration": {
			file: "DECLARE r RANGE<DATE>;\n" +
				"SELECT RANGE_START(r)",
			bqTableMetadataMap: map[string]*bq.TableMetadata{},
			expectedErrs:       []file.Error{},
		},
		"Parse datetime declaration statement": {
			file: "DECLARE target_date DATETIME DEFAULT '20230612';\n" +
				"SELECT * FROM `project.dataset.table` WHERE date = target_date",
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {
					Schema: bq.Schema{
						{
							Name: "date",
							Type: bq.DateTimeFieldType,
						},
					},
				},
			},
			expectedErrs: []file.Error{},
		},
		"Parse declared variable indented with tab": {
			file: "DECLARE my_variable INT64;\n" +
				"\tSELECT my_variable;",
			bqTableMetadataMap: map[string]*bq.TableMetadata{},
			expectedErrs:       []file.Error{},
		},
		"Parse error position after declared variable and multibyte characters": {
			file:               "DECLARE y INT64; SELECT 'あいう', y + 1, UNKNOWN_FUNC()",
			bqTableMetadataMap: map[string]*bq.TableMetadata{},
			expectedErrs: []file.Error{
				{
					Msg: "Function not found: UNKNOWN_FUNC",
					Position: lsp.Position{
						Line:      0,
						Character: 44,
					},
				},
			},
		},
		"Parse error position after declared variable and tabs": {
			// GoogleSQL expands both tabs to the next tab stop (columns 25 and 41).
			file:               "DECLARE y INT64;\tSELECT y,\tUNKNOWN_FUNC()",
			bqTableMetadataMap: map[string]*bq.TableMetadata{},
			expectedErrs: []file.Error{
				{
					Msg: "Function not found: UNKNOWN_FUNC",
					Position: lsp.Position{
						Line:      0,
						Character: 27,
					},
				},
			},
		},
		"Parse declared variable with crlf line endings": {
			file: "DECLARE my_variable INT64;\r\n" +
				"\r\n" +
				"SELECT my_variable;",
			bqTableMetadataMap: map[string]*bq.TableMetadata{},
			expectedErrs:       []file.Error{},
		},
		"Parse declared variable after multibyte characters": {
			file: "DECLARE y STRING;\n" +
				"/* あいう */ SELECT LENGTH(y),\n" +
				"UNKNOWN_FUNC();",
			bqTableMetadataMap: map[string]*bq.TableMetadata{},
			expectedErrs: []file.Error{
				{
					Msg: "Function not found: UNKNOWN_FUNC",
					Position: lsp.Position{
						Line:      2,
						Character: 0,
					},
				},
			},
		},
		"Parse create tmp function statement with returns": {
			file: "CREATE TEMP FUNCTION target_func(x INT64) RETURNS INT64 AS (x * 10);\n" +
				"SELECT target_func(10);",
			bqTableMetadataMap: map[string]*bq.TableMetadata{},
			expectedErrs:       []file.Error{},
		},
		"Parse create tmp function statement without returns": {
			file: "CREATE TEMP FUNCTION target_func(x INT64) AS (x * 10);\n" +
				"SELECT target_func(10);",
			bqTableMetadataMap: map[string]*bq.TableMetadata{},
			expectedErrs: []file.Error{
				{
					Msg: "Currently, bqls does not support function without return type.",
					Position: lsp.Position{
						Line:      0,
						Character: 0,
					},
					TermLength: 53,
					Severity:   lsp.Warning,
				},
				{
					Msg: "Function not found: target_func",
					Position: lsp.Position{
						Line:      1,
						Character: 7,
					},
				},
			},
		},
		"Report error once when a later statement triggers a declaration retry": {
			file: "DECLARE y STRING;\n" +
				"DECLARE x INT64;\n" +
				"SELECT UNKNOWN_FUNC() + LENGTH(y);\n" +
				"SELECT y;",
			bqTableMetadataMap: map[string]*bq.TableMetadata{},
			expectedErrs: []file.Error{
				{
					Msg: "Function not found: UNKNOWN_FUNC",
					Position: lsp.Position{
						Line:      2,
						Character: 7,
					},
				},
			},
		},
		"Report errors once when a later statement triggers a fix-up retry": {
			file: "SELECT UNKNOWN_FUNC();\n" +
				"SELECT unknown_column;",
			bqTableMetadataMap: map[string]*bq.TableMetadata{},
			expectedErrs: []file.Error{
				{
					Msg: "Function not found: UNKNOWN_FUNC",
					Position: lsp.Position{
						Line:      0,
						Character: 7,
					},
				},
				{
					Msg: "Unrecognized name: unknown_column",
					Position: lsp.Position{
						Line:      1,
						Character: 7,
					},
					TermLength:           14,
					IncompleteColumnName: "unknown_column",
				},
			},
		},
		"Keep analysis errors when a fix-up makes the source unparsable": {
			file: "SELECT UNKNOWN_FUNC();\n" +
				"SELECT unknown.*;",
			bqTableMetadataMap: map[string]*bq.TableMetadata{},
			expectedErrs: []file.Error{
				{
					Msg: `Syntax error: Unexpected ";"`,
					Position: lsp.Position{
						Line:      1,
						Character: 16,
					},
				},
				{
					Msg: "Function not found: UNKNOWN_FUNC",
					Position: lsp.Position{
						Line:      0,
						Character: 7,
					},
				},
				{
					Msg: "Unrecognized name: unknown",
					Position: lsp.Position{
						Line:      1,
						Character: 7,
					},
					TermLength:           7,
					IncompleteColumnName: "unknown",
				},
			},
		},
		"Report error position in the original source after a variable longer than its dummy value": {
			file: "DECLARE yyyyyy INT64;\n" +
				"DECLARE x INT64;\n" +
				"SELECT yyyyyy + UNKNOWN_FUNC();",
			bqTableMetadataMap: map[string]*bq.TableMetadata{},
			expectedErrs: []file.Error{
				{
					Msg: "Function not found: UNKNOWN_FUNC",
					Position: lsp.Position{
						Line:      2,
						Character: 16,
					},
				},
			},
		},
		"Report error position in the original source after a variable shorter than its dummy value": {
			file: "DECLARE y STRING;\n" +
				"DECLARE x INT64;\n" +
				"SELECT LENGTH(y) + UNKNOWN_FUNC();\n" +
				"SELECT y;",
			bqTableMetadataMap: map[string]*bq.TableMetadata{},
			expectedErrs: []file.Error{
				{
					Msg: "Function not found: UNKNOWN_FUNC",
					Position: lsp.Position{
						Line:      2,
						Character: 19,
					},
				},
			},
		},
		"Keep error position on a line after a replaced variable": {
			file: "DECLARE yyyyyy INT64;\n" +
				"SELECT yyyyyy;\n" +
				"SELECT UNKNOWN_FUNC();",
			bqTableMetadataMap: map[string]*bq.TableMetadata{},
			expectedErrs: []file.Error{
				{
					Msg: "Function not found: UNKNOWN_FUNC",
					Position: lsp.Position{
						Line:      2,
						Character: 7,
					},
				},
			},
		},
		"Report errors from different passes in the original source": {
			// The WHERE error is found before yyyyyy, earlier on the line, is
			// replaced; UNKNOWN_FUNC is found only after that.
			file: "DECLARE yyyyyy INT64;\n" +
				"SELECT yyyyyy, UNKNOWN_FUNC() FROM `project.dataset.table` WHERE idx",
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {
					Schema: bq.Schema{{Name: "id", Type: bq.IntegerFieldType}},
				},
			},
			expectedErrs: []file.Error{
				{
					Msg: "Unrecognized name: idx; Did you mean id?",
					Position: lsp.Position{
						Line:      1,
						Character: 65,
					},
					TermLength:           3,
					IncompleteColumnName: "idx",
				},
				{
					Msg: "Function not found: UNKNOWN_FUNC",
					Position: lsp.Position{
						Line:      1,
						Character: 15,
					},
				},
			},
		},
		"Report error at a variable whose dummy value equals its tail": {
			file: "DECLARE v1 INT64;\n" +
				"SELECT 1 FROM `project.dataset.table` WHERE v1",
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {
					Schema: bq.Schema{{Name: "id", Type: bq.IntegerFieldType}},
				},
			},
			expectedErrs: []file.Error{
				{
					Msg: "WHERE clause should return type BOOL, but returns INT64",
					Position: lsp.Position{
						Line:      1,
						Character: 44,
					},
				},
			},
		},
		"Report error at a variable whose tail repeats its dummy value": {
			file: "DECLARE v11 INT64;\n" +
				"SELECT 1 FROM `project.dataset.table` WHERE v11",
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {
					Schema: bq.Schema{{Name: "id", Type: bq.IntegerFieldType}},
				},
			},
			expectedErrs: []file.Error{
				{
					Msg: "WHERE clause should return type BOOL, but returns INT64",
					Position: lsp.Position{
						Line:      1,
						Character: 44,
					},
				},
			},
		},
		"Report create function error once when a later statement triggers a retry": {
			file: "CREATE TEMP FUNCTION target_func(x INT64) AS (x * 10);\n" +
				"DECLARE y INT64;\n" +
				"SELECT y;",
			bqTableMetadataMap: map[string]*bq.TableMetadata{},
			expectedErrs: []file.Error{
				{
					Msg: "Currently, bqls does not support function without return type.",
					Position: lsp.Position{
						Line:      0,
						Character: 0,
					},
					TermLength: 53,
					Severity:   lsp.Warning,
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
			logger := logrus.New()
			logger.SetLevel(logrus.DebugLevel)
			analyzer := file.NewAnalyzer(logger, bqClient)

			got := analyzer.ParseFile("uri", tt.file)
			defer got.Close()
			if diff := cmp.Diff(tt.expectedErrs, got.Errors, cmpopts.IgnoreUnexported()); diff != "" {
				t.Errorf("ParseFile result diff (-expect, +got)\n%s", diff)
			}
		})
	}
}

func TestAnalyzer_ParseFileDoesNotPanicOnLoneCarriageReturn(t *testing.T) {
	// GoogleSQL treats a lone "\r" as a line break but bqls does not, so the
	// reported line can be outside of the source.
	ctrl := gomock.NewController(t)
	bqClient := mock_bigquery.NewMockClient(ctrl)
	analyzer := file.NewAnalyzer(logrus.New(), bqClient)

	got := analyzer.ParseFile("uri", "SELECT 1;\rSELECT FROM t")
	defer got.Close()

	expectedErrs := []file.Error{
		{
			Msg:      "Syntax error: SELECT list must not be empty",
			Position: lsp.Position{Line: 1, Character: 7},
		},
	}
	if diff := cmp.Diff(expectedErrs, got.Errors); diff != "" {
		t.Errorf("ParseFile result diff (-expect, +got)\n%s", diff)
	}
}

func TestAnalyzer_ParseFileWithScriptStatement(t *testing.T) {
	tests := map[string]struct {
		file               string
		bqTableMetadataMap map[string]*bq.TableMetadata

		expectedErrs []file.Error
		// Only successfully analyzed non-script statements produce outputs.
		expectedRNodeLen int
	}{
		"SET with function call": {
			file:             "DECLARE x DATE;\nSET x = CURRENT_DATE();",
			expectedErrs:     []file.Error{},
			expectedRNodeLen: 0,
		},
		"SET with subquery": {
			file: "DECLARE x INT64;\nSET x = (SELECT MAX(id) FROM `project.dataset.table`);",
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {
					Schema: bq.Schema{{Name: "id", Type: bq.IntegerFieldType}},
				},
			},
			expectedErrs:     []file.Error{},
			expectedRNodeLen: 0,
		},
		"SET with unknown column in subquery": {
			file: "DECLARE x INT64;\nSET x = (SELECT MAX(unknown_column) FROM `project.dataset.table`);",
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {
					Schema: bq.Schema{{Name: "id", Type: bq.IntegerFieldType}},
				},
			},
			expectedErrs: []file.Error{
				{
					Msg:                  "Unrecognized name: unknown_column",
					Position:             lsp.Position{Line: 1, Character: 20},
					TermLength:           14,
					IncompleteColumnName: "unknown_column",
				},
			},
			expectedRNodeLen: 0,
		},
		"SET referencing declared variable": {
			file:             "DECLARE y INT64 DEFAULT 1;\nDECLARE x INT64;\nSET x = y + 1;",
			expectedErrs:     []file.Error{},
			expectedRNodeLen: 0,
		},
		"SET struct assignment with unknown function": {
			file: "DECLARE a INT64;\nDECLARE b INT64;\nSET (a, b) = (1, UNKNOWN_FUNC());",
			expectedErrs: []file.Error{
				{
					Msg:      "Function not found: UNKNOWN_FUNC",
					Position: lsp.Position{Line: 2, Character: 17},
				},
			},
			expectedRNodeLen: 0,
		},
		"SET parameter with unknown function": {
			file: "SET @p = UNKNOWN_FUNC();",
			expectedErrs: []file.Error{
				{
					Msg:      "Function not found: UNKNOWN_FUNC",
					Position: lsp.Position{Line: 0, Character: 9},
				},
			},
			expectedRNodeLen: 0,
		},
		"control flow statements": {
			file: "IF TRUE THEN SELECT 1; ELSEIF FALSE THEN SELECT 2; ELSE SELECT 3; END IF;\n" +
				"BEGIN SELECT 1; EXCEPTION WHEN ERROR THEN RAISE USING MESSAGE = 'x'; END;\n" +
				"WHILE TRUE DO BREAK; CONTINUE; END WHILE;\n" +
				"LOOP LEAVE; END LOOP;\n" +
				"REPEAT SELECT 1; UNTIL TRUE END REPEAT;\n" +
				"FOR r IN (SELECT 1 AS a) DO SELECT 1; END FOR;\n" +
				"CASE WHEN TRUE THEN SELECT 1; END CASE;\n" +
				"RETURN;",
			expectedErrs:     []file.Error{},
			expectedRNodeLen: 7,
		},
		"FOR loop variable in body": {
			file:             "FOR r IN (SELECT 1 AS a) DO\n  SELECT r.a;\nEND FOR;",
			expectedErrs:     []file.Error{},
			expectedRNodeLen: 1,
		},
		"FOR loop variable over table query": {
			file: "FOR r IN (SELECT id, name FROM `project.dataset.table`) DO\n  INSERT INTO `project.dataset.table` (id, name) VALUES (r.id, r.name);\nEND FOR;",
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {
					Schema: bq.Schema{
						{Name: "id", Type: bq.IntegerFieldType},
						{Name: "name", Type: bq.StringFieldType},
					},
				},
			},
			expectedErrs:     []file.Error{},
			expectedRNodeLen: 1,
		},
		"FOR loop variable in SET": {
			file:             "DECLARE x INT64;\nFOR r IN (SELECT 1 AS a) DO\n  SET x = r.a;\nEND FOR;",
			expectedErrs:     []file.Error{},
			expectedRNodeLen: 0,
		},
		"nested FOR loop variables": {
			file:             "FOR r IN (SELECT 1 AS a) DO\n  FOR s IN (SELECT r.a AS b) DO\n    FOR r IN (SELECT 'x' AS c) DO\n      SELECT r.c, s.b;\n    END FOR;\n  END FOR;\nEND FOR;",
			expectedErrs:     []file.Error{},
			expectedRNodeLen: 1,
		},
		"FOR loop variable with unknown field": {
			file: "FOR r IN (SELECT 1 AS a) DO\n  SELECT r.b;\nEND FOR;",
			expectedErrs: []file.Error{
				{
					Msg:                  "Field name b does not exist in STRUCT<a INT64>",
					Position:             lsp.Position{Line: 1, Character: 11},
					TermLength:           1,
					IncompleteColumnName: "r.b",
				},
			},
			expectedRNodeLen: 1,
		},
		"FOR loop variable is out of scope after the loop": {
			file: "FOR r IN (SELECT 1 AS a) DO\n  SELECT 1;\nEND FOR;\nSELECT r.a;",
			expectedErrs: []file.Error{
				{
					Msg:                  "Unrecognized name: r",
					Position:             lsp.Position{Line: 3, Character: 7},
					TermLength:           1,
					IncompleteColumnName: "r",
				},
			},
			expectedRNodeLen: 2,
		},
		"DML inside BEGIN END and IF": {
			file: "BEGIN\n  INSERT INTO p.d.t SELECT 1;\nEND;\n\nIF true THEN\n  DELETE FROM p.d.t WHERE true;\nEND IF;",
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"p.d.t": {
					Schema: bq.Schema{{Name: "x", Type: bq.IntegerFieldType}},
				},
			},
			expectedErrs:     []file.Error{},
			expectedRNodeLen: 2,
		},
		"nested DML inside UPDATE SET": {
			file: "UPDATE p.d.t SET (DELETE FROM arr WHERE true) WHERE true;\n" +
				"UPDATE p.d.t SET (INSERT arr VALUES (1)) WHERE true;\n" +
				"UPDATE p.d.t SET (UPDATE arr SET arr = 1 WHERE true) WHERE true;",
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"p.d.t": {
					Schema: bq.Schema{
						{Name: "x", Type: bq.IntegerFieldType},
						{Name: "arr", Type: bq.IntegerFieldType, Repeated: true},
					},
				},
			},
			expectedErrs:     []file.Error{},
			expectedRNodeLen: 3,
		},
		"broken SELECT inside IF": {
			file: "IF TRUE THEN\n  SELECT UNKNOWN_FUNC();\nEND IF;",
			expectedErrs: []file.Error{
				{
					Msg:      "Function not found: UNKNOWN_FUNC",
					Position: lsp.Position{Line: 1, Character: 9},
				},
			},
			expectedRNodeLen: 0,
		},
		"broken SELECT inside BEGIN END": {
			file: "BEGIN\n  SELECT UNKNOWN_FUNC();\nEND;",
			expectedErrs: []file.Error{
				{
					Msg:      "Function not found: UNKNOWN_FUNC",
					Position: lsp.Position{Line: 1, Character: 9},
				},
			},
			expectedRNodeLen: 0,
		},
		"broken SELECT inside WHILE": {
			file: "WHILE TRUE DO\n  SELECT UNKNOWN_FUNC();\nEND WHILE;",
			expectedErrs: []file.Error{
				{
					Msg:      "Function not found: UNKNOWN_FUNC",
					Position: lsp.Position{Line: 1, Character: 9},
				},
			},
			expectedRNodeLen: 0,
		},
		// SET @@system_variable is supported by the resolver, so it must still
		// be analyzed rather than skipped like other SET statements.
		"SET system variable is still analyzed": {
			file:             "SET @@dataset_id = 'foo';",
			expectedErrs:     []file.Error{},
			expectedRNodeLen: 1,
		},
		"SET system variable with wrong type": {
			file: "SET @@dataset_id = 1;",
			expectedErrs: []file.Error{
				{
					Msg:      "Expected type STRING; found INT64",
					Position: lsp.Position{Line: 0, Character: 19},
				},
			},
			expectedRNodeLen: 0,
		},
		"system variables with field paths": {
			file: "BEGIN\n" +
				"  SELECT 1;\n" +
				"EXCEPTION WHEN ERROR THEN\n" +
				"  SELECT @@error.message, @@error.stack_trace[SAFE_OFFSET(0)].line, @@script.job_id, @@row_count, @@project_id;\n" +
				"END;",
			expectedErrs:     []file.Error{},
			expectedRNodeLen: 2,
		},
		// While typing a SET, the parser error and its recovery attempt are
		// reported just like for other incomplete statements.
		"incomplete SET with trailing space": {
			file: "DECLARE x INT64;\nSET x = ",
			expectedErrs: []file.Error{
				{Msg: "Syntax error: Unexpected end of script", Position: lsp.Position{Line: 1, Character: 7}},
			},
			expectedRNodeLen: 0,
		},
		"incomplete SET without expression": {
			file: "DECLARE x INT64;\nSET x =",
			expectedErrs: []file.Error{
				{Msg: "Syntax error: Unexpected end of script", Position: lsp.Position{Line: 1, Character: 7}},
			},
			expectedRNodeLen: 0,
		},
		"incomplete SET without equal sign": {
			file: "DECLARE x INT64;\nSET x",
			expectedErrs: []file.Error{
				{Msg: "Syntax error: Expected \",\" or \"=\" but got end of script", Position: lsp.Position{Line: 1, Character: 5}},
			},
			expectedRNodeLen: 0,
		},
		"incomplete SET subquery": {
			file: "DECLARE x INT64;\nSET x = (SELECT ",
			expectedErrs: []file.Error{
				{Msg: "Syntax error: Unexpected end of script", Position: lsp.Position{Line: 1, Character: 15}},
			},
			expectedRNodeLen: 0,
		},
		"incomplete SET struct assignment": {
			file: "DECLARE a INT64;\nDECLARE b INT64;\nSET (a, b) = ",
			expectedErrs: []file.Error{
				{Msg: "Syntax error: Unexpected end of script", Position: lsp.Position{Line: 2, Character: 12}},
			},
			expectedRNodeLen: 0,
		},
		"incomplete SET parameter": {
			file: "SET @p = ",
			expectedErrs: []file.Error{
				{Msg: "Syntax error: Unexpected end of script", Position: lsp.Position{Line: 0, Character: 8}},
			},
			expectedRNodeLen: 0,
		},
		"SET nested in IF with unknown function": {
			file: "DECLARE x INT64;\nIF TRUE THEN\n  SET x = UNKNOWN_FUNC();\nEND IF;",
			expectedErrs: []file.Error{
				{
					Msg:      "Function not found: UNKNOWN_FUNC",
					Position: lsp.Position{Line: 2, Character: 10},
				},
			},
			expectedRNodeLen: 0,
		},
		"SET subquery referencing declared variable": {
			file: "DECLARE y INT64 DEFAULT 1;\nDECLARE x INT64;\nSET x = (SELECT y + id FROM `project.dataset.table`);",
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {
					Schema: bq.Schema{{Name: "id", Type: bq.IntegerFieldType}},
				},
			},
			expectedErrs:     []file.Error{},
			expectedRNodeLen: 0,
		},
		// Error positions are byte offsets within the line, like errors in
		// other statements; "あ" is 3 bytes and a tab is 1 byte.
		"SET error after multibyte identifier": {
			file: "DECLARE `あ` INT64;\nSET `あ` = UNKNOWN_FUNC();",
			expectedErrs: []file.Error{
				{
					Msg:      "Function not found: UNKNOWN_FUNC",
					Position: lsp.Position{Line: 1, Character: 12},
				},
			},
			expectedRNodeLen: 0,
		},
		"SET error after tab": {
			file: "DECLARE x INT64;\nSET\tx = UNKNOWN_FUNC();",
			expectedErrs: []file.Error{
				{
					Msg:      "Function not found: UNKNOWN_FUNC",
					Position: lsp.Position{Line: 1, Character: 8},
				},
			},
			expectedRNodeLen: 0,
		},
		"SET indented with tab referencing declared variable": {
			file:             "DECLARE my_variable INT64;\nDECLARE x INT64;\n\tSET x = my_variable;",
			expectedErrs:     []file.Error{},
			expectedRNodeLen: 0,
		},
		"SET in WHILE indented with tab referencing loop variable": {
			file:             "DECLARE i INT64 DEFAULT 0;\nWHILE i < 3 DO\n\tSET i = i + 1;\nEND WHILE;",
			expectedErrs:     []file.Error{},
			expectedRNodeLen: 0,
		},
		"procedure parameter in body": {
			file:             "CREATE PROCEDURE p.d.proc(x INT64)\nBEGIN\n  SELECT x;\nEND;",
			expectedErrs:     []file.Error{},
			expectedRNodeLen: 2,
		},
		"procedure parameter of composite type": {
			file:             "CREATE PROCEDURE p.d.proc(xs ARRAY<STRUCT<a INT64>>)\nBEGIN\n  SELECT x.a FROM UNNEST(xs) AS x;\nEND;",
			expectedErrs:     []file.Error{},
			expectedRNodeLen: 2,
		},
		"procedure OUT parameter in SET": {
			file:             "CREATE PROCEDURE p.d.proc(x INT64, OUT y STRING)\nBEGIN\n  SET y = CAST(x AS STRING);\nEND;",
			expectedErrs:     []file.Error{},
			expectedRNodeLen: 1,
		},
		"procedure parameter keeps its type": {
			file: "CREATE PROCEDURE p.d.proc(x INT64)\nBEGIN\n  SELECT x.a;\nEND;",
			expectedErrs: []file.Error{
				{
					Msg:      "Cannot access field a on a value with type INT64",
					Position: lsp.Position{Line: 2, Character: 11},
				},
			},
			expectedRNodeLen: 1,
		},
		"procedure parameter in FOR loop": {
			file:             "CREATE PROCEDURE p.d.proc(n INT64)\nBEGIN\n  FOR r IN (SELECT n AS a) DO\n    SELECT r.a + n;\n  END FOR;\nEND;",
			expectedErrs:     []file.Error{},
			expectedRNodeLen: 2,
		},
		"procedure parameter is out of scope after the procedure": {
			file: "CREATE PROCEDURE p.d.proc(x INT64)\nBEGIN\n  SELECT 1;\nEND;\nSELECT x;",
			expectedErrs: []file.Error{
				{
					Msg:                  "Unrecognized name: x",
					Position:             lsp.Position{Line: 4, Character: 7},
					TermLength:           1,
					IncompleteColumnName: "x",
				},
			},
			expectedRNodeLen: 3,
		},
		"SET with unknown function": {
			file: "DECLARE x DATE;\nSET x = UNKNOWN_FUNC();",
			expectedErrs: []file.Error{
				{
					Msg:      "Function not found: UNKNOWN_FUNC",
					Position: lsp.Position{Line: 1, Character: 8},
				},
			},
			expectedRNodeLen: 0,
		},
		"IF condition with unknown function": {
			file: "IF UNKNOWN_FUNC() THEN\n  SELECT 1;\nEND IF;",
			expectedErrs: []file.Error{
				{
					Msg:      "Function not found: UNKNOWN_FUNC",
					Position: lsp.Position{Line: 0, Character: 3},
				},
			},
			expectedRNodeLen: 1,
		},
		"ELSEIF condition with unknown function": {
			file: "IF TRUE THEN\n  SELECT 1;\nELSEIF UNKNOWN_FUNC() THEN\n  SELECT 2;\nEND IF;",
			expectedErrs: []file.Error{
				{
					Msg:      "Function not found: UNKNOWN_FUNC",
					Position: lsp.Position{Line: 2, Character: 7},
				},
			},
			expectedRNodeLen: 2,
		},
		"WHILE condition with unknown column": {
			file: "WHILE unknown_column > 0 DO\n  SELECT 1;\nEND WHILE;",
			expectedErrs: []file.Error{
				{
					Msg:                  "Unrecognized name: unknown_column",
					Position:             lsp.Position{Line: 0, Character: 6},
					TermLength:           14,
					IncompleteColumnName: "unknown_column",
				},
			},
			expectedRNodeLen: 1,
		},
		"REPEAT UNTIL condition with unknown function": {
			file: "REPEAT\n  SELECT 1;\nUNTIL UNKNOWN_FUNC() END REPEAT;",
			expectedErrs: []file.Error{
				{
					Msg:      "Function not found: UNKNOWN_FUNC",
					Position: lsp.Position{Line: 2, Character: 6},
				},
			},
			expectedRNodeLen: 1,
		},
		"CASE expression with unknown function": {
			file: "CASE UNKNOWN_FUNC()\n  WHEN 1 THEN SELECT 1;\nEND CASE;",
			expectedErrs: []file.Error{
				{
					Msg:      "Function not found: UNKNOWN_FUNC",
					Position: lsp.Position{Line: 0, Character: 5},
				},
			},
			expectedRNodeLen: 1,
		},
		"CASE WHEN condition with unknown function": {
			file: "CASE\n  WHEN UNKNOWN_FUNC() THEN SELECT 1;\nEND CASE;",
			expectedErrs: []file.Error{
				{
					Msg:      "Function not found: UNKNOWN_FUNC",
					Position: lsp.Position{Line: 1, Character: 7},
				},
			},
			expectedRNodeLen: 1,
		},
		"RAISE message with unknown function": {
			file: "RAISE USING MESSAGE = UNKNOWN_FUNC();",
			expectedErrs: []file.Error{
				{
					Msg:      "Function not found: UNKNOWN_FUNC",
					Position: lsp.Position{Line: 0, Character: 22},
				},
			},
			expectedRNodeLen: 0,
		},
		"FOR query with unknown function": {
			file: "FOR r IN (SELECT UNKNOWN_FUNC() AS a) DO\n  SELECT 1;\nEND FOR;",
			expectedErrs: []file.Error{
				{
					Msg:      "Function not found: UNKNOWN_FUNC",
					Position: lsp.Position{Line: 0, Character: 17},
				},
			},
			expectedRNodeLen: 1,
		},
		"FOR query with unknown column": {
			file: "FOR r IN (SELECT unknown_column FROM `project.dataset.table`) DO\n  SELECT 1;\nEND FOR;",
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {
					Schema: bq.Schema{{Name: "id", Type: bq.IntegerFieldType}},
				},
			},
			expectedErrs: []file.Error{
				{
					Msg:                  "Unrecognized name: unknown_column",
					Position:             lsp.Position{Line: 0, Character: 17},
					TermLength:           14,
					IncompleteColumnName: "unknown_column",
				},
			},
			expectedRNodeLen: 1,
		},
		"control flow conditions referencing declared variable": {
			file: "DECLARE x INT64 DEFAULT 1;\n" +
				"IF x > 0 THEN SELECT 1; ELSEIF x < 0 THEN SELECT 2; END IF;\n" +
				"WHILE x > 0 DO BREAK; END WHILE;\n" +
				"REPEAT SELECT 1; UNTIL x > 0 END REPEAT;\n" +
				"CASE x WHEN 1 THEN SELECT 1; END CASE;\n" +
				"RAISE USING MESSAGE = CAST(x AS STRING);",
			expectedErrs:     []file.Error{},
			expectedRNodeLen: 4,
		},
		"IF condition referencing FOR loop variable": {
			file:             "FOR r IN (SELECT 1 AS a) DO\n  IF r.a > 0 THEN\n    SELECT 1;\n  END IF;\nEND FOR;",
			expectedErrs:     []file.Error{},
			expectedRNodeLen: 1,
		},
		"IF condition referencing unknown field of FOR loop variable": {
			file: "FOR r IN (SELECT 1 AS a) DO\n  IF r.b > 0 THEN\n    SELECT 1;\n  END IF;\nEND FOR;",
			expectedErrs: []file.Error{
				{
					Msg:                  "Field name b does not exist in STRUCT<a INT64>",
					Position:             lsp.Position{Line: 1, Character: 7},
					TermLength:           1,
					IncompleteColumnName: "r.b",
				},
			},
			expectedRNodeLen: 1,
		},
		"WHILE condition referencing procedure parameter": {
			file:             "CREATE PROCEDURE p.d.proc(n INT64)\nBEGIN\n  WHILE n > 0 DO\n    SELECT n;\n  END WHILE;\nEND;",
			expectedErrs:     []file.Error{},
			expectedRNodeLen: 2,
		},
	}

	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			bqClient := mock_bigquery.NewMockClient(ctrl)
			bqClient.EXPECT().GetDefaultProject().Return("project").AnyTimes()
			for tablePath, schema := range tt.bqTableMetadataMap {
				tablePathSplitted := strings.Split(tablePath, ".")
				if len(tablePathSplitted) != 3 {
					t.Fatalf("table path length should be 3, got %s", tablePath)
				}
				bqClient.EXPECT().GetTableMetadata(gomock.Any(), tablePathSplitted[0], tablePathSplitted[1], tablePathSplitted[2]).Return(schema, nil).MinTimes(0)
			}
			analyzer := file.NewAnalyzer(logrus.New(), bqClient)

			got := analyzer.ParseFile("uri", tt.file)
			defer got.Close()
			if diff := cmp.Diff(tt.expectedErrs, got.Errors, cmpopts.IgnoreUnexported()); diff != "" {
				t.Errorf("ParseFile result diff (-expect, +got)\n%s", diff)
			}
			if len(got.RNode) != tt.expectedRNodeLen {
				t.Errorf("expected %d analyzer outputs, got %d", tt.expectedRNodeLen, len(got.RNode))
			}
		})
	}
}

func TestAnalyzer_ParseFileWithTablesCreatedInScript(t *testing.T) {
	tests := map[string]struct {
		file         string
		expectedErrs []file.Error
	}{
		"temp table created by CTAS": {
			file:         "CREATE TEMP TABLE tt AS SELECT 1 AS a;\nSELECT a FROM tt;",
			expectedErrs: []file.Error{},
		},
		"temp table with column definitions": {
			file:         "CREATE TEMP TABLE tt (a INT64, b STRING);\nSELECT a, b FROM tt;",
			expectedErrs: []file.Error{},
		},
		"temp table keeps column types": {
			file: "CREATE TEMP TABLE tt AS SELECT 1 AS a;\nSELECT a.x FROM tt;",
			expectedErrs: []file.Error{
				{
					Msg:      "Cannot access field x on a value with type INT64",
					Position: lsp.Position{Line: 1, Character: 9},
				},
			},
		},
		"temp table is not visible before it is created": {
			file: "SELECT a FROM tt;\nCREATE TEMP TABLE tt AS SELECT 1 AS a;",
			expectedErrs: []file.Error{
				{
					Msg:                  "Table not found: tt",
					Position:             lsp.Position{Line: 0, Character: 14},
					TermLength:           2,
					IncompleteColumnName: "tt",
				},
			},
		},
		"INSERT into temp table": {
			file:         "CREATE TEMP TABLE tt (a INT64);\nINSERT INTO tt (a) VALUES (1);",
			expectedErrs: []file.Error{},
		},
		"temp table created in a block is visible after it": {
			file:         "BEGIN\n  CREATE TEMP TABLE tt AS SELECT 1 AS a;\nEND;\nSELECT a FROM tt;",
			expectedErrs: []file.Error{},
		},
		"recreated temp table replaces its columns": {
			file:         "CREATE TEMP TABLE tt AS SELECT 1 AS a;\nCREATE OR REPLACE TEMP TABLE tt AS SELECT 'x' AS b;\nSELECT b FROM tt;",
			expectedErrs: []file.Error{},
		},
		"qualified table created in script": {
			file:         "CREATE TABLE `p.d.created` AS SELECT 1 AS a;\nSELECT a FROM `p.d.created`;\nSELECT a FROM p.d.created;\nSELECT a FROM d.created;",
			expectedErrs: []file.Error{},
		},
		"replaced table has the new columns": {
			file:         "CREATE OR REPLACE TABLE p.d.existing AS SELECT 'x' AS name;\nSELECT name FROM p.d.existing;",
			expectedErrs: []file.Error{},
		},
		"CREATE TABLE IF NOT EXISTS keeps an existing table": {
			file:         "CREATE TABLE IF NOT EXISTS p.d.existing (name STRING);\nSELECT id FROM p.d.existing;",
			expectedErrs: []file.Error{},
		},
	}

	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			bqClient := mock_bigquery.NewMockClient(ctrl)
			bqClient.EXPECT().GetDefaultProject().Return("p").AnyTimes()
			bqClient.EXPECT().GetTableMetadata(gomock.Any(), "p", "d", "existing").Return(&bq.TableMetadata{
				Schema: bq.Schema{{Name: "id", Type: bq.IntegerFieldType}},
			}, nil).AnyTimes()
			bqClient.EXPECT().GetTableMetadata(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, &googleapi.Error{Code: 404, Message: "Not found"}).AnyTimes()
			analyzer := file.NewAnalyzer(logrus.New(), bqClient)

			got := analyzer.ParseFile("uri", tt.file)
			defer got.Close()
			if diff := cmp.Diff(tt.expectedErrs, got.Errors, cmpopts.IgnoreUnexported()); diff != "" {
				t.Errorf("ParseFile result diff (-expect, +got)\n%s", diff)
			}
		})
	}
}

func TestAnalyzer_ParseFileWithCallStatement(t *testing.T) {
	procedure := &bq.RoutineMetadata{
		Type: "PROCEDURE",
		Arguments: []*bq.RoutineArgument{
			{Name: "x", DataType: &bq.StandardSQLDataType{TypeKind: "INT64"}},
			{Name: "ys", DataType: &bq.StandardSQLDataType{TypeKind: "ARRAY", ArrayElementType: &bq.StandardSQLDataType{TypeKind: "STRING"}}},
			{Name: "s", Mode: "OUT", DataType: &bq.StandardSQLDataType{TypeKind: "STRUCT", StructType: &bq.StandardSQLStructType{
				Fields: []*bq.StandardSQLField{{Name: "a", Type: &bq.StandardSQLDataType{TypeKind: "DATE"}}},
			}}},
		},
	}
	tests := map[string]struct {
		file         string
		expectedErrs []file.Error
	}{
		"CALL procedure in BigQuery": {
			file:         "DECLARE s STRUCT<a DATE>;\nCALL p.d.proc(1, ['a'], s);",
			expectedErrs: []file.Error{},
		},
		"CALL procedure by quoted and dataset-qualified names": {
			file:         "DECLARE s STRUCT<a DATE>;\nCALL `p.d.proc`(1, ['a'], s);\nCALL `p.d`.proc(1, ['a'], s);\nCALL d.proc(1, ['a'], s);",
			expectedErrs: []file.Error{},
		},
		"CALL procedure with an argument of a wrong type": {
			file: "DECLARE s STRUCT<a DATE>;\nCALL p.d.proc('x', ['a'], s);",
			expectedErrs: []file.Error{
				{
					Msg:      "No matching signature for procedure p.d.proc for argument types: STRING, ARRAY<STRING>, STRUCT<a DATE>. Supported signature: p.d.proc(INT64, ARRAY<STRING>, STRUCT<a DATE>)",
					Position: lsp.Position{Line: 1, Character: 5},
				},
			},
		},
		"CALL procedure created in script": {
			file:         "CREATE PROCEDURE p.d.created(x INT64)\nBEGIN\n  SELECT x;\nEND;\nCALL p.d.created(1);",
			expectedErrs: []file.Error{},
		},
		"CREATE PROCEDURE IF NOT EXISTS keeps an existing procedure": {
			file:         "DECLARE s STRUCT<a DATE>;\nCREATE PROCEDURE IF NOT EXISTS p.d.proc()\nBEGIN\n  SELECT 1;\nEND;\nCALL p.d.proc(1, ['a'], s);",
			expectedErrs: []file.Error{},
		},
		"CALL procedure that does not exist": {
			file: "CALL p.d.unknown(1);",
			expectedErrs: []file.Error{
				{
					Msg:      "Procedure not found: p.d.unknown",
					Position: lsp.Position{Line: 0, Character: 5},
				},
			},
		},
		"CALL routine that is not a procedure": {
			file: "CALL p.d.fn(1);",
			expectedErrs: []file.Error{
				{
					Msg:      "Procedure not found: p.d.fn",
					Position: lsp.Position{Line: 0, Character: 5},
				},
			},
		},
		"CALL procedure that cannot be loaded": {
			file: "CALL p.d.denied(1);",
			expectedErrs: []file.Error{
				{
					Msg:      "Procedure not found: p.d.denied (failed to load routine metadata: googleapi: Error 403: Access Denied)",
					Position: lsp.Position{Line: 0, Character: 5},
				},
			},
		},
	}

	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			bqClient := mock_bigquery.NewMockClient(ctrl)
			bqClient.EXPECT().GetDefaultProject().Return("p").AnyTimes()
			bqClient.EXPECT().GetRoutineMetadata(gomock.Any(), "p", "d", "proc").Return(procedure, nil).AnyTimes()
			bqClient.EXPECT().GetRoutineMetadata(gomock.Any(), "p", "d", "fn").Return(&bq.RoutineMetadata{
				Type:       "SCALAR_FUNCTION",
				Arguments:  []*bq.RoutineArgument{{Name: "x", DataType: &bq.StandardSQLDataType{TypeKind: "INT64"}}},
				ReturnType: &bq.StandardSQLDataType{TypeKind: "INT64"},
			}, nil).AnyTimes()
			bqClient.EXPECT().GetRoutineMetadata(gomock.Any(), "p", "d", "denied").Return(nil, &googleapi.Error{Code: 403, Message: "Access Denied"}).AnyTimes()
			bqClient.EXPECT().GetRoutineMetadata(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, &googleapi.Error{Code: 404, Message: "Not found"}).AnyTimes()
			analyzer := file.NewAnalyzer(logrus.New(), bqClient)

			got := analyzer.ParseFile("uri", tt.file)
			defer got.Close()
			if diff := cmp.Diff(tt.expectedErrs, got.Errors, cmpopts.IgnoreUnexported()); diff != "" {
				t.Errorf("ParseFile result diff (-expect, +got)\n%s", diff)
			}
		})
	}
}

func TestAnalyzer_ParseFile_StatementResults(t *testing.T) {
	type statementResult struct {
		hasOutput bool
		errMsg    string
	}
	tests := map[string]struct {
		file               string
		bqTableMetadataMap map[string]*bq.TableMetadata

		expected []statementResult
	}{
		"failed statement followed by successful statement": {
			file: "SELECT UNKNOWN_FUNC();\nSELECT id FROM `project.dataset.table`",
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {
					Schema: bq.Schema{{Name: "id", Type: bq.IntegerFieldType}},
				},
			},
			expected: []statementResult{
				{errMsg: "Function not found: UNKNOWN_FUNC"},
				{hasOutput: true},
			},
		},
		"CREATE TEMP FUNCTION without RETURNS": {
			file: "CREATE TEMP FUNCTION f(x INT64) AS (x);\nSELECT 1",
			expected: []statementResult{
				{errMsg: "Currently, bqls does not support function without return type."},
				{hasOutput: true},
			},
		},
		"DECLARE has no entry": {
			file: "DECLARE x INT64;\nSELECT x",
			expected: []statementResult{
				{hasOutput: true},
			},
		},
		"statement fixed by a fix-up after a statement failing for good": {
			file: "SELECT UNKNOWN_FUNC();\nSELECT unknown_col FROM `project.dataset.table`",
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {
					Schema: bq.Schema{{Name: "id", Type: bq.IntegerFieldType}},
				},
			},
			expected: []statementResult{
				{errMsg: "Function not found: UNKNOWN_FUNC"},
				{hasOutput: true},
			},
		},
		"SET assignment has an entry without output": {
			file: "DECLARE x INT64;\nSET x = 1;\nSELECT x",
			expected: []statementResult{
				{},
				{hasOutput: true},
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
			analyzer := file.NewAnalyzer(logrus.New(), bqClient)

			got := analyzer.ParseFile("uri", tt.file)
			defer got.Close()

			results := make([]statementResult, len(got.Statements))
			for i, s := range got.Statements {
				if s.Output != nil && s.Err != nil {
					t.Errorf("statement %d has both output and error", i)
				}
				results[i].hasOutput = s.Output != nil
				if s.Err != nil {
					results[i].errMsg = s.Err.Msg
				}
			}
			if diff := cmp.Diff(tt.expected, results, cmp.AllowUnexported(statementResult{})); diff != "" {
				t.Errorf("statement results diff (-expect, +got)\n%s", diff)
			}
		})
	}
}

func TestParsedFile_FindTargetAnalyzeOutput(t *testing.T) {
	tests := map[string]struct {
		file               string
		bqTableMetadataMap map[string]*bq.TableMetadata
		position           lsp.Position

		expectedOK bool
	}{
		// Each retry fixes one reference to x, so the retry limit is reached
		// before the third statement is analyzed.
		"statement not analyzed because the retry limit is reached": {
			file: "DECLARE x INT64 DEFAULT 1;\nSELECT a FROM `p.d.t1`;\nSELECT b FROM `p.d.t2` WHERE b IN (x,x,x,x,x,x,x,x,x,x,x,x)",
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"p.d.t1": {Schema: bq.Schema{{Name: "a", Type: bq.IntegerFieldType}}},
				"p.d.t2": {Schema: bq.Schema{{Name: "b", Type: bq.IntegerFieldType}}},
			},
			position:   lsp.Position{Line: 2, Character: 7},
			expectedOK: false,
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
			analyzer := file.NewAnalyzer(logrus.New(), bqClient)

			got := analyzer.ParseFile("uri", tt.file)
			defer got.Close()

			_, ok := got.FindTargetAnalyzeOutput(got.TermOffset(tt.position))
			if ok != tt.expectedOK {
				t.Errorf("FindTargetAnalyzeOutput ok = %v, want %v", ok, tt.expectedOK)
			}
		})
	}
}

func TestAnalyzer_ParseFileWithIncompleteQueryReportsOnlyUserError(t *testing.T) {
	tests := map[string]struct {
		file    string
		wantErr file.Error
	}{
		"WHERE before a line comment": {
			file:    "SELECT id FROM `project.dataset.table` WHERE -- a b",
			wantErr: file.Error{Msg: "Syntax error: Unexpected end of script", Position: lsp.Position{Character: 51}},
		},
		"GROUP BY before a line comment": {
			file:    "SELECT id FROM `project.dataset.table` GROUP BY -- c",
			wantErr: file.Error{Msg: "Syntax error: Unexpected end of script", Position: lsp.Position{Character: 52}},
		},
		"WITH clause before a line comment": {
			file:    "WITH t AS (SELECT 1) -- c",
			wantErr: file.Error{Msg: "Syntax error: Unexpected end of script", Position: lsp.Position{Character: 25}},
		},
		"expression before a line comment": {
			file:    "SELECT 1 + -- c",
			wantErr: file.Error{Msg: "Syntax error: Unexpected end of script", Position: lsp.Position{Character: 15}},
		},
		"SET without expression": {
			file:    "DECLARE x INT64;\nSET x =",
			wantErr: file.Error{Msg: "Syntax error: Unexpected end of script", Position: lsp.Position{Line: 1, Character: 7}},
		},
		"binary operator": {
			file:    "SELECT 1 +",
			wantErr: file.Error{Msg: "Syntax error: Unexpected end of script", Position: lsp.Position{Character: 10}},
		},
		"CASE": {
			file:    "SELECT CASE",
			wantErr: file.Error{Msg: "Syntax error: Unexpected end of script", Position: lsp.Position{Character: 11}},
		},
		"ARRAY type": {
			file:    "SELECT ARRAY<",
			wantErr: file.Error{Msg: "Syntax error: Unexpected end of script", Position: lsp.Position{Character: 13}},
		},
		"DECLARE DEFAULT": {
			file:    "DECLARE x DEFAULT",
			wantErr: file.Error{Msg: "Syntax error: Unexpected end of script", Position: lsp.Position{Character: 17}},
		},
		"IF THEN": {
			file:    "IF TRUE THEN",
			wantErr: file.Error{Msg: "Syntax error: Unexpected end of script", Position: lsp.Position{Character: 12}},
		},
		"SET parenthesis": {
			file:    "DECLARE x INT64;\nSET x = (",
			wantErr: file.Error{Msg: "Syntax error: Unexpected end of script", Position: lsp.Position{Line: 1, Character: 9}},
		},
		"IN parenthesis": {
			file:    "SELECT 1 IN (",
			wantErr: file.Error{Msg: "Syntax error: Unexpected end of script", Position: lsp.Position{Character: 13}},
		},
		"SET parenthesis before a line comment": {
			file:    "DECLARE x INT64;\nSET x = ( -- c",
			wantErr: file.Error{Msg: "Syntax error: Unexpected end of script", Position: lsp.Position{Line: 1, Character: 14}},
		},
		"function body": {
			file:    "CREATE TEMP FUNCTION f() AS (",
			wantErr: file.Error{Msg: "Syntax error: Unexpected end of script", Position: lsp.Position{Character: 29}},
		},
		"GROUP BY in a subquery": {
			file:    "SELECT 1 FROM (SELECT id FROM `project.dataset.table` GROUP BY",
			wantErr: file.Error{Msg: "Syntax error: Unexpected end of script", Position: lsp.Position{Character: 62}},
		},
		"ORDER in a window": {
			file:    "SELECT ROW_NUMBER() OVER (ORDER",
			wantErr: file.Error{Msg: "Syntax error: Expected @ for hint or keyword BY but got end of script", Position: lsp.Position{Character: 31}},
		},
		"UNION ALL in a subquery": {
			file:    "SELECT 1 FROM (SELECT 1 UNION ALL",
			wantErr: file.Error{Msg: `Syntax error: Expected "(" or keyword SELECT or keyword TABLE but got end of script`, Position: lsp.Position{Character: 33}},
		},
	}

	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			bqClient := mock_bigquery.NewMockClient(ctrl)
			bqClient.EXPECT().GetDefaultProject().Return("project").AnyTimes()
			bqClient.EXPECT().GetTableMetadata(gomock.Any(), "project", "dataset", "table").Return(&bq.TableMetadata{
				Schema: bq.Schema{{Name: "id", Type: bq.IntegerFieldType}},
			}, nil).AnyTimes()
			analyzer := file.NewAnalyzer(logrus.New(), bqClient)

			got := analyzer.ParseFile("uri", tt.file)
			defer got.Close()

			want := []file.Error{}
			if tt.wantErr.Msg != "" {
				want = []file.Error{tt.wantErr}
			}
			if diff := cmp.Diff(want, got.Errors, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("ParseFile errors diff (-want, +got)\n%s", diff)
			}
		})
	}
}

func TestParsedFile_ExtractSQLOfExpressionStartingAtDeclaredVariable(t *testing.T) {
	ctrl := gomock.NewController(t)
	bqClient := mock_bigquery.NewMockClient(ctrl)
	bqClient.EXPECT().GetDefaultProject().Return("project").AnyTimes()
	analyzer := file.NewAnalyzer(logrus.New(), bqClient)

	src := "DECLARE yyyyyy INT64;\nSELECT yyyyyy + 1"
	got := analyzer.ParseFile("uri", src)
	defer got.Close()

	pos := lsp.Position{Line: 1, Character: len("SELECT ")}
	binExpr, ok := file.SearchAstNode[*googlesql.ASTBinaryExpression](got.Node, got.TermOffset(pos))
	if !ok {
		t.Fatalf("no binary expression at %+v in %q", pos, got.FixedSrc)
	}
	loc, _ := binExpr.GetParseLocationRange()

	sql, ok := got.ExtractSQL(loc)
	if !ok || sql != "yyyyyy + 1" {
		t.Errorf("ExtractSQL = %q, %v, want %q", sql, ok, "yyyyyy + 1")
	}
	rng, ok := got.ToLspRange(loc)
	wantRange := lsp.Range{Start: pos, End: lsp.Position{Line: 1, Character: len("SELECT yyyyyy + 1")}}
	if diff := cmp.Diff(wantRange, rng); !ok || diff != "" {
		t.Errorf("ToLspRange diff (-want, +got)\n%s", diff)
	}
}

func TestParsedFile_ToLspRangeKeepsLeadingWhitespaceOfDeletedClause(t *testing.T) {
	ctrl := gomock.NewController(t)
	bqClient := mock_bigquery.NewMockClient(ctrl)
	bqClient.EXPECT().GetDefaultProject().Return("project").AnyTimes()
	bqClient.EXPECT().GetTableMetadata(gomock.Any(), "project", "dataset", "table").Return(&bq.TableMetadata{
		Schema: bq.Schema{{Name: "id", Type: bq.IntegerFieldType}},
	}, nil).AnyTimes()
	analyzer := file.NewAnalyzer(logrus.New(), bqClient)

	got := analyzer.ParseFile("uri", "\n\n  SELECT id FROM `project.dataset.table` GROUP BY")
	defer got.Close()

	pos := lsp.Position{Line: 2, Character: len("  SELECT ")}
	path, ok := file.SearchAstNode[*googlesql.ASTPathExpression](got.Node, got.TermOffset(pos))
	if !ok {
		t.Fatalf("no path expression at %+v in %q", pos, got.FixedSrc)
	}
	loc, _ := path.GetParseLocationRange()
	rng, ok := got.ToLspRange(loc)
	wantRange := lsp.Range{Start: pos, End: lsp.Position{Line: 2, Character: len("  SELECT id")}}
	if diff := cmp.Diff(wantRange, rng); !ok || diff != "" {
		t.Errorf("ToLspRange diff (-want, +got)\n%s", diff)
	}
}

func TestAnalyzer_ParseFileAnalyzesQueryEndingInIncompleteClause(t *testing.T) {
	tests := map[string]struct {
		file    string
		wantErr file.Error
	}{
		"ORDER": {
			file:    "SELECT id FROM `project.dataset.table` ORDER",
			wantErr: file.Error{Msg: "Syntax error: Expected @ for hint or keyword BY but got end of script", Position: lsp.Position{Character: 44}},
		},
		"lowercase group by": {
			file:    "select id from `project.dataset.table` group by",
			wantErr: file.Error{Msg: "Syntax error: Unexpected end of script", Position: lsp.Position{Character: 47}},
		},
		"UNION ALL": {
			file:    "SELECT id FROM `project.dataset.table` UNION ALL",
			wantErr: file.Error{Msg: `Syntax error: Expected "(" or keyword SELECT or keyword TABLE but got end of script`, Position: lsp.Position{Character: 48}},
		},
		"UNION before a line comment": {
			file:    "SELECT id FROM `project.dataset.table` UNION -- c",
			wantErr: file.Error{Msg: "Syntax error: Expected keyword ALL or keyword DISTINCT but got end of script", Position: lsp.Position{Character: 49}},
		},
	}

	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			bqClient := mock_bigquery.NewMockClient(ctrl)
			bqClient.EXPECT().GetDefaultProject().Return("project").AnyTimes()
			bqClient.EXPECT().GetTableMetadata(gomock.Any(), "project", "dataset", "table").Return(&bq.TableMetadata{
				Schema: bq.Schema{{Name: "id", Type: bq.IntegerFieldType}},
			}, nil).AnyTimes()
			analyzer := file.NewAnalyzer(logrus.New(), bqClient)

			got := analyzer.ParseFile("uri", tt.file)
			defer got.Close()

			if diff := cmp.Diff([]file.Error{tt.wantErr}, got.Errors); diff != "" {
				t.Errorf("ParseFile errors diff (-want, +got)\n%s", diff)
			}
			if len(got.RNode) != 1 {
				t.Errorf("the query was not analyzed: got %d analyzer outputs, want 1", len(got.RNode))
			}
		})
	}
}
