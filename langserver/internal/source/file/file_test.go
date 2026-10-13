package file_test

import (
	"fmt"
	"strings"
	"testing"

	bq "cloud.google.com/go/bigquery"
	"github.com/golang/mock/gomock"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/kitagry/bqls/langserver/internal/bigquery"
	"github.com/kitagry/bqls/langserver/internal/bigquery/mock_bigquery"
	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/kitagry/bqls/langserver/internal/source/file"
	"github.com/sirupsen/logrus"
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
			if diff := cmp.Diff(tt.expectedFixOffsets, got.FixOffsets); diff != "" {
				t.Errorf("FixOffsets diff (-expect, +got)\n%s", diff)
			}
			if len(got.RNode) == 0 {
				t.Errorf("failed to analyze")
			}
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
				bqClient.EXPECT().GetTableMetadata(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, fmt.Errorf("not found"))
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
			file: "SET @@dataset_id = 'foo';",
			expectedErrs: []file.Error{
				{
					Msg:                  "Unrecognized name: @@dataset_id",
					Position:             lsp.Position{Line: 0, Character: 4},
					TermLength:           12,
					IncompleteColumnName: "@@dataset_id",
				},
			},
			expectedRNodeLen: 0,
		},
		// While typing a SET, the parser error and its recovery attempt are
		// reported just like for other incomplete statements.
		"incomplete SET with trailing space": {
			file: "DECLARE x INT64;\nSET x = ",
			expectedErrs: []file.Error{
				{Msg: "Syntax error: Unexpected end of script", Position: lsp.Position{Line: 1, Character: 7}},
				{Msg: "Syntax error: Unexpected keyword SELECT", Position: lsp.Position{Line: 1, Character: 8}},
			},
			expectedRNodeLen: 0,
		},
		"incomplete SET without expression": {
			file: "DECLARE x INT64;\nSET x =",
			expectedErrs: []file.Error{
				{Msg: "Syntax error: Unexpected end of script", Position: lsp.Position{Line: 1, Character: 7}},
				{Msg: "Syntax error: Unexpected keyword SELECT", Position: lsp.Position{Line: 1, Character: 8}},
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
				{Msg: "Syntax error: Unexpected keyword SELECT", Position: lsp.Position{Line: 1, Character: 16}},
			},
			expectedRNodeLen: 0,
		},
		"incomplete SET struct assignment": {
			file: "DECLARE a INT64;\nDECLARE b INT64;\nSET (a, b) = ",
			expectedErrs: []file.Error{
				{Msg: "Syntax error: Unexpected end of script", Position: lsp.Position{Line: 2, Character: 12}},
				{Msg: "Syntax error: Unexpected keyword SELECT", Position: lsp.Position{Line: 2, Character: 13}},
			},
			expectedRNodeLen: 0,
		},
		"incomplete SET parameter": {
			file: "SET @p = ",
			expectedErrs: []file.Error{
				{Msg: "Syntax error: Unexpected end of script", Position: lsp.Position{Line: 0, Character: 8}},
				{Msg: "Syntax error: Unexpected keyword SELECT", Position: lsp.Position{Line: 0, Character: 9}},
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
			if diff := cmp.Diff(tt.expectedErrs, got.Errors, cmpopts.IgnoreUnexported()); diff != "" {
				t.Errorf("ParseFile result diff (-expect, +got)\n%s", diff)
			}
			if len(got.RNode) != tt.expectedRNodeLen {
				t.Errorf("expected %d analyzer outputs, got %d", tt.expectedRNodeLen, len(got.RNode))
			}
		})
	}
}
