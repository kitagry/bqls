package completion

import (
	"os"
	"slices"
	"strings"
	"testing"

	bq "cloud.google.com/go/bigquery"
	"github.com/golang/mock/gomock"
	"github.com/kitagry/bqls/langserver/internal/bigquery/mock_bigquery"
	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/kitagry/bqls/langserver/internal/source/file"
	"github.com/kitagry/bqls/langserver/internal/source/helper"
	"github.com/sirupsen/logrus"
)

func TestCompletor_CompleteKeyword(t *testing.T) {
	tests := map[string]struct {
		files              map[lsp.DocumentURI]string
		bqTableMetadataMap map[string]*bq.TableMetadata

		expectContains    []CompletionItem
		expectNotContains []string
	}{
		"Complete SELECT keyword": {
			files: map[lsp.DocumentURI]string{
				"a.sql": "|",
			},
			bqTableMetadataMap: map[string]*bq.TableMetadata{},
			expectContains: []CompletionItem{
				{
					Kind:    lsp.CIKKeyword,
					NewText: "SELECT ",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "The SELECT statement is used to query data from a table.",
					},
				},
			},
			expectNotContains: []string{"FROM ", "WHERE "},
		},
		"Complete SELECT keyword with half-baked": {
			files: map[lsp.DocumentURI]string{
				"a.sql": "S|",
			},
			bqTableMetadataMap: map[string]*bq.TableMetadata{},
			expectContains: []CompletionItem{
				{
					Kind:        lsp.CIKKeyword,
					NewText:     "SELECT ",
					TypedPrefix: "S",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "The SELECT statement is used to query data from a table.",
					},
				},
			},
		},
		"Complete FROM keyword": {
			files: map[lsp.DocumentURI]string{
				"a.sql": "SELECT * |",
			},
			bqTableMetadataMap: map[string]*bq.TableMetadata{},
			expectContains: []CompletionItem{
				{
					Kind:    lsp.CIKKeyword,
					NewText: "FROM ",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "The FROM clause specifies the table to query data from.",
					},
				},
			},
			expectNotContains: []string{"SELECT ", "WHERE "},
		},
		"Complete WHERE and GROUP BY keywords": {
			files: map[lsp.DocumentURI]string{
				"a.sql": "SELECT * FROM `project.dataset.table` |",
			},
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {},
			},
			expectContains: []CompletionItem{
				{
					Kind:    lsp.CIKKeyword,
					NewText: "WHERE ",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "The WHERE clause is used to filter records.",
					},
				},
				{
					Kind:    lsp.CIKKeyword,
					NewText: "GROUP BY ",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "The GROUP BY clause groups rows that have the same values.",
					},
				},
			},
			expectNotContains: []string{"SELECT ", "FROM "},
		},
		"Complete WHERE and GROUP BY keywords with half-baked": {
			files: map[lsp.DocumentURI]string{
				"a.sql": "SELECT * FROM `project.dataset.table` W|",
			},
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {},
			},
			expectContains: []CompletionItem{
				{
					Kind:        lsp.CIKKeyword,
					NewText:     "WHERE ",
					TypedPrefix: "W",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "The WHERE clause is used to filter records.",
					},
				},
			},
		},
		"Complete GROUP BY after WHERE": {
			files: map[lsp.DocumentURI]string{
				"a.sql": "SELECT * FROM `project.dataset.table` WHERE col = 1 |",
			},
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {},
			},
			expectContains: []CompletionItem{
				{
					Kind:    lsp.CIKKeyword,
					NewText: "GROUP BY ",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "The GROUP BY clause groups rows that have the same values.",
					},
				},
			},
			expectNotContains: []string{"SELECT ", "FROM ", "WHERE "},
		},
		"Complete ORDER BY after GROUP BY": {
			files: map[lsp.DocumentURI]string{
				"a.sql": "SELECT * FROM `project.dataset.table` GROUP BY col |",
			},
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {},
			},
			expectContains: []CompletionItem{
				{
					Kind:    lsp.CIKKeyword,
					NewText: "ORDER BY ",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "The ORDER BY clause is used to sort the result set.",
					},
				},
			},
			expectNotContains: []string{"SELECT ", "FROM ", "WHERE ", "GROUP BY "},
		},
		"Complete LIMIT after ORDER BY": {
			files: map[lsp.DocumentURI]string{
				"a.sql": "SELECT * FROM `project.dataset.table` ORDER BY col |",
			},
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {},
			},
			expectContains: []CompletionItem{
				{
					Kind:    lsp.CIKKeyword,
					NewText: "LIMIT ",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "The LIMIT clause is used to limit the number of rows returned.",
					},
				},
			},
			expectNotContains: []string{"SELECT ", "FROM ", "WHERE ", "GROUP BY ", "ORDER BY "},
		},
		"Complete ASC and DESC after ORDER BY column": {
			files: map[lsp.DocumentURI]string{
				"a.sql": "SELECT * FROM `project.dataset.table` ORDER BY col |",
			},
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {},
			},
			expectContains: []CompletionItem{
				{
					Kind:    lsp.CIKKeyword,
					NewText: "ASC",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "Sort in ascending order (default).",
					},
				},
				{
					Kind:    lsp.CIKKeyword,
					NewText: "DESC",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "Sort in descending order.",
					},
				},
			},
			expectNotContains: []string{"SELECT ", "FROM ", "WHERE "},
		},
		"Complete LIMIT after ORDER BY with ASC": {
			files: map[lsp.DocumentURI]string{
				"a.sql": "SELECT * FROM `project.dataset.table` ORDER BY col ASC |",
			},
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {},
			},
			expectContains: []CompletionItem{
				{
					Kind:    lsp.CIKKeyword,
					NewText: "LIMIT ",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "The LIMIT clause is used to limit the number of rows returned.",
					},
				},
			},
			expectNotContains: []string{"SELECT ", "FROM ", "WHERE ", "ORDER BY ", "ASC", "DESC"},
		},
		"Complete LIMIT after ORDER BY with DESC": {
			files: map[lsp.DocumentURI]string{
				"a.sql": "SELECT * FROM `project.dataset.table` ORDER BY col DESC |",
			},
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {},
			},
			expectContains: []CompletionItem{
				{
					Kind:    lsp.CIKKeyword,
					NewText: "LIMIT ",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "The LIMIT clause is used to limit the number of rows returned.",
					},
				},
			},
			expectNotContains: []string{"SELECT ", "FROM ", "WHERE ", "ORDER BY ", "ASC", "DESC"},
		},
		"Complete OFFSET after LIMIT": {
			files: map[lsp.DocumentURI]string{
				"a.sql": "SELECT * FROM `project.dataset.table` LIMIT 10 |",
			},
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {},
			},
			expectContains: []CompletionItem{
				{
					Kind:    lsp.CIKKeyword,
					NewText: "OFFSET ",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "The OFFSET clause is used to skip a specified number of rows.",
					},
				},
			},
			expectNotContains: []string{"SELECT ", "FROM ", "WHERE ", "LIMIT "},
		},
		"Complete nothing after OFFSET": {
			files: map[lsp.DocumentURI]string{
				"a.sql": "SELECT * FROM `project.dataset.table` LIMIT 10 OFFSET 5 |",
			},
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {},
			},
			expectContains:    []CompletionItem{},
			expectNotContains: []string{"SELECT ", "FROM ", "WHERE ", "LIMIT ", "OFFSET "},
		},
		"Complete JOIN after FROM table": {
			files: map[lsp.DocumentURI]string{
				"a.sql": "SELECT * FROM `project.dataset.table1` |",
			},
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table1": {},
			},
			expectContains: []CompletionItem{
				{
					Kind:    lsp.CIKKeyword,
					NewText: "JOIN ",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "INNER JOIN - returns rows when there is a match in both tables.",
					},
				},
				{
					Kind:    lsp.CIKKeyword,
					NewText: "LEFT JOIN ",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "LEFT JOIN - returns all rows from the left table, and matched rows from the right table.",
					},
				},
				{
					Kind:    lsp.CIKKeyword,
					NewText: "RIGHT JOIN ",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "RIGHT JOIN - returns all rows from the right table, and matched rows from the left table.",
					},
				},
				{
					Kind:    lsp.CIKKeyword,
					NewText: "FULL OUTER JOIN ",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "FULL OUTER JOIN - returns all rows when there is a match in either table.",
					},
				},
				{
					Kind:    lsp.CIKKeyword,
					NewText: "CROSS JOIN ",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "CROSS JOIN - returns the Cartesian product of both tables.",
					},
				},
			},
			expectNotContains: []string{"SELECT ", "FROM "},
		},
		"Complete ON after JOIN table": {
			files: map[lsp.DocumentURI]string{
				"a.sql": "SELECT * FROM `project.dataset.table1` JOIN `project.dataset.table2` |",
			},
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table1": {},
				"project.dataset.table2": {},
			},
			expectContains: []CompletionItem{
				{
					Kind:    lsp.CIKKeyword,
					NewText: "ON ",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "The ON clause specifies the join condition between tables.",
					},
				},
			},
			expectNotContains: []string{"SELECT ", "FROM ", "JOIN "},
		},
		"Complete HAVING after GROUP BY": {
			files: map[lsp.DocumentURI]string{
				"a.sql": "SELECT col, COUNT(*) FROM `project.dataset.table` GROUP BY col |",
			},
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {},
			},
			expectContains: []CompletionItem{
				{
					Kind:    lsp.CIKKeyword,
					NewText: "HAVING ",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "The HAVING clause is used to filter groups based on aggregate functions.",
					},
				},
			},
			expectNotContains: []string{"SELECT ", "FROM ", "WHERE "},
		},
		"Complete ORDER BY and LIMIT after HAVING": {
			files: map[lsp.DocumentURI]string{
				"a.sql": "SELECT col, COUNT(*) FROM `project.dataset.table` GROUP BY col HAVING COUNT(*) > 1 |",
			},
			bqTableMetadataMap: map[string]*bq.TableMetadata{
				"project.dataset.table": {},
			},
			expectContains: []CompletionItem{
				{
					Kind:    lsp.CIKKeyword,
					NewText: "ORDER BY ",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "The ORDER BY clause is used to sort the result set.",
					},
				},
				{
					Kind:    lsp.CIKKeyword,
					NewText: "LIMIT ",
					Documentation: lsp.MarkupContent{
						Kind:  lsp.MKPlainText,
						Value: "The LIMIT clause is used to limit the number of rows returned.",
					},
				},
			},
			expectNotContains: []string{"SELECT ", "FROM ", "WHERE ", "GROUP BY ", "HAVING "},
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
			logger.SetOutput(os.Stdout)

			analyzer := file.NewAnalyzer(logger, bqClient)
			completor := New(logger, analyzer, bqClient)

			files, path, position, err := helper.GetLspPosition(tt.files)
			if err != nil {
				t.Fatal(err)
			}

			parsedFile := analyzer.ParseFile(path, files[path])
			defer parsedFile.Close()

			got := completor.completeKeyword(t.Context(), parsedFile, position)

			// Check that all expected items are present
			for _, expected := range tt.expectContains {
				if !slices.Contains(got, expected) {
					t.Errorf("Expected completion item not found:\n%+v\nGot:\n%+v", expected, got)
				}
			}

			// Check that none of the unexpected items are present (by NewText only)
			for _, unexpectedText := range tt.expectNotContains {
				for _, item := range got {
					if item.NewText == unexpectedText {
						t.Errorf("Unexpected completion item found with NewText=%q:\n%+v", unexpectedText, item)
						break
					}
				}
			}
		})
	}
}
