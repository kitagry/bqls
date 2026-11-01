package lint_test

import (
	"context"
	"fmt"
	"testing"

	bq "cloud.google.com/go/bigquery"
	"github.com/golang/mock/gomock"
	"github.com/google/go-cmp/cmp"
	"github.com/kitagry/bqls/langserver/internal/bigquery/mock_bigquery"
	"github.com/kitagry/bqls/langserver/internal/lint"
	"github.com/kitagry/bqls/langserver/internal/source/file"
	"github.com/sirupsen/logrus"
)

func intColumns(names ...string) bq.Schema {
	s := make(bq.Schema, len(names))
	for i, n := range names {
		s[i] = &bq.FieldSchema{Name: n, Type: bq.IntegerFieldType}
	}
	return s
}

var insertTables = map[string]bq.Schema{
	"p.d.t":     intColumns("a", "b"),
	"p.d.s":     intColumns("a", "b"),
	"p.d.t3":    intColumns("a", "b", "c"),
	"p.d.s3":    intColumns("a", "b", "c"),
	"p.d.edges": intColumns("src", "dst"),
	"p.d.users": {
		{Name: "id", Type: bq.IntegerFieldType},
		{Name: "first_name", Type: bq.StringFieldType},
		{Name: "last_name", Type: bq.StringFieldType},
	},
	"p.d.staging_users": {
		{Name: "id", Type: bq.IntegerFieldType},
		{Name: "first_name", Type: bq.StringFieldType},
		{Name: "last_name", Type: bq.StringFieldType},
	},
}

func parseInsertTables(t *testing.T, src string) file.ParsedFile {
	t.Helper()
	ctrl := gomock.NewController(t)
	bqClient := mock_bigquery.NewMockClient(ctrl)
	bqClient.EXPECT().GetDefaultProject().Return("p").AnyTimes()
	bqClient.EXPECT().GetTableMetadata(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, project, dataset, table string) (*bq.TableMetadata, error) {
			s, ok := insertTables[project+"."+dataset+"."+table]
			if !ok {
				return nil, fmt.Errorf("table %s.%s.%s not found", project, dataset, table)
			}
			return &bq.TableMetadata{Type: bq.RegularTable, Schema: s}, nil
		}).AnyTimes()
	parsed := file.NewAnalyzer(logrus.New(), bqClient).ParseFile("file:///a.sql", src)
	if len(parsed.Errors) > 0 {
		t.Fatalf("unexpected analysis errors: %v", parsed.Errors)
	}
	return parsed
}

func TestInsertColumnOrder(t *testing.T) {
	type report struct {
		// sub is the reported source text.
		sub string
		// table is empty when the statement has a column list.
		table string
		got   string
		want  string
	}
	tests := map[string]struct {
		src  string
		want []report
	}{
		"swapped literals": {
			src:  "INSERT INTO p.d.t SELECT 2 AS b, 1 AS a",
			want: []report{{sub: "2 AS b, 1 AS a", table: "p.d.t", got: "`b`, `a`", want: "`a`, `b`"}},
		},
		"swapped columns from another table": {
			src:  "INSERT INTO p.d.users\nSELECT id, last_name, first_name FROM p.d.staging_users",
			want: []report{{sub: "id, last_name, first_name", table: "p.d.users", got: "`last_name`, `first_name`", want: "`first_name`, `last_name`"}},
		},
		"implicit aliases of paths": {
			src:  "INSERT INTO p.d.t SELECT s.b, s.a FROM p.d.s AS s",
			want: []report{{sub: "s.b, s.a", table: "p.d.t", got: "`b`, `a`", want: "`a`, `b`"}},
		},
		"INSERT without INTO": {
			src:  "INSERT p.d.t SELECT b, a FROM p.d.s",
			want: []report{{sub: "b, a", table: "p.d.t", got: "`b`, `a`", want: "`a`, `b`"}},
		},
		"names compare case-insensitively": {
			src:  "INSERT INTO p.d.t SELECT 2 AS B, 1 AS A",
			want: []report{{sub: "2 AS B, 1 AS A", table: "p.d.t", got: "`B`, `A`", want: "`a`, `b`"}},
		},
		"only the misplaced columns are named": {
			src:  "INSERT INTO p.d.t3 SELECT 0 AS a, 2 AS c, 1 AS b",
			want: []report{{sub: "0 AS a, 2 AS c, 1 AS b", table: "p.d.t3", got: "`c`, `b`", want: "`b`, `c`"}},
		},
		"unnamed expressions are left out": {
			src:  "INSERT INTO p.d.t3 SELECT 1 AS b, 0 AS a, 2 + 3",
			want: []report{{sub: "1 AS b, 0 AS a, 2 + 3", table: "p.d.t3", got: "`b`, `a`", want: "`a`, `b`"}},
		},
		"column list": {
			src:  "INSERT INTO p.d.t (b, a) SELECT a, b FROM p.d.s",
			want: []report{{sub: "a, b", got: "`a`, `b`", want: "`b`, `a`"}},
		},
		"column list with fewer columns than the table": {
			src:  "INSERT INTO p.d.t3 (c, a) SELECT a, c FROM p.d.s3",
			want: []report{{sub: "a, c", got: "`a`, `c`", want: "`c`, `a`"}},
		},
		"query with WITH": {
			src:  "INSERT INTO p.d.t WITH c AS (SELECT 1 AS a, 2 AS b) SELECT b, a FROM c",
			want: []report{{sub: "b, a", table: "p.d.t", got: "`b`, `a`", want: "`a`, `b`"}},
		},
		"parenthesized query": {
			src:  "INSERT INTO p.d.t (SELECT 2 AS b, 1 AS a)",
			want: []report{{sub: "2 AS b, 1 AS a", table: "p.d.t", got: "`b`, `a`", want: "`a`, `b`"}},
		},
		"set operation takes its first input's names": {
			src:  "INSERT INTO p.d.t SELECT 2 AS b, 1 AS a UNION ALL SELECT 4 AS b, 3 AS a",
			want: []report{{sub: "2 AS b, 1 AS a", table: "p.d.t", got: "`b`, `a`", want: "`a`, `b`"}},
		},
		"after another statement": {
			src:  "SELECT 1;\nINSERT INTO p.d.t SELECT 2 AS b, 1 AS a",
			want: []report{{sub: "2 AS b, 1 AS a", table: "p.d.t", got: "`b`, `a`", want: "`a`, `b`"}},
		},
		"same order": {
			src: "INSERT INTO p.d.t SELECT 1 AS a, 2 AS b",
		},
		"column list in the query's order": {
			src: "INSERT INTO p.d.t (b, a) SELECT b, a FROM p.d.s",
		},
		"aliases that put source columns in place": {
			src: "INSERT INTO p.d.t SELECT b AS a, a AS b FROM p.d.s",
		},
		"names merely differ": {
			src: "INSERT INTO p.d.t SELECT 1 AS c, 2 AS d",
		},
		"shifted names are not a reordering": {
			src: "INSERT INTO p.d.t3 SELECT 1 AS b, 2 AS c, 3 AS d",
		},
		"unnamed expressions only": {
			src: "INSERT INTO p.d.t SELECT 2 + 0, 1 + 0",
		},
		"swap over the target table is intended": {
			src: "INSERT INTO p.d.edges SELECT dst, src FROM p.d.edges",
		},
		"swap over a CTE reading the target table is intended": {
			src: "INSERT INTO p.d.edges WITH e AS (SELECT * FROM p.d.edges) SELECT dst, src FROM e",
		},
		"SELECT * is skipped": {
			src: "INSERT INTO p.d.t SELECT * FROM (SELECT 2 AS b, 1 AS a)",
		},
		"VALUES is skipped": {
			src: "INSERT INTO p.d.t (b, a) VALUES (1, 2)",
		},
		"pipe query is skipped": {
			src: "INSERT INTO p.d.t FROM p.d.s |> SELECT b, a",
		},
		"set operation BY NAME is skipped": {
			src: "INSERT INTO p.d.t SELECT 2 AS b, 1 AS a UNION ALL BY NAME SELECT 3 AS a, 4 AS b",
		},
		"suppressed by comment": {
			src: "INSERT INTO p.d.t\n-- bqls:ignore insert-column-order\nSELECT 2 AS b, 1 AS a",
		},
	}

	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			var want []lint.Diagnostic
			for _, r := range tt.want {
				msg := fmt.Sprintf("INSERT matches columns to the column list by position, not by name, but this query has %s where the column list has %s; reorder the columns", r.got, r.want)
				if r.table != "" {
					msg = fmt.Sprintf("INSERT without a column list matches columns to `%s` by position, not by name, but this query has %s where the table has %s; reorder the columns or add a column list", r.table, r.got, r.want)
				}
				want = append(want, lint.Diagnostic{
					Rule:     "insert-column-order",
					Range:    spanOf(t, tt.src, r.sub, 0),
					Message:  msg,
					Severity: lint.SeverityWarning,
				})
			}
			got := lint.Run(t.Context(), parseInsertTables(t, tt.src), lint.Config{}, lint.Env{}, lint.InsertColumnOrder)
			if diff := cmp.Diff(want, got); diff != "" {
				t.Errorf("diff (-want +got):\n%s", diff)
			}
		})
	}
}

func TestInsertColumnOrderConfig(t *testing.T) {
	parsed := parseInsertTables(t, "INSERT INTO p.d.t SELECT 2 AS b, 1 AS a")
	off := lint.Config{Rules: map[string]bool{"insert-column-order": false}}
	if got := lint.Run(t.Context(), parsed, off, lint.Env{}, lint.InsertColumnOrder); len(got) != 0 {
		t.Errorf("disabled rule reported %+v", got)
	}
	if got := lint.Run(t.Context(), parsed, lint.Config{}, lint.Env{}, lint.InsertColumnOrder); len(got) != 1 {
		t.Errorf("rule should be enabled by default, got %+v", got)
	}
}
