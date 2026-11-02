package lint_test

import (
	"context"
	"fmt"
	"testing"

	bq "cloud.google.com/go/bigquery"
	"github.com/golang/mock/gomock"
	"github.com/kitagry/bqls/langserver/internal/bigquery/mock_bigquery"
	"github.com/kitagry/bqls/langserver/internal/lint"
	"github.com/kitagry/bqls/langserver/internal/source/file"
	"github.com/sirupsen/logrus"
)

var nullableTables = map[string]*bq.TableMetadata{
	"p.d.a": {Type: bq.RegularTable, Schema: bq.Schema{
		{Name: "id", Type: bq.IntegerFieldType, Required: true},
		{Name: "v", Type: bq.IntegerFieldType},
	}},
	"p.d.b": {Type: bq.RegularTable, Schema: bq.Schema{
		{Name: "uid", Type: bq.IntegerFieldType},
		{Name: "rid", Type: bq.IntegerFieldType, Required: true},
		{Name: "z", Type: bq.IntegerFieldType},
		{Name: "s", Type: bq.RecordFieldType, Schema: bq.Schema{{Name: "f", Type: bq.IntegerFieldType}}},
	}},
	"p.d.v": {Type: bq.ViewTable, Schema: bq.Schema{{Name: "uid", Type: bq.IntegerFieldType}}},
}

func lookupNullableTable(_ context.Context, path string) (*bq.TableMetadata, error) {
	m, ok := nullableTables[path]
	if !ok {
		return nil, fmt.Errorf("table %s not found", path)
	}
	return m, nil
}

func parseNullable(t *testing.T, src string) file.ParsedFile {
	t.Helper()
	ctrl := gomock.NewController(t)
	bqClient := mock_bigquery.NewMockClient(ctrl)
	bqClient.EXPECT().GetDefaultProject().Return("p").AnyTimes()
	bqClient.EXPECT().GetTableMetadata(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, project, dataset, table string) (*bq.TableMetadata, error) {
			return lookupNullableTable(ctx, project+"."+dataset+"."+table)
		}).AnyTimes()
	parsed := file.NewAnalyzer(logrus.New(), bqClient).ParseFile("file:///a.sql", src)
	if len(parsed.Errors) > 0 {
		t.Fatalf("unexpected analysis errors: %v", parsed.Errors)
	}
	return parsed
}

func notInNullableMessage(column, table string) string {
	return fmt.Sprintf("NOT IN yields NULL, never TRUE, once the subquery returns a NULL, and `%s` of `%s` is NULLABLE; rewrite it with NOT EXISTS, or filter the subquery with `%s IS NOT NULL`", column, table, column)
}

func TestNotInNullable(t *testing.T) {
	type report struct {
		// text is the exact reported source text.
		text   string
		column string
		table  string
	}
	tests := map[string]struct {
		src  string
		want []report
		// fixed is the source after applying the fix; empty means no fix.
		fixed string
	}{
		"nullable column": {
			src:   "SELECT id FROM p.d.a WHERE id NOT IN (SELECT uid FROM p.d.b)",
			want:  []report{{text: "id NOT IN (SELECT uid FROM p.d.b)", column: "uid", table: "p.d.b"}},
			fixed: "SELECT id FROM p.d.a WHERE id NOT IN (SELECT uid FROM p.d.b WHERE uid IS NOT NULL)",
		},
		"not around in": {
			src:   "SELECT id FROM p.d.a WHERE NOT (id IN (SELECT uid FROM p.d.b))",
			want:  []report{{text: "NOT (id IN (SELECT uid FROM p.d.b))", column: "uid", table: "p.d.b"}},
			fixed: "SELECT id FROM p.d.a WHERE NOT (id IN (SELECT uid FROM p.d.b WHERE uid IS NOT NULL))",
		},
		"aliased column": {
			src:   "SELECT id FROM p.d.a WHERE id NOT IN (SELECT b.uid AS k FROM p.d.b AS b)",
			want:  []report{{text: "id NOT IN (SELECT b.uid AS k FROM p.d.b AS b)", column: "uid", table: "p.d.b"}},
			fixed: "SELECT id FROM p.d.a WHERE id NOT IN (SELECT b.uid AS k FROM p.d.b AS b WHERE b.uid IS NOT NULL)",
		},
		"filter on another column": {
			src:   "SELECT id FROM p.d.a WHERE id NOT IN (SELECT uid FROM p.d.b WHERE z > 0)",
			want:  []report{{text: "id NOT IN (SELECT uid FROM p.d.b WHERE z > 0)", column: "uid", table: "p.d.b"}},
			fixed: "SELECT id FROM p.d.a WHERE id NOT IN (SELECT uid FROM p.d.b WHERE z > 0 AND uid IS NOT NULL)",
		},
		"through a cte": {
			src:   "WITH c AS (SELECT uid FROM p.d.b) SELECT id FROM p.d.a WHERE id NOT IN (SELECT uid FROM c)",
			want:  []report{{text: "id NOT IN (SELECT uid FROM c)", column: "uid", table: "p.d.b"}},
			fixed: "WITH c AS (SELECT uid FROM p.d.b) SELECT id FROM p.d.a WHERE id NOT IN (SELECT uid FROM c WHERE uid IS NOT NULL)",
		},
		"through a derived table": {
			src:   "SELECT id FROM p.d.a WHERE id NOT IN (SELECT k FROM (SELECT uid AS k FROM p.d.b))",
			want:  []report{{text: "id NOT IN (SELECT k FROM (SELECT uid AS k FROM p.d.b))", column: "uid", table: "p.d.b"}},
			fixed: "SELECT id FROM p.d.a WHERE id NOT IN (SELECT k FROM (SELECT uid AS k FROM p.d.b) WHERE k IS NOT NULL)",
		},
		"group by key": {
			src:   "SELECT id FROM p.d.a WHERE id NOT IN (SELECT uid FROM p.d.b GROUP BY uid)",
			want:  []report{{text: "id NOT IN (SELECT uid FROM p.d.b GROUP BY uid)", column: "uid", table: "p.d.b"}},
			fixed: "SELECT id FROM p.d.a WHERE id NOT IN (SELECT uid FROM p.d.b WHERE uid IS NOT NULL GROUP BY uid)",
		},
		"distinct": {
			src:   "SELECT id FROM p.d.a WHERE id NOT IN (SELECT DISTINCT uid FROM p.d.b)",
			want:  []report{{text: "id NOT IN (SELECT DISTINCT uid FROM p.d.b)", column: "uid", table: "p.d.b"}},
			fixed: "SELECT id FROM p.d.a WHERE id NOT IN (SELECT DISTINCT uid FROM p.d.b WHERE uid IS NOT NULL)",
		},
		"in select list": {
			src:   "SELECT id NOT IN (SELECT uid FROM p.d.b) AS missing FROM p.d.a",
			want:  []report{{text: "id NOT IN (SELECT uid FROM p.d.b)", column: "uid", table: "p.d.b"}},
			fixed: "SELECT id NOT IN (SELECT uid FROM p.d.b WHERE uid IS NOT NULL) AS missing FROM p.d.a",
		},
		"inside a cte": {
			src:   "WITH c AS (SELECT id FROM p.d.a WHERE id NOT IN (SELECT uid FROM p.d.b)) SELECT id FROM c",
			want:  []report{{text: "id NOT IN (SELECT uid FROM p.d.b)", column: "uid", table: "p.d.b"}},
			fixed: "WITH c AS (SELECT id FROM p.d.a WHERE id NOT IN (SELECT uid FROM p.d.b WHERE uid IS NOT NULL)) SELECT id FROM c",
		},
		"multiline": {
			src:   "SELECT id\nFROM p.d.a\nWHERE id NOT IN (\n  SELECT uid FROM p.d.b\n)",
			want:  []report{{text: "id NOT IN (\n  SELECT uid FROM p.d.b\n)", column: "uid", table: "p.d.b"}},
			fixed: "SELECT id\nFROM p.d.a\nWHERE id NOT IN (\n  SELECT uid FROM p.d.b WHERE uid IS NOT NULL\n)",
		},

		"or filter is parenthesized": {
			src:   "SELECT id FROM p.d.a WHERE id NOT IN (SELECT uid FROM p.d.b WHERE z > 0 OR z < -1)",
			want:  []report{{text: "id NOT IN (SELECT uid FROM p.d.b WHERE z > 0 OR z < -1)", column: "uid", table: "p.d.b"}},
			fixed: "SELECT id FROM p.d.a WHERE id NOT IN (SELECT uid FROM p.d.b WHERE (z > 0 OR z < -1) AND uid IS NOT NULL)",
		},
		"trailing comment after the from clause": {
			src:   "SELECT id FROM p.d.a WHERE id NOT IN (SELECT uid FROM p.d.b -- c\n)",
			want:  []report{{text: "id NOT IN (SELECT uid FROM p.d.b -- c\n)", column: "uid", table: "p.d.b"}},
			fixed: "SELECT id FROM p.d.a WHERE id NOT IN (SELECT uid FROM p.d.b WHERE uid IS NOT NULL -- c\n)",
		},
		"group by all": {
			src:   "SELECT id FROM p.d.a WHERE id NOT IN (SELECT uid FROM p.d.b GROUP BY ALL)",
			want:  []report{{text: "id NOT IN (SELECT uid FROM p.d.b GROUP BY ALL)", column: "uid", table: "p.d.b"}},
			fixed: "SELECT id FROM p.d.a WHERE id NOT IN (SELECT uid FROM p.d.b WHERE uid IS NOT NULL GROUP BY ALL)",
		},
		"left join in the subquery": {
			src:   "SELECT id FROM p.d.a WHERE id NOT IN (SELECT uid FROM p.d.b LEFT JOIN p.d.a ON TRUE)",
			want:  []report{{text: "id NOT IN (SELECT uid FROM p.d.b LEFT JOIN p.d.a ON TRUE)", column: "uid", table: "p.d.b"}},
			fixed: "SELECT id FROM p.d.a WHERE id NOT IN (SELECT uid FROM p.d.b LEFT JOIN p.d.a ON TRUE WHERE uid IS NOT NULL)",
		},
		"order by in the subquery": {
			src:   "SELECT id FROM p.d.a WHERE id NOT IN (SELECT uid FROM p.d.b ORDER BY z)",
			want:  []report{{text: "id NOT IN (SELECT uid FROM p.d.b ORDER BY z)", column: "uid", table: "p.d.b"}},
			fixed: "SELECT id FROM p.d.a WHERE id NOT IN (SELECT uid FROM p.d.b WHERE uid IS NOT NULL ORDER BY z)",
		},

		// A new filter would change which rows LIMIT or QUALIFY keep, or there
		// is no column expression to filter on.
		"limit": {
			src:  "SELECT id FROM p.d.a WHERE id NOT IN (SELECT uid FROM p.d.b LIMIT 10)",
			want: []report{{text: "id NOT IN (SELECT uid FROM p.d.b LIMIT 10)", column: "uid", table: "p.d.b"}},
		},
		"qualify": {
			src:  "SELECT id FROM p.d.a WHERE id NOT IN (SELECT uid FROM p.d.b WHERE TRUE QUALIFY ROW_NUMBER() OVER (ORDER BY z) = 1)",
			want: []report{{text: "id NOT IN (SELECT uid FROM p.d.b WHERE TRUE QUALIFY ROW_NUMBER() OVER (ORDER BY z) = 1)", column: "uid", table: "p.d.b"}},
		},
		// Super-aggregate rows hold a NULL that no WHERE can drop.
		"rollup": {
			src:  "SELECT id FROM p.d.a WHERE id NOT IN (SELECT uid FROM p.d.b GROUP BY ROLLUP(uid))",
			want: []report{{text: "id NOT IN (SELECT uid FROM p.d.b GROUP BY ROLLUP(uid))", column: "uid", table: "p.d.b"}},
		},
		"cube": {
			src:  "SELECT id FROM p.d.a WHERE id NOT IN (SELECT uid FROM p.d.b GROUP BY CUBE(uid, z))",
			want: []report{{text: "id NOT IN (SELECT uid FROM p.d.b GROUP BY CUBE(uid, z))", column: "uid", table: "p.d.b"}},
		},
		"grouping sets": {
			src:  "SELECT id FROM p.d.a WHERE id NOT IN (SELECT uid FROM p.d.b GROUP BY GROUPING SETS(uid, z))",
			want: []report{{text: "id NOT IN (SELECT uid FROM p.d.b GROUP BY GROUPING SETS(uid, z))", column: "uid", table: "p.d.b"}},
		},
		"pipe operator after the select": {
			src:  "SELECT id FROM p.d.a WHERE id NOT IN (SELECT uid FROM p.d.b |> WHERE TRUE)",
			want: []report{{text: "id NOT IN (SELECT uid FROM p.d.b |> WHERE TRUE)", column: "uid", table: "p.d.b"}},
		},
		"select star": {
			src:  "SELECT id FROM p.d.a WHERE id NOT IN (SELECT * FROM (SELECT uid FROM p.d.b))",
			want: []report{{text: "id NOT IN (SELECT * FROM (SELECT uid FROM p.d.b))", column: "uid", table: "p.d.b"}},
		},

		// The subquery cannot yield NULL, or bqls cannot tell.
		"required column":        {src: "SELECT v FROM p.d.a WHERE v NOT IN (SELECT rid FROM p.d.b)"},
		"in":                     {src: "SELECT id FROM p.d.a WHERE id IN (SELECT uid FROM p.d.b)"},
		"not exists":             {src: "SELECT id FROM p.d.a AS a WHERE NOT EXISTS (SELECT 1 FROM p.d.b AS b WHERE b.uid = a.id)"},
		"literal list":           {src: "SELECT id FROM p.d.a WHERE id NOT IN (1, 2)"},
		"is not null":            {src: "SELECT id FROM p.d.a WHERE id NOT IN (SELECT uid FROM p.d.b WHERE uid IS NOT NULL)"},
		"is not null among ands": {src: "SELECT id FROM p.d.a WHERE id NOT IN (SELECT uid FROM p.d.b WHERE z > 0 AND uid IS NOT NULL)"},
		"comparison drops null":  {src: "SELECT id FROM p.d.a WHERE id NOT IN (SELECT uid FROM p.d.b WHERE uid > 0)"},
		"inner join on the column": {
			src: "SELECT id FROM p.d.a WHERE id NOT IN (SELECT b.uid FROM p.d.b AS b JOIN p.d.a AS a2 ON b.uid = a2.id)",
		},
		"having on the column": {
			src: "SELECT id FROM p.d.a WHERE id NOT IN (SELECT uid FROM p.d.b GROUP BY uid HAVING uid IS NOT NULL)",
		},
		"is not null inside a cte": {
			src: "WITH c AS (SELECT uid FROM p.d.b WHERE uid IS NOT NULL) SELECT id FROM p.d.a WHERE id NOT IN (SELECT uid FROM c)",
		},
		"is not null outside a derived table": {
			src: "SELECT id FROM p.d.a WHERE id NOT IN (SELECT k FROM (SELECT uid AS k FROM p.d.b) WHERE k IS NOT NULL)",
		},
		"ifnull":           {src: "SELECT id FROM p.d.a WHERE id NOT IN (SELECT IFNULL(uid, 0) FROM p.d.b)"},
		"coalesce":         {src: "SELECT id FROM p.d.a WHERE id NOT IN (SELECT COALESCE(uid, 0) FROM p.d.b)"},
		"aggregate":        {src: "SELECT id FROM p.d.a WHERE id NOT IN (SELECT MAX(uid) FROM p.d.b)"},
		"arithmetic":       {src: "SELECT id FROM p.d.a WHERE id NOT IN (SELECT uid + 1 FROM p.d.b)"},
		"literal":          {src: "SELECT id FROM p.d.a WHERE id NOT IN (SELECT 1)"},
		"struct field":     {src: "SELECT id FROM p.d.a WHERE id NOT IN (SELECT s.f FROM p.d.b)"},
		"unnest":           {src: "SELECT id FROM p.d.a WHERE id NOT IN (SELECT x FROM UNNEST([1, 2]) AS x)"},
		"union":            {src: "SELECT id FROM p.d.a WHERE id NOT IN (SELECT uid FROM p.d.b UNION ALL SELECT rid FROM p.d.b)"},
		"view":             {src: "SELECT id FROM p.d.a WHERE id NOT IN (SELECT uid FROM p.d.v)"},
		"pseudo column":    {src: "SELECT id FROM p.d.a WHERE id NOT IN (SELECT 1 FROM p.d.b)"},
		"no not in at all": {src: "SELECT uid FROM p.d.b"},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			parsed := parseNullable(t, tt.src)
			got := lint.Run(t.Context(), parsed, lint.Config{}, lint.Env{LookupTable: lookupNullableTable}, lint.NotInNullable)

			var gotReports []report
			for _, d := range got {
				if d.Rule != "not-in-nullable" || d.Severity != lint.SeverityWarning || (len(d.SuggestedFixes) != 0) != (tt.fixed != "") {
					t.Errorf("unexpected diagnostic metadata: %+v", d)
				}
				r := report{text: textInRange(tt.src, d)}
				for _, w := range tt.want {
					if d.Message == notInNullableMessage(w.column, w.table) {
						r.column, r.table = w.column, w.table
					}
				}
				if r.column == "" {
					t.Errorf("unexpected message %q", d.Message)
				}
				gotReports = append(gotReports, r)
			}
			if fmt.Sprint(gotReports) != fmt.Sprint(tt.want) {
				t.Errorf("reports = %+v, want %+v", gotReports, tt.want)
			}
			if tt.fixed == "" {
				return
			}
			fixed := applyFixes(t, tt.src, got)
			if fixed != tt.fixed {
				t.Errorf("fixed = %q, want %q", fixed, tt.fixed)
			}
			parseNullable(t, fixed)
		})
	}
}

func TestNotInNullableUnknownTable(t *testing.T) {
	src := "SELECT id FROM p.d.a WHERE id NOT IN (SELECT uid FROM p.d.b)"
	parsed := parseNullable(t, src)
	// Without table metadata the column mode is unknown.
	if got := lint.Run(t.Context(), parsed, lint.Config{}, lint.Env{}, lint.NotInNullable); len(got) != 0 {
		t.Errorf("got %+v, want no report without metadata", got)
	}
}

func TestNotInNullableSuppressed(t *testing.T) {
	src := "SELECT id FROM p.d.a\n-- bqls:ignore not-in-nullable\nWHERE id NOT IN (SELECT uid FROM p.d.b)"
	parsed := parseNullable(t, src)
	if got := lint.Run(t.Context(), parsed, lint.Config{}, lint.Env{LookupTable: lookupNullableTable}, lint.NotInNullable); len(got) != 0 {
		t.Errorf("got %+v, want suppressed", got)
	}
}

func TestNotInNullableConfig(t *testing.T) {
	src := "SELECT id FROM p.d.a WHERE id NOT IN (SELECT uid FROM p.d.b)"
	parsed := parseNullable(t, src)
	env := lint.Env{LookupTable: lookupNullableTable}
	off := lint.Config{Rules: map[string]bool{"not-in-nullable": false}}
	if got := lint.Run(t.Context(), parsed, off, env, lint.NotInNullable); len(got) != 0 {
		t.Errorf("disabled rule reported %+v", got)
	}
	if got := lint.Run(t.Context(), parsed, lint.Config{}, env, lint.NotInNullable); len(got) != 1 {
		t.Errorf("rule should be enabled by default, got %+v", got)
	}
}
