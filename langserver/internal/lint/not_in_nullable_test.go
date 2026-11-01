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
	}{
		"nullable column": {
			src:  "SELECT id FROM p.d.a WHERE id NOT IN (SELECT uid FROM p.d.b)",
			want: []report{{text: "id NOT IN (SELECT uid FROM p.d.b)", column: "uid", table: "p.d.b"}},
		},
		"not around in": {
			src:  "SELECT id FROM p.d.a WHERE NOT (id IN (SELECT uid FROM p.d.b))",
			want: []report{{text: "NOT (id IN (SELECT uid FROM p.d.b))", column: "uid", table: "p.d.b"}},
		},
		"aliased column": {
			src:  "SELECT id FROM p.d.a WHERE id NOT IN (SELECT b.uid AS k FROM p.d.b AS b)",
			want: []report{{text: "id NOT IN (SELECT b.uid AS k FROM p.d.b AS b)", column: "uid", table: "p.d.b"}},
		},
		"filter on another column": {
			src:  "SELECT id FROM p.d.a WHERE id NOT IN (SELECT uid FROM p.d.b WHERE z > 0)",
			want: []report{{text: "id NOT IN (SELECT uid FROM p.d.b WHERE z > 0)", column: "uid", table: "p.d.b"}},
		},
		"through a cte": {
			src:  "WITH c AS (SELECT uid FROM p.d.b) SELECT id FROM p.d.a WHERE id NOT IN (SELECT uid FROM c)",
			want: []report{{text: "id NOT IN (SELECT uid FROM c)", column: "uid", table: "p.d.b"}},
		},
		"through a derived table": {
			src:  "SELECT id FROM p.d.a WHERE id NOT IN (SELECT k FROM (SELECT uid AS k FROM p.d.b))",
			want: []report{{text: "id NOT IN (SELECT k FROM (SELECT uid AS k FROM p.d.b))", column: "uid", table: "p.d.b"}},
		},
		"group by key": {
			src:  "SELECT id FROM p.d.a WHERE id NOT IN (SELECT uid FROM p.d.b GROUP BY uid)",
			want: []report{{text: "id NOT IN (SELECT uid FROM p.d.b GROUP BY uid)", column: "uid", table: "p.d.b"}},
		},
		"distinct": {
			src:  "SELECT id FROM p.d.a WHERE id NOT IN (SELECT DISTINCT uid FROM p.d.b)",
			want: []report{{text: "id NOT IN (SELECT DISTINCT uid FROM p.d.b)", column: "uid", table: "p.d.b"}},
		},
		"in select list": {
			src:  "SELECT id NOT IN (SELECT uid FROM p.d.b) AS missing FROM p.d.a",
			want: []report{{text: "id NOT IN (SELECT uid FROM p.d.b)", column: "uid", table: "p.d.b"}},
		},
		"inside a cte": {
			src:  "WITH c AS (SELECT id FROM p.d.a WHERE id NOT IN (SELECT uid FROM p.d.b)) SELECT id FROM c",
			want: []report{{text: "id NOT IN (SELECT uid FROM p.d.b)", column: "uid", table: "p.d.b"}},
		},
		"multiline": {
			src:  "SELECT id\nFROM p.d.a\nWHERE id NOT IN (\n  SELECT uid FROM p.d.b\n)",
			want: []report{{text: "id NOT IN (\n  SELECT uid FROM p.d.b\n)", column: "uid", table: "p.d.b"}},
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
				if d.Rule != "not-in-nullable" || d.Severity != lint.SeverityWarning || len(d.SuggestedFixes) != 0 {
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
