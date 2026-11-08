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

var arrayTables = map[string]*bq.TableMetadata{
	"p.d.t": {Type: bq.RegularTable, Schema: bq.Schema{
		{Name: "id", Type: bq.IntegerFieldType, Required: true},
		{Name: "v", Type: bq.IntegerFieldType},
	}},
	"p.d.arr": {Type: bq.RegularTable, Schema: bq.Schema{
		{Name: "a", Type: bq.IntegerFieldType, Repeated: true},
	}},
	"p.d.view": {Type: bq.ViewTable, Schema: bq.Schema{{Name: "v", Type: bq.IntegerFieldType}}},
}

func lookupArrayTable(_ context.Context, path string) (*bq.TableMetadata, error) {
	m, ok := arrayTables[path]
	if !ok {
		return nil, fmt.Errorf("table %s not found", path)
	}
	return m, nil
}

func parseArrayTables(t *testing.T, src string) file.ParsedFile {
	t.Helper()
	ctrl := gomock.NewController(t)
	bqClient := mock_bigquery.NewMockClient(ctrl)
	bqClient.EXPECT().GetDefaultProject().Return("p").AnyTimes()
	bqClient.EXPECT().GetTableMetadata(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, project, dataset, table string) (*bq.TableMetadata, error) {
			return lookupArrayTable(ctx, project+"."+dataset+"."+table)
		}).AnyTimes()
	parsed := file.NewAnalyzer(logrus.New(), bqClient).ParseFile("file:///a.sql", src)
	if len(parsed.Errors) > 0 {
		t.Fatalf("unexpected analysis errors: %v", parsed.Errors)
	}
	return parsed
}

const (
	nullableV   = "`v` of `p.d.t` is NULLABLE"
	nullLiteral = "it has a NULL element"
)

func arrayNullElementMessage(reason string, arrayAgg bool) string {
	remedy := "filter out the NULLs before building the array"
	if arrayAgg {
		remedy = "add IGNORE NULLS, which drops the NULLs and so leaves the array shorter, or filter them out"
	}
	return fmt.Sprintf("this array may hold a NULL element, as %s, and BigQuery fails with \"Array cannot have a null element\" once one reaches the result; %s", reason, remedy)
}

func TestArrayNullElement(t *testing.T) {
	type report struct {
		// text is the exact reported source text.
		text    string
		message string
	}
	agg := func(text string) report {
		return report{text: text, message: arrayNullElementMessage(nullableV, true)}
	}
	tests := map[string]struct {
		src  string
		want []report
		// fixed is the source after applying the fix; empty means no fix.
		fixed string
	}{
		"array_agg of a nullable column": {
			src:   "SELECT ARRAY_AGG(v) AS a FROM p.d.t",
			want:  []report{agg("ARRAY_AGG(v)")},
			fixed: "SELECT ARRAY_AGG(v IGNORE NULLS) AS a FROM p.d.t",
		},
		"group by": {
			src:   "SELECT id, ARRAY_AGG(v) AS a FROM p.d.t GROUP BY id",
			want:  []report{agg("ARRAY_AGG(v)")},
			fixed: "SELECT id, ARRAY_AGG(v IGNORE NULLS) AS a FROM p.d.t GROUP BY id",
		},
		"order by and limit": {
			src:   "SELECT ARRAY_AGG(v ORDER BY id LIMIT 3) AS a FROM p.d.t",
			want:  []report{agg("ARRAY_AGG(v ORDER BY id LIMIT 3)")},
			fixed: "SELECT ARRAY_AGG(v IGNORE NULLS ORDER BY id LIMIT 3) AS a FROM p.d.t",
		},
		"distinct": {
			src:   "SELECT ARRAY_AGG(DISTINCT v) AS a FROM p.d.t",
			want:  []report{agg("ARRAY_AGG(DISTINCT v)")},
			fixed: "SELECT ARRAY_AGG(DISTINCT v IGNORE NULLS) AS a FROM p.d.t",
		},
		"lower case": {
			src:   "select array_agg(v) as a from p.d.t",
			want:  []report{agg("array_agg(v)")},
			fixed: "select array_agg(v IGNORE NULLS) as a from p.d.t",
		},
		"through a cte": {
			src:   "WITH c AS (SELECT ARRAY_AGG(v) AS a FROM p.d.t) SELECT a FROM c",
			want:  []report{agg("ARRAY_AGG(v)")},
			fixed: "WITH c AS (SELECT ARRAY_AGG(v IGNORE NULLS) AS a FROM p.d.t) SELECT a FROM c",
		},
		"through a derived table": {
			src:   "SELECT b FROM (SELECT ARRAY_AGG(v) AS b FROM p.d.t)",
			want:  []report{agg("ARRAY_AGG(v)")},
			fixed: "SELECT b FROM (SELECT ARRAY_AGG(v IGNORE NULLS) AS b FROM p.d.t)",
		},
		"element through a cte": {
			src:   "WITH c AS (SELECT v AS w FROM p.d.t) SELECT ARRAY_AGG(w) AS a FROM c",
			want:  []report{agg("ARRAY_AGG(w)")},
			fixed: "WITH c AS (SELECT v AS w FROM p.d.t) SELECT ARRAY_AGG(w IGNORE NULLS) AS a FROM c",
		},
		"filter on another column": {
			src:   "SELECT ARRAY_AGG(v) AS a FROM p.d.t WHERE id > 0",
			want:  []report{agg("ARRAY_AGG(v)")},
			fixed: "SELECT ARRAY_AGG(v IGNORE NULLS) AS a FROM p.d.t WHERE id > 0",
		},
		"create table as select": {
			src:   "CREATE TABLE p.d.x AS SELECT ARRAY_AGG(v) AS a FROM p.d.t",
			want:  []report{agg("ARRAY_AGG(v)")},
			fixed: "CREATE TABLE p.d.x AS SELECT ARRAY_AGG(v IGNORE NULLS) AS a FROM p.d.t",
		},
		"insert select": {
			src:   "INSERT INTO p.d.arr (a) SELECT ARRAY_AGG(v) FROM p.d.t",
			want:  []report{agg("ARRAY_AGG(v)")},
			fixed: "INSERT INTO p.d.arr (a) SELECT ARRAY_AGG(v IGNORE NULLS) FROM p.d.t",
		},
		"multiline": {
			src:   "SELECT\n  ARRAY_AGG(\n    v\n  ) AS a\nFROM p.d.t",
			want:  []report{agg("ARRAY_AGG(\n    v\n  )")},
			fixed: "SELECT\n  ARRAY_AGG(\n    v IGNORE NULLS\n  ) AS a\nFROM p.d.t",
		},
		"two columns": {
			src:   "SELECT ARRAY_AGG(v) AS a, ARRAY_AGG(id) AS b, ARRAY_AGG(v ORDER BY id) AS c FROM p.d.t",
			want:  []report{agg("ARRAY_AGG(v)"), agg("ARRAY_AGG(v ORDER BY id)")},
			fixed: "SELECT ARRAY_AGG(v IGNORE NULLS) AS a, ARRAY_AGG(id) AS b, ARRAY_AGG(v IGNORE NULLS ORDER BY id) AS c FROM p.d.t",
		},

		"parenthesized argument": {
			src:   "SELECT ARRAY_AGG((v)) AS a FROM p.d.t",
			want:  []report{agg("ARRAY_AGG((v))")},
			fixed: "SELECT ARRAY_AGG((v) IGNORE NULLS) AS a FROM p.d.t",
		},
		"null argument": {
			src:   "SELECT ARRAY_AGG(NULL) AS a FROM p.d.t",
			want:  []report{{text: "ARRAY_AGG(NULL)", message: arrayNullElementMessage(nullLiteral, true)}},
			fixed: "SELECT ARRAY_AGG(NULL IGNORE NULLS) AS a FROM p.d.t",
		},
		"select star": {
			src:   "SELECT * FROM (SELECT ARRAY_AGG(v) AS a FROM p.d.t)",
			want:  []report{agg("ARRAY_AGG(v)")},
			fixed: "SELECT * FROM (SELECT ARRAY_AGG(v IGNORE NULLS) AS a FROM p.d.t)",
		},
		"after another statement": {
			src:   "DECLARE n INT64 DEFAULT 1;\nSELECT ARRAY_AGG(v) AS a FROM p.d.t WHERE id > n",
			want:  []report{agg("ARRAY_AGG(v)")},
			fixed: "DECLARE n INT64 DEFAULT 1;\nSELECT ARRAY_AGG(v IGNORE NULLS) AS a FROM p.d.t WHERE id > n",
		},

		// RESPECT NULLS asks for the NULLs, so dropping them is not offered.
		"respect nulls": {
			src:  "SELECT ARRAY_AGG(v RESPECT NULLS) AS a FROM p.d.t",
			want: []report{agg("ARRAY_AGG(v RESPECT NULLS)")},
		},
		// BigQuery rejects IGNORE NULLS on analytic ARRAY_AGG.
		"analytic": {
			src: "SELECT ARRAY_AGG(v) OVER (PARTITION BY id) AS a FROM p.d.t",
			want: []report{{
				text:    "ARRAY_AGG(v) OVER (PARTITION BY id)",
				message: arrayNullElementMessage(nullableV, false),
			}},
		},
		"array subquery": {
			src:  "SELECT ARRAY(SELECT v FROM p.d.t) AS a",
			want: []report{{text: "ARRAY(SELECT v FROM p.d.t)", message: arrayNullElementMessage(nullableV, false)}},
		},
		"array literal with null": {
			src:  "SELECT [1, NULL] AS a",
			want: []report{{text: "[1, NULL]", message: arrayNullElementMessage(nullLiteral, false)}},
		},
		"array literal with a cast null": {
			src:  "SELECT [1, CAST(NULL AS INT64)] AS a",
			want: []report{{text: "[1, CAST(NULL AS INT64)]", message: arrayNullElementMessage(nullLiteral, false)}},
		},
		"array constructor with a nullable column": {
			src:  "SELECT [v, 1] AS a FROM p.d.t",
			want: []report{{text: "[v, 1]", message: arrayNullElementMessage(nullableV, false)}},
		},
		"typed array constructor": {
			src:  "SELECT ARRAY<INT64>[v] AS a FROM p.d.t",
			want: []report{{text: "ARRAY<INT64>[v]", message: arrayNullElementMessage(nullableV, false)}},
		},
		"insert values": {
			src:  "INSERT INTO p.d.arr (a) VALUES ([1, NULL])",
			want: []report{{text: "[1, NULL]", message: arrayNullElementMessage(nullLiteral, false)}},
		},

		// The array never holds a NULL, or bqls cannot tell.
		"ignore nulls":             {src: "SELECT ARRAY_AGG(v IGNORE NULLS) AS a FROM p.d.t"},
		"required column":          {src: "SELECT ARRAY_AGG(id) AS a FROM p.d.t"},
		"view column":              {src: "SELECT ARRAY_AGG(v) AS a FROM p.d.view"},
		"is not null":              {src: "SELECT ARRAY_AGG(v) AS a FROM p.d.t WHERE v IS NOT NULL"},
		"comparison drops null":    {src: "SELECT ARRAY_AGG(v) AS a FROM p.d.t WHERE v > 0"},
		"is not null in a cte":     {src: "WITH c AS (SELECT v FROM p.d.t WHERE v IS NOT NULL) SELECT ARRAY_AGG(v) AS a FROM c"},
		"ifnull":                   {src: "SELECT ARRAY_AGG(IFNULL(v, 0)) AS a FROM p.d.t"},
		"struct element":           {src: "SELECT ARRAY_AGG(STRUCT(v)) AS a FROM p.d.t"},
		"array literal":            {src: "SELECT [1, 2] AS a"},
		"array subquery not null":  {src: "SELECT ARRAY(SELECT v FROM p.d.t WHERE v IS NOT NULL) AS a"},
		"array of table array":     {src: "SELECT ARRAY(SELECT e FROM p.d.arr, UNNEST(a) AS e) AS a"},
		"array length":             {src: "SELECT ARRAY_LENGTH(ARRAY_AGG(v)) AS n FROM p.d.t"},
		"to json string":           {src: "SELECT TO_JSON_STRING([1, NULL]) AS j"},
		"unnested in the query":    {src: "WITH c AS (SELECT ARRAY_AGG(v) AS a FROM p.d.t) SELECT e FROM c, UNNEST(c.a) AS e"},
		"length outside a cte":     {src: "WITH c AS (SELECT ARRAY_AGG(v) AS a FROM p.d.t) SELECT ARRAY_LENGTH(a) AS n FROM c"},
		"array filtered on":        {src: "WITH c AS (SELECT ARRAY_AGG(v) AS a FROM p.d.t) SELECT a FROM c WHERE NOT EXISTS (SELECT 1 FROM UNNEST(a) AS e WHERE e IS NULL)"},
		"create view":              {src: "CREATE VIEW p.d.x AS SELECT ARRAY_AGG(v) AS a FROM p.d.t"},
		"declare default":          {src: "DECLARE a ARRAY<INT64> DEFAULT [1, NULL];\nSELECT ARRAY_LENGTH(a) AS n"},
		"union of a nullable side": {src: "SELECT ARRAY_AGG(v) AS a FROM p.d.t UNION ALL SELECT [1]"},
		// A declared variable is analyzed as its default value, but SET may
		// change it.
		"declared variable element":   {src: "DECLARE n INT64;\nSET n = 1;\nSELECT [1, n] AS a"},
		"declared variable in agg":    {src: "DECLARE n INT64;\nSET n = 1;\nSELECT ARRAY_AGG(n) AS a FROM p.d.t"},
		"declared variable in a list": {src: "DECLARE n INT64;\nSET n = 1;\nSELECT [v, n] AS a FROM p.d.t WHERE v IS NOT NULL"},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			parsed := parseArrayTables(t, tt.src)
			got := lint.Run(t.Context(), parsed, lint.Config{Rules: map[string]bool{"array-null-element": true}}, lint.Env{LookupTable: lookupArrayTable}, lint.ArrayNullElement)

			var gotReports []report
			for _, d := range got {
				if d.Rule != "array-null-element" || d.Severity != lint.SeverityWarning {
					t.Errorf("unexpected diagnostic metadata: %+v", d)
				}
				gotReports = append(gotReports, report{text: textInRange(tt.src, d), message: d.Message})
				// Dropping the NULLs shortens the array, so applying it is
				// the user's call.
				for _, fix := range d.SuggestedFixes {
					if !fix.ChangesResult {
						t.Errorf("fix %q has ChangesResult = false, want true", fix.Title)
					}
				}
			}
			if fmt.Sprint(gotReports) != fmt.Sprint(tt.want) {
				t.Errorf("reports = %+v, want %+v", gotReports, tt.want)
			}
			fixed := applyFixes(t, tt.src, got)
			if tt.fixed == "" {
				if fixed != tt.src {
					t.Errorf("unexpected fix: %q", fixed)
				}
				return
			}
			if fixed != tt.fixed {
				t.Errorf("fixed = %q, want %q", fixed, tt.fixed)
			}
			refixed := lint.Run(t.Context(), parseArrayTables(t, fixed), lint.Config{Rules: map[string]bool{"array-null-element": true}}, lint.Env{LookupTable: lookupArrayTable}, lint.ArrayNullElement)
			if len(refixed) != 0 {
				t.Errorf("fixed source still reports %+v", refixed)
			}
		})
	}
}

func TestArrayNullElementUnknownTable(t *testing.T) {
	parsed := parseArrayTables(t, "SELECT ARRAY_AGG(v) AS a FROM p.d.t")
	// Without table metadata the column mode is unknown.
	got := lint.Run(t.Context(), parsed, lint.Config{Rules: map[string]bool{"array-null-element": true}}, lint.Env{}, lint.ArrayNullElement)
	if len(got) != 0 {
		t.Errorf("got %+v, want no report without metadata", got)
	}
}

func TestArrayNullElementSuppressed(t *testing.T) {
	parsed := parseArrayTables(t, "-- bqls:ignore array-null-element\nSELECT ARRAY_AGG(v) AS a FROM p.d.t")
	got := lint.Run(t.Context(), parsed, lint.Config{Rules: map[string]bool{"array-null-element": true}}, lint.Env{LookupTable: lookupArrayTable}, lint.ArrayNullElement)
	if len(got) != 0 {
		t.Errorf("got %+v, want suppressed", got)
	}
}

func TestArrayNullElementDisabledByDefault(t *testing.T) {
	parsed := parseArrayTables(t, "SELECT ARRAY_AGG(v) AS a FROM p.d.t")
	if got := lint.Run(t.Context(), parsed, lint.Config{}, lint.Env{LookupTable: lookupArrayTable}, lint.ArrayNullElement); len(got) != 0 {
		t.Errorf("rule should be disabled by default, got %+v", got)
	}
}
