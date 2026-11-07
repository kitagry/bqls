package lint_test

import (
	"testing"

	"github.com/kitagry/bqls/langserver/internal/lint"
)

func comparisonImplicitCastMessage(from, to string) string {
	msg := "columns of different types are compared: " + from + " is implicitly cast to " + to
	if to == "FLOAT64" {
		return msg + ", which cannot represent every value exactly, so different values may compare equal; give both columns the same type, or write an explicit CAST"
	}
	return msg + "; mismatched column types usually point to a schema design mistake, so give both columns the same type, or write an explicit CAST"
}

func TestComparisonImplicitCast(t *testing.T) {
	type report struct {
		// text is the exact text of the reported range.
		text, from, to string
	}
	tests := map[string]struct {
		src  string
		want []report
	}{
		"int64 and float64 in WHERE": {
			src:  "SELECT 1 FROM p.d.t WHERE i = f",
			want: []report{{"i = f", "INT64", "FLOAT64"}},
		},
		"cast on the right": {
			src:  "SELECT 1 FROM p.d.t WHERE f > i",
			want: []report{{"f > i", "INT64", "FLOAT64"}},
		},
		"int64 and numeric": {
			src:  "SELECT 1 FROM p.d.t WHERE i <= n",
			want: []report{{"i <= n", "INT64", "NUMERIC"}},
		},
		"numeric and float64": {
			src:  "SELECT 1 FROM p.d.t WHERE n != f",
			want: []report{{"n != f", "NUMERIC", "FLOAT64"}},
		},
		"comma join in WHERE": {
			src:  "SELECT 1 FROM p.d.t AS a, p.d.t AS b WHERE a.i = b.f",
			want: []report{{"a.i = b.f", "INT64", "FLOAT64"}},
		},
		"BETWEEN columns": {
			src:  "SELECT 1 FROM p.d.t WHERE i BETWEEN f AND n",
			want: []report{{"i BETWEEN f AND n", "INT64", "FLOAT64"}},
		},
		"IN list of columns": {
			src:  "SELECT 1 FROM p.d.t WHERE i IN (f, n)",
			want: []report{{"i IN (f, n)", "INT64", "FLOAT64"}},
		},
		"NOT IN list of columns": {
			src:  "SELECT 1 FROM p.d.t WHERE i NOT IN (f, n)",
			want: []report{{"i NOT IN (f, n)", "INT64", "FLOAT64"}},
		},
		"NOT BETWEEN columns": {
			src:  "SELECT 1 FROM p.d.t WHERE i NOT BETWEEN f AND n",
			want: []report{{"i NOT BETWEEN f AND n", "INT64", "FLOAT64"}},
		},
		"IS DISTINCT FROM": {
			src:  "SELECT 1 FROM p.d.t WHERE i IS DISTINCT FROM f",
			want: []report{{"i IS DISTINCT FROM f", "INT64", "FLOAT64"}},
		},
		"in the SELECT list": {
			src:  "SELECT i = f AS same FROM p.d.t",
			want: []report{{"i = f", "INT64", "FLOAT64"}},
		},
		"through a CTE": {
			src:  "WITH c AS (SELECT i, f FROM p.d.t) SELECT 1 FROM c WHERE c.i = c.f",
			want: []report{{"c.i = c.f", "INT64", "FLOAT64"}},
		},
		"same type": {
			src: "SELECT 1 FROM p.d.t AS a, p.d.t AS b WHERE a.i = b.i",
		},
		// A literal or parameter on one side is usually cast on purpose.
		"literal": {
			src: "SELECT 1 FROM p.d.t WHERE i = 1.5 OR f > 1 OR i BETWEEN f AND 10",
		},
		"explicit CAST": {
			src: "SELECT 1 FROM p.d.t WHERE CAST(i AS FLOAT64) = f",
		},
		// Computed values may differ in type for good reasons.
		"computed value": {
			src: "SELECT 1 FROM p.d.t WHERE i + 1 = f OR i = f * 2",
		},
		"aggregates": {
			src: "SELECT i FROM p.d.t GROUP BY i HAVING COUNT(*) > AVG(f)",
		},
		// join-key-implicit-cast does not report these, so this rule does.
		"JOIN ON other than a join key": {
			src:  "SELECT 1 FROM p.d.t AS a JOIN p.d.t AS b ON a.i < b.f OR (a.i = b.i AND a.i = a.f)",
			want: []report{{"a.i < b.f", "INT64", "FLOAT64"}, {"a.i = a.f", "INT64", "FLOAT64"}},
		},
		// Join keys are join-key-implicit-cast's concern.
		"JOIN ON": {
			src: "SELECT 1 FROM p.d.t AS a JOIN p.d.t AS b ON a.i = b.f",
		},
		// date-bound-on-datetime's concern.
		"date and datetime": {
			src: "SELECT 1 FROM p.d.t WHERE dt <= d",
		},
		"suppressed by comment": {
			src: "SELECT 1 FROM p.d.t\n-- bqls:ignore comparison-implicit-cast\nWHERE i = f",
		},
	}

	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			got := lint.Run(t.Context(), parseTyped(t, tt.src), lint.Config{}, lint.Env{}, lint.ComparisonImplicitCast)
			if len(got) != len(tt.want) {
				t.Fatalf("got %d diagnostics, want %d: %+v", len(got), len(tt.want), got)
			}
			for i, w := range tt.want {
				d := got[i]
				if text := textInRange(tt.src, d); text != w.text {
					t.Errorf("reported text = %q, want %q", text, w.text)
				}
				if want := comparisonImplicitCastMessage(w.from, w.to); d.Rule != "comparison-implicit-cast" || d.Severity != lint.SeverityWarning || d.Message != want || len(d.SuggestedFixes) != 0 {
					t.Errorf("diagnostic = %+v, want warning %q without a fix", d, want)
				}
			}
		})
	}
}
