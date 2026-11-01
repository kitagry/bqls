package lint_test

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/kitagry/bqls/langserver/internal/lint"
)

func selfComparisonMessage(column string) string {
	return "Both sides of this comparison refer to the same column `" + column +
		"`, so the result does not depend on its value; one side is likely meant to be another column"
}

func selfEqualityMessage(column string) string {
	return selfComparisonMessage(column) + " (write `" + column + " IS NOT NULL` to test for NULL)"
}

func TestSelfComparison(t *testing.T) {
	// parseWithTable gives every table a single INT64 column `x`.
	type report struct {
		// text is the exact reported source text.
		text    string
		message string
	}
	tests := map[string]struct {
		src  string
		want []report
	}{
		"join condition on one side only": {
			src:  "SELECT * FROM p.d.t AS a JOIN p.d.u AS b ON a.x = a.x",
			want: []report{{"a.x = a.x", selfEqualityMessage("a.x")}},
		},
		"bare column in WHERE": {
			src:  "SELECT * FROM p.d.t WHERE x = x",
			want: []report{{"x = x", selfEqualityMessage("x")}},
		},
		"qualified and bare names of the same column": {
			src:  "SELECT * FROM p.d.t AS t WHERE t.x = x",
			want: []report{{"t.x = x", selfEqualityMessage("t.x")}},
		},
		"<>": {
			src:  "SELECT * FROM p.d.t WHERE x <> x",
			want: []report{{"x <> x", selfComparisonMessage("x")}},
		},
		"<": {
			src:  "SELECT * FROM p.d.t WHERE x < x",
			want: []report{{"x < x", selfComparisonMessage("x")}},
		},
		">=": {
			src:  "SELECT * FROM p.d.t WHERE x >= x",
			want: []report{{"x >= x", selfComparisonMessage("x")}},
		},
		"IS NOT DISTINCT FROM": {
			src:  "SELECT * FROM p.d.t WHERE x IS NOT DISTINCT FROM x",
			want: []report{{"x IS NOT DISTINCT FROM x", selfComparisonMessage("x")}},
		},
		"correlated column in a subquery": {
			src:  "SELECT * FROM p.d.t AS a WHERE EXISTS (SELECT 1 FROM p.d.u AS b WHERE a.x = a.x)",
			want: []report{{"a.x = a.x", selfEqualityMessage("a.x")}},
		},
		"pipe WHERE": {
			src:  "FROM p.d.t |> WHERE x = x",
			want: []report{{"x = x", selfEqualityMessage("x")}},
		},
		"MERGE ON": {
			src:  "MERGE p.d.t AS T USING p.d.u AS S ON T.x = T.x WHEN MATCHED THEN DELETE",
			want: []report{{"T.x = T.x", selfEqualityMessage("T.x")}},
		},
		"CTE referenced twice is reported once": {
			src:  "WITH c AS (SELECT x FROM p.d.t WHERE x = x) SELECT * FROM c JOIN c AS d USING (x)",
			want: []report{{"x = x", selfEqualityMessage("x")}},
		},
		"struct field": {
			src: "WITH c AS (SELECT STRUCT(1 AS f) AS s) SELECT * FROM c WHERE s.f = s.f",
		},
		"same-named columns of different tables": {
			src: "SELECT * FROM p.d.t AS a JOIN p.d.t AS b ON a.x = b.x",
		},
		"JOIN USING": {
			src: "SELECT * FROM p.d.t AS a JOIN p.d.u AS b USING (x)",
		},
		"column compared with an expression of itself": {
			src: "SELECT * FROM p.d.t WHERE x = x + 0",
		},
		// NaN is not equal to itself, so `f != f` is a NaN check.
		"floating-point column": {
			src: "WITH c AS (SELECT CAST('NaN' AS FLOAT64) AS f) SELECT * FROM c WHERE f != f",
		},
		"bare name of a same-named variable is left to variable-column-shadow": {
			src: "DECLARE x INT64 DEFAULT 1;\nSELECT * FROM p.d.t WHERE x = x",
		},
		"self comparison with another variable in scope": {
			src:  "DECLARE y INT64 DEFAULT 1;\nSELECT * FROM p.d.t WHERE x = x",
			want: []report{{"x = x", selfEqualityMessage("x")}},
		},
		"suppressed by comment": {
			src: "-- bqls:ignore self-comparison\nSELECT * FROM p.d.t WHERE x = x",
		},
	}

	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			got := lint.Run(t.Context(), parseWithTable(t, tt.src), lint.Config{}, lint.Env{}, lint.SelfComparison)
			var want []lint.Diagnostic
			for _, r := range tt.want {
				want = append(want, lint.Diagnostic{
					Rule:     "self-comparison",
					Range:    spanOf(t, tt.src, r.text, 0),
					Message:  r.message,
					Severity: lint.SeverityWarning,
				})
			}
			if diff := cmp.Diff(want, got); diff != "" {
				t.Fatalf("diff (-want +got):\n%s", diff)
			}
		})
	}
}
