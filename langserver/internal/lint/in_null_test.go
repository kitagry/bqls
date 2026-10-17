package lint_test

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/kitagry/bqls/langserver/internal/lint"
)

func TestInNull(t *testing.T) {
	const inMsg = "IN with only NULL values always yields NULL"
	const notInMsg = "NOT IN with only NULL values always yields NULL"
	tests := map[string]struct {
		src  string
		want []lint.Diagnostic
	}{
		"IN with a single NULL": {
			src:  "SELECT x FROM UNNEST([1]) AS x WHERE x IN (NULL)",
			want: []lint.Diagnostic{{Rule: "in-null", Range: rng(0, 37, 0, 48), Message: inMsg, Severity: lint.SeverityWarning}},
		},
		"NOT IN with a single NULL": {
			src:  "SELECT x FROM UNNEST([1]) AS x WHERE x NOT IN (NULL)",
			want: []lint.Diagnostic{{Rule: "in-null", Range: rng(0, 37, 0, 52), Message: notInMsg, Severity: lint.SeverityWarning}},
		},
		"NOT IN on a later line": {
			src:  "SELECT x FROM UNNEST([1]) AS x\nWHERE x NOT IN (NULL)",
			want: []lint.Diagnostic{{Rule: "in-null", Range: rng(1, 6, 1, 21), Message: notInMsg, Severity: lint.SeverityWarning}},
		},
		"IN with several NULLs": {
			src:  "SELECT x FROM UNNEST([1]) AS x WHERE x IN (NULL, NULL)",
			want: []lint.Diagnostic{{Rule: "in-null", Range: rng(0, 37, 0, 54), Message: inMsg, Severity: lint.SeverityWarning}},
		},
		"IN with a typed NULL": {
			src:  "SELECT x FROM UNNEST([1]) AS x WHERE x IN (CAST(NULL AS INT64))",
			want: []lint.Diagnostic{{Rule: "in-null", Range: rng(0, 37, 0, 63), Message: inMsg, Severity: lint.SeverityWarning}},
		},
		"IN with a parenthesized NULL": {
			src:  "SELECT x FROM UNNEST([1]) AS x WHERE x IN ((NULL))",
			want: []lint.Diagnostic{{Rule: "in-null", Range: rng(0, 37, 0, 50), Message: inMsg, Severity: lint.SeverityWarning}},
		},
		// The user wrote NOT around an IN, so the IN itself is what to point at.
		"NOT around a parenthesized IN": {
			src:  "SELECT x FROM UNNEST([1]) AS x WHERE NOT (x IN (NULL))",
			want: []lint.Diagnostic{{Rule: "in-null", Range: rng(0, 42, 0, 53), Message: inMsg, Severity: lint.SeverityWarning}},
		},
		"after non-ASCII text on the same line": {
			src:  "SELECT 'あいう' AS s, x FROM UNNEST([1]) AS x WHERE x IN (NULL)",
			want: []lint.Diagnostic{{Rule: "in-null", Range: rng(0, 55, 0, 66), Message: inMsg, Severity: lint.SeverityWarning}},
		},
		// A non-NULL element can still match, so the result is not always NULL.
		"IN with NULL and a non-NULL value is fine": {
			src:  "SELECT x FROM UNNEST([1]) AS x WHERE x IN (1, NULL)",
			want: nil,
		},
		"IN with non-NULL values is fine": {
			src:  "SELECT x FROM UNNEST([1]) AS x WHERE x NOT IN (1, 2)",
			want: nil,
		},
		// Declared variables are analyzed as dummy literals, which must not be
		// mistaken for a NULL literal.
		"IN with a declared variable is fine": {
			src:  "DECLARE v INT64;\nSELECT x FROM UNNEST([1]) AS x WHERE x IN (v)",
			want: nil,
		},
		"IN UNNEST is out of scope": {
			src:  "SELECT x FROM UNNEST([1]) AS x WHERE x IN UNNEST([CAST(NULL AS INT64)])",
			want: nil,
		},
		"IN subquery is out of scope": {
			src:  "SELECT x FROM UNNEST([1]) AS x WHERE x IN (SELECT CAST(NULL AS INT64))",
			want: nil,
		},
		"suppressed by comment": {
			src:  "SELECT x FROM UNNEST([1]) AS x\n-- bqls:ignore in-null\nWHERE x NOT IN (NULL)",
			want: nil,
		},
	}

	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			parsed := parse(t, tt.src)
			got := lint.Run(t.Context(), parsed, lint.Config{}, lint.Env{}, lint.InNull)
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("diff (-want +got):\n%s", diff)
			}
		})
	}
}
