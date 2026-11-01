package lint_test

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/kitagry/bqls/langserver/internal/lint"
	"github.com/kitagry/bqls/langserver/internal/lsp"
)

func unusedTempFunction(name string, nameRange lsp.Range, fixEdits ...lsp.TextEdit) lint.Diagnostic {
	d := lint.Diagnostic{
		Rule:     "unused-temp-function",
		Range:    nameRange,
		Message:  "Function `" + name + "` is defined but never used",
		Severity: lint.SeverityInformation,
	}
	if len(fixEdits) > 0 {
		d.SuggestedFixes = []lint.SuggestedFix{{Title: "Remove unused function `" + name + "`", Edits: fixEdits}}
	}
	return d
}

func TestUnusedTempFunction(t *testing.T) {
	tests := map[string]struct {
		src  string
		want []lint.Diagnostic
		// wantFixed is src after applying every fix; it must analyze cleanly.
		wantFixed string
	}{
		"unused function": {
			src:       "CREATE TEMP FUNCTION f(x INT64) RETURNS INT64 AS (x + 1);\nSELECT 1",
			want:      []lint.Diagnostic{unusedTempFunction("f", rng(0, 21, 0, 22), del(rng(0, 0, 1, 0)))},
			wantFixed: "SELECT 1",
		},
		"TEMPORARY keyword": {
			src:       "CREATE TEMPORARY FUNCTION f(x INT64) RETURNS INT64 AS (x + 1);\nSELECT 1",
			want:      []lint.Diagnostic{unusedTempFunction("f", rng(0, 26, 0, 27), del(rng(0, 0, 1, 0)))},
			wantFixed: "SELECT 1",
		},
		"used in SELECT": {
			src: "CREATE TEMP FUNCTION f(x INT64) RETURNS INT64 AS (x + 1);\nSELECT f(1)",
		},
		"used with different case": {
			src: "CREATE TEMP FUNCTION Foo(x INT64) RETURNS INT64 AS (x + 1);\nSELECT foo(1)",
		},
		"used in a WHERE of a table query": {
			src: "CREATE TEMP FUNCTION f(x INT64) RETURNS INT64 AS (x + 1);\nSELECT * FROM p.d.t WHERE f(x) = 2",
		},
		"used by another function that is used": {
			src: "CREATE TEMP FUNCTION f(x INT64) RETURNS INT64 AS (x + 1);\nCREATE TEMP FUNCTION g(x INT64) RETURNS INT64 AS (f(x) * 2);\nSELECT g(1)",
		},
		"only used by an unused function": {
			src:  "CREATE TEMP FUNCTION f(x INT64) RETURNS INT64 AS (x + 1);\nCREATE TEMP FUNCTION g(x INT64) RETURNS INT64 AS (f(x) * 2);\nSELECT 1",
			want: []lint.Diagnostic{unusedTempFunction("g", rng(1, 21, 1, 22), del(rng(1, 0, 2, 0)))},
		},
		"used inside EXECUTE IMMEDIATE string": {
			src: "CREATE TEMP FUNCTION f(x INT64) RETURNS INT64 AS (x + 1);\nEXECUTE IMMEDIATE 'SELECT f(1)'",
		},
		"name shared with an alias is not reported": {
			src: "CREATE TEMP FUNCTION f(x INT64) RETURNS INT64 AS (x + 1);\nSELECT 1 AS f",
		},
		"JavaScript function unused": {
			src:       "CREATE TEMP FUNCTION f(x FLOAT64) RETURNS FLOAT64 LANGUAGE js AS 'return x;';\nSELECT 1",
			want:      []lint.Diagnostic{unusedTempFunction("f", rng(0, 21, 0, 22), del(rng(0, 0, 1, 0)))},
			wantFixed: "SELECT 1",
		},
		"file with only function definitions is skipped": {
			src: "CREATE TEMP FUNCTION f(x INT64) RETURNS INT64 AS (x + 1);\nCREATE TEMP FUNCTION g(x INT64) RETURNS INT64 AS (x + 2);",
		},
		"persistent function is not reported": {
			src: "CREATE FUNCTION d.f(x INT64) RETURNS INT64 AS (x + 1);\nSELECT 1",
		},
		"no fix when a comment line precedes the function": {
			src:  "-- adds one\nCREATE TEMP FUNCTION f(x INT64) RETURNS INT64 AS (x + 1);\nSELECT 1",
			want: []lint.Diagnostic{unusedTempFunction("f", rng(1, 21, 1, 22))},
		},
		"no fix when a trailing comment follows the function": {
			src:  "CREATE TEMP FUNCTION f(x INT64) RETURNS INT64 AS (x + 1); -- adds one\nSELECT 1",
			want: []lint.Diagnostic{unusedTempFunction("f", rng(0, 21, 0, 22))},
		},
		"suppressed by comment": {
			src: "-- bqls:ignore unused-temp-function\nCREATE TEMP FUNCTION f(x INT64) RETURNS INT64 AS (x + 1);\nSELECT 1",
		},
	}

	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			got := lint.Run(t.Context(), parseWithTable(t, tt.src), lint.Config{}, lint.Env{}, lint.UnusedTempFunction)
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Fatalf("diff (-want +got):\n%s", diff)
			}
			if tt.wantFixed == "" {
				return
			}
			fixed := applyFixes(t, tt.src, got)
			if diff := cmp.Diff(tt.wantFixed, fixed); diff != "" {
				t.Fatalf("fixed source diff (-want +got):\n%s", diff)
			}
			if after := lint.Run(t.Context(), parseWithTable(t, fixed), lint.Config{}, lint.Env{}, lint.UnusedTempFunction); len(after) > 0 {
				t.Errorf("diagnostics remain after fix: %v", after)
			}
		})
	}
}

func unusedTempFunctionArgument(arg, fn string, argRange lsp.Range) lint.Diagnostic {
	return lint.Diagnostic{
		Rule:     "unused-temp-function-argument",
		Range:    argRange,
		Message:  "Argument `" + arg + "` of function `" + fn + "` is never used",
		Severity: lint.SeverityInformation,
	}
}

func TestUnusedTempFunctionArgument(t *testing.T) {
	tests := map[string]struct {
		src  string
		want []lint.Diagnostic
	}{
		"all arguments used": {
			src: "CREATE TEMP FUNCTION f(x INT64, y INT64) RETURNS INT64 AS (x + y);\nSELECT f(1, 2)",
		},
		"unused argument": {
			src:  "CREATE TEMP FUNCTION f(x INT64, y INT64) RETURNS INT64 AS (x + 1);\nSELECT f(1, 2)",
			want: []lint.Diagnostic{unusedTempFunctionArgument("y", "f", rng(0, 32, 0, 33))},
		},
		"several unused arguments": {
			src: "CREATE TEMP FUNCTION f(x INT64, y INT64) RETURNS INT64 AS (1);\nSELECT f(1, 2)",
			want: []lint.Diagnostic{
				unusedTempFunctionArgument("x", "f", rng(0, 23, 0, 24)),
				unusedTempFunctionArgument("y", "f", rng(0, 32, 0, 33)),
			},
		},
		"used with different case": {
			src: "CREATE TEMP FUNCTION f(Foo INT64) RETURNS INT64 AS (foo + 1);\nSELECT f(1)",
		},
		"used in a subquery": {
			src: "CREATE TEMP FUNCTION f(x INT64) RETURNS INT64 AS ((SELECT MAX(v) FROM UNNEST([1, x]) AS v));\nSELECT f(1)",
		},
		"used in a CASE": {
			src: "CREATE TEMP FUNCTION f(x INT64, y INT64) RETURNS INT64 AS (CASE WHEN x > 0 THEN y ELSE 0 END);\nSELECT f(1, 2)",
		},
		"name shared with a column is not reported": {
			src: "CREATE TEMP FUNCTION f(x INT64) RETURNS INT64 AS ((SELECT x FROM p.d.t LIMIT 1));\nSELECT f(1)",
		},
		"templated argument unused": {
			src:  "CREATE TEMP FUNCTION f(x ANY TYPE, y ANY TYPE) AS (x);\nSELECT 1",
			want: []lint.Diagnostic{unusedTempFunctionArgument("y", "f", rng(0, 35, 0, 36))},
		},
		"unused function still reports its unused argument": {
			src:  "CREATE TEMP FUNCTION f(x INT64) RETURNS INT64 AS (1);\nSELECT 1",
			want: []lint.Diagnostic{unusedTempFunctionArgument("x", "f", rng(0, 23, 0, 24))},
		},
		"JavaScript function is not reported": {
			src: "CREATE TEMP FUNCTION f(x FLOAT64, y FLOAT64) RETURNS FLOAT64 LANGUAGE js AS 'return x;';\nSELECT f(1, 2)",
		},
		"persistent function is not reported": {
			src: "CREATE FUNCTION d.f(x INT64) RETURNS INT64 AS (1);\nSELECT 1",
		},
		"suppressed by comment": {
			src: "-- bqls:ignore unused-temp-function-argument\nCREATE TEMP FUNCTION f(x INT64) RETURNS INT64 AS (1);\nSELECT f(1)",
		},
	}

	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			got := lint.Run(t.Context(), parseWithTable(t, tt.src), lint.Config{}, lint.Env{}, lint.UnusedTempFunctionArgument)
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Fatalf("diff (-want +got):\n%s", diff)
			}
		})
	}
}
