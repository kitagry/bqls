package lint_test

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/kitagry/bqls/langserver/internal/lint"
	"github.com/kitagry/bqls/langserver/internal/lsp"
)

func variableColumnShadow(name string, r lsp.Range) lint.Diagnostic {
	return lint.Diagnostic{
		Rule:  "variable-column-shadow",
		Range: r,
		Message: "`" + name + "` refers to the column, not the variable `" + name +
			"`, because a column takes precedence over a variable of the same name; this compares the column with itself. Rename the variable (e.g. `v_" + name + "`) to use it here",
		Severity: lint.SeverityWarning,
	}
}

func TestVariableColumnShadow(t *testing.T) {
	// parseWithTable gives every table a single INT64 column `x`.
	tests := map[string]struct {
		src  string
		want []lint.Diagnostic
	}{
		"bare column compared with itself": {
			src:  "DECLARE x INT64 DEFAULT 1;\nSELECT * FROM p.d.t WHERE x = x",
			want: []lint.Diagnostic{variableColumnShadow("x", rng(1, 30, 1, 31))},
		},
		"qualified column compared with the bare name": {
			src:  "DECLARE x INT64 DEFAULT 1;\nSELECT * FROM p.d.t AS t WHERE t.x = x",
			want: []lint.Diagnostic{variableColumnShadow("x", rng(1, 37, 1, 38))},
		},
		"bare name on the left": {
			src:  "DECLARE x INT64 DEFAULT 1;\nSELECT * FROM p.d.t AS t WHERE x >= t.x",
			want: []lint.Diagnostic{variableColumnShadow("x", rng(1, 31, 1, 32))},
		},
		"variable name differs in case": {
			src:  "DECLARE X INT64 DEFAULT 1;\nSELECT * FROM p.d.t WHERE x != x",
			want: []lint.Diagnostic{variableColumnShadow("x", rng(1, 31, 1, 32))},
		},
		"backquoted bare name": {
			src:  "DECLARE x INT64 DEFAULT 1;\nSELECT * FROM p.d.t AS t WHERE t.x = `x`",
			want: []lint.Diagnostic{variableColumnShadow("x", rng(1, 37, 1, 40))},
		},
		"IS DISTINCT FROM": {
			src:  "DECLARE x INT64 DEFAULT 1;\nSELECT * FROM p.d.t WHERE x IS DISTINCT FROM x",
			want: []lint.Diagnostic{variableColumnShadow("x", rng(1, 45, 1, 46))},
		},
		"pipe WHERE": {
			src:  "DECLARE x INT64 DEFAULT 1;\nFROM p.d.t |> WHERE x = x",
			want: []lint.Diagnostic{variableColumnShadow("x", rng(1, 24, 1, 25))},
		},
		"inside a BEGIN block": {
			src:  "BEGIN\n  DECLARE x INT64 DEFAULT 1;\n  SELECT * FROM p.d.t WHERE x = x;\nEND",
			want: []lint.Diagnostic{variableColumnShadow("x", rng(2, 32, 2, 33))},
		},
		"inside a procedure body": {
			src:  "CREATE TEMP PROCEDURE pr()\nBEGIN\n  DECLARE x INT64 DEFAULT 1;\n  SELECT * FROM p.d.t WHERE x = x;\nEND",
			want: []lint.Diagnostic{variableColumnShadow("x", rng(3, 32, 3, 33))},
		},
		"variable compared with the column": {
			src: "DECLARE y INT64 DEFAULT 1;\nSELECT * FROM p.d.t WHERE x = y",
		},
		"self comparison without a same-named variable": {
			src: "DECLARE y INT64 DEFAULT 1;\nSELECT * FROM p.d.t WHERE x = x",
		},
		"both sides qualified": {
			src: "DECLARE x INT64 DEFAULT 1;\nSELECT * FROM p.d.t AS t WHERE t.x = t.x",
		},
		"same-named column of different tables": {
			src: "DECLARE x INT64 DEFAULT 1;\nSELECT * FROM p.d.t AS t JOIN p.d.u AS u ON t.x = u.x",
		},
		"column used without comparing it with itself": {
			src: "DECLARE x INT64 DEFAULT 1;\nSELECT x + 1 FROM p.d.t WHERE x > 0 GROUP BY x",
		},
		"variable declared in a block that does not enclose the statement": {
			src: "BEGIN\n  DECLARE x INT64 DEFAULT 1;\n  SELECT x;\nEND;\nSELECT * FROM p.d.t WHERE x = x",
		},
		"suppressed by comment": {
			src: "DECLARE x INT64 DEFAULT 1;\n-- bqls:ignore variable-column-shadow\nSELECT * FROM p.d.t WHERE x = x",
		},
	}

	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			got := lint.Run(t.Context(), parseWithTable(t, tt.src), lint.Config{}, lint.Env{}, lint.VariableColumnShadow)
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Fatalf("diff (-want +got):\n%s", diff)
			}
		})
	}
}
