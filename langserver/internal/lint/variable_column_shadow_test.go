package lint_test

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/kitagry/bqls/langserver/internal/lint"
	"github.com/kitagry/bqls/langserver/internal/lsp"
)

// variableColumnShadow builds the expected diagnostic; with a qualifier, the
// fix inserts it before each of the positions.
func variableColumnShadow(name string, r lsp.Range, qualifier string, at ...lsp.Position) lint.Diagnostic {
	d := lint.Diagnostic{
		Rule:  "variable-column-shadow",
		Range: r,
		Message: "`" + name + "` refers to the column, not the variable `" + name +
			"`, because a column takes precedence over a variable of the same name; this compares the column with itself. Rename the variable (e.g. `v_" + name + "`) to use it here",
		Severity: lint.SeverityWarning,
	}
	if qualifier != "" {
		var edits []lsp.TextEdit
		for _, p := range at {
			edits = append(edits, lsp.TextEdit{Range: lsp.Range{Start: p, End: p}, NewText: qualifier + "."})
		}
		d.SuggestedFixes = []lint.SuggestedFix{{
			Title: "Qualify `" + name + "` as the column `" + qualifier + "." + name + "`",
			Edits: edits,
		}}
	}
	return d
}

func pos(line, char int) lsp.Position {
	return lsp.Position{Line: line, Character: char}
}

func TestVariableColumnShadow(t *testing.T) {
	// parseWithTable gives every table a single INT64 column `x`.
	tests := map[string]struct {
		src  string
		want []lint.Diagnostic
		// wantFixed is src after applying every fix; it must analyze cleanly
		// and no longer be reported.
		wantFixed string
	}{
		"bare column compared with itself": {
			src:       "DECLARE x INT64 DEFAULT 1;\nSELECT * FROM p.d.t WHERE x = x",
			want:      []lint.Diagnostic{variableColumnShadow("x", rng(1, 30, 1, 31), "t", pos(1, 26), pos(1, 30))},
			wantFixed: "DECLARE x INT64 DEFAULT 1;\nSELECT * FROM p.d.t WHERE t.x = t.x",
		},
		"qualified column compared with the bare name": {
			src:       "DECLARE x INT64 DEFAULT 1;\nSELECT * FROM p.d.t AS t WHERE t.x = x",
			want:      []lint.Diagnostic{variableColumnShadow("x", rng(1, 37, 1, 38), "t", pos(1, 37))},
			wantFixed: "DECLARE x INT64 DEFAULT 1;\nSELECT * FROM p.d.t AS t WHERE t.x = t.x",
		},
		"bare name on the left": {
			src:       "DECLARE x INT64 DEFAULT 1;\nSELECT * FROM p.d.t AS a WHERE x >= a.x",
			want:      []lint.Diagnostic{variableColumnShadow("x", rng(1, 31, 1, 32), "a", pos(1, 31))},
			wantFixed: "DECLARE x INT64 DEFAULT 1;\nSELECT * FROM p.d.t AS a WHERE a.x >= a.x",
		},
		"variable name differs in case": {
			src:       "DECLARE X INT64 DEFAULT 1;\nSELECT * FROM p.d.t WHERE x != x",
			want:      []lint.Diagnostic{variableColumnShadow("x", rng(1, 31, 1, 32), "t", pos(1, 26), pos(1, 31))},
			wantFixed: "DECLARE X INT64 DEFAULT 1;\nSELECT * FROM p.d.t WHERE t.x != t.x",
		},
		"backquoted bare name": {
			src:       "DECLARE x INT64 DEFAULT 1;\nSELECT * FROM p.d.t AS t WHERE t.x = `x`",
			want:      []lint.Diagnostic{variableColumnShadow("x", rng(1, 37, 1, 40), "t", pos(1, 37))},
			wantFixed: "DECLARE x INT64 DEFAULT 1;\nSELECT * FROM p.d.t AS t WHERE t.x = t.`x`",
		},
		"alias kept as written": {
			src:       "DECLARE x INT64 DEFAULT 1;\nSELECT * FROM p.d.t AS `select` WHERE `select`.x = x",
			want:      []lint.Diagnostic{variableColumnShadow("x", rng(1, 51, 1, 52), "`select`", pos(1, 51))},
			wantFixed: "DECLARE x INT64 DEFAULT 1;\nSELECT * FROM p.d.t AS `select` WHERE `select`.x = `select`.x",
		},
		"implicit alias of a partly backquoted path": {
			src:       "DECLARE x INT64 DEFAULT 1;\nSELECT * FROM `p.d`.t WHERE x = x",
			want:      []lint.Diagnostic{variableColumnShadow("x", rng(1, 32, 1, 33), "t", pos(1, 28), pos(1, 32))},
			wantFixed: "DECLARE x INT64 DEFAULT 1;\nSELECT * FROM `p.d`.t WHERE t.x = t.x",
		},
		"column of a joined table": {
			src:       "DECLARE x INT64 DEFAULT 1;\nSELECT * FROM UNNEST([1]) AS y, p.d.t AS b WHERE b.x = x",
			want:      []lint.Diagnostic{variableColumnShadow("x", rng(1, 55, 1, 56), "b", pos(1, 55))},
			wantFixed: "DECLARE x INT64 DEFAULT 1;\nSELECT * FROM UNNEST([1]) AS y, p.d.t AS b WHERE b.x = b.x",
		},
		"JOIN ON": {
			src:       "DECLARE x INT64 DEFAULT 1;\nSELECT * FROM UNNEST([1]) AS y JOIN p.d.t AS b ON b.x = x",
			want:      []lint.Diagnostic{variableColumnShadow("x", rng(1, 56, 1, 57), "b", pos(1, 56))},
			wantFixed: "DECLARE x INT64 DEFAULT 1;\nSELECT * FROM UNNEST([1]) AS y JOIN p.d.t AS b ON b.x = b.x",
		},
		"SELECT list": {
			src:       "DECLARE x INT64 DEFAULT 1;\nSELECT x = x FROM p.d.t",
			want:      []lint.Diagnostic{variableColumnShadow("x", rng(1, 11, 1, 12), "t", pos(1, 7), pos(1, 11))},
			wantFixed: "DECLARE x INT64 DEFAULT 1;\nSELECT t.x = t.x FROM p.d.t",
		},
		"IS DISTINCT FROM": {
			src:       "DECLARE x INT64 DEFAULT 1;\nSELECT * FROM p.d.t WHERE x IS DISTINCT FROM x",
			want:      []lint.Diagnostic{variableColumnShadow("x", rng(1, 45, 1, 46), "t", pos(1, 26), pos(1, 45))},
			wantFixed: "DECLARE x INT64 DEFAULT 1;\nSELECT * FROM p.d.t WHERE t.x IS DISTINCT FROM t.x",
		},
		"pipe WHERE": {
			src:       "DECLARE x INT64 DEFAULT 1;\nFROM p.d.t |> WHERE x = x",
			want:      []lint.Diagnostic{variableColumnShadow("x", rng(1, 24, 1, 25), "t", pos(1, 20), pos(1, 24))},
			wantFixed: "DECLARE x INT64 DEFAULT 1;\nFROM p.d.t |> WHERE t.x = t.x",
		},
		"inside a BEGIN block": {
			src:       "BEGIN\n  DECLARE x INT64 DEFAULT 1;\n  SELECT * FROM p.d.t WHERE x = x;\nEND",
			want:      []lint.Diagnostic{variableColumnShadow("x", rng(2, 32, 2, 33), "t", pos(2, 28), pos(2, 32))},
			wantFixed: "BEGIN\n  DECLARE x INT64 DEFAULT 1;\n  SELECT * FROM p.d.t WHERE t.x = t.x;\nEND",
		},
		"inside a procedure body": {
			src:       "CREATE TEMP PROCEDURE pr()\nBEGIN\n  DECLARE x INT64 DEFAULT 1;\n  SELECT * FROM p.d.t WHERE x = x;\nEND",
			want:      []lint.Diagnostic{variableColumnShadow("x", rng(3, 32, 3, 33), "t", pos(3, 28), pos(3, 32))},
			wantFixed: "CREATE TEMP PROCEDURE pr()\nBEGIN\n  DECLARE x INT64 DEFAULT 1;\n  SELECT * FROM p.d.t WHERE t.x = t.x;\nEND",
		},
		// The table's range variable is not visible outside the subquery.
		"column read through a subquery has no fix": {
			src:  "DECLARE x INT64 DEFAULT 1;\nSELECT * FROM (SELECT x FROM p.d.t AS t) WHERE x = x",
			want: []lint.Diagnostic{variableColumnShadow("x", rng(1, 51, 1, 52), "")},
		},
		"column read through a CTE has no fix": {
			src:  "DECLARE x INT64 DEFAULT 1;\nWITH c AS (SELECT x FROM p.d.t) SELECT * FROM c WHERE x = x",
			want: []lint.Diagnostic{variableColumnShadow("x", rng(1, 58, 1, 59), "")},
		},
		// Qualifying an outer column with its range variable could bind to a
		// same-named range variable of the inner query.
		"correlated column has no fix": {
			src:  "DECLARE x INT64 DEFAULT 1;\nSELECT (SELECT 1 FROM UNNEST([1]) WHERE x = x) FROM p.d.t",
			want: []lint.Diagnostic{variableColumnShadow("x", rng(1, 44, 1, 45), "")},
		},
		// BigQuery names `p.d.t` by `t`, while the analyzer only accepts the
		// whole path, so neither is safe.
		"implicit alias of a backquoted path has no fix": {
			src:  "DECLARE x INT64 DEFAULT 1;\nSELECT * FROM `p.d.t` WHERE x = x",
			want: []lint.Diagnostic{variableColumnShadow("x", rng(1, 32, 1, 33), "")},
		},
		"pipe WHERE after a pipe SELECT has no fix": {
			src:  "DECLARE x INT64 DEFAULT 1;\nFROM p.d.t |> SELECT x |> WHERE x = x",
			want: []lint.Diagnostic{variableColumnShadow("x", rng(1, 36, 1, 37), "")},
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
			if tt.wantFixed == "" {
				return
			}
			fixed := applyFixes(t, tt.src, got)
			if diff := cmp.Diff(tt.wantFixed, fixed); diff != "" {
				t.Fatalf("fixed source diff (-want +got):\n%s", diff)
			}
			if after := lint.Run(t.Context(), parseWithTable(t, fixed), lint.Config{}, lint.Env{}, lint.VariableColumnShadow); len(after) != 0 {
				t.Errorf("diagnostics after fix: %+v", after)
			}
		})
	}
}
