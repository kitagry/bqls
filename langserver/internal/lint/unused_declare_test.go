package lint_test

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/kitagry/bqls/langserver/internal/lint"
	"github.com/kitagry/bqls/langserver/internal/lsp"
)

func unusedDeclare(name string, nameRange lsp.Range, fixEdits ...lsp.TextEdit) lint.Diagnostic {
	d := lint.Diagnostic{
		Rule:     "unused-declare",
		Range:    nameRange,
		Message:  "Variable `" + name + "` is declared but never used",
		Severity: lint.SeverityInformation,
	}
	if len(fixEdits) > 0 {
		d.SuggestedFixes = []lint.SuggestedFix{{Title: "Remove unused variable `" + name + "`", Edits: fixEdits}}
	}
	return d
}

func TestUnusedDeclare(t *testing.T) {
	tests := map[string]struct {
		src  string
		want []lint.Diagnostic
		// wantFixed is src after applying every fix; it must analyze cleanly.
		wantFixed string
	}{
		"unused variable": {
			src:       "DECLARE x INT64;\nSELECT 1",
			want:      []lint.Diagnostic{unusedDeclare("x", rng(0, 8, 0, 9), del(rng(0, 0, 1, 0)))},
			wantFixed: "SELECT 1",
		},
		"unused variable with a default": {
			src:       "DECLARE x INT64 DEFAULT 1;\nSELECT 1",
			want:      []lint.Diagnostic{unusedDeclare("x", rng(0, 8, 0, 9), del(rng(0, 0, 1, 0)))},
			wantFixed: "SELECT 1",
		},
		"used in SELECT": {
			src: "DECLARE x INT64 DEFAULT 1;\nSELECT x",
		},
		"used in a WHERE of a table query": {
			src: "DECLARE x INT64 DEFAULT 1;\nSELECT * FROM p.d.t WHERE x = 1",
		},
		"used on the right of SET": {
			src: "DECLARE x INT64 DEFAULT 1;\nDECLARE y INT64;\nSET y = x;\nSELECT y",
		},
		"only assigned by SET is not reported": {
			src: "DECLARE x INT64;\nSET x = 1;\nSELECT 1",
		},
		"used in IF condition": {
			src: "DECLARE x INT64 DEFAULT 1;\nIF x > 0 THEN SELECT 1; END IF",
		},
		"used in WHILE condition": {
			src: "DECLARE n INT64 DEFAULT 0;\nWHILE n < 3 DO SET n = n + 1; END WHILE",
		},
		"used in EXECUTE IMMEDIATE USING": {
			src: "DECLARE x INT64 DEFAULT 1;\nEXECUTE IMMEDIATE 'SELECT ?' USING x",
		},
		"used as EXECUTE IMMEDIATE INTO target": {
			src: "DECLARE x INT64;\nEXECUTE IMMEDIATE 'SELECT 1' INTO x",
		},
		"used in the DEFAULT of another variable": {
			src: "DECLARE a INT64 DEFAULT 1;\nDECLARE b INT64 DEFAULT a;\nSELECT b",
		},
		"name shared with an alias is not reported": {
			src: "DECLARE x INT64;\nSELECT 1 AS x",
		},
		"unused inside a block": {
			src:       "BEGIN\n  DECLARE x INT64;\n  SELECT 1;\nEND",
			want:      []lint.Diagnostic{unusedDeclare("x", rng(1, 10, 1, 11), del(rng(1, 2, 2, 2)))},
			wantFixed: "BEGIN\n  SELECT 1;\nEND",
		},
		"used inside a procedure body": {
			src: "CREATE PROCEDURE d.pr()\nBEGIN\n  DECLARE x INT64 DEFAULT 1;\n  SELECT x;\nEND",
		},
		"same name in two scopes, one used": {
			src: "BEGIN\n  DECLARE x INT64;\n  SELECT 1;\nEND;\nBEGIN\n  DECLARE x INT64;\n  SELECT x;\nEND",
		},
		"first of several variables unused": {
			src:       "DECLARE x, y INT64 DEFAULT 1;\nSELECT y",
			want:      []lint.Diagnostic{unusedDeclare("x", rng(0, 8, 0, 9), del(rng(0, 8, 0, 11)))},
			wantFixed: "DECLARE y INT64 DEFAULT 1;\nSELECT y",
		},
		"last of several variables unused": {
			src:       "DECLARE x, y INT64;\nSELECT x",
			want:      []lint.Diagnostic{unusedDeclare("y", rng(0, 11, 0, 12), del(rng(0, 9, 0, 12)))},
			wantFixed: "DECLARE x INT64;\nSELECT x",
		},
		"all of several variables unused offers no fix": {
			src: "DECLARE x, y INT64;\nSELECT 1",
			want: []lint.Diagnostic{
				unusedDeclare("x", rng(0, 8, 0, 9)),
				unusedDeclare("y", rng(0, 11, 0, 12)),
			},
		},
		"no fix when a comment line precedes the declaration": {
			src:  "-- the counter\nDECLARE x INT64;\nSELECT 1",
			want: []lint.Diagnostic{unusedDeclare("x", rng(1, 8, 1, 9))},
		},
		"no fix when a trailing comment follows the declaration": {
			src:  "DECLARE x INT64; -- the counter\nSELECT 1",
			want: []lint.Diagnostic{unusedDeclare("x", rng(0, 8, 0, 9))},
		},
		"suppressed by comment": {
			src: "-- bqls:ignore unused-declare\nDECLARE x INT64;\nSELECT 1",
		},
	}

	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			got := lint.Run(t.Context(), parseWithTable(t, tt.src), lint.Config{}, lint.Env{}, lint.UnusedDeclare)
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
			if after := lint.Run(t.Context(), parseWithTable(t, fixed), lint.Config{}, lint.Env{}, lint.UnusedDeclare); len(after) > 0 {
				t.Errorf("diagnostics remain after fix: %v", after)
			}
		})
	}
}
