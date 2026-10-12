package lint_test

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/kitagry/bqls/langserver/internal/lint"
	"github.com/kitagry/bqls/langserver/internal/lsp"
)

func unusedCTE(name string, nameRange lsp.Range, fixEdits ...lsp.TextEdit) lint.Diagnostic {
	d := lint.Diagnostic{
		Rule:     "unused-cte",
		Range:    nameRange,
		Message:  "CTE `" + name + "` is defined but never used",
		Severity: lint.SeverityInformation,
	}
	if len(fixEdits) > 0 {
		d.SuggestedFixes = []lint.SuggestedFix{{Title: "Remove unused CTE `" + name + "`", Edits: fixEdits}}
	}
	return d
}

func del(r lsp.Range) lsp.TextEdit {
	return lsp.TextEdit{Range: r, NewText: ""}
}

func TestUnusedCTE(t *testing.T) {
	tests := map[string]struct {
		src  string
		want []lint.Diagnostic
	}{
		"all CTEs used": {
			src:  "WITH a AS (SELECT 1 AS x), b AS (SELECT * FROM a) SELECT * FROM b",
			want: nil,
		},
		"last entry unused": {
			src:  "WITH a AS (SELECT 1 AS x), b AS (SELECT 2 AS x) SELECT * FROM a",
			want: []lint.Diagnostic{unusedCTE("b", rng(0, 27, 0, 28), del(rng(0, 25, 0, 47)))},
		},
		"first entry unused": {
			src:  "WITH a AS (SELECT 1 AS x), b AS (SELECT 2 AS x) SELECT * FROM b",
			want: []lint.Diagnostic{unusedCTE("a", rng(0, 5, 0, 6), del(rng(0, 5, 0, 27)))},
		},
		"middle entry unused on its own line": {
			src:  "WITH\n  a AS (SELECT 1 AS x),\n  b AS (SELECT 2 AS x),\n  c AS (SELECT * FROM a)\nSELECT * FROM c",
			want: []lint.Diagnostic{unusedCTE("b", rng(2, 2, 2, 3), del(rng(1, 22, 2, 22)))},
		},
		"only entry unused removes WITH": {
			src:  "WITH a AS (SELECT 1 AS x)\nSELECT 2 AS y",
			want: []lint.Diagnostic{unusedCTE("a", rng(0, 5, 0, 6), del(rng(0, 0, 1, 0)))},
		},
		"used from a scalar subquery": {
			src:  "WITH a AS (SELECT 1 AS x) SELECT (SELECT MAX(x) FROM a) AS m",
			want: nil,
		},
		"used only by an unused CTE": {
			src:  "WITH a AS (SELECT 1 AS x), b AS (SELECT * FROM a) SELECT 1 AS y",
			want: []lint.Diagnostic{unusedCTE("b", rng(0, 27, 0, 28), del(rng(0, 25, 0, 49)))},
		},
		"used from a nested WITH": {
			src:  "WITH a AS (SELECT 1 AS x) SELECT * FROM (WITH b AS (SELECT * FROM a) SELECT * FROM b)",
			want: nil,
		},
		"outer CTE shadowed by an inner CTE of the same name": {
			src:  "WITH a AS (SELECT 1 AS x) SELECT * FROM (WITH a AS (SELECT 2 AS x) SELECT * FROM a)",
			want: []lint.Diagnostic{unusedCTE("a", rng(0, 5, 0, 6), del(rng(0, 0, 0, 26)))},
		},
		"inner CTE unused while outer of the same name is used": {
			src:  "WITH a AS (SELECT 1 AS x) SELECT * FROM a JOIN (WITH a AS (SELECT 2 AS x) SELECT 3 AS x) USING (x)",
			want: []lint.Diagnostic{unusedCTE("a", rng(0, 53, 0, 54), del(rng(0, 48, 0, 74)))},
		},
		"recursive self reference is not a use": {
			src:  "WITH RECURSIVE r AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM r WHERE n < 3) SELECT 1 AS y",
			want: []lint.Diagnostic{unusedCTE("r", rng(0, 15, 0, 16), del(rng(0, 0, 0, 78)))},
		},
		"recursive WITH reorders entries by dependency": {
			src:  "WITH RECURSIVE c AS (SELECT * FROM b), b AS (SELECT * FROM a), a AS (SELECT 1 AS x), d AS (SELECT 1 AS x) SELECT * FROM c",
			want: []lint.Diagnostic{unusedCTE("d", rng(0, 85, 0, 86), del(rng(0, 83, 0, 105)))},
		},
		"CREATE TABLE AS SELECT after DECLARE": {
			src:  "DECLARE v INT64;\nCREATE TABLE t AS WITH a AS (SELECT 1 AS x) SELECT v AS x",
			want: []lint.Diagnostic{unusedCTE("a", rng(1, 23, 1, 24), del(rng(1, 18, 1, 44)))},
		},
		"each statement of a script": {
			src: "WITH a AS (SELECT 1 AS x) SELECT 1 AS y;\nWITH a AS (SELECT 1 AS x) SELECT * FROM a;\nWITH b AS (SELECT 1 AS x) SELECT 1 AS y",
			want: []lint.Diagnostic{
				unusedCTE("a", rng(0, 5, 0, 6), del(rng(0, 0, 0, 26))),
				unusedCTE("b", rng(2, 5, 2, 6), del(rng(2, 0, 2, 26))),
			},
		},
		// Removing the separator would also delete the user's comment.
		"no fix when a comment separates entries": {
			src:  "WITH a AS (SELECT 1 AS x), -- keep\nb AS (SELECT 2 AS x) SELECT * FROM a",
			want: []lint.Diagnostic{unusedCTE("b", rng(1, 0, 1, 1))},
		},
		"suppressed by comment": {
			src:  "-- bqls:ignore unused-cte\nWITH a AS (SELECT 1 AS x) SELECT 1 AS y",
			want: nil,
		},
	}

	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			parsed := parse(t, tt.src)
			got := lint.Run(t.Context(), parsed, lint.Config{}, lint.Env{}, lint.UnusedCTE)
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("diff (-want +got):\n%s", diff)
			}
		})
	}
}
