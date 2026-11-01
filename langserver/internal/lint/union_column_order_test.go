package lint_test

import (
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/kitagry/bqls/langserver/internal/lint"
)

func TestUnionColumnOrder(t *testing.T) {
	type report struct {
		// sub is the reported source text and n its occurrence index.
		sub  string
		n    int
		op   string
		got  string
		want string
	}
	tests := map[string]struct {
		src  string
		want []report
	}{
		"swapped literals": {
			src:  "SELECT 1 AS a, 2 AS b UNION ALL SELECT 2 AS b, 1 AS a",
			want: []report{{sub: "2 AS b, 1 AS a", op: "UNION ALL", got: "`b`, `a`", want: "`a`, `b`"}},
		},
		"swapped aliases over different tables": {
			src:  "SELECT x AS a, x + 1 AS b FROM p.d.t1 UNION ALL SELECT x + 1 AS b, x AS a FROM p.d.t2",
			want: []report{{sub: "x + 1 AS b, x AS a", op: "UNION ALL", got: "`b`, `a`", want: "`a`, `b`"}},
		},
		"implicit aliases of paths": {
			src:  "SELECT t.a, t.b FROM (SELECT x AS a, x AS b FROM p.d.t1) t UNION ALL SELECT u.b, u.a FROM (SELECT x AS a, x AS b FROM p.d.t2) u",
			want: []report{{sub: "u.b, u.a", op: "UNION ALL", got: "`b`, `a`", want: "`a`, `b`"}},
		},
		"names compare case-insensitively": {
			src:  "SELECT 1 AS a, 2 AS b UNION ALL SELECT 2 AS B, 1 AS A",
			want: []report{{sub: "2 AS B, 1 AS A", op: "UNION ALL", got: "`B`, `A`", want: "`a`, `b`"}},
		},
		"only the misplaced columns are named": {
			src:  "SELECT 0 AS id, 1 AS a, 2 AS b UNION ALL SELECT 0 AS id, 2 AS b, 1 AS a",
			want: []report{{sub: "0 AS id, 2 AS b, 1 AS a", op: "UNION ALL", got: "`b`, `a`", want: "`a`, `b`"}},
		},
		"unnamed expressions are left out": {
			src:  "SELECT 1 AS a, 2 AS b, 'x' AS src UNION ALL SELECT 2 AS b, 1 AS a, 'y'",
			want: []report{{sub: "2 AS b, 1 AS a, 'y'", op: "UNION ALL", got: "`b`, `a`", want: "`a`, `b`"}},
		},
		"UNION DISTINCT": {
			src:  "SELECT 1 AS a, 2 AS b UNION DISTINCT SELECT 2 AS b, 1 AS a",
			want: []report{{sub: "2 AS b, 1 AS a", op: "UNION DISTINCT", got: "`b`, `a`", want: "`a`, `b`"}},
		},
		"EXCEPT DISTINCT": {
			src:  "SELECT 1 AS a, 2 AS b EXCEPT DISTINCT SELECT 2 AS b, 1 AS a",
			want: []report{{sub: "2 AS b, 1 AS a", op: "EXCEPT DISTINCT", got: "`b`, `a`", want: "`a`, `b`"}},
		},
		"every later input is compared with the first": {
			src: "SELECT 1 AS a, 2 AS b UNION ALL SELECT 2 AS b, 1 AS a UNION ALL SELECT 1 AS a, 2 AS b UNION ALL SELECT 2 AS b, 1 AS a",
			want: []report{
				{sub: "2 AS b, 1 AS a", n: 0, op: "UNION ALL", got: "`b`, `a`", want: "`a`, `b`"},
				{sub: "2 AS b, 1 AS a", n: 1, op: "UNION ALL", got: "`b`, `a`", want: "`a`, `b`"},
			},
		},
		"parenthesized input": {
			src:  "SELECT 1 AS a, 2 AS b UNION ALL (SELECT 2 AS b, 1 AS a LIMIT 1)",
			want: []report{{sub: "2 AS b, 1 AS a", op: "UNION ALL", got: "`b`, `a`", want: "`a`, `b`"}},
		},
		"parenthesized first and last inputs": {
			src:  "(SELECT 1 AS a, 2 AS b) UNION ALL (SELECT 2 AS b, 1 AS a)",
			want: []report{{sub: "2 AS b, 1 AS a", op: "UNION ALL", got: "`b`, `a`", want: "`a`, `b`"}},
		},
		"nested set operation takes its first input's names": {
			src:  "(SELECT 1 AS a, 2 AS b UNION DISTINCT SELECT 3 AS a, 4 AS b) UNION ALL SELECT 2 AS b, 1 AS a",
			want: []report{{sub: "2 AS b, 1 AS a", op: "UNION ALL", got: "`b`, `a`", want: "`a`, `b`"}},
		},
		"inside a CTE": {
			src:  "WITH c AS (SELECT 1 AS a, 2 AS b UNION ALL SELECT 2 AS b, 1 AS a) SELECT a FROM c",
			want: []report{{sub: "2 AS b, 1 AS a", op: "UNION ALL", got: "`b`, `a`", want: "`a`, `b`"}},
		},
		"inside a CTE on several lines": {
			src:  "WITH c AS (\n  SELECT 1 AS a, 2 AS b\n  UNION ALL\n  SELECT 2 AS b, 1 AS a\n)\nSELECT a FROM c",
			want: []report{{sub: "2 AS b, 1 AS a", op: "UNION ALL", got: "`b`, `a`", want: "`a`, `b`"}},
		},
		"in a subquery": {
			src:  "SELECT a FROM (SELECT 1 AS a, 2 AS b UNION ALL SELECT 2 AS b, 1 AS a)",
			want: []report{{sub: "2 AS b, 1 AS a", op: "UNION ALL", got: "`b`, `a`", want: "`a`, `b`"}},
		},
		"INSERT SELECT": {
			src:  "INSERT INTO p.d.t (x) SELECT a FROM (SELECT 1 AS a, 2 AS b UNION ALL SELECT 2 AS b, 1 AS a)",
			want: []report{{sub: "2 AS b, 1 AS a", op: "UNION ALL", got: "`b`, `a`", want: "`a`, `b`"}},
		},
		"same order": {
			src: "SELECT 1 AS a, 2 AS b UNION ALL SELECT 3 AS a, 4 AS b",
		},
		"aliases that put source columns in place": {
			src: "SELECT t.a AS a, t.b AS b FROM (SELECT x AS a, x + 1 AS b FROM p.d.t1) t UNION ALL SELECT t.b AS a, t.a AS b FROM (SELECT x AS a, x + 1 AS b FROM p.d.t2) t",
		},
		"names merely differ": {
			src: "SELECT 1 AS a, 2 AS b UNION ALL SELECT 1 AS c, 2 AS d",
		},
		"shifted names are not a reordering": {
			src: "SELECT 1 AS a, 2 AS b UNION ALL SELECT 1 AS b, 2 AS c",
		},
		"unnamed expressions only": {
			src: "SELECT 1, 2 UNION ALL SELECT 2, 1",
		},
		"swap over the same table is intended": {
			src: "SELECT x AS a, x + 1 AS b FROM p.d.t UNION ALL SELECT x + 1 AS b, x AS a FROM p.d.t",
		},
		"swap over the same CTE is intended": {
			src: "WITH e AS (SELECT x AS src, x + 1 AS dst FROM p.d.t) SELECT src, dst FROM e UNION ALL SELECT dst, src FROM e",
		},
		"SELECT * is skipped": {
			src: "SELECT * FROM (SELECT 1 AS a, 2 AS b) UNION ALL SELECT 2 AS b, 1 AS a",
		},
		"SELECT AS STRUCT is skipped": {
			src: "SELECT ARRAY(SELECT AS STRUCT 1 AS a, 2 AS b UNION ALL SELECT AS STRUCT 2 AS b, 1 AS a)",
		},
		"CORRESPONDING": {
			src: "SELECT 1 AS a, 2 AS b UNION ALL CORRESPONDING SELECT 2 AS b, 1 AS a",
		},
		"BY NAME": {
			src: "SELECT 1 AS a, 2 AS b UNION ALL BY NAME SELECT 2 AS b, 1 AS a",
		},
		"pipe UNION ALL is skipped": {
			src: "SELECT 1 AS a, 2 AS b |> UNION ALL (SELECT 2 AS b, 1 AS a)",
		},
		"suppressed by comment": {
			src: "SELECT 1 AS a, 2 AS b UNION ALL\n-- bqls:ignore union-column-order\nSELECT 2 AS b, 1 AS a",
		},
	}

	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			var want []lint.Diagnostic
			for _, r := range tt.want {
				want = append(want, lint.Diagnostic{
					Rule:     "union-column-order",
					Range:    spanOf(t, tt.src, r.sub, r.n),
					Message:  fmt.Sprintf("%s matches columns by position, not by name, but this input has %s where the first input has %s; reorder the columns or use %s BY NAME", r.op, r.got, r.want, r.op),
					Severity: lint.SeverityWarning,
				})
			}
			got := lint.Run(t.Context(), parseWithTable(t, tt.src), lint.Config{}, lint.Env{}, lint.UnionColumnOrder)
			if diff := cmp.Diff(want, got); diff != "" {
				t.Errorf("diff (-want +got):\n%s", diff)
			}
		})
	}
}

func TestUnionColumnOrderConfig(t *testing.T) {
	parsed := parseWithTable(t, "SELECT 1 AS a, 2 AS b UNION ALL SELECT 2 AS b, 1 AS a")
	off := lint.Config{Rules: map[string]bool{"union-column-order": false}}
	if got := lint.Run(t.Context(), parsed, off, lint.Env{}, lint.UnionColumnOrder); len(got) != 0 {
		t.Errorf("disabled rule reported %+v", got)
	}
	if got := lint.Run(t.Context(), parsed, lint.Config{}, lint.Env{}, lint.UnionColumnOrder); len(got) != 1 {
		t.Errorf("rule should be enabled by default, got %+v", got)
	}
}
