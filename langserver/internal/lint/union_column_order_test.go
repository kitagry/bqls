package lint_test

import (
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
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
			if diff := cmp.Diff(want, got, cmpopts.IgnoreFields(lint.Diagnostic{}, "SuggestedFixes")); diff != "" {
				t.Errorf("diff (-want +got):\n%s", diff)
			}
		})
	}
}

func TestUnionColumnOrderFix(t *testing.T) {
	tests := map[string]struct {
		src string
		// wantFixed is src after applying every fix; it is src itself when no
		// fix is offered.
		wantFixed string
	}{
		"swapped literals": {
			src:       "SELECT 1 AS a, 2 AS b UNION ALL SELECT 2 AS b, 1 AS a",
			wantFixed: "SELECT 1 AS a, 2 AS b UNION ALL SELECT 1 AS a, 2 AS b",
		},
		"aliases move with their expressions": {
			src:       "SELECT x AS a, x + 1 AS b FROM p.d.t1 UNION ALL SELECT x + 1 AS b, x AS a FROM p.d.t2",
			wantFixed: "SELECT x AS a, x + 1 AS b FROM p.d.t1 UNION ALL SELECT x AS a, x + 1 AS b FROM p.d.t2",
		},
		"implicit aliases of paths": {
			src:       "SELECT t.a, t.b FROM (SELECT x AS a, x AS b FROM p.d.t1) t UNION ALL SELECT u.b, u.a FROM (SELECT x AS a, x AS b FROM p.d.t2) u",
			wantFixed: "SELECT t.a, t.b FROM (SELECT x AS a, x AS b FROM p.d.t1) t UNION ALL SELECT u.a, u.b FROM (SELECT x AS a, x AS b FROM p.d.t2) u",
		},
		"only the misplaced columns move": {
			src:       "SELECT 0 AS id, 1 AS a, 2 AS b, 3 AS c UNION ALL SELECT 0 AS id, 3 AS c, 1 AS a, 2 AS b",
			wantFixed: "SELECT 0 AS id, 1 AS a, 2 AS b, 3 AS c UNION ALL SELECT 0 AS id, 1 AS a, 2 AS b, 3 AS c",
		},
		"unnamed expressions stay": {
			src:       "SELECT 1 AS a, 2 AS b, 'x' AS src UNION ALL SELECT 2 AS b, 1 AS a, 'y'",
			wantFixed: "SELECT 1 AS a, 2 AS b, 'x' AS src UNION ALL SELECT 1 AS a, 2 AS b, 'y'",
		},
		"names compare case-insensitively": {
			src:       "SELECT 1 AS a, 2 AS b UNION ALL SELECT 2 AS B, 1 AS A",
			wantFixed: "SELECT 1 AS a, 2 AS b UNION ALL SELECT 1 AS A, 2 AS B",
		},
		"layout is kept": {
			src:       "SELECT 1 AS a,\n  2 AS b\nUNION ALL\nSELECT 2 AS b,\n  1 AS a",
			wantFixed: "SELECT 1 AS a,\n  2 AS b\nUNION ALL\nSELECT 1 AS a,\n  2 AS b",
		},
		"every later input is fixed": {
			src:       "SELECT 1 AS a, 2 AS b UNION ALL SELECT 2 AS b, 1 AS a UNION ALL SELECT 4 AS b, 3 AS a",
			wantFixed: "SELECT 1 AS a, 2 AS b UNION ALL SELECT 1 AS a, 2 AS b UNION ALL SELECT 3 AS a, 4 AS b",
		},
		"parenthesized input": {
			src:       "SELECT 1 AS a, 2 AS b UNION ALL (SELECT 2 AS b, 1 AS a LIMIT 1)",
			wantFixed: "SELECT 1 AS a, 2 AS b UNION ALL (SELECT 1 AS a, 2 AS b LIMIT 1)",
		},
		"inside a CTE": {
			src:       "WITH c AS (SELECT 1 AS a, 2 AS b UNION ALL SELECT 2 AS b, 1 AS a) SELECT a FROM c",
			wantFixed: "WITH c AS (SELECT 1 AS a, 2 AS b UNION ALL SELECT 1 AS a, 2 AS b) SELECT a FROM c",
		},
		"grouped by name": {
			src:       "SELECT 1 AS a, 2 AS b UNION ALL SELECT x + 1 AS b, x AS a FROM p.d.t GROUP BY x",
			wantFixed: "SELECT 1 AS a, 2 AS b UNION ALL SELECT x AS a, x + 1 AS b FROM p.d.t GROUP BY x",
		},
		"ordinal in the first input is kept": {
			src:       "SELECT x AS a, x + 1 AS b FROM p.d.t1 GROUP BY 1, 2 UNION ALL SELECT 2 AS b, 1 AS a",
			wantFixed: "SELECT x AS a, x + 1 AS b FROM p.d.t1 GROUP BY 1, 2 UNION ALL SELECT 1 AS a, 2 AS b",
		},

		// The diagnostic is reported without a fix.
		"GROUP BY ordinal would change meaning": {
			src:       "SELECT 1 AS a, 2 AS b UNION ALL SELECT x + 1 AS b, x AS a FROM p.d.t GROUP BY 2",
			wantFixed: "SELECT 1 AS a, 2 AS b UNION ALL SELECT x + 1 AS b, x AS a FROM p.d.t GROUP BY 2",
		},
		"ORDER BY ordinal would change meaning": {
			src:       "SELECT 1 AS a, 2 AS b UNION ALL (SELECT x + 1 AS b, x AS a FROM p.d.t ORDER BY 1 LIMIT 1)",
			wantFixed: "SELECT 1 AS a, 2 AS b UNION ALL (SELECT x + 1 AS b, x AS a FROM p.d.t ORDER BY 1 LIMIT 1)",
		},
		"comment in the select list": {
			src:       "SELECT 1 AS a, 2 AS b UNION ALL SELECT 2 AS b, -- second\n1 AS a",
			wantFixed: "SELECT 1 AS a, 2 AS b UNION ALL SELECT 2 AS b, -- second\n1 AS a",
		},
		"duplicate names": {
			src:       "SELECT 1 AS a, 2 AS b, 3 AS a, 4 AS b UNION ALL SELECT 2 AS b, 1 AS a, 4 AS b, 3 AS a",
			wantFixed: "SELECT 1 AS a, 2 AS b, 3 AS a, 4 AS b UNION ALL SELECT 2 AS b, 1 AS a, 4 AS b, 3 AS a",
		},
		"nested set operation input": {
			src:       "SELECT 1 AS a, 2 AS b UNION ALL (SELECT 2 AS b, 1 AS a UNION DISTINCT SELECT 4 AS b, 3 AS a)",
			wantFixed: "SELECT 1 AS a, 2 AS b UNION ALL (SELECT 2 AS b, 1 AS a UNION DISTINCT SELECT 4 AS b, 3 AS a)",
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			got := lint.Run(t.Context(), parseWithTable(t, tt.src), lint.Config{}, lint.Env{}, lint.UnionColumnOrder)
			if len(got) == 0 {
				t.Fatal("no diagnostic reported")
			}
			fixed := applyFixes(t, tt.src, got)
			if fixed != tt.wantFixed {
				t.Fatalf("fixed = %q, want %q", fixed, tt.wantFixed)
			}
			if fixed == tt.src {
				return
			}
			if left := lint.Run(t.Context(), parseWithTable(t, fixed), lint.Config{}, lint.Env{}, lint.UnionColumnOrder); len(left) != 0 {
				t.Errorf("fixed query still reports %+v", left)
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
