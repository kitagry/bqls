package lint_test

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/kitagry/bqls/langserver/internal/lint"
)

const crossJoinMessage = "Join has no join condition, so it returns every combination of rows from both sides; add a condition, or write CROSS JOIN if this is intended"

func TestCrossJoin(t *testing.T) {
	type report struct {
		// sub is the reported source text and n its occurrence index.
		sub string
		n   int
	}
	tests := map[string]struct {
		src  string
		want []report
	}{
		"comma join": {
			src:  "SELECT a.x FROM p.d.a, p.d.b",
			want: []report{{sub: "p.d.b"}},
		},
		"comma join with alias": {
			src:  "SELECT a.x FROM p.d.a AS a, p.d.b AS b",
			want: []report{{sub: "p.d.b AS b"}},
		},
		"JOIN ON TRUE": {
			src:  "SELECT a.x FROM p.d.a JOIN p.d.b ON TRUE",
			want: []report{{sub: "p.d.b"}},
		},
		"INNER JOIN ON TRUE": {
			src:  "SELECT a.x FROM p.d.a INNER JOIN p.d.b ON TRUE",
			want: []report{{sub: "p.d.b"}},
		},
		"WHERE filters each side separately": {
			src:  "SELECT a.x FROM p.d.a, p.d.b WHERE a.x = 1 AND b.x = 2",
			want: []report{{sub: "p.d.b"}},
		},
		"third table left unconditioned": {
			src:  "SELECT a.x FROM p.d.a JOIN p.d.b ON a.x = b.x, p.d.c",
			want: []report{{sub: "p.d.c"}},
		},
		"comma join with a multi-row subquery": {
			src:  "SELECT a.x FROM p.d.a, (SELECT x FROM p.d.b GROUP BY x)",
			want: []report{{sub: "(SELECT x FROM p.d.b GROUP BY x)"}},
		},
		"inside a CTE": {
			src:  "WITH c AS (SELECT a.x FROM p.d.a, p.d.b) SELECT x FROM c",
			want: []report{{sub: "p.d.b"}},
		},
		"multiline": {
			src:  "SELECT a.x\nFROM p.d.a,\n  p.d.b",
			want: []report{{sub: "p.d.b"}},
		},
		"INSERT SELECT": {
			src:  "INSERT INTO p.d.t (x) SELECT a.x FROM p.d.a, p.d.b",
			want: []report{{sub: "p.d.b", n: 0}},
		},
		"pipe JOIN is not located at its keyword, so it is skipped": {
			src: "FROM p.d.a |> JOIN p.d.b ON TRUE",
		},
		"explicit CROSS JOIN": {
			src: "SELECT a.x FROM p.d.a CROSS JOIN p.d.b",
		},
		"comma join conditioned in WHERE": {
			src: "SELECT a.x FROM p.d.a, p.d.b WHERE a.x = b.x",
		},
		"conditioned in one conjunct of WHERE": {
			src: "SELECT a.x FROM p.d.a, p.d.b WHERE a.x > 0 AND a.x = b.x",
		},
		"condition on one side of OR counts": {
			src: "SELECT a.x FROM p.d.a, p.d.b WHERE a.x = b.x OR a.x IS NULL",
		},
		"three tables conditioned in WHERE": {
			src: "SELECT a.x FROM p.d.a, p.d.b, p.d.c WHERE a.x = b.x AND b.x = c.x",
		},
		"third table conditioned to only one side": {
			src:  "SELECT a.x FROM p.d.a, p.d.b, p.d.c WHERE a.x = c.x",
			want: []report{{sub: "p.d.b"}},
		},
		"each side conditioned only to itself": {
			src:  "SELECT a.x FROM p.d.a, p.d.b, p.d.c WHERE a.x = a.x + 1 AND b.x = c.x",
			want: []report{{sub: "p.d.b"}},
		},
		"conditioned through a third table": {
			src: "SELECT a.x FROM p.d.a, p.d.b, p.d.c WHERE a.x = c.x AND b.x = c.x",
		},
		"comma join conditioned by a later ON": {
			src: "SELECT a.x FROM p.d.a, p.d.b JOIN p.d.c ON a.x = c.x AND b.x = c.x",
		},
		"conditioned through a subquery": {
			src: "SELECT ax FROM (SELECT a.x AS ax, b.x AS bx FROM p.d.a, p.d.b) WHERE ax = bx",
		},
		"conditioned through a CTE": {
			src: "WITH c AS (SELECT a.x AS ax, b.x AS bx FROM p.d.a, p.d.b) SELECT ax FROM c WHERE ax = bx",
		},
		"conditioned on derived values": {
			src: "SELECT a.x FROM (SELECT x + 1 AS y, x FROM p.d.a) a, p.d.b WHERE a.y = b.x",
		},
		"JOIN ON condition": {
			src: "SELECT a.x FROM p.d.a JOIN p.d.b ON a.x = b.x",
		},
		"JOIN USING": {
			src: "SELECT x FROM p.d.a JOIN p.d.b USING (x)",
		},
		"LEFT JOIN ON TRUE": {
			src: "SELECT a.x FROM p.d.a LEFT JOIN p.d.b ON TRUE",
		},
		"FULL JOIN ON TRUE": {
			src: "SELECT a.x FROM p.d.a FULL JOIN p.d.b ON TRUE",
		},
		"UNNEST of a column": {
			src: "SELECT a.x, e FROM (SELECT x, [x] AS arr FROM p.d.a) a, UNNEST(a.arr) AS e",
		},
		"UNNEST of a literal": {
			src: "SELECT a.x, e FROM p.d.a, UNNEST([1, 2]) AS e",
		},
		"aggregate subquery without GROUP BY": {
			src: "SELECT a.x FROM p.d.a, (SELECT MAX(x) AS m FROM p.d.b)",
		},
		"aggregate subquery on the left": {
			src: "SELECT x FROM (SELECT MAX(x) AS m FROM p.d.b), p.d.a",
		},
		"SELECT without FROM": {
			src: "SELECT a.x FROM p.d.a, (SELECT 1 AS k)",
		},
		"LIMIT 1": {
			src: "SELECT a.x FROM p.d.a, (SELECT x AS y FROM p.d.b LIMIT 1)",
		},
		"single-row CTE": {
			src: "WITH params AS (SELECT DATE '2020-01-01' AS d) SELECT a.x FROM p.d.a, params",
		},
		"single-row CTE through a filter": {
			src: "WITH params AS (SELECT MAX(x) AS m FROM p.d.b) SELECT a.x FROM p.d.a, (SELECT m FROM params WHERE m > 0)",
		},
		"CREATE VIEW": {
			src: "CREATE VIEW p.d.v AS SELECT a.x FROM p.d.a, p.d.b",
		},
		"unused CTE": {
			src: "WITH c AS (SELECT a.x FROM p.d.a, p.d.b) SELECT 1 AS y",
		},
		"suppressed by comment": {
			src: "-- bqls:ignore cross-join\nSELECT a.x FROM p.d.a, p.d.b",
		},
	}

	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			var want []lint.Diagnostic
			for _, r := range tt.want {
				want = append(want, lint.Diagnostic{
					Rule:     "cross-join",
					Range:    spanOf(t, tt.src, r.sub, r.n),
					Message:  crossJoinMessage,
					Severity: lint.SeverityWarning,
				})
			}
			got := lint.Run(t.Context(), parseWithTable(t, tt.src), lint.Config{}, lint.Env{}, lint.CrossJoin)
			if diff := cmp.Diff(want, got); diff != "" {
				t.Errorf("diff (-want +got):\n%s", diff)
			}
		})
	}
}

func TestCrossJoinConfig(t *testing.T) {
	parsed := parseWithTable(t, "SELECT a.x FROM p.d.a, p.d.b")
	off := lint.Config{Rules: map[string]bool{"cross-join": false}}
	if got := lint.Run(t.Context(), parsed, off, lint.Env{}, lint.CrossJoin); len(got) != 0 {
		t.Errorf("disabled rule reported %+v", got)
	}
	if got := lint.Run(t.Context(), parsed, lint.Config{}, lint.Env{}, lint.CrossJoin); len(got) != 1 {
		t.Errorf("rule should be enabled by default, got %+v", got)
	}
}
