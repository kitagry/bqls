package lint_test

import (
	"strings"
	"testing"

	"github.com/kitagry/bqls/langserver/internal/lint"
)

const orderByMessage = "ORDER BY in a subquery has no effect on the result because BigQuery does not preserve row order; it only adds sorting cost"

func TestOrderByInSubquery(t *testing.T) {
	tests := map[string]struct {
		src string
		// want is the exact text of each reported range, in order.
		want []string
		// wantFixed is src after applying every fix; it must analyze cleanly.
		wantFixed string
	}{
		"derived table": {
			src:       "SELECT x FROM (SELECT x FROM p.d.t ORDER BY x)",
			want:      []string{"ORDER BY x"},
			wantFixed: "SELECT x FROM (SELECT x FROM p.d.t)",
		},
		"derived table with filter": {
			src:       "SELECT x FROM (SELECT x FROM p.d.t ORDER BY x DESC) WHERE x > 1",
			want:      []string{"ORDER BY x DESC"},
			wantFixed: "SELECT x FROM (SELECT x FROM p.d.t) WHERE x > 1",
		},
		"multiline derived table": {
			src:       "SELECT x\nFROM (\n  SELECT x\n  FROM p.d.t\n  ORDER BY x\n)",
			want:      []string{"ORDER BY x"},
			wantFixed: "SELECT x\nFROM (\n  SELECT x\n  FROM p.d.t\n)",
		},
		"cte": {
			src:       "WITH a AS (SELECT x FROM p.d.t ORDER BY x) SELECT x FROM a",
			want:      []string{"ORDER BY x"},
			wantFixed: "WITH a AS (SELECT x FROM p.d.t) SELECT x FROM a",
		},
		"cte with outer order by": {
			src:       "WITH a AS (SELECT x FROM p.d.t ORDER BY x) SELECT x FROM a ORDER BY x",
			want:      []string{"ORDER BY x"},
			wantFixed: "WITH a AS (SELECT x FROM p.d.t) SELECT x FROM a ORDER BY x",
		},
		"in subquery": {
			src:       "SELECT x FROM p.d.t WHERE x IN (SELECT x FROM p.d.t ORDER BY x)",
			want:      []string{"ORDER BY x"},
			wantFixed: "SELECT x FROM p.d.t WHERE x IN (SELECT x FROM p.d.t)",
		},
		"exists subquery": {
			src:       "SELECT x FROM p.d.t WHERE EXISTS (SELECT 1 FROM p.d.t ORDER BY x)",
			want:      []string{"ORDER BY x"},
			wantFixed: "SELECT x FROM p.d.t WHERE EXISTS (SELECT 1 FROM p.d.t)",
		},
		"join input": {
			src:       "SELECT a.x FROM (SELECT x FROM p.d.t ORDER BY x) AS a JOIN p.d.t USING (x)",
			want:      []string{"ORDER BY x"},
			wantFixed: "SELECT a.x FROM (SELECT x FROM p.d.t) AS a JOIN p.d.t USING (x)",
		},
		"nested derived tables": {
			src:       "SELECT x FROM (SELECT x FROM (SELECT x FROM p.d.t ORDER BY x) ORDER BY x)",
			want:      []string{"ORDER BY x", "ORDER BY x"},
			wantFixed: "SELECT x FROM (SELECT x FROM (SELECT x FROM p.d.t))",
		},
		"outer order by does not excuse inner": {
			src:       "SELECT x FROM (SELECT x FROM p.d.t ORDER BY x) ORDER BY x",
			want:      []string{"ORDER BY x"},
			wantFixed: "SELECT x FROM (SELECT x FROM p.d.t) ORDER BY x",
		},
		"comment on the clause line keeps the query valid": {
			src:  "SELECT x FROM (SELECT x FROM p.d.t -- source\nORDER BY x)",
			want: []string{"ORDER BY x"},
			// No fix: deleting the line break would comment out the closing parenthesis.
			wantFixed: "SELECT x FROM (SELECT x FROM p.d.t -- source\nORDER BY x)",
		},

		// Shapes where the order can matter, or that are out of scope.
		"limit": {src: "SELECT x FROM (SELECT x FROM p.d.t ORDER BY x LIMIT 10)"},
		"limit in cte": {
			src: "WITH a AS (SELECT x FROM p.d.t ORDER BY x LIMIT 1) SELECT x FROM a",
		},
		"outermost query": {src: "SELECT x FROM p.d.t ORDER BY x"},
		"outermost query ordered by an unselected expression": {
			src: "SELECT x FROM p.d.t ORDER BY x + 1",
		},
		"outermost grouped query ordered by an aggregate": {
			src: "SELECT x FROM p.d.t GROUP BY x ORDER BY COUNT(*)",
		},
		"outermost distinct query":      {src: "SELECT DISTINCT x FROM p.d.t ORDER BY x"},
		"outermost parenthesized query": {src: "(SELECT x FROM p.d.t ORDER BY x)"},
		"outermost query with cte": {
			src: "WITH a AS (SELECT x FROM p.d.t) SELECT x FROM a ORDER BY x",
		},
		"window function": {src: "SELECT SUM(x) OVER (ORDER BY x) FROM p.d.t"},
		"array_agg":       {src: "SELECT ARRAY_AGG(x ORDER BY x) FROM p.d.t"},
		"array subquery": {
			src: "SELECT ARRAY(SELECT x FROM p.d.t ORDER BY x) AS a",
		},
		"array subquery in unnest": {
			src: "SELECT u FROM UNNEST(ARRAY(SELECT x FROM p.d.t ORDER BY x)) AS u",
		},
		"scalar subquery": {
			src: "SELECT (SELECT x FROM p.d.t ORDER BY x) AS a",
		},
		"outer limit relies on order": {
			src: "SELECT x FROM (SELECT x FROM p.d.t ORDER BY x) LIMIT 10",
		},
		"outer limit through filter": {
			src: "SELECT x FROM (SELECT x FROM p.d.t ORDER BY x) WHERE x > 1 LIMIT 10",
		},
		"outer window function relies on order": {
			src: "SELECT x, ROW_NUMBER() OVER () FROM (SELECT x FROM p.d.t ORDER BY x)",
		},
		"outer array_agg relies on order": {
			src: "SELECT ARRAY_AGG(x) FROM (SELECT x FROM p.d.t ORDER BY x)",
		},
		"cte consumed with limit": {
			src: "WITH a AS (SELECT x FROM p.d.t ORDER BY x) SELECT x FROM a LIMIT 10",
		},
		"cte consumed by array_agg": {
			src: "WITH a AS (SELECT x FROM p.d.t ORDER BY x) SELECT ARRAY_AGG(x) FROM a",
		},
		"cte consumed by another cte that is limited": {
			src: "WITH a AS (SELECT x FROM p.d.t ORDER BY x), b AS (SELECT x FROM a LIMIT 1) SELECT x FROM b",
		},
		"cte referenced both ways": {
			src: "WITH a AS (SELECT x FROM p.d.t ORDER BY x) SELECT x FROM a UNION ALL SELECT x FROM a LIMIT 3",
		},
		"set operation branch": {
			src: "SELECT x FROM p.d.t UNION ALL (SELECT x FROM p.d.t ORDER BY x)",
		},
		"insert select": {src: "INSERT INTO p.d.t (x) SELECT x FROM p.d.t ORDER BY x"},
		"insert select from derived table": {
			src: "INSERT INTO p.d.t (x) SELECT x FROM (SELECT x FROM p.d.t ORDER BY x)",
		},
		"create table as select": {
			src: "CREATE TABLE p.d.n AS SELECT x FROM (SELECT x FROM p.d.t ORDER BY x)",
		},
		"pipe order by": {src: "SELECT x FROM p.d.t |> ORDER BY x |> SELECT x"},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			parsed := parseWithTable(t, tt.src)
			got := lint.Run(t.Context(), parsed, lint.Config{}, lint.Env{}, lint.OrderByInSubquery)

			var gotText []string
			for _, d := range got {
				if d.Rule != "order-by-in-subquery" || d.Message != orderByMessage || d.Severity != lint.SeverityWarning {
					t.Errorf("unexpected diagnostic metadata: %+v", d)
				}
				gotText = append(gotText, textInRange(tt.src, d))
			}
			if strings.Join(gotText, "|") != strings.Join(tt.want, "|") {
				t.Fatalf("reported ranges = %q, want %q", gotText, tt.want)
			}
			if len(got) == 0 {
				return
			}

			fixed := applyFixes(t, tt.src, got)
			if fixed != tt.wantFixed {
				t.Errorf("fixed = %q, want %q", fixed, tt.wantFixed)
			}
			parseWithTable(t, fixed)
		})
	}
}

func TestOrderByInSubquerySuppressed(t *testing.T) {
	src := "SELECT x FROM (\n  SELECT x FROM p.d.t\n  -- bqls:ignore order-by-in-subquery\n  ORDER BY x\n)"
	parsed := parseWithTable(t, src)
	if got := lint.Run(t.Context(), parsed, lint.Config{}, lint.Env{}, lint.OrderByInSubquery); len(got) != 0 {
		t.Errorf("got %+v, want suppressed", got)
	}
}

func TestOrderByInSubqueryConfig(t *testing.T) {
	src := "SELECT x FROM (SELECT x FROM p.d.t ORDER BY x)"
	parsed := parseWithTable(t, src)
	off := lint.Config{Rules: map[string]bool{"order-by-in-subquery": false}}
	if got := lint.Run(t.Context(), parsed, off, lint.Env{}, lint.OrderByInSubquery); len(got) != 0 {
		t.Errorf("disabled rule reported %+v", got)
	}
	if got := lint.Run(t.Context(), parsed, lint.Config{}, lint.Env{}, lint.OrderByInSubquery); len(got) != 1 {
		t.Errorf("rule should be enabled by default, got %+v", got)
	}
}

func textInRange(src string, d lint.Diagnostic) string {
	lines := strings.SplitAfter(src, "\n")
	offset := func(line, char int) int {
		n := char
		for _, l := range lines[:line] {
			n += len(l)
		}
		return n
	}
	r := d.Range
	return src[offset(r.Start.Line, r.Start.Character):offset(r.End.Line, r.End.Character)]
}
