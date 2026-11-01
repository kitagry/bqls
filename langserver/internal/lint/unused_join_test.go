package lint_test

import (
	"strings"
	"testing"

	"github.com/kitagry/bqls/langserver/internal/lint"
)

const unusedJoinMessage = "no column of the joined table is used outside its join condition; the join only adds cost (or duplicates rows when several rows match), so it can likely be removed"

func TestUnusedJoin(t *testing.T) {
	tests := map[string]struct {
		src string
		// want is the exact text of each reported range, in order.
		want []string
	}{
		"left join used only in on": {
			src:  "SELECT a.x FROM p.d.t AS a LEFT JOIN p.d.t AS b ON a.x = b.x",
			want: []string{"LEFT JOIN"},
		},
		"left outer join": {
			src:  "SELECT a.x FROM p.d.t AS a LEFT OUTER JOIN p.d.t AS b ON a.x = b.x",
			want: []string{"LEFT OUTER JOIN"},
		},
		"left join using": {
			src:  "SELECT x FROM p.d.t AS a LEFT JOIN p.d.t AS b USING (x)",
			want: []string{"LEFT JOIN"},
		},
		"select star with using": {
			src:  "SELECT * FROM p.d.t AS a LEFT JOIN p.d.t AS b USING (x)",
			want: []string{"LEFT JOIN"},
		},
		"left join on true": {
			src:  "SELECT a.x FROM p.d.t AS a LEFT JOIN p.d.t AS b ON TRUE",
			want: []string{"LEFT JOIN"},
		},
		"count star": {
			src:  "SELECT COUNT(*) FROM p.d.t AS a LEFT JOIN p.d.t AS b ON a.x = b.x",
			want: []string{"LEFT JOIN"},
		},
		"right join with unused left side": {
			src:  "SELECT b.x FROM p.d.t AS a RIGHT JOIN p.d.t AS b ON a.x = b.x",
			want: []string{"RIGHT JOIN"},
		},
		"right join using resolves to the right side": {
			src:  "SELECT x FROM p.d.t AS a RIGHT JOIN p.d.t AS b USING (x)",
			want: []string{"RIGHT JOIN"},
		},
		"left join of a subquery": {
			src:  "SELECT a.x FROM p.d.t AS a LEFT JOIN (SELECT x FROM p.d.t) AS b ON a.x = b.x",
			want: []string{"LEFT JOIN"},
		},
		"left join of a cte": {
			src:  "WITH c AS (SELECT x FROM p.d.t) SELECT a.x FROM p.d.t AS a LEFT JOIN c ON a.x = c.x",
			want: []string{"LEFT JOIN"},
		},
		"left join inside a cte": {
			src:  "WITH c AS (SELECT a.x FROM p.d.t AS a LEFT JOIN p.d.t AS b ON a.x = b.x) SELECT x FROM c",
			want: []string{"LEFT JOIN"},
		},
		"left join inside a subquery": {
			src:  "SELECT x FROM (SELECT a.x FROM p.d.t AS a LEFT JOIN p.d.t AS b ON a.x = b.x)",
			want: []string{"LEFT JOIN"},
		},
		"only the unused one of two joins": {
			src:  "SELECT a.x, c.x FROM p.d.t AS a LEFT JOIN p.d.t AS b ON a.x = b.x LEFT JOIN p.d.t AS c ON a.x = c.x",
			want: []string{"LEFT JOIN"},
		},
		"multiline": {
			src:  "SELECT a.x\nFROM p.d.t AS a\nLEFT JOIN p.d.t AS b\n  ON a.x = b.x",
			want: []string{"LEFT JOIN"},
		},

		// The joined side is used, or the join is out of scope.
		"used in select list":   {src: "SELECT a.x, b.x FROM p.d.t AS a LEFT JOIN p.d.t AS b ON a.x = b.x"},
		"used in where":         {src: "SELECT a.x FROM p.d.t AS a LEFT JOIN p.d.t AS b ON a.x = b.x WHERE b.x IS NULL"},
		"used in group by":      {src: "SELECT COUNT(*) FROM p.d.t AS a LEFT JOIN p.d.t AS b ON a.x = b.x GROUP BY b.x"},
		"used in order by":      {src: "SELECT a.x FROM p.d.t AS a LEFT JOIN p.d.t AS b ON a.x = b.x ORDER BY b.x"},
		"used in an aggregate":  {src: "SELECT COUNT(b.x) FROM p.d.t AS a LEFT JOIN p.d.t AS b ON a.x = b.x"},
		"select star":           {src: "SELECT * FROM p.d.t AS a LEFT JOIN p.d.t AS b ON a.x = b.x"},
		"select table star":     {src: "SELECT b.* FROM p.d.t AS a LEFT JOIN p.d.t AS b ON a.x = b.x"},
		"select table as value": {src: "SELECT b FROM p.d.t AS a LEFT JOIN p.d.t AS b ON a.x = b.x"},
		"used by a later join": {
			src: "SELECT a.x, c.x FROM p.d.t AS a LEFT JOIN p.d.t AS b ON a.x = b.x LEFT JOIN p.d.t AS c ON b.x = c.x",
		},
		"used in a correlated subquery": {
			src: "SELECT a.x, (SELECT COUNT(*) FROM p.d.t AS c WHERE c.x = b.x) AS n FROM p.d.t AS a LEFT JOIN p.d.t AS b ON a.x = b.x",
		},
		"used by an outer query": {
			src: "SELECT y FROM (SELECT a.x, b.x AS y FROM p.d.t AS a LEFT JOIN p.d.t AS b ON a.x = b.x)",
		},
		"used through a cte": {
			src: "WITH c AS (SELECT a.x, b.x AS y FROM p.d.t AS a LEFT JOIN p.d.t AS b ON a.x = b.x) SELECT y FROM c",
		},
		"right join using the left side": {
			src: "SELECT a.x FROM p.d.t AS a RIGHT JOIN p.d.t AS b ON a.x = b.x",
		},
		"inner join":  {src: "SELECT a.x FROM p.d.t AS a JOIN p.d.t AS b ON a.x = b.x"},
		"cross join":  {src: "SELECT a.x FROM p.d.t AS a CROSS JOIN p.d.t AS b"},
		"comma join":  {src: "SELECT a.x FROM p.d.t AS a, p.d.t AS b"},
		"full join":   {src: "SELECT a.x FROM p.d.t AS a FULL JOIN p.d.t AS b ON a.x = b.x"},
		"left unnest": {src: "SELECT a.x FROM p.d.t AS a LEFT JOIN UNNEST([1, 2]) AS u"},
		"no join":     {src: "SELECT x FROM p.d.t"},
		"insert select": {
			src: "INSERT INTO p.d.t (x) SELECT a.x FROM p.d.t AS a LEFT JOIN p.d.t AS b ON a.x = b.x WHERE b.x IS NULL",
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			parsed := parseWithTable(t, tt.src)
			got := lint.Run(t.Context(), parsed, lint.Config{}, lint.Env{}, lint.UnusedJoin)

			var gotText []string
			for _, d := range got {
				if d.Rule != "unused-join" || d.Message != unusedJoinMessage || d.Severity != lint.SeverityInformation || len(d.SuggestedFixes) != 0 {
					t.Errorf("unexpected diagnostic metadata: %+v", d)
				}
				gotText = append(gotText, textInRange(tt.src, d))
			}
			if strings.Join(gotText, "|") != strings.Join(tt.want, "|") {
				t.Errorf("reported ranges = %q, want %q", gotText, tt.want)
			}
		})
	}
}

func TestUnusedJoinSuppressed(t *testing.T) {
	src := "SELECT a.x\nFROM p.d.t AS a\n-- bqls:ignore unused-join\nLEFT JOIN p.d.t AS b ON a.x = b.x"
	parsed := parseWithTable(t, src)
	if got := lint.Run(t.Context(), parsed, lint.Config{}, lint.Env{}, lint.UnusedJoin); len(got) != 0 {
		t.Errorf("got %+v, want suppressed", got)
	}
}

func TestUnusedJoinConfig(t *testing.T) {
	src := "SELECT a.x FROM p.d.t AS a LEFT JOIN p.d.t AS b ON a.x = b.x"
	parsed := parseWithTable(t, src)
	off := lint.Config{Rules: map[string]bool{"unused-join": false}}
	if got := lint.Run(t.Context(), parsed, off, lint.Env{}, lint.UnusedJoin); len(got) != 0 {
		t.Errorf("disabled rule reported %+v", got)
	}
	if got := lint.Run(t.Context(), parsed, lint.Config{}, lint.Env{}, lint.UnusedJoin); len(got) != 1 {
		t.Errorf("rule should be enabled by default, got %+v", got)
	}
}
