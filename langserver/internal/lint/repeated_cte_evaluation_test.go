package lint_test

import (
	"fmt"
	"testing"

	"github.com/kitagry/bqls/langserver/internal/lint"
)

func repeatedCTEEvaluationMessage(name string, n int) string {
	return fmt.Sprintf("CTE `%s` is evaluated %d times, once per reference, because BigQuery does not materialize non-recursive CTEs, so its table reads and its joins, aggregations or window functions run again each time; if it is expensive, store its result in a temporary table", name, n)
}

func TestRepeatedCTEEvaluation(t *testing.T) {
	type report struct {
		// text is the exact reported source text.
		text string
		n    int
	}
	tests := map[string]struct {
		src  string
		want []report
	}{
		"aggregate referenced twice": {
			src:  "WITH agg AS (SELECT id, SUM(v) AS total FROM p.d.t GROUP BY id)\nSELECT * FROM agg AS a JOIN agg AS b ON a.id = b.id + 1",
			want: []report{{"agg", 2}},
		},
		"join referenced from a subquery": {
			src:  "WITH j AS (SELECT a.id, b.v FROM p.d.t AS a JOIN p.d.t AS b USING (id))\nSELECT id FROM j WHERE v > (SELECT AVG(v) FROM j)",
			want: []report{{"j", 2}},
		},
		"window function in a set operation": {
			src:  "WITH w AS (SELECT id, ROW_NUMBER() OVER (PARTITION BY id ORDER BY ts) AS rn FROM p.d.t)\nSELECT id FROM w WHERE rn = 1 UNION ALL SELECT id FROM w WHERE rn = 2",
			want: []report{{"w", 2}},
		},
		// b runs once per evaluation of a.
		"referenced once from a CTE referenced twice": {
			src:  "WITH agg AS (SELECT id, SUM(v) AS total FROM p.d.t GROUP BY id),\nf AS (SELECT * FROM agg WHERE total > 0)\nSELECT * FROM f AS x JOIN f AS y USING (id)",
			want: []report{{"agg", 2}},
		},
		// The table is read through another CTE.
		"aggregate over a CTE that reads a table": {
			src:  "WITH src AS (SELECT * FROM p.d.t),\nagg AS (SELECT id, COUNT(*) AS c FROM src GROUP BY id)\nSELECT * FROM agg AS x JOIN agg AS y USING (id)",
			want: []report{{"agg", 2}},
		},
		"three references": {
			src:  "WITH agg AS (SELECT id, MAX(v) AS m FROM p.d.t GROUP BY id)\nSELECT (SELECT COUNT(*) FROM agg), (SELECT MAX(m) FROM agg), (SELECT MIN(m) FROM agg)",
			want: []report{{"agg", 3}},
		},
		"in INSERT": {
			src:  "INSERT INTO p.d.t (id, ts, v)\nWITH agg AS (SELECT id, MAX(ts) AS ts, SUM(v) AS v FROM p.d.t GROUP BY id)\nSELECT * FROM agg UNION ALL SELECT * FROM agg",
			want: []report{{"agg", 2}},
		},
		"referenced once": {
			src: "WITH agg AS (SELECT id, SUM(v) AS total FROM p.d.t GROUP BY id) SELECT * FROM agg",
		},
		// Only filtering or projecting a table costs a second scan, which
		// is usually what the readable version is worth.
		"no join, aggregate or window": {
			src: "WITH f AS (SELECT id, v FROM p.d.t WHERE v > 0) SELECT * FROM f AS a JOIN f AS b USING (id)",
		},
		// The aggregate runs per row over an array, not over the table.
		"aggregate over an array in a projection": {
			src: "WITH f AS (SELECT id, (SELECT COUNT(*) FROM UNNEST([v, v]) AS x) AS c FROM p.d.t)\nSELECT * FROM f AS a JOIN f AS b USING (id)",
		},
		"correlated aggregate over a table in a projection": {
			src:  "WITH f AS (SELECT id, (SELECT MAX(u.v) FROM p.d.t AS u WHERE u.id = o.id) AS m FROM p.d.t AS o)\nSELECT * FROM f AS a JOIN f AS b USING (id)",
			want: []report{{"f", 2}},
		},
		"no table": {
			src: "WITH n AS (SELECT x, COUNT(*) AS c FROM UNNEST([1, 2, 2]) AS x GROUP BY x) SELECT * FROM n AS a JOIN n AS b USING (x)",
		},
		"recursive CTE": {
			src: "WITH RECURSIVE r AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM r WHERE n < 3), agg AS (SELECT COUNT(*) AS c FROM p.d.t GROUP BY id) SELECT * FROM r AS a JOIN r AS b USING (n) CROSS JOIN agg",
		},
		"unreferenced": {
			src: "WITH agg AS (SELECT id, SUM(v) AS total FROM p.d.t GROUP BY id) SELECT 1",
		},
		"suppressed by comment": {
			src: "WITH\n-- bqls:ignore repeated-cte-evaluation\nagg AS (SELECT id, SUM(v) AS total FROM p.d.t GROUP BY id)\nSELECT * FROM agg AS a JOIN agg AS b USING (id)",
		},
	}

	cfg := lint.Config{Rules: map[string]bool{"repeated-cte-evaluation": true}}
	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			got := lint.Run(t.Context(), parseEvents(t, tt.src), cfg, lint.Env{}, lint.RepeatedCTEEvaluation)
			if len(got) != len(tt.want) {
				t.Fatalf("got %d diagnostics, want %d: %+v", len(got), len(tt.want), got)
			}
			for i, w := range tt.want {
				d := got[i]
				if text := textInRange(tt.src, d); text != w.text {
					t.Errorf("reported text = %q, want %q", text, w.text)
				}
				if want := repeatedCTEEvaluationMessage(w.text, w.n); d.Rule != "repeated-cte-evaluation" || d.Severity != lint.SeverityWarning || d.Message != want || len(d.SuggestedFixes) != 0 {
					t.Errorf("diagnostic = %+v, want warning %q without a fix", d, want)
				}
			}
		})
	}
}

func TestRepeatedCTEEvaluation_OffByDefault(t *testing.T) {
	src := "WITH agg AS (SELECT id, SUM(v) AS total FROM p.d.t GROUP BY id) SELECT * FROM agg AS a JOIN agg AS b USING (id)"
	if got := lint.Run(t.Context(), parseEvents(t, src), lint.Config{}, lint.Env{}, lint.RepeatedCTEEvaluation); len(got) != 0 {
		t.Errorf("got %+v, want nothing while the rule is off by default", got)
	}
}
