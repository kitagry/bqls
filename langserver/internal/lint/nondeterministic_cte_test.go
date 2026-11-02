package lint_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/kitagry/bqls/langserver/internal/lint"
	"github.com/kitagry/bqls/langserver/internal/lsp"
)

func TestNondeterministicCTE(t *testing.T) {
	type report struct {
		// call is the reported function name as written and n its occurrence index.
		call  string
		n     int
		fn    string
		cte   string
		times int
	}
	tests := map[string]struct {
		src  string
		want []report
	}{
		"RAND column read by two references": {
			src:  "WITH a AS (SELECT RAND() AS r) SELECT x.r, y.r FROM a x CROSS JOIN a y",
			want: []report{{call: "RAND", fn: "RAND", cte: "a", times: 2}},
		},
		"GENERATE_UUID column read by two references": {
			src:  "WITH a AS (SELECT GENERATE_UUID() AS id, x FROM p.d.t) SELECT id FROM a UNION ALL SELECT id FROM a",
			want: []report{{call: "GENERATE_UUID", fn: "GENERATE_UUID", cte: "a", times: 2}},
		},
		"lowercase call": {
			src:  "WITH a AS (SELECT rand() AS r) SELECT (SELECT r FROM a) = (SELECT r FROM a) AS same",
			want: []report{{call: "rand", fn: "RAND", cte: "a", times: 2}},
		},
		"three references": {
			src:  "WITH a AS (SELECT RAND() AS r) SELECT * FROM a UNION ALL SELECT * FROM a UNION ALL SELECT * FROM a",
			want: []report{{call: "RAND", fn: "RAND", cte: "a", times: 3}},
		},
		"every call in the CTE": {
			src: "WITH a AS (SELECT RAND() AS r, GENERATE_UUID() AS u) SELECT * FROM a UNION ALL SELECT * FROM a",
			want: []report{
				{call: "RAND", fn: "RAND", cte: "a", times: 2},
				{call: "GENERATE_UUID", fn: "GENERATE_UUID", cte: "a", times: 2},
			},
		},
		"single reference": {
			src: "WITH a AS (SELECT RAND() AS r) SELECT * FROM a",
		},
		"current time functions are fixed per statement": {
			src: "WITH a AS (SELECT CURRENT_TIMESTAMP() AS ts, CURRENT_DATE() AS d, CURRENT_DATETIME() AS dt, CURRENT_TIME() AS tm) SELECT * FROM a UNION ALL SELECT * FROM a",
		},
		"deterministic CTE": {
			src: "WITH a AS (SELECT x FROM p.d.t) SELECT * FROM a UNION ALL SELECT * FROM a",
		},
		"call outside any CTE": {
			src: "WITH a AS (SELECT x FROM p.d.t) SELECT RAND() AS r FROM a UNION ALL SELECT RAND() AS r FROM a",
		},
		// Recursive CTEs are materialized.
		"recursive CTE": {
			src: "WITH RECURSIVE r AS (SELECT RAND() AS v, 1 AS n UNION ALL SELECT v, n + 1 FROM r WHERE n < 3) SELECT * FROM r UNION ALL SELECT * FROM r",
		},
		"references from an unused CTE are not evaluated": {
			src: "WITH a AS (SELECT RAND() AS r), b AS (SELECT * FROM a UNION ALL SELECT * FROM a) SELECT * FROM a",
		},
		"CTE used once by a CTE referenced twice": {
			src:  "WITH a AS (SELECT RAND() AS r), b AS (SELECT r FROM a) SELECT * FROM b UNION ALL SELECT * FROM b",
			want: []report{{call: "RAND", fn: "RAND", cte: "a", times: 2}},
		},
		"CTE used by two CTEs": {
			src:  "WITH a AS (SELECT RAND() AS r), b AS (SELECT r FROM a), c AS (SELECT r FROM a) SELECT * FROM b UNION ALL SELECT * FROM c",
			want: []report{{call: "RAND", fn: "RAND", cte: "a", times: 2}},
		},
		"nested WITH inside a CTE referenced twice": {
			src:  "WITH b AS (WITH a AS (SELECT RAND() AS r) SELECT r FROM a) SELECT * FROM b UNION ALL SELECT * FROM b",
			want: []report{{call: "RAND", fn: "RAND", cte: "a", times: 2}},
		},
		"each statement of a script": {
			src:  "WITH a AS (SELECT RAND() AS r) SELECT * FROM a;\nWITH a AS (SELECT RAND() AS r) SELECT * FROM a UNION ALL SELECT * FROM a",
			want: []report{{call: "RAND", n: 1, fn: "RAND", cte: "a", times: 2}},
		},
		// Values that no second evaluation reads cannot disagree.
		"column read by only one reference": {
			src: "WITH a AS (SELECT RAND() AS r, x FROM p.d.t) SELECT (SELECT COUNT(*) FROM a) AS n, (SELECT MAX(r) FROM a) AS m",
		},
		"column not selected from either reference": {
			src: "WITH a AS (SELECT RAND() AS r, x FROM p.d.t) SELECT b.x FROM a JOIN a AS b USING (x)",
		},
		"column excluded by SELECT * EXCEPT": {
			src: "WITH a AS (SELECT RAND() AS r, x FROM p.d.t) SELECT * EXCEPT (r) FROM a UNION ALL SELECT * EXCEPT (r) FROM a",
		},
		"column dropped by a CTE in between": {
			src: "WITH a AS (SELECT RAND() AS r, 1 AS x), b AS (SELECT x FROM a) SELECT * FROM b UNION ALL SELECT * FROM b",
		},
		"column dropped after a set operation": {
			src: "WITH a AS (SELECT RAND() AS r, 1 AS x) SELECT x FROM (SELECT * FROM a UNION ALL SELECT * FROM a)",
		},
		"EXISTS does not read values": {
			src: "WITH a AS (SELECT RAND() AS r) SELECT EXISTS(SELECT * FROM a) AND EXISTS(SELECT * FROM a) AS e",
		},
		"only the call whose column is read twice": {
			src:  "WITH a AS (SELECT RAND() AS r, GENERATE_UUID() AS u) SELECT r FROM a UNION ALL SELECT r FROM a",
			want: []report{{call: "RAND", fn: "RAND", cte: "a", times: 2}},
		},
		"aggregate of a random column": {
			src:  "WITH a AS (SELECT SUM(RAND()) AS s FROM p.d.t) SELECT * FROM a UNION ALL SELECT * FROM a",
			want: []report{{call: "RAND", fn: "RAND", cte: "a", times: 2}},
		},
		"INSERT reads the column": {
			src:  "INSERT INTO p.d.t (x) WITH a AS (SELECT CAST(RAND() * 10 AS INT64) AS x) SELECT x FROM a UNION ALL SELECT x FROM a",
			want: []report{{call: "RAND", fn: "RAND", cte: "a", times: 2}},
		},
		// A call that decides which rows the CTE returns is observed by every evaluation.
		"WHERE filter": {
			src:  "WITH a AS (SELECT x FROM p.d.t WHERE RAND() < 0.5) SELECT (SELECT COUNT(*) FROM a) AS c1, (SELECT COUNT(*) FROM a) AS c2",
			want: []report{{call: "RAND", fn: "RAND", cte: "a", times: 2}},
		},
		"WHERE filter on a derived column": {
			src:  "WITH a AS (SELECT * FROM (SELECT x, RAND() AS r FROM p.d.t) WHERE r < 0.5) SELECT (SELECT COUNT(*) FROM a) AS c1, (SELECT COUNT(*) FROM a) AS c2",
			want: []report{{call: "RAND", fn: "RAND", cte: "a", times: 2}},
		},
		"GROUP BY a random key": {
			src:  "WITH a AS (SELECT CAST(RAND() * 10 AS INT64) AS k, COUNT(*) AS n FROM p.d.t GROUP BY k) SELECT (SELECT SUM(n) FROM a) AS s, (SELECT COUNT(*) FROM a) AS c",
			want: []report{{call: "RAND", fn: "RAND", cte: "a", times: 2}},
		},
		"ORDER BY RAND() with LIMIT": {
			src:  "WITH a AS (SELECT x FROM p.d.t ORDER BY RAND() LIMIT 10) SELECT x FROM a UNION ALL SELECT x FROM a",
			want: []report{{call: "RAND", fn: "RAND", cte: "a", times: 2}},
		},
		"window ordered by RAND()": {
			src:  "WITH a AS (SELECT x, ROW_NUMBER() OVER (ORDER BY RAND()) AS rn FROM p.d.t) SELECT x FROM a WHERE rn = 1 UNION ALL SELECT x FROM a WHERE rn = 1",
			want: []report{{call: "RAND", fn: "RAND", cte: "a", times: 2}},
		},
		// bqls cannot tell that the rows do not overlap.
		"references reading disjoint rows": {
			src:  "WITH a AS (SELECT GENERATE_UUID() AS id, x FROM p.d.t) SELECT id FROM a WHERE x = 1 UNION ALL SELECT id FROM a WHERE x = 2",
			want: []report{{call: "GENERATE_UUID", fn: "GENERATE_UUID", cte: "a", times: 2}},
		},
		// Without LIMIT the order of a CTE cannot reach the result.
		"ORDER BY RAND() without LIMIT": {
			src: "WITH a AS (SELECT x FROM p.d.t ORDER BY RAND()) SELECT x FROM a UNION ALL SELECT x FROM a",
		},
		"suppressed by comment": {
			src: "-- bqls:ignore nondeterministic-cte\nWITH a AS (SELECT RAND() AS r) SELECT * FROM a UNION ALL SELECT * FROM a",
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			var want []lint.Diagnostic
			for _, r := range tt.want {
				want = append(want, lint.Diagnostic{
					Rule:     "nondeterministic-cte",
					Range:    occurrenceRange(t, tt.src, r.call, r.n),
					Message:  fmt.Sprintf("%s() in CTE `%s` returns a different value for each of its %d evaluations, because BigQuery evaluates a non-recursive CTE once per reference", r.fn, r.cte, r.times),
					Severity: lint.SeverityWarning,
				})
			}
			got := lint.Run(t.Context(), parseWithTable(t, tt.src), lint.Config{}, lint.Env{}, lint.NondeterministicCTE)
			if diff := cmp.Diff(want, got); diff != "" {
				t.Fatalf("diff (-want +got):\n%s", diff)
			}
		})
	}
}

// occurrenceRange returns the range of the n-th occurrence of sub in src.
func occurrenceRange(t *testing.T, src, sub string, n int) lsp.Range {
	t.Helper()
	start := -1
	for i := 0; i <= n; i++ {
		next := strings.Index(src[start+1:], sub)
		if next < 0 {
			t.Fatalf("occurrence %d of %q not found", n, sub)
		}
		start += next + 1
	}
	line := strings.Count(src[:start], "\n")
	col := start - (strings.LastIndex(src[:start], "\n") + 1)
	return rng(line, col, line, col+len(sub))
}
