package lint_test

import (
	"testing"

	"github.com/kitagry/bqls/langserver/internal/lint"
)

const (
	limitWithoutOrderByMessage  = "LIMIT without ORDER BY keeps an arbitrary set of rows, which can differ between runs; add ORDER BY to decide which rows are kept"
	offsetWithoutOrderByMessage = "LIMIT ... OFFSET without ORDER BY skips an arbitrary set of rows, so pages can overlap or miss rows between runs; add ORDER BY to page through a fixed order"
)

func TestLimitWithoutOrderBy(t *testing.T) {
	type report struct {
		// text is the exact reported source text.
		text    string
		message string
	}
	tests := map[string]struct {
		src  string
		want []report
	}{
		"derived table": {
			src:  "SELECT * FROM (SELECT id FROM p.d.t LIMIT 3)",
			want: []report{{"LIMIT 3", limitWithoutOrderByMessage}},
		},
		"CTE": {
			src:  "WITH c AS (\n  SELECT id FROM p.d.t LIMIT 10\n)\nSELECT * FROM c",
			want: []report{{"LIMIT 10", limitWithoutOrderByMessage}},
		},
		"scalar subquery": {
			src:  "SELECT (SELECT v FROM p.d.t LIMIT 1) AS v",
			want: []report{{"LIMIT 1", limitWithoutOrderByMessage}},
		},
		"IN subquery": {
			src:  "SELECT id FROM p.d.t WHERE id IN (SELECT v FROM p.d.t LIMIT 100)",
			want: []report{{"LIMIT 100", limitWithoutOrderByMessage}},
		},
		"ARRAY subquery": {
			src:  "SELECT ARRAY(SELECT v FROM p.d.t LIMIT 5) AS vs",
			want: []report{{"LIMIT 5", limitWithoutOrderByMessage}},
		},
		"INSERT SELECT": {
			src:  "INSERT INTO p.d.t (id, ts, v) SELECT id, ts, v FROM p.d.t LIMIT 10",
			want: []report{{"LIMIT 10", limitWithoutOrderByMessage}},
		},
		"CREATE TABLE AS SELECT": {
			src:  "CREATE TABLE p.d.sample AS SELECT * FROM p.d.t LIMIT 1000",
			want: []report{{"LIMIT 1000", limitWithoutOrderByMessage}},
		},
		"top level with OFFSET": {
			src:  "SELECT id FROM p.d.t LIMIT 10 OFFSET 20",
			want: []report{{"LIMIT 10 OFFSET 20", offsetWithoutOrderByMessage}},
		},
		"pipe LIMIT in a subquery": {
			src:  "SELECT * FROM (FROM p.d.t |> LIMIT 3)",
			want: []report{{"LIMIT 3", limitWithoutOrderByMessage}},
		},
		"UNION ALL operand": {
			src:  "SELECT * FROM ((SELECT id FROM p.d.t LIMIT 1) UNION ALL (SELECT id FROM p.d.t ORDER BY ts LIMIT 1))",
			want: []report{{"LIMIT 1", limitWithoutOrderByMessage}},
		},
		// A preview of the result.
		"top level": {
			src: "SELECT id FROM p.d.t LIMIT 10",
		},
		"top level with WITH": {
			src: "WITH c AS (SELECT id FROM p.d.t) SELECT id FROM c LIMIT 10",
		},
		"top level pipe": {
			src: "FROM p.d.t |> LIMIT 10",
		},
		"ORDER BY": {
			src: "SELECT * FROM (SELECT id FROM p.d.t ORDER BY ts LIMIT 3 OFFSET 1)",
		},
		"pipe ORDER BY before WHERE": {
			src: "SELECT * FROM (FROM p.d.t |> ORDER BY ts |> WHERE id > 1 |> LIMIT 3)",
		},
		"LIMIT 0": {
			src: "SELECT * FROM (SELECT id FROM p.d.t LIMIT 0)",
		},
		// Whether a row exists does not depend on which rows are kept.
		"EXISTS": {
			src: "SELECT id FROM p.d.t AS a WHERE EXISTS (SELECT 1 FROM p.d.t AS b WHERE b.v = a.id LIMIT 1)",
		},
		"at most one row": {
			src: "SELECT (SELECT MAX(v) FROM p.d.t LIMIT 1) AS v",
		},
		"unreferenced CTE": {
			src: "WITH c AS (SELECT id FROM p.d.t LIMIT 3) SELECT 1",
		},
		"suppressed by comment": {
			src: "SELECT * FROM (\n  SELECT id FROM p.d.t\n  -- bqls:ignore limit-without-order-by\n  LIMIT 3\n)",
		},
	}

	cfg := lint.Config{Rules: map[string]bool{"limit-without-order-by": true}}
	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			got := lint.Run(t.Context(), parseEvents(t, tt.src), cfg, lint.Env{}, lint.LimitWithoutOrderBy)
			if len(got) != len(tt.want) {
				t.Fatalf("got %d diagnostics, want %d: %+v", len(got), len(tt.want), got)
			}
			for i, w := range tt.want {
				d := got[i]
				if text := textInRange(tt.src, d); text != w.text {
					t.Errorf("reported text = %q, want %q", text, w.text)
				}
				if d.Rule != "limit-without-order-by" || d.Severity != lint.SeverityWarning || d.Message != w.message || len(d.SuggestedFixes) != 0 {
					t.Errorf("diagnostic = %+v, want rule limit-without-order-by, warning, message %q, no fix", d, w.message)
				}
			}
		})
	}
}

func TestLimitWithoutOrderBy_OffByDefault(t *testing.T) {
	src := "SELECT * FROM (SELECT id FROM p.d.t LIMIT 3)"
	if got := lint.Run(t.Context(), parseEvents(t, src), lint.Config{}, lint.Env{}, lint.LimitWithoutOrderBy); len(got) != 0 {
		t.Errorf("got %+v, want nothing while the rule is off by default", got)
	}
}
