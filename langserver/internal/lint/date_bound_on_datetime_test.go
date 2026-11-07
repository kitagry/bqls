package lint_test

import (
	"testing"

	"github.com/kitagry/bqls/langserver/internal/lint"
)

func dateBoundOnDatetimeMessage(typ, kind string) string {
	msg := "this " + typ + " is compared with a date, which stands for midnight at the start of that day, so "
	switch kind {
	case "upper":
		return msg + "the rest of that day is left out; compare with < the next day instead"
	case "after":
		return msg + "the rest of that day is kept; compare with >= the next day instead"
	}
	return msg + "only midnight matches; compare the date part instead, e.g. DATE(x) = ..."
}

func TestDateBoundOnDatetime(t *testing.T) {
	type report struct {
		// text is the exact reported source text.
		text, typ, kind string
		// fixed is src after applying the fix; empty when there is none.
		fixed string
	}
	const ts = " FROM p.d.t, UNNEST([CURRENT_TIMESTAMP()]) AS ts"
	tests := map[string]struct {
		src  string
		want []report
	}{
		"DATETIME <= DATE literal": {
			src:  "SELECT 1 FROM p.d.t WHERE dt <= DATE '2024-01-31'",
			want: []report{{"dt <= DATE '2024-01-31'", "DATETIME", "upper", "SELECT 1 FROM p.d.t WHERE dt < DATE '2024-02-01'"}},
		},
		"DATETIME <= date string": {
			src:  "SELECT 1 FROM p.d.t WHERE dt <= '2024-12-31'",
			want: []report{{"dt <= '2024-12-31'", "DATETIME", "upper", "SELECT 1 FROM p.d.t WHERE dt < '2025-01-01'"}},
		},
		"TIMESTAMP <= date string": {
			src:  "SELECT 1" + ts + " WHERE ts <= \"2024-02-28\"",
			want: []report{{"ts <= \"2024-02-28\"", "TIMESTAMP", "upper", "SELECT 1" + ts + " WHERE ts < \"2024-02-29\""}},
		},
		"bound on the left": {
			src:  "SELECT 1 FROM p.d.t WHERE DATE '2024-01-31' >= dt",
			want: []report{{"DATE '2024-01-31' >= dt", "DATETIME", "upper", "SELECT 1 FROM p.d.t WHERE DATE '2024-02-01' > dt"}},
		},
		"greater than a date": {
			src:  "SELECT 1 FROM p.d.t WHERE dt > DATE '2024-01-31'",
			want: []report{{"dt > DATE '2024-01-31'", "DATETIME", "after", "SELECT 1 FROM p.d.t WHERE dt >= DATE '2024-02-01'"}},
		},
		"less than on the left": {
			src:  "SELECT 1 FROM p.d.t WHERE '2024-01-31' < dt",
			want: []report{{"'2024-01-31' < dt", "DATETIME", "after", "SELECT 1 FROM p.d.t WHERE '2024-02-01' <= dt"}},
		},
		"BETWEEN upper bound": {
			src:  "SELECT 1" + ts + " WHERE ts BETWEEN '2024-01-01' AND '2024-01-31'",
			want: []report{{"ts BETWEEN '2024-01-01' AND '2024-01-31'", "TIMESTAMP", "upper", ""}},
		},
		"equal": {
			src:  "SELECT 1 FROM p.d.t WHERE dt = DATE '2024-01-31'",
			want: []report{{"dt = DATE '2024-01-31'", "DATETIME", "equal", ""}},
		},
		"DATE column": {
			src:  "SELECT 1 FROM p.d.t WHERE dt <= d",
			want: []report{{"dt <= d", "DATETIME", "upper", ""}},
		},
		"CURRENT_DATE": {
			src:  "SELECT 1 FROM p.d.t WHERE dt <= CURRENT_DATE()",
			want: []report{{"dt <= CURRENT_DATE()", "DATETIME", "upper", ""}},
		},
		"lower bounds": {
			src: "SELECT 1 FROM p.d.t WHERE dt >= DATE '2024-01-01' AND dt < DATE '2024-02-01' AND '2024-01-01' <= dt AND DATE '2024-02-01' > dt",
		},
		"BETWEEN lower bound only": {
			src: "SELECT 1 FROM p.d.t WHERE dt BETWEEN DATE '2024-01-01' AND DATETIME '2024-01-31 23:59:59'",
		},
		"time of day written": {
			src: "SELECT 1 FROM p.d.t WHERE dt <= '2024-01-31 23:59:59' OR dt <= DATETIME '2024-01-31 00:00:00'",
		},
		"DATE compared with DATE": {
			src: "SELECT 1 FROM p.d.t WHERE d <= DATE '2024-01-31'",
		},
		"suppressed by comment": {
			src: "SELECT 1 FROM p.d.t\n-- bqls:ignore date-bound-on-datetime\nWHERE dt <= DATE '2024-01-31'",
		},
	}

	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			got := lint.Run(t.Context(), parseTyped(t, tt.src), lint.Config{}, lint.Env{}, lint.DateBoundOnDatetime)
			if len(got) != len(tt.want) {
				t.Fatalf("got %d diagnostics, want %d: %+v", len(got), len(tt.want), got)
			}
			for i, w := range tt.want {
				d := got[i]
				if text := textInRange(tt.src, d); text != w.text {
					t.Errorf("reported text = %q, want %q", text, w.text)
				}
				if want := dateBoundOnDatetimeMessage(w.typ, w.kind); d.Rule != "date-bound-on-datetime" || d.Severity != lint.SeverityWarning || d.Message != want {
					t.Errorf("diagnostic = %+v, want warning %q", d, want)
				}
				if w.fixed == "" {
					if len(d.SuggestedFixes) != 0 {
						t.Errorf("got fixes %+v, want none", d.SuggestedFixes)
					}
					continue
				}
				// The fix changes which rows match, to the ones meant.
				if len(d.SuggestedFixes) != 1 || !d.SuggestedFixes[0].ChangesResult {
					t.Fatalf("fixes = %+v, want one that changes the result", d.SuggestedFixes)
				}
				fixed := applyFixes(t, tt.src, []lint.Diagnostic{d})
				if fixed != w.fixed {
					t.Errorf("fixed = %q, want %q", fixed, w.fixed)
				}
				if after := lint.Run(t.Context(), parseTyped(t, fixed), lint.Config{}, lint.Env{}, lint.DateBoundOnDatetime); len(after) != 0 {
					t.Errorf("diagnostics after fix: %+v", after)
				}
			}
		})
	}
}
