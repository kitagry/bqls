package lint_test

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/kitagry/bqls/langserver/internal/lint"
)

const (
	lastValueDefaultFrameMessage = "LAST_VALUE has no window frame, so it uses the default RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW and returns the value of the current row (or of its last peer), not of the last row of the partition; add ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING to the window"
	nthValueDefaultFrameMessage  = "NTH_VALUE has no window frame, so it uses the default RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW and returns NULL for the rows before the Nth row of the partition; add ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING to the window"
)

func TestWindowDefaultFrame(t *testing.T) {
	diag := func(line, start int, fn, msg string) lint.Diagnostic {
		return lint.Diagnostic{
			Rule:     "window-default-frame",
			Range:    rng(line, start, line, start+len(fn)),
			Message:  msg,
			Severity: lint.SeverityWarning,
		}
	}
	const full = "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING"
	tests := map[string]struct {
		src  string
		want []lint.Diagnostic
		// wantFixed is src after applying every fix; it must analyze cleanly
		// and no longer be reported. Every reported case has a fix.
		wantFixed string
	}{
		"LAST_VALUE with ORDER BY and no frame": {
			src:       "SELECT LAST_VALUE(v) OVER (PARTITION BY id ORDER BY ts) FROM d.t",
			want:      []lint.Diagnostic{diag(0, 7, "LAST_VALUE", lastValueDefaultFrameMessage)},
			wantFixed: "SELECT LAST_VALUE(v) OVER (PARTITION BY id ORDER BY ts " + full + ") FROM d.t",
		},
		"LAST_VALUE without PARTITION BY": {
			src:       "SELECT LAST_VALUE(v) OVER (ORDER BY ts DESC) FROM d.t",
			want:      []lint.Diagnostic{diag(0, 7, "LAST_VALUE", lastValueDefaultFrameMessage)},
			wantFixed: "SELECT LAST_VALUE(v) OVER (ORDER BY ts DESC " + full + ") FROM d.t",
		},
		"LAST_VALUE RESPECT NULLS": {
			src:       "SELECT LAST_VALUE(v RESPECT NULLS) OVER (ORDER BY ts) FROM d.t",
			want:      []lint.Diagnostic{diag(0, 7, "LAST_VALUE", lastValueDefaultFrameMessage)},
			wantFixed: "SELECT LAST_VALUE(v RESPECT NULLS) OVER (ORDER BY ts " + full + ") FROM d.t",
		},
		"comment before the closing parenthesis is kept": {
			src:       "SELECT LAST_VALUE(v) OVER (\n  ORDER BY ts -- latest\n) FROM d.t",
			want:      []lint.Diagnostic{diag(0, 7, "LAST_VALUE", lastValueDefaultFrameMessage)},
			wantFixed: "SELECT LAST_VALUE(v) OVER (\n  ORDER BY ts " + full + " -- latest\n) FROM d.t",
		},
		"NTH_VALUE with n > 1": {
			src:       "SELECT NTH_VALUE(v, 2) OVER (PARTITION BY id ORDER BY ts) FROM d.t",
			want:      []lint.Diagnostic{diag(0, 7, "NTH_VALUE", nthValueDefaultFrameMessage)},
			wantFixed: "SELECT NTH_VALUE(v, 2) OVER (PARTITION BY id ORDER BY ts " + full + ") FROM d.t",
		},
		"bare named window": {
			src:       "SELECT LAST_VALUE(v) OVER w FROM d.t WINDOW w AS (PARTITION BY id ORDER BY ts)",
			want:      []lint.Diagnostic{diag(0, 7, "LAST_VALUE", lastValueDefaultFrameMessage)},
			wantFixed: "SELECT LAST_VALUE(v) OVER (w " + full + ") FROM d.t WINDOW w AS (PARTITION BY id ORDER BY ts)",
		},
		"parenthesized named window": {
			src:       "SELECT LAST_VALUE(v) OVER (w) FROM d.t WINDOW w AS (PARTITION BY id ORDER BY ts)",
			want:      []lint.Diagnostic{diag(0, 7, "LAST_VALUE", lastValueDefaultFrameMessage)},
			wantFixed: "SELECT LAST_VALUE(v) OVER (w " + full + ") FROM d.t WINDOW w AS (PARTITION BY id ORDER BY ts)",
		},
		"named window that adds ORDER BY": {
			src:       "SELECT LAST_VALUE(v) OVER (w ORDER BY ts) FROM d.t WINDOW w AS (PARTITION BY id)",
			want:      []lint.Diagnostic{diag(0, 7, "LAST_VALUE", lastValueDefaultFrameMessage)},
			wantFixed: "SELECT LAST_VALUE(v) OVER (w ORDER BY ts " + full + ") FROM d.t WINDOW w AS (PARTITION BY id)",
		},
		"inside a CTE on a later line": {
			src:       "WITH c AS (\n  SELECT id, LAST_VALUE(v) OVER (PARTITION BY id ORDER BY ts) AS last_v FROM d.t\n)\nSELECT * FROM c",
			want:      []lint.Diagnostic{diag(1, 13, "LAST_VALUE", lastValueDefaultFrameMessage)},
			wantFixed: "WITH c AS (\n  SELECT id, LAST_VALUE(v) OVER (PARTITION BY id ORDER BY ts " + full + ") AS last_v FROM d.t\n)\nSELECT * FROM c",
		},
		"in QUALIFY": {
			src:       "SELECT id FROM d.t WHERE TRUE QUALIFY v = LAST_VALUE(v) OVER (PARTITION BY id ORDER BY ts)",
			want:      []lint.Diagnostic{diag(0, 42, "LAST_VALUE", lastValueDefaultFrameMessage)},
			wantFixed: "SELECT id FROM d.t WHERE TRUE QUALIFY v = LAST_VALUE(v) OVER (PARTITION BY id ORDER BY ts " + full + ")",
		},
		"two calls": {
			src: "SELECT LAST_VALUE(v) OVER w, NTH_VALUE(v, 3) OVER w FROM d.t WINDOW w AS (ORDER BY ts)",
			want: []lint.Diagnostic{
				diag(0, 7, "LAST_VALUE", lastValueDefaultFrameMessage),
				diag(0, 29, "NTH_VALUE", nthValueDefaultFrameMessage),
			},
			wantFixed: "SELECT LAST_VALUE(v) OVER (w " + full + "), NTH_VALUE(v, 3) OVER (w " + full + ") FROM d.t WINDOW w AS (ORDER BY ts)",
		},
		"explicit ROWS frame": {
			src: "SELECT LAST_VALUE(v) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM d.t",
		},
		// Writing the default frame out states that it is meant.
		"explicit default RANGE frame": {
			src: "SELECT LAST_VALUE(v) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM d.t",
		},
		"frame from a named window": {
			src: "SELECT LAST_VALUE(v) OVER w FROM d.t WINDOW w AS (ORDER BY ts ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)",
		},
		// The default frame always starts at the start of the partition.
		"FIRST_VALUE": {
			src: "SELECT FIRST_VALUE(v) OVER (PARTITION BY id ORDER BY ts) FROM d.t",
		},
		"NTH_VALUE with n = 1": {
			src: "SELECT NTH_VALUE(v, 1) OVER (PARTITION BY id ORDER BY ts) FROM d.t",
		},
		// Forward fill: the last non-NULL value up to the current row.
		"LAST_VALUE IGNORE NULLS": {
			src: "SELECT LAST_VALUE(v IGNORE NULLS) OVER (PARTITION BY id ORDER BY ts) FROM d.t",
		},
		// Running totals are what the default frame is for.
		"running aggregate": {
			src: "SELECT SUM(v) OVER (PARTITION BY id ORDER BY ts), ARRAY_AGG(v) OVER (ORDER BY ts) FROM d.t",
		},
		// Every row is a peer of every other, so the frame is the partition.
		"ORDER BY only the PARTITION BY key": {
			src: "SELECT LAST_VALUE(v) OVER (PARTITION BY id ORDER BY id) FROM d.t",
		},
		"suppressed by comment": {
			src: "SELECT\n-- bqls:ignore window-default-frame\nLAST_VALUE(v) OVER (ORDER BY ts) FROM d.t",
		},
	}

	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			cfg := lint.Config{Rules: map[string]bool{"window-default-frame": true}}
			got := lint.Run(t.Context(), parseEvents(t, tt.src), cfg, lint.Env{}, lint.WindowDefaultFrame)
			if diff := cmp.Diff(tt.want, got, cmpopts.IgnoreFields(lint.Diagnostic{}, "SuggestedFixes")); diff != "" {
				t.Fatalf("diff (-want +got):\n%s", diff)
			}
			for _, d := range got {
				if len(d.SuggestedFixes) != 1 {
					t.Fatalf("got %d fixes, want 1: %+v", len(d.SuggestedFixes), d)
				}
			}
			if tt.wantFixed == "" {
				return
			}
			fixed := applyFixes(t, tt.src, got)
			if diff := cmp.Diff(tt.wantFixed, fixed); diff != "" {
				t.Fatalf("fixed source diff (-want +got):\n%s", diff)
			}
			if after := lint.Run(t.Context(), parseEvents(t, fixed), cfg, lint.Env{}, lint.WindowDefaultFrame); len(after) != 0 {
				t.Errorf("diagnostics after fix: %+v", after)
			}
		})
	}
}
