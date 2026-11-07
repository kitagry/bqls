package lint_test

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/kitagry/bqls/langserver/internal/lint"
)

const castDivisionRoundsMessage = "CAST to INT64 rounds the FLOAT64 result of an integer division to the nearest integer, halfway cases away from zero (CAST(7 / 2 AS INT64) is 4); use DIV for integer division that truncates toward zero, or ROUND / FLOOR / CEIL to state the rounding"

func TestCastDivisionRounds(t *testing.T) {
	type report struct {
		// text is the exact reported source text.
		text string
		// fixed is the text the fix writes in its place.
		fixed string
	}
	tests := map[string]struct {
		src  string
		want []report
	}{
		"column divided by an integer literal": {
			src:  "SELECT CAST(i / 2 AS INT64) FROM d.t",
			want: []report{{"CAST(i / 2 AS INT64)", "DIV(i, 2)"}},
		},
		"integer literals": {
			src:  "SELECT CAST(7 / 2 AS INT64)",
			want: []report{{"CAST(7 / 2 AS INT64)", "DIV(7, 2)"}},
		},
		"computed integer operands": {
			src:  "SELECT CAST((i + 1) / (i - 1) AS INT64) FROM d.t",
			want: []report{{"CAST((i + 1) / (i - 1) AS INT64)", "DIV(i + 1, i - 1)"}},
		},
		"negative literal": {
			src:  "SELECT CAST(i / -2 AS INT64) FROM d.t",
			want: []report{{"CAST(i / -2 AS INT64)", "DIV(i, -2)"}},
		},
		"parenthesized division": {
			src:  "SELECT CAST((i / 2) AS INT64) FROM d.t",
			want: []report{{"CAST((i / 2) AS INT64)", "DIV(i, 2)"}},
		},
		"SAFE_CAST": {
			src:  "SELECT SAFE_CAST(i / 2 AS INT64) FROM d.t",
			want: []report{{"SAFE_CAST(i / 2 AS INT64)", "DIV(i, 2)"}},
		},
		"SAFE_DIVIDE": {
			src:  "SELECT CAST(SAFE_DIVIDE(i, 2) AS INT64) FROM d.t",
			want: []report{{"CAST(SAFE_DIVIDE(i, 2) AS INT64)", "SAFE.DIV(i, 2)"}},
		},
		"in a CTE on a later line": {
			src:  "WITH c AS (\n  SELECT CAST(i / 2 AS INT64) AS h FROM d.t\n)\nSELECT h FROM c",
			want: []report{{"CAST(i / 2 AS INT64)", "DIV(i, 2)"}},
		},
		"in WHERE": {
			src:  "SELECT i FROM d.t WHERE CAST(i / 2 AS INT64) = 3",
			want: []report{{"CAST(i / 2 AS INT64)", "DIV(i, 2)"}},
		},
		"FLOAT64 column": {
			src: "SELECT CAST(f / 2 AS INT64) FROM d.t",
		},
		"FLOAT64 literal": {
			src: "SELECT CAST(i / 2.0 AS INT64) FROM d.t",
		},
		"exponent literal": {
			src: "SELECT CAST(i / 1e2 AS INT64) FROM d.t",
		},
		// NUMERIC division is exact up to its scale, so the rounding is less
		// surprising and DIV is not an obvious replacement.
		"NUMERIC operand": {
			src: "SELECT CAST(n / 2 AS INT64) FROM d.t",
		},
		"rounding stated": {
			src: "SELECT CAST(FLOOR(i / 2) AS INT64), CAST(ROUND(i / 2) AS INT64) FROM d.t",
		},
		"cast to another type": {
			src: "SELECT CAST(i / 2 AS NUMERIC), CAST(i / 2 AS STRING) FROM d.t",
		},
		"DIV": {
			src: "SELECT DIV(i, 2) FROM d.t",
		},
		"IEEE_DIVIDE": {
			src: "SELECT CAST(IEEE_DIVIDE(i, 2) AS INT64) FROM d.t",
		},
		"suppressed by comment": {
			src: "SELECT\n-- bqls:ignore cast-division-rounds\nCAST(i / 2 AS INT64) FROM d.t",
		},
	}

	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			f := parseTyped(t, tt.src)
			got := lint.Run(t.Context(), f, lint.Config{}, lint.Env{}, lint.CastDivisionRounds)
			if len(got) != len(tt.want) {
				t.Fatalf("got %d diagnostics, want %d: %+v", len(got), len(tt.want), got)
			}
			for i, w := range tt.want {
				d := got[i]
				if text := textInRange(tt.src, d); text != w.text {
					t.Errorf("reported text = %q, want %q", text, w.text)
				}
				wantDiag := lint.Diagnostic{Rule: "cast-division-rounds", Message: castDivisionRoundsMessage, Severity: lint.SeverityWarning}
				if diff := cmp.Diff(wantDiag, d, cmpopts.IgnoreFields(lint.Diagnostic{}, "Range", "SuggestedFixes")); diff != "" {
					t.Errorf("diagnostic diff (-want +got):\n%s", diff)
				}
				if len(d.SuggestedFixes) != 1 {
					t.Fatalf("got %d fixes, want 1", len(d.SuggestedFixes))
				}
				fix := d.SuggestedFixes[0]
				if want := "Replace with " + w.fixed; fix.Title != want {
					t.Errorf("fix title = %q, want %q", fix.Title, want)
				}
				// DIV truncates where CAST rounds, so applying it is the
				// user's call.
				if !fix.ChangesResult {
					t.Error("fix.ChangesResult = false, want true")
				}
				if len(fix.Edits) != 1 || fix.Edits[0].Range != d.Range || fix.Edits[0].NewText != w.fixed {
					t.Errorf("fix edits = %+v, want %q over the reported range", fix.Edits, w.fixed)
				}
			}
			for _, d := range got {
				fixed := applyFixes(t, tt.src, []lint.Diagnostic{d})
				if after := lint.Run(t.Context(), parseTyped(t, fixed), lint.Config{}, lint.Env{}, lint.CastDivisionRounds); len(after) != 0 {
					t.Errorf("diagnostics after fix %q: %+v", fixed, after)
				}
			}
		})
	}
}
