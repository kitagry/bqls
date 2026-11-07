package lint_test

import (
	"testing"

	"github.com/kitagry/bqls/langserver/internal/lint"
)

func setOperationImplicitCastMessage(from string) string {
	return "this column is " + from + " here but FLOAT64 in another input of the set operation, so it is implicitly cast to FLOAT64, which cannot represent every " + from + " value exactly (e.g. INT64 values above 2^53); give the inputs the same type, or write an explicit CAST"
}

func TestSetOperationImplicitCast(t *testing.T) {
	type report struct {
		// text is the exact text of the reported range.
		text, from string
	}
	tests := map[string]struct {
		src  string
		want []report
	}{
		"int64 widened by float64": {
			src:  "SELECT i FROM p.d.t UNION ALL SELECT f FROM p.d.t",
			want: []report{{"i", "INT64"}},
		},
		"second input widened": {
			src:  "SELECT f AS x FROM p.d.t UNION ALL SELECT i AS x FROM p.d.t",
			want: []report{{"i AS x", "INT64"}},
		},
		"numeric widened": {
			src:  "SELECT s, n FROM p.d.t UNION DISTINCT SELECT s, f FROM p.d.t",
			want: []report{{"n", "NUMERIC"}},
		},
		"three inputs": {
			src:  "SELECT i FROM p.d.t UNION ALL SELECT n FROM p.d.t UNION ALL SELECT f FROM p.d.t",
			want: []report{{"i", "INT64"}, {"n", "NUMERIC"}},
		},
		"computed value": {
			src:  "SELECT i + 1 FROM p.d.t EXCEPT DISTINCT SELECT f FROM p.d.t",
			want: []report{{"i + 1", "INT64"}},
		},
		"ORDER BY and LIMIT": {
			src:  "SELECT i FROM p.d.t UNION ALL SELECT f FROM p.d.t ORDER BY 1 LIMIT 10",
			want: []report{{"i", "INT64"}},
		},
		"after a WITH clause": {
			src:  "WITH c AS (SELECT n AS x FROM p.d.t UNION ALL SELECT n FROM p.d.t UNION ALL SELECT n FROM p.d.t)\nSELECT x FROM c UNION ALL SELECT f FROM p.d.t",
			want: []report{{"x", "NUMERIC"}},
		},
		"CORRESPONDING": {
			src:  "SELECT s, f AS i FROM p.d.t UNION ALL CORRESPONDING SELECT n, i, s FROM p.d.t",
			want: []report{{"i", "INT64"}},
		},
		// The NUMERIC column comes from both inputs, not from i alone.
		"nested set operation": {
			src:  "(SELECT i FROM p.d.t UNION ALL SELECT n FROM p.d.t) UNION DISTINCT SELECT f FROM p.d.t",
			want: []report{{"SELECT i FROM p.d.t UNION ALL SELECT n FROM p.d.t", "NUMERIC"}},
		},
		"columns not written out": {
			src:  "SELECT * FROM (SELECT i, i AS j FROM p.d.t) UNION ALL SELECT f, f FROM p.d.t",
			want: []report{{"SELECT * FROM (SELECT i, i AS j FROM p.d.t)", "INT64"}},
		},
		"in a CTE": {
			src:  "WITH u AS (\n  SELECT i AS x FROM p.d.t\n  UNION ALL\n  SELECT f AS x FROM p.d.t\n)\nSELECT x FROM u",
			want: []report{{"i AS x", "INT64"}},
		},
		// INT64 to NUMERIC is exact.
		"int64 widened by numeric": {
			src: "SELECT i FROM p.d.t UNION ALL SELECT n FROM p.d.t",
		},
		// A literal is usually a placeholder such as 0.
		"literal": {
			src: "SELECT 0 FROM p.d.t UNION ALL SELECT f FROM p.d.t",
		},
		"explicit CAST": {
			src: "SELECT CAST(i AS FLOAT64) FROM p.d.t UNION ALL SELECT f FROM p.d.t",
		},
		"same type": {
			src: "SELECT i FROM p.d.t UNION ALL SELECT i FROM p.d.t",
		},
		"suppressed by comment": {
			src: "SELECT\n-- bqls:ignore set-operation-implicit-cast\ni FROM p.d.t UNION ALL SELECT f FROM p.d.t",
		},
	}

	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			got := lint.Run(t.Context(), parseTyped(t, tt.src), lint.Config{}, lint.Env{}, lint.SetOperationImplicitCast)
			if len(got) != len(tt.want) {
				t.Fatalf("got %d diagnostics, want %d: %+v", len(got), len(tt.want), got)
			}
			for i, w := range tt.want {
				d := got[i]
				if text := textInRange(tt.src, d); text != w.text {
					t.Errorf("reported text = %q, want %q", text, w.text)
				}
				if want := setOperationImplicitCastMessage(w.from); d.Rule != "set-operation-implicit-cast" || d.Severity != lint.SeverityWarning || d.Message != want || len(d.SuggestedFixes) != 0 {
					t.Errorf("diagnostic = %+v, want warning %q without a fix", d, want)
				}
			}
		})
	}
}
