package lint_test

import (
	"testing"

	"github.com/kitagry/bqls/langserver/internal/lint"
)

func implicitOuterReferenceMessage(name string) string {
	return "`" + name + "` refers to a column of the outer query, since no table of this subquery has it; qualify it with the outer table if that is intended, otherwise the subquery may compare the outer row with itself (e.g. `x IN (SELECT x FROM t)` is TRUE for every non-NULL x)"
}

func TestImplicitOuterReference(t *testing.T) {
	type report struct {
		// text is the exact reported source text.
		text string
		// fixed is src after applying the fix; empty when there is none.
		fixed string
	}
	tests := map[string]struct {
		src  string
		want []report
	}{
		"IN subquery selecting an outer column": {
			src:  "SELECT * FROM p.d.users WHERE id IN (SELECT id FROM p.d.t)",
			want: []report{{"id", "SELECT * FROM p.d.users WHERE id IN (SELECT users.id FROM p.d.t)"}},
		},
		"EXISTS with an alias": {
			src:  "SELECT * FROM p.d.users AS u WHERE EXISTS (SELECT 1 FROM p.d.t WHERE t.a = id)",
			want: []report{{"id", "SELECT * FROM p.d.users AS u WHERE EXISTS (SELECT 1 FROM p.d.t WHERE t.a = u.id)"}},
		},
		"scalar subquery": {
			src:  "SELECT (SELECT MAX(a) FROM p.d.t WHERE b = id) AS m FROM p.d.users AS u",
			want: []report{{"id", "SELECT (SELECT MAX(a) FROM p.d.t WHERE b = u.id) AS m FROM p.d.users AS u"}},
		},
		"quoted name": {
			src:  "SELECT * FROM p.d.users AS u WHERE EXISTS (SELECT 1 FROM p.d.t WHERE a = `id`)",
			want: []report{{"`id`", "SELECT * FROM p.d.users AS u WHERE EXISTS (SELECT 1 FROM p.d.t WHERE a = u.`id`)"}},
		},
		"nested subquery": {
			src:  "SELECT * FROM p.d.users AS u WHERE EXISTS (SELECT 1 FROM p.d.t WHERE a IN (SELECT a FROM p.d.s WHERE b = id))",
			want: []report{{"id", "SELECT * FROM p.d.users AS u WHERE EXISTS (SELECT 1 FROM p.d.t WHERE a IN (SELECT a FROM p.d.s WHERE b = u.id))"}},
		},
		// t.id would refer to the subquery's own t.
		"outer alias taken inside": {
			src:  "SELECT * FROM p.d.users AS t WHERE EXISTS (SELECT 1 FROM p.d.t WHERE a = id)",
			want: []report{{"id", ""}},
		},
		"outer column from a CTE": {
			src:  "WITH c AS (SELECT id FROM p.d.users) SELECT * FROM c WHERE EXISTS (SELECT 1 FROM p.d.t WHERE a = id)",
			want: []report{{"id", ""}},
		},
		"qualified": {
			src: "SELECT * FROM p.d.users AS u WHERE EXISTS (SELECT 1 FROM p.d.t WHERE t.a = u.id)",
		},
		"inner column": {
			src: "SELECT * FROM p.d.t WHERE a IN (SELECT a FROM p.d.s)",
		},
		"suppressed by comment": {
			src: "SELECT * FROM p.d.users\n-- bqls:ignore implicit-outer-reference\nWHERE id IN (SELECT id FROM p.d.t)",
		},
	}

	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			got := lint.Run(t.Context(), parseInsertTables(t, tt.src), lint.Config{}, lint.Env{}, lint.ImplicitOuterReference)
			if len(got) != len(tt.want) {
				t.Fatalf("got %d diagnostics, want %d: %+v", len(got), len(tt.want), got)
			}
			for i, w := range tt.want {
				d := got[i]
				if text := textInRange(tt.src, d); text != w.text {
					t.Errorf("reported text = %q, want %q", text, w.text)
				}
				if want := implicitOuterReferenceMessage("id"); d.Rule != "implicit-outer-reference" || d.Severity != lint.SeverityWarning || d.Message != want {
					t.Errorf("diagnostic = %+v, want warning %q", d, want)
				}
				if w.fixed == "" {
					if len(d.SuggestedFixes) != 0 {
						t.Errorf("got fixes %+v, want none", d.SuggestedFixes)
					}
					continue
				}
				// Qualifying keeps the meaning.
				if len(d.SuggestedFixes) != 1 || d.SuggestedFixes[0].ChangesResult {
					t.Fatalf("fixes = %+v, want one that keeps the result", d.SuggestedFixes)
				}
				fixed := applyFixes(t, tt.src, []lint.Diagnostic{d})
				if fixed != w.fixed {
					t.Errorf("fixed = %q, want %q", fixed, w.fixed)
				}
				if after := lint.Run(t.Context(), parseInsertTables(t, fixed), lint.Config{}, lint.Env{}, lint.ImplicitOuterReference); len(after) != 0 {
					t.Errorf("diagnostics after fix: %+v", after)
				}
			}
		})
	}
}
