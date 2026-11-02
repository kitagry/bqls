package lint_test

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/kitagry/bqls/langserver/internal/lint"
	"github.com/kitagry/bqls/langserver/internal/lsp"
)

const (
	inNullMsg       = "IN with only NULL values always yields NULL"
	notInNullMsg    = "NOT IN with only NULL values always yields NULL"
	notInSomeNullMs = "NOT IN with a NULL value never yields TRUE"
)

func inNull(r lsp.Range, msg string, fixes ...lint.SuggestedFix) lint.Diagnostic {
	return lint.Diagnostic{Rule: "in-null", Range: r, Message: msg, Severity: lint.SeverityWarning, SuggestedFixes: fixes}
}

func replaceWith(replacement string, r lsp.Range, newText string) lint.SuggestedFix {
	return lint.SuggestedFix{Title: "Replace with " + replacement, Edits: []lsp.TextEdit{{Range: r, NewText: newText}}}
}

func removeNull(edits ...lsp.TextEdit) lint.SuggestedFix {
	return lint.SuggestedFix{Title: "Remove NULL from the NOT IN list", Edits: edits}
}

func TestInNull(t *testing.T) {
	tests := map[string]struct {
		src  string
		want []lint.Diagnostic
		// fixed is the source after applying every fix; empty means no fix.
		fixed string
	}{
		"IN with a single NULL": {
			src:   "SELECT x FROM UNNEST([1]) AS x WHERE x IN (NULL)",
			want:  []lint.Diagnostic{inNull(rng(0, 37, 0, 48), inNullMsg, replaceWith("IS NULL", rng(0, 37, 0, 48), "x IS NULL"))},
			fixed: "SELECT x FROM UNNEST([1]) AS x WHERE x IS NULL",
		},
		"NOT IN with a single NULL": {
			src:   "SELECT x FROM UNNEST([1]) AS x WHERE x NOT IN (NULL)",
			want:  []lint.Diagnostic{inNull(rng(0, 37, 0, 52), notInNullMsg, replaceWith("IS NOT NULL", rng(0, 37, 0, 52), "x IS NOT NULL"))},
			fixed: "SELECT x FROM UNNEST([1]) AS x WHERE x IS NOT NULL",
		},
		"NOT IN on a later line": {
			src:   "SELECT x FROM UNNEST([1]) AS x\nWHERE x NOT IN (NULL)",
			want:  []lint.Diagnostic{inNull(rng(1, 6, 1, 21), notInNullMsg, replaceWith("IS NOT NULL", rng(1, 6, 1, 21), "x IS NOT NULL"))},
			fixed: "SELECT x FROM UNNEST([1]) AS x\nWHERE x IS NOT NULL",
		},
		"IN with several NULLs": {
			src:   "SELECT x FROM UNNEST([1]) AS x WHERE x IN (NULL, NULL)",
			want:  []lint.Diagnostic{inNull(rng(0, 37, 0, 54), inNullMsg, replaceWith("IS NULL", rng(0, 37, 0, 54), "x IS NULL"))},
			fixed: "SELECT x FROM UNNEST([1]) AS x WHERE x IS NULL",
		},
		"IN with a typed NULL": {
			src:   "SELECT x FROM UNNEST([1]) AS x WHERE x IN (CAST(NULL AS INT64))",
			want:  []lint.Diagnostic{inNull(rng(0, 37, 0, 63), inNullMsg, replaceWith("IS NULL", rng(0, 37, 0, 63), "x IS NULL"))},
			fixed: "SELECT x FROM UNNEST([1]) AS x WHERE x IS NULL",
		},
		"IN with a SAFE_CAST NULL": {
			src:   "SELECT x FROM UNNEST([1]) AS x WHERE x IN (SAFE_CAST(NULL AS INT64))",
			want:  []lint.Diagnostic{inNull(rng(0, 37, 0, 68), inNullMsg, replaceWith("IS NULL", rng(0, 37, 0, 68), "x IS NULL"))},
			fixed: "SELECT x FROM UNNEST([1]) AS x WHERE x IS NULL",
		},
		"IN with a parenthesized NULL": {
			src:   "SELECT x FROM UNNEST([1]) AS x WHERE x IN ((NULL))",
			want:  []lint.Diagnostic{inNull(rng(0, 37, 0, 50), inNullMsg, replaceWith("IS NULL", rng(0, 37, 0, 50), "x IS NULL"))},
			fixed: "SELECT x FROM UNNEST([1]) AS x WHERE x IS NULL",
		},
		// Locations of the tested value leave out the parentheses around it.
		"IN on a parenthesized expression": {
			src:   "SELECT x FROM UNNEST([1]) AS x WHERE ((x + 1)) IN (NULL)",
			want:  []lint.Diagnostic{inNull(rng(0, 37, 0, 56), inNullMsg, replaceWith("IS NULL", rng(0, 37, 0, 56), "((x + 1)) IS NULL"))},
			fixed: "SELECT x FROM UNNEST([1]) AS x WHERE ((x + 1)) IS NULL",
		},
		"IN with a select list value": {
			src:   "SELECT x IN (NULL) AS b FROM UNNEST([1]) AS x",
			want:  []lint.Diagnostic{inNull(rng(0, 7, 0, 18), inNullMsg, replaceWith("IS NULL", rng(0, 7, 0, 18), "x IS NULL"))},
			fixed: "SELECT x IS NULL AS b FROM UNNEST([1]) AS x",
		},
		// The user wrote NOT around an IN, so the IN itself is what to point at.
		"NOT around a parenthesized IN": {
			src:   "SELECT x FROM UNNEST([1]) AS x WHERE NOT (x IN (NULL))",
			want:  []lint.Diagnostic{inNull(rng(0, 42, 0, 53), inNullMsg, replaceWith("IS NULL", rng(0, 42, 0, 53), "x IS NULL"))},
			fixed: "SELECT x FROM UNNEST([1]) AS x WHERE NOT (x IS NULL)",
		},
		// Rewriting would have to move or drop the comment.
		"NOT IN split by a comment across lines": {
			src:  "SELECT x FROM UNNEST([1]) AS x WHERE x NOT -- c\n IN (NULL)",
			want: []lint.Diagnostic{inNull(rng(0, 37, 1, 10), notInNullMsg)},
		},
		"IN with a comment in the list": {
			src:  "SELECT x FROM UNNEST([1]) AS x WHERE x IN (NULL /* c */)",
			want: []lint.Diagnostic{inNull(rng(0, 37, 0, 56), inNullMsg)},
		},
		// NOT binds looser than IN, so this is NOT (x IN (NULL)).
		"NOT before an unparenthesized IN": {
			src:   "SELECT x FROM UNNEST([1]) AS x WHERE NOT x IN (NULL)",
			want:  []lint.Diagnostic{inNull(rng(0, 41, 0, 52), inNullMsg, replaceWith("IS NULL", rng(0, 41, 0, 52), "x IS NULL"))},
			fixed: "SELECT x FROM UNNEST([1]) AS x WHERE NOT x IS NULL",
		},
		"after non-ASCII text on the same line": {
			src:  "SELECT 'あいう' AS s, x FROM UNNEST([1]) AS x WHERE x IN (NULL)",
			want: []lint.Diagnostic{inNull(rng(0, 55, 0, 66), inNullMsg, replaceWith("IS NULL", rng(0, 55, 0, 66), "x IS NULL"))},
		},

		// A NULL in a NOT IN list makes it NULL or FALSE, never TRUE.
		"NOT IN with a NULL after a value": {
			src: "SELECT x FROM UNNEST([1]) AS x WHERE x NOT IN (1, NULL)",
			want: []lint.Diagnostic{inNull(rng(0, 37, 0, 55), notInSomeNullMs,
				removeNull(lsp.TextEdit{Range: rng(0, 48, 0, 54), NewText: ""}))},
			fixed: "SELECT x FROM UNNEST([1]) AS x WHERE x NOT IN (1)",
		},
		"NOT IN with leading and parenthesized NULLs": {
			src: "SELECT x FROM UNNEST([1]) AS x WHERE x NOT IN (NULL, CAST(NULL AS INT64), 1, (NULL), 2)",
			want: []lint.Diagnostic{inNull(rng(0, 37, 0, 87), notInSomeNullMs,
				removeNull(
					lsp.TextEdit{Range: rng(0, 47, 0, 53), NewText: ""},
					lsp.TextEdit{Range: rng(0, 53, 0, 74), NewText: ""},
					lsp.TextEdit{Range: rng(0, 75, 0, 83), NewText: ""},
				))},
			fixed: "SELECT x FROM UNNEST([1]) AS x WHERE x NOT IN (1, 2)",
		},
		"NOT IN with a multiline list": {
			src: "SELECT x FROM UNNEST([1]) AS x\nWHERE x NOT IN (\n  1,\n  NULL\n)",
			want: []lint.Diagnostic{inNull(rng(1, 6, 4, 1), notInSomeNullMs,
				removeNull(lsp.TextEdit{Range: rng(2, 3, 3, 6), NewText: ""}))},
			fixed: "SELECT x FROM UNNEST([1]) AS x\nWHERE x NOT IN (\n  1\n)",
		},
		"NOT around an IN with a NULL": {
			src: "SELECT x FROM UNNEST([1]) AS x WHERE NOT (x IN (1, NULL))",
			want: []lint.Diagnostic{inNull(rng(0, 37, 0, 57), notInSomeNullMs,
				removeNull(lsp.TextEdit{Range: rng(0, 49, 0, 55), NewText: ""}))},
			fixed: "SELECT x FROM UNNEST([1]) AS x WHERE NOT (x IN (1))",
		},
		"NOT IN with a comment next to the NULL": {
			src:  "SELECT x FROM UNNEST([1]) AS x WHERE x NOT IN (1, /* c */ NULL)",
			want: []lint.Diagnostic{inNull(rng(0, 37, 0, 63), notInSomeNullMs)},
		},
		// A NULL never matches, but in a filter `x IN (1, NULL)` acts as `x IN (1)`.
		"IN with NULL and a non-NULL value is fine": {
			src:  "SELECT x FROM UNNEST([1]) AS x WHERE x IN (1, NULL)",
			want: nil,
		},
		"IN with non-NULL values is fine": {
			src:  "SELECT x FROM UNNEST([1]) AS x WHERE x NOT IN (1, 2)",
			want: nil,
		},
		// Declared variables are analyzed as dummy literals, which must not be
		// mistaken for a NULL literal.
		"IN with a declared variable is fine": {
			src:  "DECLARE v INT64;\nSELECT x FROM UNNEST([1]) AS x WHERE x IN (v)",
			want: nil,
		},
		"NOT IN with a declared variable is fine": {
			src:  "DECLARE v INT64;\nSELECT x FROM UNNEST([1]) AS x WHERE x NOT IN (1, v)",
			want: nil,
		},
		"IN UNNEST is out of scope": {
			src:  "SELECT x FROM UNNEST([1]) AS x WHERE x IN UNNEST([CAST(NULL AS INT64)])",
			want: nil,
		},
		"IN subquery is out of scope": {
			src:  "SELECT x FROM UNNEST([1]) AS x WHERE x IN (SELECT CAST(NULL AS INT64))",
			want: nil,
		},
		"suppressed by comment": {
			src:  "SELECT x FROM UNNEST([1]) AS x\n-- bqls:ignore in-null\nWHERE x NOT IN (NULL)",
			want: nil,
		},
	}

	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			parsed := parse(t, tt.src)
			got := lint.Run(t.Context(), parsed, lint.Config{}, lint.Env{}, lint.InNull)
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("diff (-want +got):\n%s", diff)
			}
			if tt.fixed == "" {
				return
			}
			fixed := applyFixes(t, tt.src, got)
			if fixed != tt.fixed {
				t.Errorf("fixed = %q, want %q", fixed, tt.fixed)
			}
			parse(t, fixed)
		})
	}
}
