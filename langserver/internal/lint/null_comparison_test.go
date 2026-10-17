package lint_test

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/google/go-cmp/cmp"
	"github.com/kitagry/bqls/langserver/internal/bigquery/mock_bigquery"
	"github.com/kitagry/bqls/langserver/internal/lint"
	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/kitagry/bqls/langserver/internal/source/file"
	"github.com/sirupsen/logrus"
)

func parse(t *testing.T, src string) file.ParsedFile {
	t.Helper()
	ctrl := gomock.NewController(t)
	bqClient := mock_bigquery.NewMockClient(ctrl)
	parsed := file.NewAnalyzer(logrus.New(), bqClient).ParseFile("file:///a.sql", src)
	if len(parsed.Errors) > 0 {
		t.Fatalf("unexpected analysis errors: %v", parsed.Errors)
	}
	return parsed
}

func rng(sl, sc, el, ec int) lsp.Range {
	return lsp.Range{Start: lsp.Position{Line: sl, Character: sc}, End: lsp.Position{Line: el, Character: ec}}
}

func TestNullComparison(t *testing.T) {
	tests := map[string]struct {
		src  string
		want []lint.Diagnostic
	}{
		"equal typed NULL": {
			src: "SELECT x FROM UNNEST([1]) AS x WHERE x = CAST(NULL AS INT64)",
			want: []lint.Diagnostic{{
				Rule:     "null-comparison",
				Range:    rng(0, 37, 0, 60),
				Message:  "Comparison with NULL using = always yields NULL; use IS NULL",
				Severity: lint.SeverityWarning,
				SuggestedFixes: []lint.SuggestedFix{{
					Title: "Replace with IS NULL",
					Edits: []lsp.TextEdit{{Range: rng(0, 37, 0, 60), NewText: "x IS NULL"}},
				}},
			}},
		},
		"typed NULL on the left with !=": {
			src: "SELECT x FROM UNNEST([1]) AS x WHERE CAST(NULL AS INT64) != x",
			want: []lint.Diagnostic{{
				Rule:     "null-comparison",
				Range:    rng(0, 37, 0, 61),
				Message:  "Comparison with NULL using != always yields NULL; use IS NOT NULL",
				Severity: lint.SeverityWarning,
				SuggestedFixes: []lint.SuggestedFix{{
					Title: "Replace with IS NOT NULL",
					Edits: []lsp.TextEdit{{Range: rng(0, 37, 0, 61), NewText: "x IS NOT NULL"}},
				}},
			}},
		},
		"<> operator with expression operand on a later line": {
			src: "SELECT x FROM UNNEST([1]) AS x\nWHERE (x + 1) <> CAST(NULL AS INT64)",
			want: []lint.Diagnostic{{
				Rule:     "null-comparison",
				Range:    rng(1, 6, 1, 36),
				Message:  "Comparison with NULL using <> always yields NULL; use IS NOT NULL",
				Severity: lint.SeverityWarning,
				SuggestedFixes: []lint.SuggestedFix{{
					Title: "Replace with IS NOT NULL",
					Edits: []lsp.TextEdit{{Range: rng(1, 6, 1, 36), NewText: "(x + 1) IS NOT NULL"}},
				}},
			}},
		},
		"declared variable operand keeps original source text": {
			src: "DECLARE v INT64 DEFAULT 1;\nSELECT v = CAST(NULL AS INT64)",
			want: []lint.Diagnostic{{
				Rule:     "null-comparison",
				Range:    rng(1, 7, 1, 30),
				Message:  "Comparison with NULL using = always yields NULL; use IS NULL",
				Severity: lint.SeverityWarning,
				SuggestedFixes: []lint.SuggestedFix{{
					Title: "Replace with IS NULL",
					Edits: []lsp.TextEdit{{Range: rng(1, 7, 1, 30), NewText: "v IS NULL"}},
				}},
			}},
		},
		// Declared variables are analyzed as dummy literals, which must not be
		// mistaken for a NULL literal.
		"comparison with declared variable is fine": {
			src:  "DECLARE v INT64;\nSELECT x FROM UNNEST([1]) AS x WHERE x = v",
			want: nil,
		},
		"comparison with variable declared with NULL default is fine": {
			src:  "DECLARE v DEFAULT NULL;\nSELECT x FROM UNNEST([1]) AS x WHERE x = v",
			want: nil,
		},
		"parenthesized typed NULL": {
			src: "SELECT x FROM UNNEST([1]) AS x WHERE x = (CAST(NULL AS INT64))",
			want: []lint.Diagnostic{{
				Rule:     "null-comparison",
				Range:    rng(0, 37, 0, 62),
				Message:  "Comparison with NULL using = always yields NULL; use IS NULL",
				Severity: lint.SeverityWarning,
				SuggestedFixes: []lint.SuggestedFix{{
					Title: "Replace with IS NULL",
					Edits: []lsp.TextEdit{{Range: rng(0, 37, 0, 62), NewText: "x IS NULL"}},
				}},
			}},
		},
		// Ranges are byte based here; the langserver converts them to UTF-16
		// for the client, so the fix is kept after non-ASCII text.
		"fix after non-ASCII text on the same line": {
			src: "SELECT 'あいう' AS s, x FROM UNNEST([1]) AS x WHERE x = CAST(NULL AS INT64)",
			want: []lint.Diagnostic{{
				Rule:     "null-comparison",
				Range:    rng(0, 55, 0, 78),
				Message:  "Comparison with NULL using = always yields NULL; use IS NULL",
				Severity: lint.SeverityWarning,
				SuggestedFixes: []lint.SuggestedFix{{
					Title: "Replace with IS NULL",
					Edits: []lsp.TextEdit{{Range: rng(0, 55, 0, 78), NewText: "x IS NULL"}},
				}},
			}},
		},
		"fix kept when non-ASCII text follows the edit": {
			src: "SELECT x FROM UNNEST([1]) AS x WHERE x = CAST(NULL AS INT64) AND 'あ' = 'あ'",
			want: []lint.Diagnostic{{
				Rule:     "null-comparison",
				Range:    rng(0, 37, 0, 60),
				Message:  "Comparison with NULL using = always yields NULL; use IS NULL",
				Severity: lint.SeverityWarning,
				SuggestedFixes: []lint.SuggestedFix{{
					Title: "Replace with IS NULL",
					Edits: []lsp.TextEdit{{Range: rng(0, 37, 0, 60), NewText: "x IS NULL"}},
				}},
			}},
		},
		"IS NULL is fine": {
			src:  "SELECT x FROM UNNEST([1]) AS x WHERE x IS NULL",
			want: nil,
		},
		"comparison with non-NULL literal is fine": {
			src:  "SELECT x FROM UNNEST([1]) AS x WHERE x = 1",
			want: nil,
		},
		"suppressed by comment": {
			src:  "SELECT x FROM UNNEST([1]) AS x\n-- bqls:ignore null-comparison\nWHERE x = CAST(NULL AS INT64)",
			want: nil,
		},
	}

	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			parsed := parse(t, tt.src)
			got := lint.Run(t.Context(), parsed, lint.Config{}, lint.Env{}, lint.NullComparison)
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("diff (-want +got):\n%s", diff)
			}
		})
	}
}
