package lint_test

import (
	"slices"
	"strings"
	"testing"

	bq "cloud.google.com/go/bigquery"
	"github.com/golang/mock/gomock"
	"github.com/google/go-cmp/cmp"
	"github.com/kitagry/bqls/langserver/internal/bigquery/mock_bigquery"
	"github.com/kitagry/bqls/langserver/internal/lint"
	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/kitagry/bqls/langserver/internal/source/file"
	"github.com/sirupsen/logrus"
)

// parseWithTable resolves every table path to a single INT64 column `x`.
func parseWithTable(t *testing.T, src string) file.ParsedFile {
	t.Helper()
	ctrl := gomock.NewController(t)
	bqClient := mock_bigquery.NewMockClient(ctrl)
	bqClient.EXPECT().GetTableMetadata(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&bq.TableMetadata{Schema: bq.Schema{{Name: "x", Type: bq.IntegerFieldType}}}, nil).AnyTimes()
	parsed := file.NewAnalyzer(logrus.New(), bqClient).ParseFile("file:///a.sql", src)
	if len(parsed.Errors) > 0 {
		t.Fatalf("unexpected analysis errors: %v", parsed.Errors)
	}
	return parsed
}

// applyFixes applies every suggested fix; edits are byte based and must not overlap.
func applyFixes(t *testing.T, src string, diags []lint.Diagnostic) string {
	t.Helper()
	lines := strings.SplitAfter(src, "\n")
	offset := func(p lsp.Position) int {
		n := p.Character
		for _, l := range lines[:p.Line] {
			n += len(l)
		}
		return n
	}
	var edits []lsp.TextEdit
	for _, d := range diags {
		for _, f := range d.SuggestedFixes {
			edits = append(edits, f.Edits...)
		}
	}
	slices.SortFunc(edits, func(a, b lsp.TextEdit) int { return offset(b.Range.Start) - offset(a.Range.Start) })
	for _, e := range edits {
		src = src[:offset(e.Range.Start)] + e.NewText + src[offset(e.Range.End):]
	}
	return src
}

func unusedCTE(name string, nameRange lsp.Range, fixEdits ...lsp.TextEdit) lint.Diagnostic {
	d := lint.Diagnostic{
		Rule:     "unused-cte",
		Range:    nameRange,
		Message:  "CTE `" + name + "` is defined but never used",
		Severity: lint.SeverityInformation,
	}
	if len(fixEdits) > 0 {
		d.SuggestedFixes = []lint.SuggestedFix{{Title: "Remove unused CTE `" + name + "`", Edits: fixEdits}}
	}
	return d
}

func del(r lsp.Range) lsp.TextEdit {
	return lsp.TextEdit{Range: r, NewText: ""}
}

func TestUnusedCTE(t *testing.T) {
	tests := map[string]struct {
		src  string
		want []lint.Diagnostic
		// wantFixed is src after applying every fix; it must analyze cleanly.
		wantFixed string
		// wantAfterFix lists CTEs still reported after the fixes, e.g. ones
		// that were only used by a removed CTE.
		wantAfterFix []string
	}{
		"all CTEs used": {
			src:  "WITH a AS (SELECT 1 AS x), b AS (SELECT * FROM a) SELECT * FROM b",
			want: nil,
		},
		"last entry unused": {
			src:       "WITH a AS (SELECT 1 AS x), b AS (SELECT 2 AS x) SELECT * FROM a",
			want:      []lint.Diagnostic{unusedCTE("b", rng(0, 27, 0, 28), del(rng(0, 25, 0, 47)))},
			wantFixed: "WITH a AS (SELECT 1 AS x) SELECT * FROM a",
		},
		"first entry unused": {
			src:       "WITH a AS (SELECT 1 AS x), b AS (SELECT 2 AS x) SELECT * FROM b",
			want:      []lint.Diagnostic{unusedCTE("a", rng(0, 5, 0, 6), del(rng(0, 5, 0, 27)))},
			wantFixed: "WITH b AS (SELECT 2 AS x) SELECT * FROM b",
		},
		"middle entry unused on its own line": {
			src:       "WITH\n  a AS (SELECT 1 AS x),\n  b AS (SELECT 2 AS x),\n  c AS (SELECT * FROM a)\nSELECT * FROM c",
			want:      []lint.Diagnostic{unusedCTE("b", rng(2, 2, 2, 3), del(rng(1, 22, 2, 22)))},
			wantFixed: "WITH\n  a AS (SELECT 1 AS x),\n  c AS (SELECT * FROM a)\nSELECT * FROM c",
		},
		"only entry unused removes WITH": {
			src:       "WITH a AS (SELECT 1 AS x)\nSELECT 2 AS y",
			want:      []lint.Diagnostic{unusedCTE("a", rng(0, 5, 0, 6), del(rng(0, 0, 1, 0)))},
			wantFixed: "SELECT 2 AS y",
		},
		"only entry with a header comment above WITH": {
			src:       "-- header\nWITH a AS (SELECT 1 AS x)\nSELECT 2 AS y",
			want:      []lint.Diagnostic{unusedCTE("a", rng(1, 5, 1, 6), del(rng(1, 0, 2, 0)))},
			wantFixed: "-- header\nSELECT 2 AS y",
		},
		"only entry followed by a comment line keeps the comment": {
			src:       "WITH a AS (SELECT 1 AS x)\n-- the query\nSELECT 2 AS y",
			want:      []lint.Diagnostic{unusedCTE("a", rng(0, 5, 0, 6), del(rng(0, 0, 1, 0)))},
			wantFixed: "-- the query\nSELECT 2 AS y",
		},
		"no fix when a comment is inside WITH before the only entry": {
			src:  "WITH /* keep me */ a AS (SELECT 1 AS x)\nSELECT 2 AS y",
			want: []lint.Diagnostic{unusedCTE("a", rng(0, 19, 0, 20))},
		},
		"no fix when a comment follows WITH RECURSIVE": {
			src:  "WITH RECURSIVE /* why */ r AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM r WHERE n < 3) SELECT 1 AS y",
			want: []lint.Diagnostic{unusedCTE("r", rng(0, 25, 0, 26))},
		},
		"no fix when a comment line precedes the only entry": {
			src:  "WITH\n  -- explain\n  a AS (SELECT 1 AS x)\nSELECT 2 AS y",
			want: []lint.Diagnostic{unusedCTE("a", rng(2, 2, 2, 3))},
		},
		"no fix when a trailing comment follows the only entry": {
			src:  "WITH a AS (SELECT 1 AS x) -- about a\nSELECT 2 AS y",
			want: []lint.Diagnostic{unusedCTE("a", rng(0, 5, 0, 6))},
		},
		"no fix when a comment line precedes the first entry": {
			src:  "WITH\n  -- about a\n  a AS (SELECT 1 AS x),\n  b AS (SELECT 2 AS x)\nSELECT * FROM b",
			want: []lint.Diagnostic{unusedCTE("a", rng(2, 2, 2, 3))},
		},
		"no fix when a trailing comment follows the last entry": {
			src:  "WITH a AS (SELECT 1 AS x),\nb AS (SELECT 2 AS x) -- trailing\nSELECT * FROM a",
			want: []lint.Diagnostic{unusedCTE("b", rng(1, 0, 1, 1))},
		},
		"no fix when a trailing comment follows a middle entry": {
			src:  "WITH a AS (SELECT 1 AS x),\nb AS (SELECT 2 AS x), -- about b\nc AS (SELECT * FROM a)\nSELECT * FROM c",
			want: []lint.Diagnostic{unusedCTE("b", rng(1, 0, 1, 1))},
		},
		// Removing the separator would also delete the user's comment.
		"no fix when a comment separates entries": {
			src:  "WITH a AS (SELECT 1 AS x), -- keep\nb AS (SELECT 2 AS x) SELECT * FROM a",
			want: []lint.Diagnostic{unusedCTE("b", rng(1, 0, 1, 1))},
		},
		"used from a scalar subquery": {
			src:  "WITH a AS (SELECT 1 AS x) SELECT (SELECT MAX(x) FROM a) AS m",
			want: nil,
		},
		"used from EXISTS": {
			src:  "WITH a AS (SELECT 1 AS x) SELECT EXISTS(SELECT * FROM a) AS e",
			want: nil,
		},
		"used from IN subquery": {
			src:  "WITH a AS (SELECT 1 AS x) SELECT 1 IN (SELECT x FROM a) AS i",
			want: nil,
		},
		"used from pipe syntax": {
			src:  "WITH a AS (SELECT 1 AS x) FROM a |> WHERE x = 1",
			want: nil,
		},
		"reference differs only in case": {
			src:  "WITH Foo AS (SELECT 1 AS x) SELECT * FROM foo",
			want: nil,
		},
		"used only by an unused CTE": {
			src:          "WITH a AS (SELECT 1 AS x), b AS (SELECT * FROM a) SELECT 1 AS y",
			want:         []lint.Diagnostic{unusedCTE("b", rng(0, 27, 0, 28), del(rng(0, 25, 0, 49)))},
			wantFixed:    "WITH a AS (SELECT 1 AS x) SELECT 1 AS y",
			wantAfterFix: []string{"a"},
		},
		"used from a nested WITH": {
			src:  "WITH a AS (SELECT 1 AS x) SELECT * FROM (WITH b AS (SELECT * FROM a) SELECT * FROM b)",
			want: nil,
		},
		"outer CTE shadowed by an inner CTE of the same name": {
			src:       "WITH a AS (SELECT 1 AS x) SELECT * FROM (WITH a AS (SELECT 2 AS x) SELECT * FROM a)",
			want:      []lint.Diagnostic{unusedCTE("a", rng(0, 5, 0, 6), del(rng(0, 0, 0, 26)))},
			wantFixed: "SELECT * FROM (WITH a AS (SELECT 2 AS x) SELECT * FROM a)",
		},
		"inner CTE unused while outer of the same name is used": {
			src:       "WITH a AS (SELECT 1 AS x) SELECT * FROM a JOIN (WITH a AS (SELECT 2 AS x) SELECT 3 AS x) USING (x)",
			want:      []lint.Diagnostic{unusedCTE("a", rng(0, 53, 0, 54), del(rng(0, 48, 0, 74)))},
			wantFixed: "WITH a AS (SELECT 1 AS x) SELECT * FROM a JOIN (SELECT 3 AS x) USING (x)",
		},
		// The inner `a` is renamed by the analyzer, possibly to a name the user also wrote.
		"user name colliding with an analyzer rename": {
			src: "WITH a AS (SELECT 1 AS x), a_1 AS (SELECT 1 AS x) SELECT * FROM (WITH a AS (SELECT 2 AS x) SELECT * FROM a_1)",
			want: []lint.Diagnostic{
				unusedCTE("a", rng(0, 5, 0, 6), del(rng(0, 5, 0, 27))),
				unusedCTE("a", rng(0, 70, 0, 71), del(rng(0, 65, 0, 91))),
			},
			wantFixed: "WITH a_1 AS (SELECT 1 AS x) SELECT * FROM (SELECT * FROM a_1)",
		},
		"recursive self reference is not a use": {
			src:       "WITH RECURSIVE r AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM r WHERE n < 3) SELECT 1 AS y",
			want:      []lint.Diagnostic{unusedCTE("r", rng(0, 15, 0, 16), del(rng(0, 0, 0, 78)))},
			wantFixed: "SELECT 1 AS y",
		},
		"recursive WITH reorders entries by dependency": {
			src:       "WITH RECURSIVE c AS (SELECT * FROM b), b AS (SELECT * FROM a), a AS (SELECT 1 AS x), d AS (SELECT 1 AS x) SELECT * FROM c",
			want:      []lint.Diagnostic{unusedCTE("d", rng(0, 85, 0, 86), del(rng(0, 83, 0, 105)))},
			wantFixed: "WITH RECURSIVE c AS (SELECT * FROM b), b AS (SELECT * FROM a), a AS (SELECT 1 AS x) SELECT * FROM c",
		},
		"CREATE TABLE AS SELECT after DECLARE": {
			src:       "DECLARE v INT64;\nCREATE TABLE t AS WITH a AS (SELECT 1 AS x) SELECT v AS x",
			want:      []lint.Diagnostic{unusedCTE("a", rng(1, 23, 1, 24), del(rng(1, 18, 1, 44)))},
			wantFixed: "DECLARE v INT64;\nCREATE TABLE t AS SELECT v AS x",
		},
		// Declared variables are substituted in the analyzed source, shifting offsets.
		"declared variable before the CTE": {
			src:       "DECLARE myvariable INT64 DEFAULT 1;\nSELECT myvariable AS y, (WITH a AS (SELECT 1) SELECT 2) AS z",
			want:      []lint.Diagnostic{unusedCTE("a", rng(1, 30, 1, 31), del(rng(1, 25, 1, 46)))},
			wantFixed: "DECLARE myvariable INT64 DEFAULT 1;\nSELECT myvariable AS y, (SELECT 2) AS z",
		},
		"CREATE VIEW": {
			src:       "CREATE VIEW v AS WITH a AS (SELECT 1 AS x), b AS (SELECT 2 AS x) SELECT * FROM a",
			want:      []lint.Diagnostic{unusedCTE("b", rng(0, 44, 0, 45), del(rng(0, 42, 0, 64)))},
			wantFixed: "CREATE VIEW v AS WITH a AS (SELECT 1 AS x) SELECT * FROM a",
		},
		"MERGE USING subquery": {
			src:       "MERGE p.d.t T USING (WITH a AS (SELECT 1 AS x), b AS (SELECT 2 AS x) SELECT * FROM a) S ON T.x = S.x WHEN MATCHED THEN DELETE",
			want:      []lint.Diagnostic{unusedCTE("b", rng(0, 48, 0, 49), del(rng(0, 46, 0, 68)))},
			wantFixed: "MERGE p.d.t T USING (WITH a AS (SELECT 1 AS x) SELECT * FROM a) S ON T.x = S.x WHEN MATCHED THEN DELETE",
		},
		"each statement of a script": {
			src: "WITH a AS (SELECT 1 AS x) SELECT 1 AS y;\nWITH a AS (SELECT 1 AS x) SELECT * FROM a;\nWITH b AS (SELECT 1 AS x) SELECT 1 AS y",
			want: []lint.Diagnostic{
				unusedCTE("a", rng(0, 5, 0, 6), del(rng(0, 0, 0, 26))),
				unusedCTE("b", rng(2, 5, 2, 6), del(rng(2, 0, 2, 26))),
			},
			wantFixed: "SELECT 1 AS y;\nWITH a AS (SELECT 1 AS x) SELECT * FROM a;\nSELECT 1 AS y",
		},
		"suppressed by comment": {
			src:  "-- bqls:ignore unused-cte\nWITH a AS (SELECT 1 AS x) SELECT 1 AS y",
			want: nil,
		},
	}

	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			got := lint.Run(t.Context(), parseWithTable(t, tt.src), lint.Config{}, lint.Env{}, lint.UnusedCTE)
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Fatalf("diff (-want +got):\n%s", diff)
			}
			if tt.wantFixed == "" {
				return
			}
			fixed := applyFixes(t, tt.src, got)
			if diff := cmp.Diff(tt.wantFixed, fixed); diff != "" {
				t.Fatalf("fixed source diff (-want +got):\n%s", diff)
			}
			var after []string
			for _, d := range lint.Run(t.Context(), parseWithTable(t, fixed), lint.Config{}, lint.Env{}, lint.UnusedCTE) {
				after = append(after, d.Message[len("CTE `"):strings.Index(d.Message, "` is")])
			}
			if diff := cmp.Diff(tt.wantAfterFix, after); diff != "" {
				t.Errorf("diagnostics after fix diff (-want +got):\n%s", diff)
			}
		})
	}
}
