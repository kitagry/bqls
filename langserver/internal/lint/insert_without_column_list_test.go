package lint_test

import (
	"testing"

	"github.com/kitagry/bqls/langserver/internal/lint"
)

func insertWithoutColumnListMessage(table string) string {
	return "INSERT without a column list writes the values to the columns of `" + table + "` by position, so a column added to or reordered in the table silently shifts them; list the target columns"
}

func TestInsertWithoutColumnList(t *testing.T) {
	type report struct {
		// text is the exact reported source text.
		text  string
		table string
	}
	tests := map[string]struct {
		src  string
		want []report
		// wantFixed is src after applying every fix; empty when no fix is
		// offered.
		wantFixed string
	}{
		"INSERT SELECT": {
			src:       "INSERT INTO p.d.t SELECT a, b FROM p.d.s",
			want:      []report{{"p.d.t", "p.d.t"}},
			wantFixed: "INSERT INTO p.d.t (a, b) SELECT a, b FROM p.d.s",
		},
		"INSERT VALUES": {
			src:       "INSERT INTO p.d.t VALUES (1, 2), (3, 4)",
			want:      []report{{"p.d.t", "p.d.t"}},
			wantFixed: "INSERT INTO p.d.t (a, b) VALUES (1, 2), (3, 4)",
		},
		"INSERT without INTO": {
			src:       "INSERT p.d.t SELECT a, b FROM p.d.s",
			want:      []report{{"p.d.t", "p.d.t"}},
			wantFixed: "INSERT p.d.t (a, b) SELECT a, b FROM p.d.s",
		},
		"SELECT *": {
			src:       "INSERT INTO p.d.t SELECT * FROM p.d.s",
			want:      []report{{"p.d.t", "p.d.t"}},
			wantFixed: "INSERT INTO p.d.t (a, b) SELECT * FROM p.d.s",
		},
		"names that need quoting": {
			src:       "INSERT INTO p.d.kw VALUES (1, 2);\nINSERT INTO p.d.dash VALUES (1, 2)",
			want:      []report{{"p.d.kw", "p.d.kw"}, {"p.d.dash", "p.d.dash"}},
			wantFixed: "INSERT INTO p.d.kw (`from`, `to`) VALUES (1, 2);\nINSERT INTO p.d.dash (`first-name`, `last name`) VALUES (1, 2)",
		},
		"quoted table path": {
			src:       "INSERT INTO `p.d.t` VALUES (1, 2)",
			want:      []report{{"`p.d.t`", "p.d.t"}},
			wantFixed: "INSERT INTO `p.d.t` (a, b) VALUES (1, 2)",
		},
		// A column list in schema order would hide the swap that
		// insert-column-order reports.
		"columns in another order get no fix": {
			src:  "INSERT INTO p.d.t SELECT 2 AS b, 1 AS a",
			want: []report{{"p.d.t", "p.d.t"}},
		},
		"column list": {
			src: "INSERT INTO p.d.t (a, b) SELECT a, b FROM p.d.s",
		},
		"MERGE": {
			src: "MERGE p.d.t AS t USING p.d.s AS s ON t.a = s.a WHEN NOT MATCHED THEN INSERT ROW",
		},
		"CREATE TABLE AS SELECT": {
			src: "CREATE TABLE p.d.n AS SELECT a, b FROM p.d.s",
		},
		"suppressed by comment": {
			src: "-- bqls:ignore insert-without-column-list\nINSERT INTO p.d.t VALUES (1, 2)",
		},
	}

	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			got := lint.Run(t.Context(), parseInsertTables(t, tt.src), lint.Config{}, lint.Env{}, lint.InsertWithoutColumnList)
			if len(got) != len(tt.want) {
				t.Fatalf("got %d diagnostics, want %d: %+v", len(got), len(tt.want), got)
			}
			for i, w := range tt.want {
				d := got[i]
				if text := textInRange(tt.src, d); text != w.text {
					t.Errorf("reported text = %q, want %q", text, w.text)
				}
				if d.Rule != "insert-without-column-list" || d.Severity != lint.SeverityWarning {
					t.Errorf("rule, severity = %q, %v", d.Rule, d.Severity)
				}
				if want := insertWithoutColumnListMessage(w.table); d.Message != want {
					t.Errorf("message = %q, want %q", d.Message, want)
				}
				wantFixes := 1
				if tt.wantFixed == "" {
					wantFixes = 0
				}
				if len(d.SuggestedFixes) != wantFixes {
					t.Fatalf("got %d fixes, want %d", len(d.SuggestedFixes), wantFixes)
				}
			}
			if tt.wantFixed == "" {
				return
			}
			fixed := applyFixes(t, tt.src, got)
			if fixed != tt.wantFixed {
				t.Fatalf("fixed = %q, want %q", fixed, tt.wantFixed)
			}
			if after := lint.Run(t.Context(), parseInsertTables(t, fixed), lint.Config{}, lint.Env{}, lint.InsertWithoutColumnList); len(after) != 0 {
				t.Errorf("diagnostics after fix: %+v", after)
			}
		})
	}
}
