package lint_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	bq "cloud.google.com/go/bigquery"
	"github.com/golang/mock/gomock"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/kitagry/bqls/langserver/internal/bigquery/mock_bigquery"
	"github.com/kitagry/bqls/langserver/internal/lint"
	"github.com/kitagry/bqls/langserver/internal/source/file"
	"github.com/sirupsen/logrus"
)

func columns(n int) bq.Schema {
	s := make(bq.Schema, n)
	for i := range s {
		s[i] = &bq.FieldSchema{Name: fmt.Sprintf("c%d", i), Type: bq.IntegerFieldType}
	}
	return s
}

var wideTables = map[string]*bq.TableMetadata{
	"p.d.wide":   {Type: bq.RegularTable, Schema: append(bq.Schema{{Name: "a", Type: bq.IntegerFieldType}}, columns(3)...)},
	"p.d.narrow": {Type: bq.RegularTable, Schema: bq.Schema{{Name: "a", Type: bq.IntegerFieldType}, {Name: "x", Type: bq.IntegerFieldType}}},
	"p.d.huge":   {Type: bq.RegularTable, Schema: columns(50)},
	"p.d.almost": {Type: bq.RegularTable, Schema: columns(49)},
	"p.d.wide2":  {Type: bq.RegularTable, Schema: namedColumns("b0", "b1", "b2", "b3")},
	"p.d.side":   {Type: bq.RegularTable, Schema: namedColumns("s0", "s1")},
	"p.d.odd":    {Type: bq.RegularTable, Schema: namedColumns("select", "b", "Group", "_x")},
	"p.d.self":   {Type: bq.RegularTable, Schema: namedColumns("self", "b", "c", "d")},
	"p.d.parted": {Type: bq.RegularTable, TimePartitioning: &bq.TimePartitioning{}, Schema: columns(4)},
}

func namedColumns(names ...string) bq.Schema {
	s := make(bq.Schema, len(names))
	for i, n := range names {
		s[i] = &bq.FieldSchema{Name: n, Type: bq.IntegerFieldType}
	}
	return s
}

func parseWide(t *testing.T, src string) file.ParsedFile {
	t.Helper()
	ctrl := gomock.NewController(t)
	bqClient := mock_bigquery.NewMockClient(ctrl)
	bqClient.EXPECT().GetDefaultProject().Return("p").AnyTimes()
	bqClient.EXPECT().GetTableMetadata(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, project, dataset, table string) (*bq.TableMetadata, error) {
			m, ok := wideTables[project+"."+dataset+"."+table]
			if !ok {
				return nil, fmt.Errorf("table %s.%s.%s not found", project, dataset, table)
			}
			return m, nil
		}).AnyTimes()
	parsed := file.NewAnalyzer(logrus.New(), bqClient).ParseFile("file:///a.sql", src)
	if len(parsed.Errors) > 0 {
		t.Fatalf("unexpected analysis errors: %v", parsed.Errors)
	}
	return parsed
}

func selectStarConfig(minColumns int) lint.Config {
	return lint.Config{
		Rules:   map[string]bool{"select-star": true},
		Options: map[string]json.RawMessage{"select-star": json.RawMessage(fmt.Sprintf(`{"min_columns":%d}`, minColumns))},
	}
}

func TestSelectStar(t *testing.T) {
	type report struct {
		// sub is the reported source text and n its occurrence index.
		sub     string
		n       int
		table   string
		columns int
	}
	tests := map[string]struct {
		src string
		// cfg defaults to the rule enabled with min_columns = 4.
		cfg    *lint.Config
		dryRun *bq.JobStatus
		want   []report
		// suffix is appended to every wanted message.
		suffix string
	}{
		"wide table": {
			src:  "SELECT * FROM p.d.wide",
			want: []report{{sub: "*", table: "p.d.wide", columns: 4}},
		},
		"narrow table": {
			src: "SELECT * FROM p.d.narrow",
		},
		"dataset-qualified table": {
			src:  "SELECT * FROM d.wide",
			want: []report{{sub: "*", table: "p.d.wide", columns: 4}},
		},
		"LIMIT does not reduce the columns read": {
			src:  "SELECT * FROM p.d.wide LIMIT 10",
			want: []report{{sub: "*", table: "p.d.wide", columns: 4}},
		},
		"dot star on the wide side of a join": {
			src:  "SELECT w.* FROM p.d.wide w JOIN p.d.narrow n ON w.a = n.a",
			want: []report{{sub: "w.*", table: "p.d.wide", columns: 4}},
		},
		"dot star on the narrow side of a join": {
			src: "SELECT n.*, w.c0 FROM p.d.wide w JOIN p.d.narrow n ON w.a = n.a",
		},
		"star over a join": {
			src:  "SELECT * FROM p.d.wide w JOIN p.d.narrow n USING (a)",
			want: []report{{sub: "*", table: "p.d.wide", columns: 4}},
		},
		"quoted table path": {
			src:  "SELECT * FROM `p.d.wide` AS w",
			want: []report{{sub: "*", table: "p.d.wide", columns: 4}},
		},
		"self join is reported once": {
			src:  "SELECT * FROM p.d.wide l JOIN p.d.wide r USING (a)",
			want: []report{{sub: "*", table: "p.d.wide", columns: 4}},
		},
		"star with UNNEST": {
			src:  "SELECT * FROM p.d.wide, UNNEST([1, 2]) AS u",
			want: []report{{sub: "*", table: "p.d.wide", columns: 4}},
		},
		"each branch of UNION ALL": {
			src: "SELECT * FROM p.d.wide UNION ALL SELECT * FROM p.d.wide",
			want: []report{
				{sub: "*", n: 0, table: "p.d.wide", columns: 4},
				{sub: "*", n: 1, table: "p.d.wide", columns: 4},
			},
		},
		"EXCEPT is a deliberate column choice": {
			src: "SELECT * EXCEPT (c0) FROM p.d.wide",
		},
		"REPLACE is a deliberate column choice": {
			src: "SELECT * REPLACE (1 AS a) FROM p.d.wide",
		},
		"dot star with EXCEPT": {
			src: "SELECT w.* EXCEPT (c0) FROM p.d.wide w",
		},
		"star over a CTE reports the CTE body only": {
			src:  "WITH c AS (SELECT * FROM p.d.wide) SELECT * FROM c",
			want: []report{{sub: "*", n: 0, table: "p.d.wide", columns: 4}},
		},
		"star over a subquery reports the subquery only": {
			src:  "SELECT * FROM (SELECT * FROM p.d.wide)",
			want: []report{{sub: "*", n: 1, table: "p.d.wide", columns: 4}},
		},
		"subquery columns pruned by the outer query": {
			src: "SELECT a FROM (SELECT * FROM p.d.wide)",
		},
		"CTE columns pruned by the main query": {
			src: "WITH c AS (SELECT * FROM p.d.wide) SELECT a, c0 FROM c",
		},
		"EXISTS reads no column": {
			src: "SELECT a FROM p.d.narrow WHERE EXISTS (SELECT * FROM p.d.wide)",
		},
		"unreferenced CTE is not evaluated": {
			src: "WITH c AS (SELECT * FROM p.d.wide) SELECT 1",
		},
		"COUNT(*) is not a select star": {
			src: "SELECT COUNT(*) FROM p.d.wide",
		},
		"CREATE TABLE AS SELECT copies whole rows on purpose": {
			src: "CREATE TABLE p.d.copy AS SELECT * FROM p.d.wide",
		},
		"INSERT copies whole rows on purpose": {
			src: "INSERT INTO p.d.wide SELECT * FROM p.d.wide",
		},
		"bytes estimate from the dry-run": {
			src:    "SELECT * FROM p.d.wide",
			dryRun: dryRunProcessing(3 << 30),
			want:   []report{{sub: "*", table: "p.d.wide", columns: 4}},
			suffix: "; the dry-run estimates the file processes 3 GiB",
		},
		"dry-run without statistics": {
			src:    "SELECT * FROM p.d.wide",
			dryRun: &bq.JobStatus{},
			want:   []report{{sub: "*", table: "p.d.wide", columns: 4}},
		},
		"default min_columns reached": {
			src:  "SELECT * FROM p.d.huge",
			cfg:  &lint.Config{Rules: map[string]bool{"select-star": true}},
			want: []report{{sub: "*", table: "p.d.huge", columns: 50}},
		},
		"default min_columns not reached": {
			src: "SELECT * FROM p.d.almost",
			cfg: &lint.Config{Rules: map[string]bool{"select-star": true}},
		},
		"disabled by default": {
			src: "SELECT * FROM p.d.huge",
			cfg: &lint.Config{},
		},
		"suppressed by an ignore comment": {
			src: "SELECT * -- bqls:ignore select-star\nFROM p.d.wide",
		},
	}
	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			cfg := selectStarConfig(4)
			if tt.cfg != nil {
				cfg = *tt.cfg
			}
			var want []lint.Diagnostic
			for _, r := range tt.want {
				want = append(want, lint.Diagnostic{
					Rule:     "select-star",
					Range:    spanOf(t, tt.src, r.sub, r.n),
					Message:  fmt.Sprintf("SELECT * reads %d columns of `%s`; list only the columns you need to reduce the bytes processed%s", r.columns, r.table, tt.suffix),
					Severity: lint.SeverityWarning,
				})
			}
			got := lint.Run(t.Context(), parseWide(t, tt.src), cfg, lint.Env{DryRun: tt.dryRun}, lint.SelectStar)
			// Fixes are covered by TestSelectStar_Fix.
			if diff := cmp.Diff(want, got, cmpopts.IgnoreFields(lint.Diagnostic{}, "SuggestedFixes")); diff != "" {
				t.Errorf("Run() diff (-want +got):\n%s", diff)
			}
		})
	}
}

func TestSelectStar_InvalidMinColumns(t *testing.T) {
	cfg := selectStarConfig(0)
	err := cfg.Validate(lint.Analyzers)
	if err == nil || !strings.Contains(err.Error(), `rule "select-star"`) || !strings.Contains(err.Error(), "min_columns") {
		t.Errorf("Validate() = %v, want an error about min_columns of select-star", err)
	}
}

func TestSelectStar_Fix(t *testing.T) {
	tests := map[string]struct {
		src string
		// wantFixed is src after applying every fix, or empty when no fix is
		// offered; it must analyze cleanly.
		wantFixed string
	}{
		"single table": {
			src:       "SELECT * FROM p.d.wide",
			wantFixed: "SELECT a, c0, c1, c2 FROM p.d.wide",
		},
		"dataset-qualified table with alias": {
			src:       "SELECT * FROM d.wide AS w",
			wantFixed: "SELECT a, c0, c1, c2 FROM d.wide AS w",
		},
		"quoted table path": {
			src:       "SELECT * FROM `p.d.wide`",
			wantFixed: "SELECT a, c0, c1, c2 FROM `p.d.wide`",
		},
		"DISTINCT": {
			src:       "SELECT DISTINCT * FROM p.d.wide",
			wantFixed: "SELECT DISTINCT a, c0, c1, c2 FROM p.d.wide",
		},
		"each branch of UNION ALL": {
			src:       "SELECT * FROM p.d.wide UNION ALL SELECT * FROM p.d.wide",
			wantFixed: "SELECT a, c0, c1, c2 FROM p.d.wide UNION ALL SELECT a, c0, c1, c2 FROM p.d.wide",
		},
		"dot star keeps its qualifier": {
			src:       "SELECT w.* FROM p.d.wide w JOIN p.d.narrow n ON w.a = n.a",
			wantFixed: "SELECT w.a, w.c0, w.c1, w.c2 FROM p.d.wide w JOIN p.d.narrow n ON w.a = n.a",
		},
		"dot star over an implicit alias": {
			src:       "SELECT wide.* FROM p.d.wide",
			wantFixed: "SELECT wide.a, wide.c0, wide.c1, wide.c2 FROM p.d.wide",
		},
		"dot star next to UNNEST": {
			src:       "SELECT w.* FROM p.d.wide w, UNNEST([1, 2]) AS u",
			wantFixed: "SELECT w.a, w.c0, w.c1, w.c2 FROM p.d.wide w, UNNEST([1, 2]) AS u",
		},
		"star over a join is qualified": {
			src:       "SELECT * FROM p.d.wide w JOIN p.d.side s ON w.a = s.s0",
			wantFixed: "SELECT w.a, w.c0, w.c1, w.c2, s.s0, s.s1 FROM p.d.wide w JOIN p.d.side s ON w.a = s.s0",
		},
		"star over a comma join without aliases": {
			src:       "SELECT * FROM p.d.wide, p.d.side",
			wantFixed: "SELECT wide.a, wide.c0, wide.c1, wide.c2, side.s0, side.s1 FROM p.d.wide, p.d.side",
		},
		"star over two wide tables is fixed once": {
			src:       "SELECT * FROM p.d.wide w CROSS JOIN p.d.wide2 v",
			wantFixed: "SELECT w.a, w.c0, w.c1, w.c2, v.b0, v.b1, v.b2, v.b3 FROM p.d.wide w CROSS JOIN p.d.wide2 v",
		},
		"keywords and case are quoted as needed": {
			src:       "SELECT * FROM p.d.odd",
			wantFixed: "SELECT `select`, b, `Group`, _x FROM p.d.odd",
		},
		"pseudo-columns are not expanded": {
			src:       "SELECT * FROM p.d.parted",
			wantFixed: "SELECT c0, c1, c2, c3 FROM p.d.parted",
		},
		"column named like another range variable is safe": {
			src:       "SELECT * FROM p.d.self AS t",
			wantFixed: "SELECT self, b, c, d FROM p.d.self AS t",
		},
		"comment before the star is kept": {
			src:       "SELECT /* all */ * FROM p.d.wide",
			wantFixed: "SELECT /* all */ a, c0, c1, c2 FROM p.d.wide",
		},
		"trailing comment keeps the list inline": {
			src:       "SELECT * -- everything\nFROM p.d.wide",
			wantFixed: "SELECT a, c0, c1, c2 -- everything\nFROM p.d.wide",
		},
		"star on its own line": {
			src:       "SELECT\n  *,\n  1 AS one\nFROM p.d.wide",
			wantFixed: "SELECT\n  a,\n  c0,\n  c1,\n  c2,\n  1 AS one\nFROM p.d.wide",
		},
		"star after SELECT in a multi-line query": {
			src:       "SELECT *\nFROM p.d.wide",
			wantFixed: "SELECT\n  a,\n  c0,\n  c1,\n  c2\nFROM p.d.wide",
		},
		"indented multi-line CTE": {
			src:       "WITH c AS (\n  SELECT *\n  FROM p.d.wide\n)\nSELECT * FROM c",
			wantFixed: "WITH c AS (\n  SELECT\n    a,\n    c0,\n    c1,\n    c2\n  FROM p.d.wide\n)\nSELECT * FROM c",
		},
		"tab indentation": {
			src:       "WITH c AS (\n\tSELECT *\n\tFROM p.d.wide\n)\nSELECT * FROM c",
			wantFixed: "WITH c AS (\n\tSELECT\n\t\ta,\n\t\tc0,\n\t\tc1,\n\t\tc2\n\tFROM p.d.wide\n)\nSELECT * FROM c",
		},
		"colliding column names": {
			src: "SELECT * FROM p.d.wide w JOIN p.d.narrow n ON w.a = n.a",
		},
		"self join": {
			src: "SELECT * FROM p.d.wide l JOIN p.d.wide r ON l.a = r.a",
		},
		"join with USING": {
			src: "SELECT * FROM p.d.wide w JOIN p.d.narrow n USING (a)",
		},
		"star with UNNEST": {
			src: "SELECT * FROM p.d.wide, UNNEST([1, 2]) AS u",
		},
		"star with a subquery": {
			src: "SELECT * FROM p.d.wide w, (SELECT 1 AS one)",
		},
		"star over a CTE joined to a table": {
			src: "WITH c AS (SELECT 1 AS one) SELECT * FROM p.d.wide w, c",
		},
		"quoted table path in a join has no writable qualifier": {
			src: "SELECT * FROM `p.d.wide` JOIN p.d.side ON TRUE",
		},
		"column named like its own range variable": {
			src: "SELECT * FROM p.d.self",
		},
		"comment inside a dot star": {
			src: "SELECT w /* all */ .* FROM p.d.wide w",
		},
	}
	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			got := lint.Run(t.Context(), parseWide(t, tt.src), selectStarConfig(4), lint.Env{}, lint.SelectStar)
			if len(got) == 0 {
				t.Fatal("no diagnostic reported")
			}
			for _, d := range got {
				for _, f := range d.SuggestedFixes {
					if want := "Expand " + textInRange(tt.src, d) + " into explicit columns"; f.Title != want {
						t.Errorf("fix title = %q, want %q", f.Title, want)
					}
				}
			}
			fixed := applyFixes(t, tt.src, got)
			if tt.wantFixed == "" {
				if fixed != tt.src {
					t.Errorf("fixed = %q, want no fix", fixed)
				}
				return
			}
			if fixed != tt.wantFixed {
				t.Errorf("fixed = %q, want %q", fixed, tt.wantFixed)
			}
			parseWide(t, fixed)
		})
	}
}
