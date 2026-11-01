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
			if diff := cmp.Diff(want, got); diff != "" {
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
