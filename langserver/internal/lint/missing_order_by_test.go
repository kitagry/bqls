package lint_test

import (
	"strings"
	"testing"

	bq "cloud.google.com/go/bigquery"
	"github.com/golang/mock/gomock"
	"github.com/kitagry/bqls/langserver/internal/bigquery/mock_bigquery"
	"github.com/kitagry/bqls/langserver/internal/lint"
	"github.com/kitagry/bqls/langserver/internal/source/file"
	"github.com/sirupsen/logrus"
)

const rowNumberMessage = "ROW_NUMBER() has no ORDER BY in its window, so it numbers rows in an arbitrary order and the rows a filter on it keeps can change from run to run; add ORDER BY to the window"

func aggLimitMessage(fn string) string {
	return fn + " with LIMIT but no ORDER BY keeps arbitrary elements, which can change from run to run; add ORDER BY inside the call"
}

const arrayIndexMessage = "ARRAY_AGG without ORDER BY returns its elements in an arbitrary order, so indexing the result picks an arbitrary element; add ORDER BY inside the call"

// parseEvents resolves every table path to the columns `id`, `ts` and `v`.
func parseEvents(t *testing.T, src string) file.ParsedFile {
	t.Helper()
	ctrl := gomock.NewController(t)
	bqClient := mock_bigquery.NewMockClient(ctrl)
	bqClient.EXPECT().GetTableMetadata(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&bq.TableMetadata{Schema: bq.Schema{
			{Name: "id", Type: bq.IntegerFieldType},
			{Name: "ts", Type: bq.TimestampFieldType},
			{Name: "v", Type: bq.IntegerFieldType},
		}}, nil).AnyTimes()
	bqClient.EXPECT().GetDefaultProject().Return("p").AnyTimes()
	parsed := file.NewAnalyzer(logrus.New(), bqClient).ParseFile("file:///a.sql", src)
	if len(parsed.Errors) > 0 {
		t.Fatalf("unexpected analysis errors: %v", parsed.Errors)
	}
	return parsed
}

func TestMissingOrderBy(t *testing.T) {
	type report struct {
		// text is the exact reported source text.
		text    string
		message string
	}
	rowNumber := report{text: "ROW_NUMBER", message: rowNumberMessage}
	tests := map[string]struct {
		src  string
		want []report
	}{
		"qualify dedup": {
			src:  "SELECT * FROM p.d.t QUALIFY ROW_NUMBER() OVER (PARTITION BY id) = 1",
			want: []report{rowNumber},
		},
		"top n per group": {
			src:  "SELECT * FROM p.d.t QUALIFY ROW_NUMBER() OVER (PARTITION BY id) <= 3",
			want: []report{rowNumber},
		},
		"without partition": {
			src:  "SELECT * FROM p.d.t QUALIFY ROW_NUMBER() OVER () <= 10",
			want: []report{rowNumber},
		},
		"named window": {
			src:  "SELECT * FROM p.d.t QUALIFY ROW_NUMBER() OVER w = 1 WINDOW w AS (PARTITION BY id)",
			want: []report{rowNumber},
		},
		"where on a derived table": {
			src:  "SELECT id, v FROM (SELECT id, v, ROW_NUMBER() OVER (PARTITION BY id) AS rn FROM p.d.t) WHERE rn = 1",
			want: []report{rowNumber},
		},
		"where through a cte": {
			src:  "WITH r AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY id) AS rn FROM p.d.t) SELECT id, v FROM r WHERE rn = 1",
			want: []report{rowNumber},
		},
		"renamed in a cte": {
			src:  "WITH r AS (SELECT id, v, ROW_NUMBER() OVER (PARTITION BY id) AS rn FROM p.d.t), s AS (SELECT id, v, rn AS n FROM r) SELECT id, v FROM s WHERE n = 1",
			want: []report{rowNumber},
		},
		"dedup in a cte consumed whole": {
			src:  "WITH d AS (SELECT * FROM p.d.t QUALIFY ROW_NUMBER() OVER (PARTITION BY id) = 1) SELECT * FROM d",
			want: []report{rowNumber},
		},
		"other columns consumed": {
			src:  "SELECT id, v FROM p.d.t QUALIFY ROW_NUMBER() OVER (PARTITION BY id) = 1",
			want: []report{rowNumber},
		},
		"join condition": {
			src:  "SELECT a.id FROM (SELECT id, ROW_NUMBER() OVER () AS rn FROM p.d.t) AS a JOIN p.d.t AS b ON a.rn = b.v",
			want: []report{rowNumber},
		},
		"array_agg with limit": {
			src:  "SELECT id, ARRAY_AGG(v LIMIT 1) FROM p.d.t GROUP BY id",
			want: []report{{text: "ARRAY_AGG", message: aggLimitMessage("ARRAY_AGG")}},
		},
		"string_agg with limit": {
			src:  "SELECT STRING_AGG(CAST(v AS STRING) LIMIT 3) FROM p.d.t",
			want: []report{{text: "STRING_AGG", message: aggLimitMessage("STRING_AGG")}},
		},
		"array_concat_agg with limit": {
			src:  "SELECT ARRAY_CONCAT_AGG([v] LIMIT 2) FROM p.d.t",
			want: []report{{text: "ARRAY_CONCAT_AGG", message: aggLimitMessage("ARRAY_CONCAT_AGG")}},
		},
		"array_agg offset": {
			src:  "SELECT id, ARRAY_AGG(v)[OFFSET(0)] FROM p.d.t GROUP BY id",
			want: []report{{text: "ARRAY_AGG", message: arrayIndexMessage}},
		},
		"array_agg safe_ordinal": {
			src:  "SELECT id, ARRAY_AGG(v)[SAFE_ORDINAL(1)] FROM p.d.t GROUP BY id",
			want: []report{{text: "ARRAY_AGG", message: arrayIndexMessage}},
		},
		"limit and offset are reported once": {
			src:  "SELECT ARRAY_AGG(t LIMIT 1)[OFFSET(0)] FROM p.d.t AS t GROUP BY id",
			want: []report{{text: "ARRAY_AGG", message: aggLimitMessage("ARRAY_AGG")}},
		},
		"indexed through a cte": {
			src:  "WITH a AS (SELECT id, ARRAY_AGG(v) AS vs FROM p.d.t GROUP BY id) SELECT id, vs[OFFSET(0)] FROM a",
			want: []report{{text: "ARRAY_AGG", message: arrayIndexMessage}},
		},
		"analytic array_agg offset": {
			src:  "SELECT (ARRAY_AGG(v) OVER (PARTITION BY id))[OFFSET(0)] FROM p.d.t",
			want: []report{{text: "ARRAY_AGG", message: arrayIndexMessage}},
		},

		// Deterministic, or order that does not matter.
		"row_number with order by": {
			src: "SELECT * FROM p.d.t QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY ts DESC) = 1",
		},
		"row_number as a surrogate id": {src: "SELECT ROW_NUMBER() OVER () AS rid, * FROM p.d.t"},
		"row_number not filtered": {
			src: "SELECT *, ROW_NUMBER() OVER (PARTITION BY id) AS rn FROM p.d.t",
		},
		"exact duplicates by every column": {
			src: "SELECT * FROM p.d.t QUALIFY ROW_NUMBER() OVER (PARTITION BY id, ts, v) = 1",
		},
		"exact duplicates by the whole row": {
			src: "SELECT * FROM p.d.t AS t QUALIFY ROW_NUMBER() OVER (PARTITION BY TO_JSON_STRING(t)) = 1",
		},
		"only partition columns consumed": {
			src: "SELECT id FROM p.d.t QUALIFY ROW_NUMBER() OVER (PARTITION BY id) = 1",
		},
		"exact duplicates in a cte": {
			src: "WITH d AS (SELECT * FROM p.d.t QUALIFY ROW_NUMBER() OVER (PARTITION BY id, ts, v) = 1) SELECT * FROM d",
		},
		"only partition columns consumed through a cte": {
			src: "WITH d AS (SELECT * FROM p.d.t QUALIFY ROW_NUMBER() OVER (PARTITION BY id) = 1) SELECT id FROM d",
		},
		"order insensitive analytic": {
			src: "SELECT * FROM p.d.t QUALIFY COUNT(*) OVER (PARTITION BY id) > 1",
		},
		"array_agg with order by and limit": {
			src: "SELECT id, ARRAY_AGG(v ORDER BY ts DESC LIMIT 1)[OFFSET(0)] FROM p.d.t GROUP BY id",
		},
		"array_agg without limit": {src: "SELECT id, ARRAY_AGG(v) FROM p.d.t GROUP BY id"},
		"string_agg without limit": {
			src: "SELECT STRING_AGG(CAST(v AS STRING), ',') FROM p.d.t",
		},
		"array_agg of a grouping key": {
			src: "SELECT ARRAY_AGG(id LIMIT 1)[OFFSET(0)] FROM p.d.t GROUP BY id",
		},
		"array_agg of exact duplicates": {
			src: "SELECT ARRAY_AGG(t LIMIT 1)[OFFSET(0)] FROM p.d.t AS t GROUP BY TO_JSON_STRING(t)",
		},
		"analytic array_agg with order by": {
			src: "SELECT (ARRAY_AGG(v) OVER (PARTITION BY id ORDER BY ts))[OFFSET(0)] FROM p.d.t",
		},
		"array literal offset": {src: "SELECT [v, id][OFFSET(0)] FROM p.d.t"},
		"array subquery offset": {
			src: "SELECT ARRAY(SELECT v FROM p.d.t)[OFFSET(0)] AS a",
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			parsed := parseEvents(t, tt.src)
			got := lint.Run(t.Context(), parsed, lint.Config{Rules: map[string]bool{"missing-order-by": true}}, lint.Env{}, lint.MissingOrderBy)

			var gotReports []report
			for _, d := range got {
				if d.Rule != "missing-order-by" || d.Severity != lint.SeverityWarning {
					t.Errorf("unexpected diagnostic metadata: %+v", d)
				}
				if len(d.SuggestedFixes) != 0 {
					t.Errorf("unexpected fixes: %+v", d.SuggestedFixes)
				}
				gotReports = append(gotReports, report{text: textInRange(tt.src, d), message: d.Message})
			}
			if len(gotReports) != len(tt.want) {
				t.Fatalf("got %+v, want %+v", gotReports, tt.want)
			}
			for i := range tt.want {
				if gotReports[i] != tt.want[i] {
					t.Errorf("report %d = %+v, want %+v", i, gotReports[i], tt.want[i])
				}
			}
		})
	}
}

func TestMissingOrderBySuppressed(t *testing.T) {
	src := "SELECT *\nFROM p.d.t\n-- bqls:ignore missing-order-by\nQUALIFY ROW_NUMBER() OVER (PARTITION BY id) = 1"
	parsed := parseEvents(t, src)
	cfg := lint.Config{Rules: map[string]bool{"missing-order-by": true}}
	if got := lint.Run(t.Context(), parsed, cfg, lint.Env{}, lint.MissingOrderBy); len(got) != 0 {
		t.Errorf("got %+v, want suppressed", got)
	}
}

func TestMissingOrderByConfig(t *testing.T) {
	src := "SELECT * FROM p.d.t QUALIFY ROW_NUMBER() OVER (PARTITION BY id) = 1"
	parsed := parseEvents(t, src)
	if got := lint.Run(t.Context(), parsed, lint.Config{}, lint.Env{}, lint.MissingOrderBy); len(got) != 0 {
		t.Errorf("rule should be disabled by default, got %+v", got)
	}
	on := lint.Config{Rules: map[string]bool{"missing-order-by": true}}
	if got := lint.Run(t.Context(), parsed, on, lint.Env{}, lint.MissingOrderBy); len(got) != 1 || !strings.Contains(got[0].Message, "ROW_NUMBER") {
		t.Errorf("enabled rule should report, got %+v", got)
	}
}
