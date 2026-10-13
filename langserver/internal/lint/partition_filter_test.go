package lint_test

import (
	"context"
	"errors"
	"fmt"
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

var partitionedTables = map[string]*bq.TableMetadata{
	"p.d.by_col": {
		Type:             bq.RegularTable,
		TimePartitioning: &bq.TimePartitioning{Type: bq.DayPartitioningType, Field: "dt"},
		Schema:           bq.Schema{{Name: "x", Type: bq.IntegerFieldType}, {Name: "dt", Type: bq.DateFieldType}},
	},
	"p.d.required": {
		Type:                   bq.RegularTable,
		TimePartitioning:       &bq.TimePartitioning{Type: bq.DayPartitioningType, Field: "dt"},
		RequirePartitionFilter: true,
		Schema:                 bq.Schema{{Name: "x", Type: bq.IntegerFieldType}, {Name: "dt", Type: bq.DateFieldType}},
	},
	"p.d.ingest": {
		Type:             bq.RegularTable,
		TimePartitioning: &bq.TimePartitioning{Type: bq.DayPartitioningType},
		Schema:           bq.Schema{{Name: "x", Type: bq.IntegerFieldType}},
	},
	"p.d.required_nested": {
		Type:             bq.RegularTable,
		TimePartitioning: &bq.TimePartitioning{Type: bq.DayPartitioningType, Field: "dt", RequirePartitionFilter: true},
		Schema:           bq.Schema{{Name: "x", Type: bq.IntegerFieldType}, {Name: "dt", Type: bq.DateFieldType}},
	},
	"p.d.ingest_hourly": {
		Type:             bq.RegularTable,
		TimePartitioning: &bq.TimePartitioning{Type: bq.HourPartitioningType},
		Schema:           bq.Schema{{Name: "x", Type: bq.IntegerFieldType}},
	},
	"p.d.ts_col": {
		Type:             bq.RegularTable,
		TimePartitioning: &bq.TimePartitioning{Type: bq.DayPartitioningType, Field: "ts"},
		Schema:           bq.Schema{{Name: "x", Type: bq.IntegerFieldType}, {Name: "ts", Type: bq.TimestampFieldType}},
	},
	"p.d.ranged": {
		Type:              bq.RegularTable,
		RangePartitioning: &bq.RangePartitioning{Field: "id", Range: &bq.RangePartitioningRange{Start: 0, End: 100, Interval: 10}},
		Schema:            bq.Schema{{Name: "id", Type: bq.IntegerFieldType}, {Name: "x", Type: bq.IntegerFieldType}},
	},
	"p.d.plain": {
		Type:   bq.RegularTable,
		Schema: bq.Schema{{Name: "x", Type: bq.IntegerFieldType}, {Name: "dt", Type: bq.DateFieldType}},
	},
	"p.d.mview": {
		Type:             bq.MaterializedView,
		TimePartitioning: &bq.TimePartitioning{Type: bq.DayPartitioningType, Field: "dt"},
		Schema:           bq.Schema{{Name: "x", Type: bq.IntegerFieldType}, {Name: "dt", Type: bq.DateFieldType}},
	},
	"p.d.by_col*": {
		Type:             bq.RegularTable,
		TimePartitioning: &bq.TimePartitioning{Type: bq.DayPartitioningType, Field: "dt"},
		Schema:           bq.Schema{{Name: "x", Type: bq.IntegerFieldType}, {Name: "dt", Type: bq.DateFieldType}},
	},
}

func lookupPartitionedTable(_ context.Context, path string) (*bq.TableMetadata, error) {
	m, ok := partitionedTables[path]
	if !ok {
		return nil, fmt.Errorf("table %s not found", path)
	}
	return m, nil
}

func parsePartitioned(t *testing.T, src string) file.ParsedFile {
	t.Helper()
	ctrl := gomock.NewController(t)
	bqClient := mock_bigquery.NewMockClient(ctrl)
	bqClient.EXPECT().GetDefaultProject().Return("p").AnyTimes()
	bqClient.EXPECT().GetTableMetadata(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, project, dataset, table string) (*bq.TableMetadata, error) {
			return lookupPartitionedTable(ctx, project+"."+dataset+"."+table)
		}).AnyTimes()
	parsed := file.NewAnalyzer(logrus.New(), bqClient).ParseFile("file:///a.sql", src)
	if len(parsed.Errors) > 0 {
		t.Fatalf("unexpected analysis errors: %v", parsed.Errors)
	}
	return parsed
}

// spanOf returns the range of the n-th (0-based) occurrence of sub in src.
func spanOf(t *testing.T, src, sub string, n int) lsp.Range {
	t.Helper()
	start := -1
	for i, from := 0, 0; i <= n; i++ {
		idx := strings.Index(src[from:], sub)
		if idx < 0 {
			t.Fatalf("occurrence %d of %q not found in %q", n, sub, src)
		}
		start = from + idx
		from = start + len(sub)
	}
	pos := func(off int) lsp.Position {
		line := strings.Count(src[:off], "\n")
		return lsp.Position{Line: line, Character: off - (strings.LastIndex(src[:off], "\n") + 1)}
	}
	return lsp.Range{Start: pos(start), End: pos(start + len(sub))}
}

func partitionFilter(r lsp.Range, table, columns, consequence string) lint.Diagnostic {
	return lint.Diagnostic{
		Rule:     "partition-filter",
		Range:    r,
		Message:  fmt.Sprintf("Query on partitioned table `%s` has no filter on partition column %s; %s", table, columns, consequence),
		Severity: lint.SeverityWarning,
	}
}

func TestPartitionFilter(t *testing.T) {
	const mayScanAll = "it may scan all partitions"
	const reject = "BigQuery will reject the query because the table requires a partition filter"
	type report struct {
		// sub is the reported source text and n its occurrence index.
		sub     string
		n       int
		table   string
		columns string
		// consequence defaults to mayScanAll.
		consequence string
	}
	tests := map[string]struct {
		src  string
		want []report
	}{
		"unfiltered": {
			src:  "SELECT x FROM p.d.by_col",
			want: []report{{sub: "p.d.by_col", table: "p.d.by_col", columns: "`dt`"}},
		},
		"filtered by WHERE": {
			src: "SELECT x FROM p.d.by_col WHERE dt = '2020-01-01'",
		},
		"filter on a different column": {
			src:  "SELECT x FROM p.d.by_col WHERE x = 1",
			want: []report{{sub: "p.d.by_col", table: "p.d.by_col", columns: "`dt`"}},
		},
		"CROSS JOIN without a condition": {
			src:  "SELECT a.x, b.x AS bx FROM p.d.plain a CROSS JOIN p.d.by_col b",
			want: []report{{sub: "p.d.by_col", table: "p.d.by_col", columns: "`dt`"}},
		},
		"filtered by JOIN ON": {
			src: "SELECT a.x FROM p.d.by_col a JOIN p.d.plain b ON a.dt = b.dt",
		},
		"filtered through a CTE column alias": {
			src: "WITH c AS (SELECT dt AS d2, x FROM p.d.by_col) SELECT x FROM c WHERE d2 = '2020-01-01'",
		},
		"filtered through a computed CTE column": {
			src: "WITH c AS (SELECT DATE_ADD(dt, INTERVAL 1 DAY) AS d2 FROM p.d.by_col) SELECT * FROM c WHERE d2 > '2020-01-01'",
		},
		"CTE unfiltered": {
			src:  "WITH c AS (SELECT x, dt FROM p.d.by_col) SELECT x FROM c WHERE x = 1",
			want: []report{{sub: "p.d.by_col", table: "p.d.by_col", columns: "`dt`"}},
		},
		"subquery filtered inside": {
			src: "SELECT * FROM (SELECT x, dt FROM p.d.by_col WHERE dt = '2020-01-01')",
		},
		"subquery filtered outside": {
			src: "SELECT x FROM (SELECT x, dt FROM p.d.by_col) WHERE dt = '2020-01-01'",
		},
		"unfiltered scalar subquery": {
			src:  "SELECT (SELECT MAX(x) FROM p.d.by_col) AS m",
			want: []report{{sub: "p.d.by_col", table: "p.d.by_col", columns: "`dt`"}},
		},
		"unfiltered scalar subquery without aggregation": {
			src:  "SELECT (SELECT x FROM p.d.by_col LIMIT 1) AS m",
			want: []report{{sub: "p.d.by_col", table: "p.d.by_col", columns: "`dt`"}},
		},
		"correlated filter in EXISTS": {
			src: "SELECT x FROM p.d.plain pl WHERE EXISTS(SELECT 1 FROM p.d.by_col b WHERE b.dt = pl.dt)",
		},
		"same table twice with only one filtered": {
			src:  "SELECT a.x FROM p.d.by_col a JOIN p.d.by_col b USING (x) WHERE a.dt = '2020-01-01'",
			want: []report{{sub: "p.d.by_col", n: 1, table: "p.d.by_col", columns: "`dt`"}},
		},
		"UNION ALL with one branch filtered": {
			src:  "SELECT x FROM p.d.by_col UNION ALL SELECT x FROM p.d.by_col WHERE dt > '2020-01-01'",
			want: []report{{sub: "p.d.by_col", table: "p.d.by_col", columns: "`dt`"}},
		},
		"filtered after UNION ALL": {
			src: "SELECT * FROM (SELECT dt FROM p.d.by_col UNION ALL SELECT dt FROM p.d.by_col) WHERE dt > '2020-01-01'",
		},
		"filtered through UNNEST": {
			src: "SELECT x FROM p.d.by_col, UNNEST([dt]) AS e WHERE e = '2020-01-01'",
		},
		// Recursive CTE columns are not linked to their terms, so the statement is skipped.
		"filtered through a recursive CTE": {
			src: "WITH RECURSIVE r AS (SELECT dt FROM p.d.by_col UNION ALL SELECT dt FROM r WHERE FALSE) SELECT * FROM r WHERE dt = '2020-01-01'",
		},
		"filtered after UNPIVOT": {
			src: "SELECT * FROM (SELECT x, dt FROM p.d.by_col) UNPIVOT(v FOR k IN (x)) WHERE dt = '2020-01-01'",
		},
		"pipe WHERE": {
			src: "FROM p.d.by_col |> WHERE dt = '2020-01-01'",
		},
		"backquoted path": {
			src:  "SELECT x FROM `p.d.by_col`",
			want: []report{{sub: "`p.d.by_col`", table: "p.d.by_col", columns: "`dt`"}},
		},
		"time travel still scans every partition": {
			src:  "SELECT x FROM p.d.by_col FOR SYSTEM_TIME AS OF CURRENT_TIMESTAMP()",
			want: []report{{sub: "p.d.by_col", table: "p.d.by_col", columns: "`dt`"}},
		},
		"require partition filter": {
			src:  "SELECT x FROM p.d.required",
			want: []report{{sub: "p.d.required", table: "p.d.required", columns: "`dt`", consequence: "BigQuery will reject the query because the table requires a partition filter"}},
		},
		"ingestion time unfiltered": {
			src:  "SELECT x FROM p.d.ingest",
			want: []report{{sub: "p.d.ingest", table: "p.d.ingest", columns: "`_PARTITIONTIME` or `_PARTITIONDATE`"}},
		},
		"ingestion time SELECT * unfiltered": {
			src:  "SELECT * FROM p.d.ingest",
			want: []report{{sub: "p.d.ingest", table: "p.d.ingest", columns: "`_PARTITIONTIME` or `_PARTITIONDATE`"}},
		},
		"ingestion time filtered by _PARTITIONTIME": {
			src: "SELECT x FROM p.d.ingest WHERE _PARTITIONTIME > '2020-01-01'",
		},
		"ingestion time filtered by _PARTITIONDATE": {
			src: "SELECT x FROM p.d.ingest WHERE _PARTITIONDATE = '2020-01-01'",
		},
		"range partitioned unfiltered": {
			src:  "SELECT x FROM p.d.ranged",
			want: []report{{sub: "p.d.ranged", table: "p.d.ranged", columns: "`id`"}},
		},
		"range partitioned filtered": {
			src: "SELECT x FROM p.d.ranged WHERE id BETWEEN 10 AND 20",
		},
		"not partitioned": {
			src: "SELECT x FROM p.d.plain",
		},
		"materialized view is skipped": {
			src: "SELECT x FROM p.d.mview",
		},
		"wildcard table is skipped": {
			src: "SELECT x FROM `p.d.by_col*`",
		},
		"CREATE VIEW defers filtering to the view's queries": {
			src: "CREATE VIEW p.d.v AS SELECT x FROM p.d.by_col",
		},
		"CREATE TABLE AS SELECT": {
			src:  "CREATE TABLE p.d.copy AS SELECT x FROM p.d.by_col",
			want: []report{{sub: "p.d.by_col", table: "p.d.by_col", columns: "`dt`"}},
		},
		"INSERT SELECT reports the source but not the target": {
			src:  "INSERT p.d.by_col (x, dt) SELECT x, dt FROM p.d.by_col",
			want: []report{{sub: "p.d.by_col", n: 1, table: "p.d.by_col", columns: "`dt`"}},
		},
		"MERGE target is skipped": {
			src: "MERGE p.d.by_col T USING (SELECT x, dt FROM p.d.by_col WHERE dt = '2020-01-01') S ON T.x = S.x WHEN MATCHED THEN DELETE",
		},
		"declared variable before the query": {
			src:  "DECLARE v INT64 DEFAULT 1;\nSELECT v AS y, x FROM p.d.by_col",
			want: []report{{sub: "p.d.by_col", table: "p.d.by_col", columns: "`dt`"}},
		},
		"each statement of a script": {
			src:  "SELECT x FROM p.d.by_col WHERE dt = '2020-01-01';\nSELECT x FROM p.d.ranged",
			want: []report{{sub: "p.d.ranged", table: "p.d.ranged", columns: "`id`"}},
		},
		"hourly ingestion time has no _PARTITIONDATE": {
			src:  "SELECT x FROM p.d.ingest_hourly",
			want: []report{{sub: "p.d.ingest_hourly", table: "p.d.ingest_hourly", columns: "`_PARTITIONTIME`"}},
		},
		"deprecated nested require partition filter": {
			src:  "SELECT x FROM p.d.required_nested",
			want: []report{{sub: "p.d.required_nested", table: "p.d.required_nested", columns: "`dt`", consequence: reject}},
		},
		// BigQuery answers these from metadata without reading any partition.
		"COUNT(*) reads no column": {
			src: "SELECT COUNT(*) FROM p.d.by_col",
		},
		"SELECT 1 reads no column": {
			src: "SELECT 1 AS one FROM p.d.by_col",
		},
		"COUNT(*) over a subquery reads no column": {
			src: "SELECT COUNT(*) FROM (SELECT x FROM p.d.by_col)",
		},
		"COUNT(*) over UNION ALL reads no column": {
			src: "SELECT COUNT(*) FROM (SELECT x FROM p.d.by_col UNION ALL SELECT x FROM p.d.plain)",
		},
		// Deduplicating or matching rows reads every column of the inputs.
		"COUNT(*) over UNION DISTINCT": {
			src:  "SELECT COUNT(*) FROM (SELECT x FROM p.d.by_col UNION DISTINCT SELECT x FROM p.d.plain)",
			want: []report{{sub: "p.d.by_col", table: "p.d.by_col", columns: "`dt`"}},
		},
		"COUNT(*) over EXCEPT DISTINCT": {
			src:  "SELECT COUNT(*) FROM (SELECT x FROM p.d.by_col EXCEPT DISTINCT SELECT x FROM p.d.plain)",
			want: []report{{sub: "p.d.by_col", table: "p.d.by_col", columns: "`dt`"}},
		},
		"COUNT(*) over INTERSECT DISTINCT": {
			src:  "SELECT COUNT(*) FROM (SELECT x FROM p.d.plain INTERSECT DISTINCT SELECT x FROM p.d.by_col)",
			want: []report{{sub: "p.d.by_col", table: "p.d.by_col", columns: "`dt`"}},
		},
		"COUNT(*) is still rejected when a partition filter is required": {
			src:  "SELECT COUNT(*) FROM p.d.required",
			want: []report{{sub: "p.d.required", table: "p.d.required", columns: "`dt`", consequence: reject}},
		},
		"COUNT(*) is still rejected when the nested flag requires a filter": {
			src:  "SELECT COUNT(*) FROM p.d.required_nested",
			want: []report{{sub: "p.d.required_nested", table: "p.d.required_nested", columns: "`dt`", consequence: reject}},
		},
		"COUNT of a column": {
			src:  "SELECT COUNT(x) FROM p.d.by_col",
			want: []report{{sub: "p.d.by_col", table: "p.d.by_col", columns: "`dt`"}},
		},
		"filtered after GROUP BY": {
			src: "SELECT * FROM (SELECT dt, COUNT(*) AS c FROM p.d.by_col GROUP BY dt) WHERE dt = '2020-01-01'",
		},
		"filtered after a window function": {
			src: "SELECT * FROM (SELECT dt, ROW_NUMBER() OVER (PARTITION BY dt) AS rn FROM p.d.by_col) WHERE dt = '2020-01-01'",
		},
		"filtered by IN subquery": {
			src: "SELECT x FROM p.d.by_col WHERE dt IN (SELECT dt FROM p.d.plain)",
		},
		"filtered by DATE of the column": {
			src: "SELECT x FROM p.d.ts_col WHERE DATE(ts) = '2020-01-01'",
		},
		"filtered by TIMESTAMP_TRUNC of the column": {
			src: "SELECT x FROM p.d.ts_col WHERE TIMESTAMP_TRUNC(ts, DAY) = '2020-01-01'",
		},
		"filtered by a query parameter": {
			src: "SELECT x FROM p.d.by_col WHERE dt = @d",
		},
		"UPDATE WHERE filters the FROM table": {
			src: "UPDATE p.d.plain t SET x = b.x FROM p.d.by_col b WHERE t.x = b.x AND b.dt = '2020-01-01' AND t.x IN (SELECT x FROM p.d.plain)",
		},
		"UPDATE FROM unfiltered": {
			src:  "UPDATE p.d.plain t SET x = b.x FROM p.d.by_col b WHERE t.x = b.x AND t.x IN (SELECT x FROM p.d.plain)",
			want: []report{{sub: "p.d.by_col", table: "p.d.by_col", columns: "`dt`"}},
		},
		"UPDATE target skipped, unfiltered subquery reported": {
			src:  "UPDATE p.d.by_col SET x = 1 WHERE x IN (SELECT x FROM p.d.by_col)",
			want: []report{{sub: "p.d.by_col", n: 1, table: "p.d.by_col", columns: "`dt`"}},
		},
		"DELETE with an IN subquery does not filter the subquery table": {
			src:  "DELETE p.d.plain t WHERE t.dt IN (SELECT dt FROM p.d.by_col) AND t.x IN (SELECT x FROM p.d.plain)",
			want: []report{{sub: "p.d.by_col", table: "p.d.by_col", columns: "`dt`"}},
		},
		"MERGE ON filters the source": {
			src: "MERGE p.d.by_col T USING p.d.by_col S ON T.x = S.x AND S.dt = '2020-01-01' WHEN MATCHED THEN DELETE",
		},
		"MERGE WHEN condition filters the source": {
			src: "MERGE p.d.by_col T USING p.d.by_col S ON T.x = S.x WHEN MATCHED AND S.dt = '2020-01-01' THEN DELETE",
		},
		"MERGE source unfiltered": {
			src:  "MERGE p.d.by_col T USING p.d.by_col S ON T.x = S.x WHEN MATCHED THEN DELETE",
			want: []report{{sub: "p.d.by_col", n: 1, table: "p.d.by_col", columns: "`dt`"}},
		},
		// BigQuery does not evaluate CTEs that are never referenced.
		"unused CTE": {
			src: "WITH c AS (SELECT * FROM p.d.by_col) SELECT 1 AS y",
		},
		"CTE used only by an unused CTE": {
			src: "WITH a AS (SELECT * FROM p.d.by_col), b AS (SELECT * FROM a) SELECT 1 AS y",
		},
		"unused nested CTE": {
			src:  "WITH a AS (SELECT x FROM p.d.ranged) SELECT * FROM (WITH b AS (SELECT * FROM p.d.by_col) SELECT * FROM a)",
			want: []report{{sub: "p.d.ranged", table: "p.d.ranged", columns: "`id`"}},
		},
		"suppressed by comment": {
			src: "-- bqls:ignore partition-filter\nSELECT x FROM p.d.by_col",
		},
	}

	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			var want []lint.Diagnostic
			for _, r := range tt.want {
				consequence := r.consequence
				if consequence == "" {
					consequence = mayScanAll
				}
				want = append(want, partitionFilter(spanOf(t, tt.src, r.sub, r.n), r.table, r.columns, consequence))
			}
			env := lint.Env{LookupTable: lookupPartitionedTable}
			got := lint.Run(t.Context(), parsePartitioned(t, tt.src), lint.Config{}, env, lint.PartitionFilter)
			if diff := cmp.Diff(want, got); diff != "" {
				t.Errorf("diff (-want +got):\n%s", diff)
			}
		})
	}
}

func TestPartitionFilter_LookupFailure(t *testing.T) {
	tests := map[string]lint.TableMetadataLookup{
		"lookup error": func(context.Context, string) (*bq.TableMetadata, error) {
			return nil, errors.New("boom")
		},
		"no metadata": func(context.Context, string) (*bq.TableMetadata, error) {
			return nil, nil
		},
		"no lookup configured": nil,
	}
	for n, lookup := range tests {
		t.Run(n, func(t *testing.T) {
			env := lint.Env{LookupTable: lookup}
			got := lint.Run(t.Context(), parsePartitioned(t, "SELECT x FROM p.d.by_col"), lint.Config{}, env, lint.PartitionFilter)
			if len(got) != 0 {
				t.Errorf("Run() = %+v, want no diagnostics", got)
			}
		})
	}
}
