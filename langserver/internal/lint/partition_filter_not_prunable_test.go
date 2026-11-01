package lint_test

import (
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/kitagry/bqls/langserver/internal/lint"
)

func TestPartitionFilterNotPrunable(t *testing.T) {
	const reject = "BigQuery will reject the query because the table requires a partition filter"
	const orReason = "a branch of OR does not filter on it"
	const subqueryReason = "it is compared with a subquery result, which is not a constant expression"
	const otherColumnReason = "it is compared with another column of the table"
	const arithmeticReason = "it is not isolated on one side of the comparison"
	const rangeReason = "functions over an integer-range partitioning column are not supported for pruning"
	type report struct {
		// sub is the reported source text and n its occurrence index.
		sub    string
		n      int
		table  string
		column string
		reason string
		// consequence defaults to "it may scan all partitions".
		consequence string
	}
	tests := map[string]struct {
		src  string
		want []report
	}{
		"constant comparison": {
			src: "SELECT x FROM p.d.by_col WHERE dt = '2020-01-01'",
		},
		"OR with another column": {
			src:  "SELECT x FROM p.d.by_col WHERE dt = '2020-01-01' OR x = 1",
			want: []report{{sub: "dt = '2020-01-01' OR x = 1", table: "p.d.by_col", column: "dt", reason: orReason}},
		},
		"OR whose branches all filter the partition column": {
			src: "SELECT x FROM p.d.by_col WHERE dt = '2020-01-01' OR (dt = '2020-02-01' AND x = 1)",
		},
		"OR with a constant branch": {
			src: "SELECT x FROM p.d.by_col WHERE dt = '2020-01-01' OR @all",
		},
		"scalar subquery": {
			src:  "SELECT x FROM p.d.by_col WHERE dt = (SELECT MAX(dt) FROM p.d.plain)",
			want: []report{{sub: "dt = (SELECT MAX(dt) FROM p.d.plain)", table: "p.d.by_col", column: "dt", reason: subqueryReason}},
		},
		"scalar subquery on the left": {
			src:  "SELECT x FROM p.d.by_col WHERE (SELECT MAX(dt) FROM p.d.plain) <= dt",
			want: []report{{sub: "(SELECT MAX(dt) FROM p.d.plain) <= dt", table: "p.d.by_col", column: "dt", reason: subqueryReason}},
		},
		"correlated scalar subquery": {
			src:  "SELECT x FROM p.d.by_col t WHERE dt = (SELECT MAX(dt) FROM p.d.plain p WHERE p.x = t.x)",
			want: []report{{sub: "dt = (SELECT MAX(dt) FROM p.d.plain p WHERE p.x = t.x)", table: "p.d.by_col", column: "dt", reason: subqueryReason}},
		},
		"correlated filter in EXISTS is not judged": {
			src: "SELECT x FROM p.d.plain pl WHERE EXISTS(SELECT 1 FROM p.d.by_col b WHERE b.dt = pl.dt)",
		},
		"QUALIFY over the partition column is not judged": {
			src: "SELECT x FROM p.d.by_col WHERE TRUE QUALIFY ROW_NUMBER() OVER (PARTITION BY dt ORDER BY x) = 1",
		},
		"IN subquery is not judged": {
			src: "SELECT x FROM p.d.by_col WHERE dt IN (SELECT dt FROM p.d.plain)",
		},
		"another column of the same table": {
			src:  "SELECT ts FROM p.d.ts_pair WHERE ts >= ts2",
			want: []report{{sub: "ts >= ts2", table: "p.d.ts_pair", column: "ts", reason: otherColumnReason}},
		},
		"combined with another column": {
			src:  "SELECT x FROM p.d.ingest WHERE TIMESTAMP_ADD(_PARTITIONTIME, INTERVAL x DAY) = TIMESTAMP '2020-01-01'",
			want: []report{{sub: "TIMESTAMP_ADD(_PARTITIONTIME, INTERVAL x DAY) = TIMESTAMP '2020-01-01'", table: "p.d.ingest", column: "_PARTITIONTIME", reason: otherColumnReason}},
		},
		"join condition with another table is not judged": {
			src: "SELECT a.x FROM p.d.by_col a JOIN p.d.plain b ON a.dt = b.dt",
		},
		"arithmetic on the column": {
			src:  "SELECT x FROM p.d.ts_col WHERE ts + INTERVAL 1 DAY > CURRENT_TIMESTAMP()",
			want: []report{{sub: "ts + INTERVAL 1 DAY > CURRENT_TIMESTAMP()", table: "p.d.ts_col", column: "ts", reason: arithmeticReason}},
		},
		"EXTRACT MONTH": {
			src:  "SELECT x FROM p.d.by_col WHERE EXTRACT(MONTH FROM dt) = 3",
			want: []report{{sub: "EXTRACT(MONTH FROM dt) = 3", table: "p.d.by_col", column: "dt", reason: "EXTRACT(MONTH) is not supported for pruning"}},
		},
		"EXTRACT YEAR": {
			src: "SELECT x FROM p.d.by_col WHERE EXTRACT(YEAR FROM dt) = 2020",
		},
		"FORMAT_TIMESTAMP with an unsupported format": {
			src:  "SELECT x FROM p.d.ts_col WHERE FORMAT_TIMESTAMP('%Y-%m', ts) = '2020-01'",
			want: []report{{sub: "FORMAT_TIMESTAMP('%Y-%m', ts) = '2020-01'", table: "p.d.ts_col", column: "ts", reason: "FORMAT_TIMESTAMP with format '%Y-%m' is not supported for pruning"}},
		},
		"FORMAT_DATE with an unsupported format": {
			src:  "SELECT x FROM p.d.by_col WHERE FORMAT_DATE('%Y%m', dt) = '202001'",
			want: []report{{sub: "FORMAT_DATE('%Y%m', dt) = '202001'", table: "p.d.by_col", column: "dt", reason: "FORMAT_DATE with format '%Y%m' is not supported for pruning"}},
		},
		"FORMAT_TIMESTAMP with a supported format": {
			src: "SELECT x FROM p.d.ts_col WHERE FORMAT_TIMESTAMP('%Y%m%d', ts) = '20200101'",
		},
		"FORMAT_TIMESTAMP with a parameter format": {
			src: "SELECT x FROM p.d.ts_col WHERE FORMAT_TIMESTAMP(@f, ts) = '20200101'",
		},
		"function over an integer-range column": {
			src:  "SELECT x FROM p.d.ranged WHERE MOD(id, 10) = 0",
			want: []report{{sub: "MOD(id, 10) = 0", table: "p.d.ranged", column: "id", reason: rangeReason}},
		},
		"integer-range BETWEEN": {
			src: "SELECT x FROM p.d.ranged WHERE id BETWEEN 10 AND 20",
		},
		"DATE of a timestamp column": {
			src: "SELECT x FROM p.d.ts_col WHERE DATE(ts) = '2020-01-01'",
		},
		"TIMESTAMP_TRUNC": {
			src: "SELECT x FROM p.d.ts_col WHERE TIMESTAMP_TRUNC(ts, MONTH) >= '2020-04-01'",
		},
		"TIMESTAMP_DIFF": {
			src: "SELECT x FROM p.d.ts_col WHERE TIMESTAMP_DIFF(ts, '2020-01-01', DAY) < 1",
		},
		"CURRENT_TIMESTAMP": {
			src: "SELECT x FROM p.d.ts_col WHERE ts = CURRENT_TIMESTAMP()",
		},
		"query parameter": {
			src: "SELECT x FROM p.d.by_col WHERE dt = @d",
		},
		"CAST is not documented and not judged": {
			src: "SELECT x FROM p.d.by_col WHERE CAST(dt AS STRING) = '2020-01-01'",
		},
		"another prunable term on the column": {
			src: "SELECT x FROM p.d.by_col WHERE dt >= '2020-01-01' AND EXTRACT(MONTH FROM dt) = 3",
		},
		"every term unprunable": {
			src: "SELECT x FROM p.d.by_col WHERE EXTRACT(MONTH FROM dt) = 3 AND dt = (SELECT MAX(dt) FROM p.d.plain)",
			want: []report{
				{sub: "EXTRACT(MONTH FROM dt) = 3", table: "p.d.by_col", column: "dt", reason: "EXTRACT(MONTH) is not supported for pruning"},
				{sub: "dt = (SELECT MAX(dt) FROM p.d.plain)", table: "p.d.by_col", column: "dt", reason: subqueryReason},
			},
		},
		"no filter is left to partition-filter": {
			src: "SELECT x FROM p.d.by_col",
		},
		"through a CTE alias": {
			src:  "WITH c AS (SELECT dt AS d2, x FROM p.d.by_col) SELECT x FROM c WHERE EXTRACT(MONTH FROM d2) = 3",
			want: []report{{sub: "EXTRACT(MONTH FROM d2) = 3", table: "p.d.by_col", column: "dt", reason: "EXTRACT(MONTH) is not supported for pruning"}},
		},
		"computed CTE column is not judged": {
			src: "WITH c AS (SELECT DATE_ADD(dt, INTERVAL 1 DAY) AS d2, x FROM p.d.by_col) SELECT x FROM c WHERE EXTRACT(MONTH FROM d2) = 3",
		},
		"filtered both inside and outside a CTE": {
			src: "WITH c AS (SELECT dt, x FROM p.d.by_col WHERE dt > '2020-01-01') SELECT x FROM c WHERE EXTRACT(MONTH FROM dt) = 3",
		},
		"ingestion-time pseudo column": {
			src:  "SELECT x FROM p.d.ingest WHERE EXTRACT(DAY FROM _PARTITIONDATE) = 1",
			want: []report{{sub: "EXTRACT(DAY FROM _PARTITIONDATE) = 1", table: "p.d.ingest", column: "_PARTITIONDATE", reason: "EXTRACT(DAY) is not supported for pruning"}},
		},
		"table requires a partition filter": {
			src:  "SELECT x FROM p.d.required WHERE EXTRACT(MONTH FROM dt) = 3",
			want: []report{{sub: "EXTRACT(MONTH FROM dt) = 3", table: "p.d.required", column: "dt", reason: "EXTRACT(MONTH) is not supported for pruning", consequence: reject}},
		},
		"DML target is skipped": {
			src: "DELETE p.d.by_col WHERE EXTRACT(MONTH FROM dt) = 3",
		},
		"view definition is skipped": {
			src: "CREATE VIEW p.d.v AS SELECT x FROM p.d.by_col WHERE EXTRACT(MONTH FROM dt) = 3",
		},
		"not partitioned": {
			src: "SELECT x FROM p.d.plain WHERE EXTRACT(MONTH FROM dt) = 3",
		},
		"suppressed by comment": {
			src: "-- bqls:ignore partition-filter-not-prunable\nSELECT x FROM p.d.by_col WHERE EXTRACT(MONTH FROM dt) = 3",
		},
	}

	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			var want []lint.Diagnostic
			for _, r := range tt.want {
				consequence := r.consequence
				if consequence == "" {
					consequence = "it may scan all partitions"
				}
				want = append(want, lint.Diagnostic{
					Rule:     "partition-filter-not-prunable",
					Range:    spanOf(t, tt.src, r.sub, r.n),
					Message:  fmt.Sprintf("Filter on partition column `%s` of `%s` cannot prune partitions because %s; %s", r.column, r.table, r.reason, consequence),
					Severity: lint.SeverityWarning,
				})
			}
			env := lint.Env{LookupTable: lookupPartitionedTable}
			cfg := lint.Config{Rules: map[string]bool{"partition-filter-not-prunable": true}}
			got := lint.Run(t.Context(), parsePartitioned(t, tt.src), cfg, env, lint.PartitionFilterNotPrunable)
			if diff := cmp.Diff(want, got); diff != "" {
				t.Errorf("diff (-want +got):\n%s", diff)
			}
		})
	}
}
