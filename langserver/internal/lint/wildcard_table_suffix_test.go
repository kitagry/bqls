package lint_test

import (
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/kitagry/bqls/langserver/internal/lint"
	"github.com/kitagry/bqls/langserver/internal/lsp"
)

func wildcardTableSuffix(r lsp.Range, table string) lint.Diagnostic {
	return lint.Diagnostic{
		Rule:     "wildcard-table-suffix",
		Range:    r,
		Message:  fmt.Sprintf("Query on wildcard table `%s` has no filter on `_TABLE_SUFFIX`; it scans every table matching the wildcard", table),
		Severity: lint.SeverityWarning,
	}
}

func TestWildcardTableSuffix(t *testing.T) {
	type report struct {
		// sub is the reported source text and n its occurrence index.
		sub string
		n   int
	}
	tests := map[string]struct {
		src  string
		want []report
	}{
		"unfiltered": {
			src:  "SELECT x FROM `p.d.ev_*`",
			want: []report{{sub: "`p.d.ev_*`"}},
		},
		"unfiltered COUNT(*) still scans every table": {
			src:  "SELECT COUNT(*) FROM `p.d.ev_*`",
			want: []report{{sub: "`p.d.ev_*`"}},
		},
		"unfiltered dataset-qualified table": {
			src:  "SELECT x FROM `d.ev_*`",
			want: []report{{sub: "`d.ev_*`"}},
		},
		"filter on a different column": {
			src:  "SELECT x FROM `p.d.ev_*` WHERE y = 1",
			want: []report{{sub: "`p.d.ev_*`"}},
		},
		"_TABLE_SUFFIX only selected": {
			src:  "SELECT _TABLE_SUFFIX AS s, x FROM `p.d.ev_*`",
			want: []report{{sub: "`p.d.ev_*`"}},
		},
		"CTE unfiltered": {
			src:  "WITH c AS (SELECT x, _TABLE_SUFFIX AS s FROM `p.d.ev_*`) SELECT x FROM c WHERE x = 1",
			want: []report{{sub: "`p.d.ev_*`"}},
		},
		"same table twice with only one filtered": {
			src:  "SELECT a.x FROM `p.d.ev_*` a JOIN `p.d.ev_*` b USING (x) WHERE a._TABLE_SUFFIX = '20200101'",
			want: []report{{sub: "`p.d.ev_*`", n: 1}},
		},
		"UNION ALL with one branch filtered": {
			src:  "SELECT x FROM `p.d.ev_*` UNION ALL SELECT x FROM `p.d.ev_*` WHERE _TABLE_SUFFIX = '20200101'",
			want: []report{{sub: "`p.d.ev_*`"}},
		},
		"filtered by WHERE": {
			src: "SELECT x FROM `p.d.ev_*` WHERE _TABLE_SUFFIX = '20200101'",
		},
		"filtered by BETWEEN": {
			src: "SELECT x FROM `p.d.ev_*` WHERE _TABLE_SUFFIX BETWEEN '20200101' AND '20200107'",
		},
		"filtered by a function of _TABLE_SUFFIX": {
			src: "SELECT x FROM `p.d.ev_*` WHERE PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) > '2020-01-01'",
		},
		"filtered by JOIN ON": {
			src: "SELECT a.x FROM `p.d.ev_*` a JOIN p.d.plain b ON a._TABLE_SUFFIX = FORMAT_DATE('%Y%m%d', b.dt)",
		},
		"filtered through a CTE column alias": {
			src: "WITH c AS (SELECT _TABLE_SUFFIX AS s, x FROM `p.d.ev_*`) SELECT x FROM c WHERE s = '20200101'",
		},
		"filtered inside a subquery": {
			src: "SELECT * FROM (SELECT x FROM `p.d.ev_*` WHERE _TABLE_SUFFIX = '20200101')",
		},
		"filtered outside a subquery": {
			src: "SELECT x FROM (SELECT x, _TABLE_SUFFIX AS s FROM `p.d.ev_*`) WHERE s = '20200101'",
		},
		"filter on one side of OR counts": {
			src: "SELECT x FROM `p.d.ev_*` WHERE _TABLE_SUFFIX = '20200101' OR x = 1",
		},
		"correlated filter in EXISTS": {
			src: "SELECT x FROM p.d.plain pl WHERE EXISTS(SELECT 1 FROM `p.d.ev_*` e WHERE e._TABLE_SUFFIX = FORMAT_DATE('%Y%m%d', pl.dt))",
		},
		"filtered in HAVING after grouping": {
			src: "SELECT _TABLE_SUFFIX AS s, COUNT(*) AS c FROM `p.d.ev_*` GROUP BY s HAVING s = '20200101'",
		},
		"non-wildcard table": {
			src: "SELECT x FROM p.d.plain",
		},
		"partitioned non-wildcard table": {
			src: "SELECT x FROM p.d.by_col WHERE dt = '2020-01-01'",
		},
		"INFORMATION_SCHEMA": {
			src: "SELECT table_name FROM p.d.INFORMATION_SCHEMA.TABLES",
		},
		"CREATE VIEW defers filtering to the view's queries": {
			src: "CREATE VIEW p.d.v AS SELECT x FROM `p.d.ev_*`",
		},
		"unused CTE": {
			src: "WITH c AS (SELECT * FROM `p.d.ev_*`) SELECT 1 AS y",
		},
		"suppressed by comment": {
			src: "-- bqls:ignore wildcard-table-suffix\nSELECT x FROM `p.d.ev_*`",
		},
	}

	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			var want []lint.Diagnostic
			for _, r := range tt.want {
				want = append(want, wildcardTableSuffix(spanOf(t, tt.src, r.sub, r.n), "p.d.ev_*"))
			}
			got := lint.Run(t.Context(), parsePartitioned(t, tt.src), lint.Config{}, lint.Env{}, lint.WildcardTableSuffix)
			if diff := cmp.Diff(want, got); diff != "" {
				t.Errorf("diff (-want +got):\n%s", diff)
			}
		})
	}
}
