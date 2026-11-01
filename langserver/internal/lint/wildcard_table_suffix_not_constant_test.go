package lint_test

import (
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/kitagry/bqls/langserver/internal/lint"
)

func TestWildcardTableSuffixNotConstant(t *testing.T) {
	const orReason = "a branch of OR does not filter on it"
	const subqueryReason = "it is compared with a subquery result, which is not a constant expression"
	const otherColumnReason = "it is compared with another column of the table"
	type report struct {
		// sub is the reported source text and n its occurrence index.
		sub    string
		n      int
		reason string
		// table defaults to p.d.ev_*.
		table string
	}
	tests := map[string]struct {
		src  string
		want []report
	}{
		"constant comparison": {
			src: "SELECT x FROM `p.d.ev_*` WHERE _TABLE_SUFFIX = '20200101'",
		},
		"constant BETWEEN": {
			src: "SELECT x FROM `p.d.ev_*` WHERE _TABLE_SUFFIX BETWEEN '20200101' AND '20200131'",
		},
		"scalar subquery": {
			src:  "SELECT x FROM `p.d.ev_*` WHERE _TABLE_SUFFIX = (SELECT MAX(CAST(x AS STRING)) FROM p.d.plain)",
			want: []report{{sub: "_TABLE_SUFFIX = (SELECT MAX(CAST(x AS STRING)) FROM p.d.plain)", reason: subqueryReason}},
		},
		"constant term limits the scan": {
			src: "SELECT x FROM `p.d.ev_*` WHERE _TABLE_SUFFIX BETWEEN '40' AND '60' AND _TABLE_SUFFIX = (SELECT MAX(CAST(x AS STRING)) FROM p.d.plain)",
		},
		"another column of the same table": {
			src:  "SELECT x FROM `p.d.ev_*` WHERE _TABLE_SUFFIX = CAST(x AS STRING)",
			want: []report{{sub: "_TABLE_SUFFIX = CAST(x AS STRING)", reason: otherColumnReason}},
		},
		"OR with another column": {
			src:  "SELECT x FROM `p.d.ev_*` WHERE _TABLE_SUFFIX = '0' OR y = 1",
			want: []report{{sub: "_TABLE_SUFFIX = '0' OR y = 1", reason: orReason}},
		},
		"OR whose branches all filter the suffix": {
			src: "SELECT x FROM `p.d.ev_*` WHERE _TABLE_SUFFIX = '0' OR _TABLE_SUFFIX = '4'",
		},
		"function over the suffix is not judged": {
			src: "SELECT x FROM `p.d.ev_*` WHERE PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) >= '2020-01-01'",
		},
		"IN subquery is not judged": {
			src: "SELECT x FROM `p.d.ev_*` WHERE _TABLE_SUFFIX IN (SELECT CAST(x AS STRING) FROM p.d.plain)",
		},
		"through a CTE alias": {
			src:  "WITH c AS (SELECT x, _TABLE_SUFFIX AS s FROM `p.d.ev_*`) SELECT x FROM c WHERE s = (SELECT MAX(CAST(x AS STRING)) FROM p.d.plain)",
			want: []report{{sub: "s = (SELECT MAX(CAST(x AS STRING)) FROM p.d.plain)", reason: subqueryReason}},
		},
		"no filter is left to wildcard-table-suffix": {
			src: "SELECT x FROM `p.d.ev_*`",
		},
		"a filter on another column does not limit the tables": {
			src:  "SELECT x FROM `p.d.by_col*` WHERE _TABLE_SUFFIX = (SELECT MAX(CAST(x AS STRING)) FROM p.d.plain) AND dt = '2020-01-01'",
			want: []report{{sub: "_TABLE_SUFFIX = (SELECT MAX(CAST(x AS STRING)) FROM p.d.plain)", reason: subqueryReason, table: "p.d.by_col*"}},
		},
		"suppressed by comment": {
			src: "-- bqls:ignore wildcard-table-suffix-not-constant\nSELECT x FROM `p.d.ev_*` WHERE _TABLE_SUFFIX = (SELECT MAX(CAST(x AS STRING)) FROM p.d.plain)",
		},
	}

	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			var want []lint.Diagnostic
			for _, r := range tt.want {
				table := r.table
				if table == "" {
					table = "p.d.ev_*"
				}
				want = append(want, lint.Diagnostic{
					Rule:     "wildcard-table-suffix-not-constant",
					Range:    spanOf(t, tt.src, r.sub, r.n),
					Message:  fmt.Sprintf("Filter on `_TABLE_SUFFIX` of wildcard table `%s` does not limit the tables scanned because %s; it scans every table matching the wildcard", table, r.reason),
					Severity: lint.SeverityWarning,
				})
			}
			env := lint.Env{LookupTable: lookupPartitionedTable}
			cfg := lint.Config{Rules: map[string]bool{"wildcard-table-suffix-not-constant": true}}
			got := lint.Run(t.Context(), parsePartitioned(t, tt.src), cfg, env, lint.WildcardTableSuffixNotConstant)
			if diff := cmp.Diff(want, got); diff != "" {
				t.Errorf("diff (-want +got):\n%s", diff)
			}
		})
	}
}
