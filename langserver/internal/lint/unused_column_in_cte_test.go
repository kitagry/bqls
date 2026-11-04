package lint_test

import (
	"regexp"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/kitagry/bqls/langserver/internal/lint"
)

var unusedColumnMessage = regexp.MustCompile("^Column `(.+)` of CTE `(.+)` is never used$")

// reportedColumns returns each diagnostic as `cte.column`.
func reportedColumns(t *testing.T, diags []lint.Diagnostic) []string {
	t.Helper()
	var got []string
	for _, d := range diags {
		m := unusedColumnMessage.FindStringSubmatch(d.Message)
		if m == nil {
			t.Fatalf("unexpected message %q", d.Message)
		}
		got = append(got, m[2]+"."+m[1])
	}
	return got
}

func TestUnusedColumnInCTE_Detection(t *testing.T) {
	tests := map[string]struct {
		src  string
		want []string
	}{
		"column not read by the final SELECT": {
			src:  "WITH a AS (SELECT 1 AS x, 2 AS y) SELECT x FROM a",
			want: []string{"a.y"},
		},
		"every column read": {
			src:  "WITH a AS (SELECT 1 AS x, 2 AS y) SELECT x, y FROM a",
			want: nil,
		},
		"column only filtered on in WHERE": {
			src:  "WITH a AS (SELECT 1 AS x, 2 AS y) SELECT x FROM a WHERE y > 0",
			want: nil,
		},
		"column only used in JOIN ON": {
			src:  "WITH a AS (SELECT 1 AS x, 2 AS k) SELECT a.x FROM a JOIN `p.d.t` AS t ON a.k = t.x",
			want: nil,
		},
		"column only used in JOIN USING": {
			src:  "WITH a AS (SELECT 1 AS x, 2 AS y), b AS (SELECT 1 AS x, 3 AS z) SELECT y, z FROM a JOIN b USING (x)",
			want: nil,
		},
		"column only used in QUALIFY": {
			src:  "WITH a AS (SELECT 1 AS x, 2 AS y) SELECT x FROM a WHERE TRUE QUALIFY ROW_NUMBER() OVER (ORDER BY y) = 1",
			want: nil,
		},
		"column only used in GROUP BY": {
			src:  "WITH a AS (SELECT 1 AS x, 2 AS y) SELECT COUNT(x) AS n FROM a GROUP BY y",
			want: nil,
		},
		"column only used in ORDER BY": {
			src:  "WITH a AS (SELECT 1 AS x, 2 AS y) SELECT x FROM a ORDER BY y",
			want: nil,
		},
		"column only used in an aggregate": {
			src:  "WITH a AS (SELECT 1 AS x, 2 AS y) SELECT x, SUM(y) AS s FROM a GROUP BY x",
			want: nil,
		},
		"SELECT * reads every column": {
			src:  "WITH a AS (SELECT 1 AS x, 2 AS y) SELECT * FROM a",
			want: nil,
		},
		"t.* reads every column": {
			src:  "WITH a AS (SELECT 1 AS x, 2 AS y) SELECT t.* FROM a AS t",
			want: nil,
		},
		"SELECT * in a subquery whose result is not read further": {
			src:  "WITH a AS (SELECT 1 AS x, 2 AS y) SELECT x FROM (SELECT * FROM a)",
			want: nil,
		},
		"the range variable as a value reads every column": {
			src:  "WITH a AS (SELECT 1 AS x, 2 AS y) SELECT TO_JSON_STRING(t) AS j FROM a AS t",
			want: nil,
		},
		"SELECT * EXCEPT drops the column": {
			src:  "WITH a AS (SELECT 1 AS x, 2 AS y) SELECT * EXCEPT (y) FROM a",
			want: []string{"a.y"},
		},
		"input of a set operation": {
			src:  "WITH a AS (SELECT 1 AS x, 2 AS y) SELECT x, y FROM a UNION ALL SELECT 3, 4",
			want: nil,
		},
		"IN subquery": {
			src:  "WITH a AS (SELECT 1 AS x, 2 AS y) SELECT x FROM a WHERE x IN (SELECT y FROM a)",
			want: nil,
		},
		"scalar subquery": {
			src:  "WITH a AS (SELECT 1 AS x, 2 AS y) SELECT (SELECT MAX(y) FROM a) AS m",
			want: []string{"a.x"},
		},
		"references are combined": {
			src:  "WITH a AS (SELECT 1 AS x, 2 AS y, 3 AS z) SELECT x FROM a UNION ALL SELECT y FROM a",
			want: []string{"a.z"},
		},
		"column passed on to another CTE counts as used": {
			src:  "WITH a AS (SELECT 1 AS x, 2 AS y), b AS (SELECT x, y FROM a) SELECT x FROM b",
			want: []string{"b.y"},
		},
		"column read by another CTE's expression counts as used": {
			src:  "WITH a AS (SELECT 1 AS x, 2 AS y), b AS (SELECT x, y + 1 AS y2 FROM a) SELECT x FROM b",
			want: []string{"b.y2"},
		},
		"CTE in a CREATE TABLE AS SELECT": {
			src:  "CREATE TABLE `p.d.n` AS WITH a AS (SELECT 1 AS x, 2 AS y) SELECT x FROM a",
			want: []string{"a.y"},
		},
		"CTE in an INSERT": {
			src:  "INSERT INTO `p.d.t` (x) WITH a AS (SELECT 1 AS x, 2 AS y) SELECT x FROM a",
			want: []string{"a.y"},
		},
		"CTE in a CREATE VIEW": {
			src:  "CREATE VIEW `p.d.v` AS WITH a AS (SELECT 1 AS x, 2 AS y) SELECT x FROM a",
			want: []string{"a.y"},
		},
		"CTE in an EXPORT DATA": {
			src:  "EXPORT DATA OPTIONS (uri = 'gs://b/*.csv', format = 'CSV') AS WITH a AS (SELECT 1 AS x, 2 AS y) SELECT x FROM a",
			want: []string{"a.y"},
		},
		"nested WITH in a subquery": {
			src:  "SELECT x FROM (WITH a AS (SELECT 1 AS x, 2 AS y) SELECT x FROM a)",
			want: []string{"a.y"},
		},
		"column without alias": {
			src:  "WITH a AS (SELECT x, x + 1 AS y FROM `p.d.t`) SELECT y FROM a",
			want: []string{"a.x"},
		},
		"anonymous column is named by its expression": {
			src:  "WITH a AS (SELECT x, x  +  1 FROM `p.d.t`) SELECT x FROM a",
			want: []string{"a.x + 1"},
		},
		"each statement is checked on its own": {
			src:  "WITH a AS (SELECT 1 AS x, 2 AS y) SELECT x FROM a;\nWITH a AS (SELECT 1 AS x, 2 AS y) SELECT y FROM a;",
			want: []string{"a.y", "a.x"},
		},
		"pipe query drops the column": {
			src:  "WITH a AS (SELECT 1 AS x, 2 AS y) FROM a |> DROP y",
			want: []string{"a.y"},
		},
		"pipe query filters on the column": {
			src:  "WITH a AS (SELECT 1 AS x, 2 AS y, 3 AS z) FROM a |> WHERE y > 0 |> SELECT x",
			want: []string{"a.z"},
		},
		"ORDER BY and LIMIT in a CTE pass columns on": {
			src:  "WITH a AS (SELECT 1 AS x, 2 AS y), b AS (SELECT x FROM a ORDER BY y LIMIT 1) SELECT x FROM b",
			want: nil,
		},
		"shadowed CTE": {
			src:  "WITH a AS (SELECT 1 AS x, 2 AS y) SELECT x FROM (WITH a AS (SELECT 1 AS x, 2 AS z) SELECT x, z FROM a) WHERE x IN (SELECT y FROM a)",
			want: []string{"a.x"},
		},

		// Skipped
		"recursive CTE": {
			src:  "WITH RECURSIVE r AS (SELECT 1 AS n, 2 AS m UNION ALL SELECT n + 1, m FROM r WHERE n < 3) SELECT n FROM r",
			want: nil,
		},
		"non-recursive entry of a WITH RECURSIVE": {
			src:  "WITH RECURSIVE a AS (SELECT 1 AS x, 2 AS y) SELECT x FROM a",
			want: nil,
		},
		"CTE body is a set operation": {
			src:  "WITH a AS (SELECT 1 AS x, 2 AS y UNION ALL SELECT 3, 4) SELECT x FROM a",
			want: nil,
		},
		"CTE body is SELECT DISTINCT": {
			src:  "WITH a AS (SELECT DISTINCT x, x + 1 AS y FROM `p.d.t`) SELECT x FROM a",
			want: nil,
		},
		"CTE body groups by ALL": {
			src:  "WITH a AS (SELECT x AS k1, x + 1 AS k2, COUNT(*) AS n FROM `p.d.t` GROUP BY ALL) SELECT k1, n FROM a",
			want: nil,
		},
		"CTE body selects *": {
			src:  "WITH a AS (SELECT *, 2 AS y FROM `p.d.t`) SELECT y FROM a",
			want: nil,
		},
		"CTE body is SELECT AS STRUCT": {
			src:  "WITH a AS (SELECT AS STRUCT 1 AS x, 2 AS y) SELECT x FROM a",
			want: nil,
		},
		"CTE body is a pipe query": {
			src:  "WITH a AS (FROM `p.d.t` |> SELECT x, x + 1 AS y) SELECT x FROM a",
			want: nil,
		},
		"no column read, e.g. counting rows": {
			src:  "WITH a AS (SELECT 1 AS x, 2 AS y) SELECT COUNT(*) AS n FROM a",
			want: nil,
		},
		"CTE never referenced is left to unused-cte": {
			src:  "WITH a AS (SELECT 1 AS x, 2 AS y) SELECT 1 AS n",
			want: nil,
		},
		"CTE pivoted": {
			src:  "WITH a AS (SELECT 1 AS x, 2 AS y, 3 AS z, 4 AS w) SELECT one FROM a PIVOT (SUM(y) FOR x IN (1 AS one))",
			want: nil,
		},
		"CTE unpivoted": {
			src:  "WITH a AS (SELECT 1 AS x, 2 AS y, 3 AS z) SELECT x, v FROM a UNPIVOT (v FOR k IN (y))",
			want: nil,
		},
		"statement with a scan bqls does not follow": {
			src:  "WITH a AS (SELECT 1 AS x, 2 AS y) SELECT * FROM a MATCH_RECOGNIZE (ORDER BY x MEASURES COUNT(*) AS c PATTERN (A) DEFINE A AS TRUE)",
			want: nil,
		},
		"statement whose result bqls does not follow": {
			src:  "MERGE `p.d.t` AS t USING (WITH a AS (SELECT 1 AS x, 2 AS y) SELECT x FROM a) AS s ON t.x = s.x WHEN NOT MATCHED THEN INSERT (x) VALUES (s.x)",
			want: nil,
		},
		"suppressed": {
			src:  "-- bqls:ignore unused-column-in-cte\nWITH a AS (SELECT 1 AS x, 2 AS y) SELECT x FROM a",
			want: nil,
		},
	}
	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			diags := lint.Run(t.Context(), parseWithTable(t, tt.src), lint.Config{}, lint.Env{}, lint.UnusedColumnInCTE)
			if diff := cmp.Diff(tt.want, reportedColumns(t, diags)); diff != "" {
				t.Errorf("reported columns diff (-want +got):\n%s", diff)
			}
		})
	}
}

func TestUnusedColumnInCTE_Fix(t *testing.T) {
	tests := map[string]struct {
		src string
		// wantFixed is src after applying every fix; "" means no fix is offered.
		wantFixed string
	}{
		"last column": {
			src:       "WITH a AS (SELECT 1 AS x, 2 AS y) SELECT x FROM a",
			wantFixed: "WITH a AS (SELECT 1 AS x) SELECT x FROM a",
		},
		"first column": {
			src:       "WITH a AS (SELECT 1 AS x, 2 AS y) SELECT y FROM a",
			wantFixed: "WITH a AS (SELECT 2 AS y) SELECT y FROM a",
		},
		"middle column on its own line": {
			src:       "WITH a AS (\n  SELECT\n    1 AS x,\n    2 AS y,\n    3 AS z\n)\nSELECT x, z FROM a",
			wantFixed: "WITH a AS (\n  SELECT\n    1 AS x,\n    3 AS z\n)\nSELECT x, z FROM a",
		},
		"column without alias": {
			src:       "WITH a AS (SELECT x, x + 1 AS y FROM `p.d.t`) SELECT y FROM a",
			wantFixed: "WITH a AS (SELECT x + 1 AS y FROM `p.d.t`) SELECT y FROM a",
		},
		"two leading columns": {
			src:       "WITH a AS (SELECT 1 AS x, 2 AS y, 3 AS z) SELECT z FROM a",
			wantFixed: "WITH a AS (SELECT 3 AS z) SELECT z FROM a",
		},
		"two trailing columns": {
			src:       "WITH a AS (SELECT 1 AS x, 2 AS y, 3 AS z) SELECT x FROM a",
			wantFixed: "WITH a AS (SELECT 1 AS x) SELECT x FROM a",
		},
		"columns around a kept one": {
			src:       "WITH a AS (SELECT 1 AS x, 2 AS y, 3 AS z) SELECT y FROM a",
			wantFixed: "WITH a AS (SELECT 2 AS y) SELECT y FROM a",
		},
		"integer literal in HAVING is not an ordinal": {
			src:       "WITH a AS (SELECT x AS k, x + 1 AS y, COUNT(*) AS n FROM `p.d.t` GROUP BY x HAVING COUNT(*) > 1) SELECT k, n FROM a",
			wantFixed: "WITH a AS (SELECT x AS k, COUNT(*) AS n FROM `p.d.t` GROUP BY x HAVING COUNT(*) > 1) SELECT k, n FROM a",
		},
		"aggregate removed from a query with GROUP BY": {
			src:       "WITH a AS (SELECT x AS k, MAX(x) AS m FROM `p.d.t` GROUP BY x) SELECT k FROM a",
			wantFixed: "WITH a AS (SELECT x AS k FROM `p.d.t` GROUP BY x) SELECT k FROM a",
		},
		"no fix when the only aggregate of a query without GROUP BY is removed": {
			src: "WITH a AS (SELECT MAX(x) AS m, 1 AS c FROM `p.d.t`) SELECT c FROM a",
		},
		"no fix when a trailing comment follows the column": {
			src: "WITH a AS (SELECT\n  1 AS x,\n  2 AS y -- why\n) SELECT x FROM a",
		},
		"no fix when a comment line precedes the first column": {
			src: "WITH a AS (SELECT\n  -- why\n  1 AS x,\n  2 AS y\n) SELECT y FROM a",
		},
		"no fix when a comment is between the columns": {
			src: "WITH a AS (SELECT 1 AS x, /* why */ 2 AS y) SELECT x FROM a",
		},
		"no fix when GROUP BY uses the alias": {
			src: "WITH a AS (SELECT x AS k, COUNT(*) AS n FROM `p.d.t` GROUP BY k) SELECT n FROM a",
		},
		"no fix when ORDER BY uses the alias": {
			src: "WITH a AS (SELECT x, x + 1 AS y FROM `p.d.t` ORDER BY y LIMIT 1) SELECT x FROM a",
		},
		"no fix when GROUP BY uses an ordinal": {
			src: "WITH a AS (SELECT x + 1 AS y, x AS k, COUNT(*) AS n FROM `p.d.t` GROUP BY 2, 1) SELECT k, n FROM a",
		},
	}
	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			diags := lint.Run(t.Context(), parseWithTable(t, tt.src), lint.Config{}, lint.Env{}, lint.UnusedColumnInCTE)
			if len(diags) == 0 {
				t.Fatal("expected a diagnostic")
			}
			fixed := applyFixes(t, tt.src, diags)
			if tt.wantFixed == "" {
				if fixed != tt.src {
					t.Fatalf("expected no fix, got:\n%s", fixed)
				}
				return
			}
			if diff := cmp.Diff(tt.wantFixed, fixed); diff != "" {
				t.Fatalf("fixed source diff (-want +got):\n%s", diff)
			}
			if after := lint.Run(t.Context(), parseWithTable(t, fixed), lint.Config{}, lint.Env{}, lint.UnusedColumnInCTE); len(after) > 0 {
				t.Errorf("diagnostics remain after fix: %v", after)
			}
		})
	}
}
