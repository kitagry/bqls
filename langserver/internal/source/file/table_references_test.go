package file_test

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/kitagry/bqls/langserver/internal/source/file"
)

func TestTableReferences(t *testing.T) {
	tests := map[string]struct {
		src  string
		want []file.TableID
	}{
		"fully qualified table": {
			src:  "SELECT * FROM p.d.t",
			want: []file.TableID{{Project: "p", Dataset: "d", Table: "t"}},
		},
		"dataset qualified table gets the default project": {
			src:  "SELECT * FROM d.t",
			want: []file.TableID{{Project: "default", Dataset: "d", Table: "t"}},
		},
		"quoted path is split at every dot": {
			src:  "SELECT * FROM `p.d.t`",
			want: []file.TableID{{Project: "p", Dataset: "d", Table: "t"}},
		},
		"wildcard table keeps the wildcard": {
			src:  "SELECT * FROM `p.d.events_*`",
			want: []file.TableID{{Project: "p", Dataset: "d", Table: "events_*"}},
		},
		"DML target table": {
			src:  "INSERT INTO p.d.t (x) SELECT x FROM p.d.u",
			want: []file.TableID{{Project: "p", Dataset: "d", Table: "t"}, {Project: "p", Dataset: "d", Table: "u"}},
		},
		"CTE name is not a table": {
			src:  "WITH c AS (SELECT 1 AS x) SELECT * FROM c",
			want: nil,
		},
		"INFORMATION_SCHEMA view has no table metadata": {
			src:  "SELECT * FROM p.d.INFORMATION_SCHEMA.TABLES",
			want: nil,
		},
		"table referenced twice is listed once": {
			src:  "SELECT * FROM p.d.t JOIN p.d.t AS t2 USING (x)",
			want: []file.TableID{{Project: "p", Dataset: "d", Table: "t"}},
		},
		"tables of every statement of a script": {
			src:  "SELECT * FROM p.d.a;\nSELECT * FROM p.d.b;",
			want: []file.TableID{{Project: "p", Dataset: "d", Table: "a"}, {Project: "p", Dataset: "d", Table: "b"}},
		},
		"syntax error yields nothing": {
			src:  "SELECT * FROM p.d.t WHERE",
			want: nil,
		},
	}
	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			got := file.TableReferences(tt.src, func() string { return "default" })
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("TableReferences() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
