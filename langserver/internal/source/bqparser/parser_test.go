package bqparser_test

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/kitagry/bqls/langserver/internal/source/bqparser"
)

func TestParse_Empty(t *testing.T) {
	root := bqparser.Parse("")
	if root.ChildCount() != 0 {
		t.Errorf("expected 0 children for empty SQL, got %d", root.ChildCount())
	}
}

func TestParse_SimpleSelect(t *testing.T) {
	root := bqparser.Parse("SELECT 1")
	if root.ChildCount() == 0 {
		t.Fatal("expected at least 1 child")
	}
	stmt := root.NamedChild(0)
	if stmt.Kind() != "select_statement" {
		t.Errorf("expected select_statement, got %s", stmt.Kind())
	}
}

func TestParse_SelectClause(t *testing.T) {
	sql := "SELECT id FROM `project.dataset.table`"
	root := bqparser.Parse(sql)
	stmt := root.NamedChild(0)

	clauses := namedChildKinds(stmt)
	if !clauses["select_clause"] {
		t.Error("expected select_clause in select_statement")
	}
	if !clauses["from_clause"] {
		t.Error("expected from_clause in select_statement")
	}
}

func TestParse_AllClauses(t *testing.T) {
	sql := "SELECT id FROM t WHERE id > 1 GROUP BY id HAVING count(*) > 1 ORDER BY id LIMIT 10 OFFSET 5"
	root := bqparser.Parse(sql)
	stmt := root.NamedChild(0)

	want := []string{"select_clause", "from_clause", "where_clause", "group_by_clause", "having_clause", "order_by_clause", "limit_clause", "offset_clause"}
	clauses := namedChildKinds(stmt)
	for _, clause := range want {
		if !clauses[clause] {
			t.Errorf("expected %s in select_statement", clause)
		}
	}
}

func TestParse_ByteRange(t *testing.T) {
	sql := "SELECT id FROM t WHERE id > 1"
	root := bqparser.Parse(sql)
	stmt := root.NamedChild(0)

	for i := uint(0); i < stmt.NamedChildCount(); i++ {
		child := stmt.NamedChild(i)
		if child.Kind() == "from_clause" {
			got := sql[child.StartByte():child.EndByte()]
			want := "FROM t"
			if got != want {
				t.Errorf("from_clause byte range: got %q, want %q", got, want)
			}
		}
		if child.Kind() == "where_clause" {
			got := sql[child.StartByte():child.EndByte()]
			want := "WHERE id > 1"
			if got != want {
				t.Errorf("where_clause byte range: got %q, want %q", got, want)
			}
		}
	}
}

// TestParse_CTE verifies that the select_statement inside a CTE body is accessible.
func TestParse_CTE(t *testing.T) {
	sql := "WITH cte AS (SELECT id FROM t) SELECT * FROM cte"
	root := bqparser.Parse(sql)

	stmt := root.NamedChild(0)
	if stmt.Kind() != "select_statement" {
		t.Fatalf("expected select_statement at root, got %s", stmt.Kind())
	}

	clauses := namedChildKinds(stmt)
	if !clauses["with_clause"] {
		t.Error("expected with_clause in select_statement")
	}

	// find the with_clause child
	var withClause *bqparser.Node
	for i := uint(0); i < stmt.NamedChildCount(); i++ {
		child := stmt.NamedChild(i)
		if child.Kind() == "with_clause" {
			withClause = child
			break
		}
	}
	if withClause == nil {
		t.Fatal("with_clause not found")
	}

	// find the cte node inside with_clause
	var cteNode *bqparser.Node
	for i := uint(0); i < withClause.NamedChildCount(); i++ {
		child := withClause.NamedChild(i)
		if child.Kind() == "cte" {
			cteNode = child
			break
		}
	}
	if cteNode == nil {
		t.Fatal("cte node not found in with_clause")
	}

	found := false
	for i := uint(0); i < cteNode.NamedChildCount(); i++ {
		if cteNode.NamedChild(i).Kind() == "select_statement" {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected select_statement inside cte node")
	}
}

// TestParse_Subquery_DoesNotLeakClauses verifies that clauses inside a subquery
// are not treated as children of the outer select_statement.
func TestParse_Subquery_DoesNotLeakClauses(t *testing.T) {
	sql := "SELECT id FROM (SELECT id FROM t WHERE id > 1) sub WHERE sub.id < 10"
	root := bqparser.Parse(sql)
	stmt := root.NamedChild(0)

	whereCount := 0
	for i := uint(0); i < stmt.NamedChildCount(); i++ {
		if stmt.NamedChild(i).Kind() == "where_clause" {
			whereCount++
		}
	}
	if whereCount != 1 {
		t.Errorf("expected 1 where_clause in outer statement, got %d", whereCount)
	}
}

// TestParse_StatementEnd verifies that a select_statement ends at ';' or at the
// END of a script block, so that statements in a script are not merged.
func TestParse_StatementEnd(t *testing.T) {
	tests := map[string]struct {
		sql  string
		want []string
	}{
		"semicolon": {
			sql:  "SELECT STRUCT(1 AS a).;",
			want: []string{"SELECT STRUCT(1 AS a)."},
		},
		"multiple statements": {
			sql:  "SELECT 1; SELECT 2 FROM t;",
			want: []string{"SELECT 1", "SELECT 2 FROM t"},
		},
		"IF block": {
			sql:  "IF true THEN\n  SELECT 1;\nEND IF;",
			want: []string{"SELECT 1"},
		},
		"END of FOR block without semicolon": {
			sql:  "FOR r IN (SELECT 1 AS a) DO\n  SELECT r.a\nEND FOR;",
			want: []string{"SELECT 1 AS a", "SELECT r.a"},
		},
		"CASE END is not a block end": {
			sql:  "SELECT CASE WHEN a THEN 1 ELSE 2 END AS b FROM t",
			want: []string{"SELECT CASE WHEN a THEN 1 ELSE 2 END AS b FROM t"},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			root := bqparser.Parse(tt.sql)
			var got []string
			for i := uint(0); i < root.NamedChildCount(); i++ {
				stmt := root.NamedChild(i)
				got = append(got, tt.sql[stmt.StartByte():stmt.EndByte()])
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("select_statement ranges mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func namedChildKinds(node *bqparser.Node) map[string]bool {
	result := map[string]bool{}
	for i := uint(0); i < node.NamedChildCount(); i++ {
		result[node.NamedChild(i).Kind()] = true
	}
	return result
}
