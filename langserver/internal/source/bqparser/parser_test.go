package bqparser_test

import (
	"strings"
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

func TestParse_TrailingBackslashInUnterminatedString(t *testing.T) {
	sql := `SELECT "a\`
	root := bqparser.Parse(sql)
	if root.ChildCount() == 0 {
		t.Fatal("expected at least 1 child")
	}
	if got := root.NamedChild(0).EndByte(); got != uint(len(sql)) {
		t.Errorf("select_statement end: got %d, want %d", got, len(sql))
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
		"FROM-first statement": {
			sql:  "FROM t |> WHERE x = 1;\nFROM u",
			want: []string{"FROM t |> WHERE x = 1", "FROM u"},
		},
		"FROM of another statement is not a statement start": {
			sql:  "DELETE FROM t WHERE true",
			want: nil,
		},
		"semicolon in a hash comment": {
			sql:  "SELECT 1 # ;\nFROM t",
			want: []string{"SELECT 1 # ;\nFROM t"},
		},
		"semicolon in a triple-quoted string": {
			sql:  "SELECT '''a';''' FROM t",
			want: []string{"SELECT '''a';''' FROM t"},
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

func TestFromClauseHasJoinWithoutOn(t *testing.T) {
	tests := map[string]struct {
		sql  string
		want bool
	}{
		"JOIN without ON": {
			sql:  "SELECT * FROM a JOIN b",
			want: true,
		},
		"JOIN in a hash comment": {
			sql:  "SELECT * FROM a # JOIN b\n",
			want: false,
		},
		"JOIN in a hash comment right after a word": {
			sql:  "SELECT * FROM a# JOIN b\n",
			want: false,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			stmt := bqparser.Parse(tt.sql).NamedChild(0)
			var fromClause *bqparser.Node
			for i := uint(0); i < stmt.NamedChildCount(); i++ {
				if child := stmt.NamedChild(i); child.Kind() == "from_clause" {
					fromClause = child
				}
			}
			if fromClause == nil {
				t.Fatal("from_clause not found")
			}
			if got := bqparser.FromClauseHasJoinWithoutOn(fromClause, tt.sql); got != tt.want {
				t.Errorf("FromClauseHasJoinWithoutOn() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLastPipeOperatorEnd(t *testing.T) {
	tests := map[string]struct {
		src    string
		want   uint
		wantOk bool
	}{
		"right after pipe operator": {
			src:    "FROM t |> ",
			want:   9,
			wantOk: true,
		},
		"pipe operator without spaces": {
			src:    "FROM t|>W",
			want:   8,
			wantOk: true,
		},
		"last pipe operator": {
			src:    "FROM t |> WHERE x = 1 |> SEL",
			want:   24,
			wantOk: true,
		},
		"no pipe operator": {
			src:    "SELECT * FROM t ",
			wantOk: false,
		},
		"pipe operator in a closed subquery": {
			src:    "SELECT * FROM (FROM t |> WHERE x = 1) ",
			wantOk: false,
		},
		"pipe operator in the current subquery": {
			src:    "SELECT * FROM (FROM t |> ",
			want:   24,
			wantOk: true,
		},
		"pipe operator in a previous statement": {
			src:    "FROM t |> WHERE x = 1;\nSELECT ",
			wantOk: false,
		},
		"pipe operator in a string": {
			src:    "SELECT '|>' ",
			wantOk: false,
		},
		"pipe operator in a comment": {
			src:    "SELECT 1 -- |>\n",
			wantOk: false,
		},
		"backslash at the end of an unterminated double-quoted string": {
			src:    `SELECT "a\`,
			wantOk: false,
		},
		"backslash at the end of an unterminated single-quoted string": {
			src:    `FROM t |> SELECT 'a\`,
			want:   9,
			wantOk: true,
		},
		"backslash at the end of an unterminated backtick identifier": {
			src:    "SELECT * FROM `a\\",
			wantOk: false,
		},
		"pipe operator in a hash comment": {
			src:    "SELECT 1 # |>\n",
			wantOk: false,
		},
		"pipe operator in a hash comment right after a word": {
			src:    "SELECT 1# |>\n",
			wantOk: false,
		},
		"pipe operator in a dash comment right after a word": {
			src:    "SELECT 1-- |>\n",
			wantOk: false,
		},
		"pipe operator in a block comment right after a word": {
			src:    "SELECT 1/* |> */",
			wantOk: false,
		},
		"parenthesis in a hash comment": {
			src:    "FROM t |> SELECT 1 # (\n",
			want:   9,
			wantOk: true,
		},
		"pipe operator after a hash comment": {
			src:    "SELECT 1 # comment\n|> ",
			want:   21,
			wantOk: true,
		},
		"pipe operator in a triple single-quoted string": {
			src:    "SELECT '''a'|>'''",
			wantOk: false,
		},
		"pipe operator in a triple double-quoted string": {
			src:    `SELECT """a"|>"""`,
			wantOk: false,
		},
		"pipe operator in a multi-line triple-quoted string": {
			src:    "SELECT '''a\n|>\n'''",
			wantOk: false,
		},
		"pipe operator in a raw triple-quoted string": {
			src:    "SELECT r'''a'|>'''",
			wantOk: false,
		},
		"pipe operator in a bytes triple-quoted string": {
			src:    `SELECT b"""a"|>"""`,
			wantOk: false,
		},
		"pipe operator in a raw bytes triple-quoted string": {
			src:    `SELECT rb"""a"|>"""`,
			wantOk: false,
		},
		"escaped quote in a triple-quoted string": {
			src:    `SELECT """a\"""|>"""`,
			wantOk: false,
		},
		"pipe operator in an unterminated triple-quoted string": {
			src:    "FROM t |> SELECT '''a |> ",
			want:   9,
			wantOk: true,
		},
		"backslash at the end of an unterminated triple-quoted string": {
			src:    `SELECT """a\`,
			wantOk: false,
		},
		"pipe operator after a triple-quoted string": {
			src:    "SELECT '''a''' |> ",
			want:   17,
			wantOk: true,
		},
		"pipe operator after an empty string": {
			src:    "SELECT '' |> ",
			want:   12,
			wantOk: true,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			got, ok := bqparser.LastPipeOperatorEnd(tt.src)
			if ok != tt.wantOk {
				t.Fatalf("ok: got %v, want %v", ok, tt.wantOk)
			}
			if got != tt.want {
				t.Errorf("got %d, want %d", got, tt.want)
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

func TestSelectListItemOnNextLine(t *testing.T) {
	tests := map[string]struct {
		src    string
		offset string // the error is reported at the first occurrence of this text

		wantWord string
		wantOK   bool
	}{
		"function call on the next line": {
			src:      "SELECT id,\n  cu\n  COALESCE(name, '')",
			offset:   "(",
			wantWord: "cu",
			wantOK:   true,
		},
		"pipe select": {
			src:      "FROM t |> SELECT id,\n  cu\n  'x'",
			offset:   "'x'",
			wantWord: "cu",
			wantOK:   true,
		},
		"same line": {
			src:    "SELECT id, cu COALESCE(name, '')",
			offset: "(",
		},
		"operator before the item": {
			src:    "SELECT id +\n  COALESCE(name, '')",
			offset: "(",
		},
		"from clause of a subquery": {
			src:    "SELECT * FROM (SELECT 1 FROM t\n  x\n  COALESCE(name, ''))",
			offset: "(name",
		},
		"error further into the line": {
			src:    "SELECT id\n  + b c d",
			offset: "d",
		},
		"inside a function call": {
			src:    "SELECT CONCAT(a\n  b(1))",
			offset: "(1",
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			start, end, ok := bqparser.SelectListItemOnNextLine(tt.src, uint(strings.Index(tt.src, tt.offset)))
			if ok != tt.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tt.wantOK)
			}
			if ok && tt.src[start:end] != tt.wantWord {
				t.Errorf("word = %q, want %q", tt.src[start:end], tt.wantWord)
			}
		})
	}
}
