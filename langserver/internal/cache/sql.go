package cache

import (
	"github.com/goccy/go-zetasql"
	"github.com/goccy/go-zetasql/ast"
)

type SQL struct {
	RawText string
	Node    ast.Node
	Errors  []error
}

func NewSQL(rawText string) *SQL {
	sql := &SQL{}
	sql.RawText = rawText

	node, err := zetasql.ParseScript(rawText, zetasql.NewParserOptions(), zetasql.ErrorMessageOneLine)
	if err != nil {
		sql.Errors = []error{err}
		return sql
	}

	sql.Node = node
	return sql
}

func (s *SQL) GetStatementNodes() (stmts []ast.StatementNode) {
	ast.Walk(s.Node, func(n ast.Node) error {
		if n.IsStatement() {
			stmts = append(stmts, n)
		}
		return nil
	})
	return
}
