package cache

import (
	"github.com/goccy/go-zetasql/ast"
)

type SQL struct {
	RawText string
	Node    ast.Node
	Errors  []error
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
