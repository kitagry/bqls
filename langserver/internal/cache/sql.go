package cache

import (
	"fmt"
	"strings"

	"github.com/goccy/go-zetasql/ast"
)

type SQL struct {
	RawText string
	Node    ast.Node
	Errors  []Error
}

func (s *SQL) GetTables() ([]string, error) {
	tables := make([]string, 0)
	err := ast.Walk(s.Node, func(n ast.Node) error {
		if n.IsTableExpression() {
			n, ok := n.(*ast.TablePathExpressionNode)
			if !ok {
				return nil
			}

			names := n.PathExpr().Names()
			if len(names) == 0 {
				return nil
			}

			nameStrs := make([]string, len(names))
			for i, name := range names {
				nameStrs[i] = name.Name()
			}

			tables = append(tables, strings.Join(nameStrs, "."))
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to walk node: %v", err)
	}
	return tables, nil
}
