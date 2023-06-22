package source

import (
	"strings"

	"github.com/goccy/go-zetasql/ast"
)

func createTableNameFromTablePathExpressionNode(node *ast.TablePathExpressionNode) (string, bool) {
	pathExpr := node.PathExpr()
	if pathExpr == nil {
		return "", false
	}

	pathNames := make([]string, len(pathExpr.Names()))
	for i, n := range pathExpr.Names() {
		pathNames[i] = n.Name()
	}

	return strings.Join(pathNames, "."), true
}
