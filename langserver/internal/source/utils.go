package source

import (
	"strings"

	"github.com/goccy/go-zetasql/ast"
)

func createTableNameFromTablePathExpressionNode(node *ast.TablePathExpressionNode) (string, bool) {
	var pathExpr *ast.PathExpressionNode
	if unnestExpr := node.UnnestExpr(); unnestExpr != nil {
		ex, ok := unnestExpr.Expression().(*ast.PathExpressionNode)
		if ok {
			pathExpr = ex
		}
	}

	if pExpr := node.PathExpr(); pExpr != nil {
		pathExpr = pExpr
	}

	if pathExpr == nil {
		return "", false
	}

	pathNames := make([]string, len(pathExpr.Names()))
	for i, n := range pathExpr.Names() {
		pathNames[i] = n.Name()
	}

	return strings.Join(pathNames, "."), true
}
