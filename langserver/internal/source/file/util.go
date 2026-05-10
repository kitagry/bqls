package file

import (
	"strings"

	googlesql "github.com/goccy/go-googlesql"
)

func CreateTableNameFromTablePathExpressionNode(node *googlesql.ASTTablePathExpression) (string, bool) {
	var pathExpr *googlesql.ASTPathExpression

	unnestExpr, _ := node.UnnestExpr()
	if unnestExpr != nil {
		expr, err := unnestExpr.Expression()
		if err == nil {
			if pe, ok := expr.(*googlesql.ASTPathExpression); ok {
				pathExpr = pe
			}
		}
	}

	if pe, _ := node.PathExpr(); pe != nil {
		pathExpr = pe
	}

	if pathExpr == nil {
		return "", false
	}

	names, err := pathExpr.ToIdentifierVector()
	if err != nil || len(names) == 0 {
		return "", false
	}

	return strings.Join(names, "."), true
}
