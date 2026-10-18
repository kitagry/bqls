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

	return joinPathExpression(pathExpr)
}

func joinPathExpression(pathExpr *googlesql.ASTPathExpression) (string, bool) {
	names, err := pathExpr.ToIdentifierVector()
	if err != nil || len(names) == 0 {
		return "", false
	}
	return strings.Join(names, "."), true
}

// DMLTargetTableName returns the table name when node is the target table of
// a DML statement, which the AST holds as a path expression rather than a
// table path expression.
func DMLTargetTableName(node *googlesql.ASTPathExpression) (string, bool) {
	parent, err := node.Parent()
	if err != nil || parent == nil {
		return "", false
	}
	target := dmlTargetPath(parent)
	if target == nil || locStart(target) != locStart(node) || locEnd(target) != locEnd(node) {
		return "", false
	}
	return joinPathExpression(node)
}
