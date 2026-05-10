package file

import (
	googlesql "github.com/goccy/go-googlesql"
)

func SearchAstNode[T googlesql.ASTNode](node googlesql.ASTNode, termOffset int) (T, bool) {
	var targetNode T
	var found bool
	Walk(node, func(n googlesql.ASTNode) error { //nolint
		typed, ok := n.(T)
		if !ok {
			return nil
		}
		startOffset := locStart(n)
		endOffset := locEnd(n)
		if startOffset < 0 || endOffset < 0 {
			return nil
		}
		if startOffset <= termOffset && termOffset <= endOffset {
			targetNode = typed
			found = true
		}
		return nil
	})
	return targetNode, found
}

func SearchResolvedAstNode[T googlesql.ResolvedNode](output *googlesql.AnalyzerOutput, termOffset int) (T, bool) {
	var targetNode T
	var found bool
	stmt, err := output.ResolvedStatement()
	if err != nil || stmt == nil {
		return targetNode, false
	}
	WalkResolved(stmt, func(n googlesql.ResolvedNode) error { //nolint
		typed, ok := n.(T)
		if !ok {
			return nil
		}
		startOffset := resolvedLocStart(n)
		endOffset := resolvedLocEnd(n)
		if startOffset < 0 || endOffset < 0 {
			return nil
		}
		if startOffset <= termOffset && termOffset <= endOffset {
			targetNode = typed
			found = true
		}
		return nil
	})
	return targetNode, found
}

func ListResolvedAstNode[T googlesql.ResolvedNode](output *googlesql.AnalyzerOutput) []T {
	result := make([]T, 0)
	stmt, err := output.ResolvedStatement()
	if err != nil || stmt == nil {
		return result
	}
	WalkResolved(stmt, func(n googlesql.ResolvedNode) error { //nolint
		typed, ok := n.(T)
		if !ok {
			return nil
		}
		result = append(result, typed)
		return nil
	})
	return result
}

type astNode interface {
	*googlesql.ASTTablePathExpression | *googlesql.ASTPathExpression | *googlesql.ASTSelectColumn | *googlesql.ASTFunctionCall
}

func LookupNode[T astNode](n googlesql.ASTNode) (T, bool) {
	if n == nil {
		return nil, false
	}

	result, ok := n.(T)
	if ok {
		return result, true
	}

	parent, err := n.Parent()
	if err != nil || parent == nil {
		return nil, false
	}
	return LookupNode[T](parent)
}

func ListAstNode[T googlesql.ASTNode](n *googlesql.ASTScript) []T {
	result := make([]T, 0)
	Walk(n, func(n googlesql.ASTNode) error { //nolint
		typed, ok := n.(T)
		if !ok {
			return nil
		}
		result = append(result, typed)
		return nil
	})
	return result
}
