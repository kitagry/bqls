package file

import (
	"github.com/goccy/go-zetasql"
	"github.com/goccy/go-zetasql/ast"
	rast "github.com/goccy/go-zetasql/resolved_ast"
	"github.com/goccy/go-zetasql/types"
)

type locationRangeNode interface {
	ParseLocationRange() *types.ParseLocationRange
}

func SearchAstNode[T locationRangeNode](node ast.Node, termOffset int) (T, bool) {
	var targetNode T
	var found bool
	ast.Walk(node, func(n ast.Node) error {
		node, ok := n.(T)
		if !ok {
			return nil
		}
		lRange := node.ParseLocationRange()
		if lRange == nil {
			return nil
		}
		startOffset := lRange.Start().ByteOffset()
		endOffset := lRange.End().ByteOffset()
		if startOffset <= termOffset && termOffset <= endOffset {
			targetNode = node
			found = true
		}
		return nil
	})
	return targetNode, found
}

func SearchResolvedAstNode[T locationRangeNode](output *zetasql.AnalyzerOutput, termOffset int) (T, bool) {
	var targetNode T
	var found bool
	rast.Walk(output.Statement(), func(n rast.Node) error {
		node, ok := n.(T)
		if !ok {
			return nil
		}
		lRange := node.ParseLocationRange()
		if lRange == nil {
			return nil
		}
		startOffset := lRange.Start().ByteOffset()
		endOffset := lRange.End().ByteOffset()
		if startOffset <= termOffset && termOffset <= endOffset {
			targetNode = node
			found = true
		}
		return nil
	})

	if found {
		return targetNode, found
	}
	return targetNode, false
}

func ListResolvedAstNode[T locationRangeNode](output *zetasql.AnalyzerOutput) []T {
	result := make([]T, 0)
	rast.Walk(output.Statement(), func(n rast.Node) error {
		node, ok := n.(T)
		if !ok {
			return nil
		}
		result = append(result, node)
		return nil
	})

	return result
}

type astNode interface {
	*ast.TablePathExpressionNode | *ast.PathExpressionNode | *ast.SelectColumnNode
}

func LookupNode[T astNode](n ast.Node) (T, bool) {
	if n == nil {
		return nil, false
	}

	result, ok := n.(T)
	if ok {
		return result, true
	}

	return LookupNode[T](n.Parent())
}
