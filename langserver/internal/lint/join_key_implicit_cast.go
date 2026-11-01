package lint

import (
	googlesql "github.com/goccy/go-googlesql"
	"github.com/kitagry/bqls/langserver/internal/source/file"
)

var JoinKeyImplicitCast = &Analyzer{
	Name:           "join-key-implicit-cast",
	Doc:            "reports join keys in ON / USING whose types differ, so one side is implicitly cast",
	DefaultEnabled: true,
	Run:            runJoinKeyImplicitCast,
}

func joinKeyImplicitCastMessage(from, to string) string {
	msg := "join keys of different types are compared: " + from + " is implicitly cast to " + to
	if to == "FLOAT64" {
		return msg + ", which cannot represent every value exactly, so different keys may compare equal; give both columns the same type, or write an explicit CAST"
	}
	return msg + "; mismatched key types usually point to a schema design mistake, so give both columns the same type, or write an explicit CAST"
}

var joinKeyComparisons = map[string]bool{
	"$equal":                true,
	"$is_not_distinct_from": true,
}

// Only a comparison of a column from each input counts as a join key: a
// literal, parameter or computed value on either side may be cast on
// purpose, and an explicit CAST states the intent.
func runJoinKeyImplicitCast(pass *Pass) {
	for _, output := range pass.File.RNode {
		for _, join := range file.ListResolvedAstNode[*googlesql.ResolvedJoinScan](output) {
			cond, err := join.JoinExpr()
			if err != nil || cond == nil {
				continue
			}
			leftScan, err := join.LeftScan()
			if err != nil {
				continue
			}
			rightScan, err := join.RightScan()
			if err != nil {
				continue
			}
			left, ok := scanColumnIDs(leftScan)
			if !ok {
				continue
			}
			right, ok := scanColumnIDs(rightScan)
			if !ok {
				continue
			}
			for _, term := range splitOperands(cond, "$and") {
				reportJoinKeyImplicitCast(pass, join, term, left, right)
			}
		}
	}
}

func reportJoinKeyImplicitCast(pass *Pass, join *googlesql.ResolvedJoinScan, term googlesql.ResolvedExprNode, left, right map[int32]bool) {
	call, ok := term.(*googlesql.ResolvedFunctionCall)
	if !ok {
		return
	}
	if name, ok := functionName(call); !ok || !joinKeyComparisons[name] {
		return
	}
	args, err := call.ArgumentList()
	if err != nil || len(args) != 2 {
		return
	}
	cast, other := args[0], args[1]
	if _, ok := implicitCastOperand(cast); !ok {
		cast, other = other, cast
	}
	casted, ok := implicitCastOperand(cast)
	if !ok {
		return
	}
	castedID, ok := columnRefID(casted)
	if !ok {
		return
	}
	otherID, ok := columnRefID(other)
	if !ok {
		return
	}
	if !(left[castedID] && right[otherID]) && !(right[castedID] && left[otherID]) {
		return
	}
	from, ok := exprTypeName(casted)
	if !ok {
		return
	}
	to, ok := exprTypeName(cast)
	if !ok {
		return
	}

	// USING keys have no location of their own, so the JOIN keyword stands in.
	loc, err := call.GetParseLocationRangeOrNULL()
	if err != nil || loc == nil {
		loc, err = join.GetParseLocationRangeOrNULL()
		if err != nil || loc == nil {
			return
		}
	}
	r, ok := pass.File.OffsetsToLspRange(file.ParseLocStart(loc), file.ParseLocEnd(loc))
	if !ok {
		return
	}
	pass.Report(Diagnostic{
		Range:    r,
		Message:  joinKeyImplicitCastMessage(from, to),
		Severity: SeverityWarning,
	})
}

// implicitCastOperand returns the operand of a cast the analyzer inserted.
// A CAST written in the query carries a parse location; an inserted one does not.
func implicitCastOperand(expr googlesql.ResolvedExprNode) (googlesql.ResolvedExprNode, bool) {
	cast, ok := expr.(*googlesql.ResolvedCast)
	if !ok {
		return nil, false
	}
	loc, err := cast.GetParseLocationRangeOrNULL()
	if err != nil || loc != nil {
		return nil, false
	}
	inner, err := cast.Expr()
	return inner, err == nil && inner != nil
}

func exprTypeName(expr googlesql.ResolvedExprNode) (string, bool) {
	typ, err := expr.Type()
	if err != nil || typ == nil {
		return "", false
	}
	// TypeName is defined on each concrete type, not on the type interface.
	named, ok := typ.(interface {
		TypeName(mode googlesql.ProductMode) (string, error)
	})
	if !ok {
		return "", false
	}
	name, err := named.TypeName(googlesql.ProductModeProductExternal)
	return name, err == nil
}
