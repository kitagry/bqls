package lint

import (
	"strings"

	googlesql "github.com/goccy/go-googlesql"
	"github.com/kitagry/bqls/langserver/internal/source/file"
)

var ComparisonImplicitCast = &Analyzer{
	Name:           "comparison-implicit-cast",
	Doc:            "reports comparisons outside join keys between columns of different types, so one side is implicitly cast",
	DefaultEnabled: true,
	Run:            runComparisonImplicitCast,
}

func comparisonImplicitCastMessage(from, to string) string {
	msg := "columns of different types are compared: " + from + " is implicitly cast to " + to
	if to == "FLOAT64" {
		return msg + ", which cannot represent every value exactly, so different values may compare equal; give both columns the same type, or write an explicit CAST"
	}
	return msg + "; mismatched column types usually point to a schema design mistake, so give both columns the same type, or write an explicit CAST"
}

var columnComparisons = map[string]bool{
	"$equal":                true,
	"$not_equal":            true,
	"$less":                 true,
	"$less_or_equal":        true,
	"$greater":              true,
	"$greater_or_equal":     true,
	"$between":              true,
	"$not_between":          true,
	"$in":                   true,
	"$not_in":               true,
	"$is_distinct_from":     true,
	"$is_not_distinct_from": true,
}

// As in join-key-implicit-cast, only columns count: a literal, parameter or
// computed value may be cast on purpose. A DATE compared with a DATETIME is
// left to date-bound-on-datetime, and JOIN ... ON to join-key-implicit-cast.
func runComparisonImplicitCast(pass *Pass) {
	for _, output := range pass.File.RNode {
		joinConds := make(map[offsets]bool)
		for _, join := range file.ListResolvedAstNode[*googlesql.ResolvedJoinScan](output) {
			cond, err := join.JoinExpr()
			if err != nil || isNilNode(cond) {
				continue
			}
			walkLive(cond, func(n googlesql.ResolvedNode) {
				if call, ok := n.(*googlesql.ResolvedFunctionCall); ok {
					joinConds[resolvedOffsets(call)] = true
				}
			})
		}
		for _, call := range file.ListResolvedAstNode[*googlesql.ResolvedFunctionCall](output) {
			name, ok := functionName(call)
			if !ok || !columnComparisons[name] {
				continue
			}
			o := resolvedOffsets(call)
			if o.start < 0 || joinConds[o] {
				continue
			}
			from, to, ok := castBetweenColumns(call)
			if !ok || from == "DATE" && to == "DATETIME" {
				continue
			}
			r, ok := pass.File.OffsetsToLspRange(o.start, o.end)
			if !ok {
				continue
			}
			pass.Report(Diagnostic{Range: r, Message: comparisonImplicitCastMessage(from, to), Severity: SeverityWarning})
		}
	}
}

// castBetweenColumns returns the types of the first implicit cast among the
// arguments when every argument is a column, cast or not.
func castBetweenColumns(call *googlesql.ResolvedFunctionCall) (string, string, bool) {
	args, err := call.ArgumentList()
	if err != nil || len(args) < 2 {
		return "", "", false
	}
	var from, to string
	for _, arg := range args {
		inner, cast := implicitCastOperand(arg)
		if !cast {
			inner = arg
		}
		if !isNamedColumnRef(inner) {
			return "", "", false
		}
		if !cast || from != "" {
			continue
		}
		f, ok := exprTypeName(inner)
		t, ok2 := exprTypeName(arg)
		if !ok || !ok2 {
			return "", "", false
		}
		from, to = f, t
	}
	return from, to, from != ""
}

// isNamedColumnRef reports whether expr refers to a column of a table, CTE or
// subquery output, not to a value the query computes such as an aggregate,
// whose internal table names start with "$".
func isNamedColumnRef(expr googlesql.ResolvedExprNode) bool {
	ref, ok := expr.(*googlesql.ResolvedColumnRef)
	if !ok {
		return false
	}
	col, err := ref.Column()
	if err != nil || col == nil {
		return false
	}
	table, err := col.TableName()
	return err == nil && !strings.HasPrefix(table, "$")
}
