package lint

import (
	"fmt"

	googlesql "github.com/goccy/go-googlesql"
	"github.com/kitagry/bqls/langserver/internal/source/file"
)

var SelfComparison = &Analyzer{
	Name:           "self-comparison",
	Doc:            "reports a comparison whose two operands are the same column, which is usually a typo for another column",
	DefaultEnabled: true,
	Run:            runSelfComparison,
}

func runSelfComparison(pass *Pass) {
	scopes := variableScopes(pass.File.Node)
	for _, st := range pass.File.Statements {
		if st.Output == nil {
			continue
		}
		vars := variablesInScope(scopes, astOffsets(st.Stmt))
		for _, call := range file.ListResolvedAstNode[*googlesql.ResolvedFunctionCall](st.Output) {
			checkSelfComparison(pass, call, vars)
		}
	}
}

func checkSelfComparison(pass *Pass, call *googlesql.ResolvedFunctionCall, vars map[string]bool) {
	args, ok := selfComparisonOperands(call)
	if !ok {
		return
	}
	// variable-column-shadow reports this with the actual cause.
	if len(shadowingOperands(pass, args, vars)) > 0 {
		return
	}
	// NaN is not equal to itself, so comparing a float with itself is a way
	// to test for NaN.
	if isFloatingPoint(args[0]) {
		return
	}

	// Comparisons synthesized by the analyzer, such as for JOIN USING, have
	// no source location.
	callLoc, err := call.GetParseLocationRangeOrNULL()
	if err != nil || callLoc == nil {
		return
	}
	r, ok := pass.File.ToLspRange(callLoc)
	if !ok {
		return
	}
	leftLoc, err := args[0].GetParseLocationRangeOrNULL()
	if err != nil || leftLoc == nil {
		return
	}
	column, ok := pass.File.ExtractSQL(leftLoc)
	if !ok {
		return
	}

	msg := fmt.Sprintf("Both sides of this comparison refer to the same column `%s`, so the result does not depend on its value; one side is likely meant to be another column", column)
	if name, _ := functionName(call); name == "$equal" {
		msg += fmt.Sprintf(" (write `%s IS NOT NULL` to test for NULL)", column)
	}
	pass.Report(Diagnostic{Range: r, Message: msg, Severity: SeverityWarning})
}

func isFloatingPoint(expr googlesql.ResolvedExprNode) bool {
	typ, err := expr.Type()
	if err != nil || typ == nil {
		return false
	}
	ok, err := typ.IsFloatingPoint()
	return err == nil && ok
}
