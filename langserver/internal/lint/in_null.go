package lint

import (
	"strings"

	googlesql "github.com/goccy/go-googlesql"
	"github.com/kitagry/bqls/langserver/internal/source/file"
)

var InNull = &Analyzer{
	Name:           "in-null",
	Doc:            "reports `IN (NULL)` / `NOT IN (NULL)` lists of only NULLs, which always yield NULL",
	DefaultEnabled: true,
	Run:            runInNull,
}

func runInNull(pass *Pass) {
	for _, output := range pass.File.RNode {
		for _, call := range file.ListResolvedAstNode[*googlesql.ResolvedFunctionCall](output) {
			checkInNull(pass, call)
		}
	}
}

func checkInNull(pass *Pass, call *googlesql.ResolvedFunctionCall) {
	name, ok := functionName(call)
	if !ok {
		return
	}
	op, in := "IN", call
	switch name {
	case "$in":
	case "$not":
		// `x NOT IN (...)` resolves to $not($in(...)) where only $not carries the
		// location; a location on the inner $in means the user wrote NOT (x IN ...),
		// which the $in itself reports.
		args, err := call.ArgumentList()
		if err != nil || len(args) != 1 {
			return
		}
		inner, ok := args[0].(*googlesql.ResolvedFunctionCall)
		if !ok {
			return
		}
		if innerName, ok := functionName(inner); !ok || innerName != "$in" {
			return
		}
		if loc, err := inner.GetParseLocationRangeOrNULL(); err != nil || loc != nil {
			return
		}
		op, in = "NOT IN", inner
	default:
		return
	}

	callLoc, err := call.GetParseLocationRangeOrNULL()
	if err != nil || callLoc == nil {
		return
	}
	callRange, ok := pass.File.ToLspRange(callLoc)
	if !ok {
		return
	}

	// The first argument is the tested value; the rest is the IN list.
	args, err := in.ArgumentList()
	if err != nil || len(args) < 2 {
		return
	}
	for _, elem := range args[1:] {
		if !isWrittenNull(pass, elem) {
			return
		}
	}

	pass.Report(Diagnostic{
		Range:    callRange,
		Message:  op + " with only NULL values always yields NULL",
		Severity: SeverityWarning,
	})
}

func functionName(call *googlesql.ResolvedFunctionCall) (string, bool) {
	fn, err := call.Function()
	if err != nil || fn == nil {
		return "", false
	}
	name, err := fn.Name()
	return name, err == nil
}

// isWrittenNull reports whether expr is a NULL literal the user actually
// wrote, because declared variables are analyzed as substituted literals.
func isWrittenNull(pass *Pass, expr googlesql.ResolvedExprNode) bool {
	if !isNullLiteral(expr) {
		return false
	}
	loc, err := expr.GetParseLocationRangeOrNULL()
	if err != nil || loc == nil {
		return false
	}
	src, ok := pass.File.ExtractSQL(loc)
	return ok && nullSourcePattern.MatchString(strings.TrimSpace(src))
}
