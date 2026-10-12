package lint

import (
	"fmt"
	"strings"

	googlesql "github.com/goccy/go-googlesql"
	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/kitagry/bqls/langserver/internal/source/file"
)

var NullComparison = &Analyzer{
	Name:           "null-comparison",
	Doc:            "reports `= NULL` / `!= NULL` comparisons, which always yield NULL",
	DefaultEnabled: true,
	Run:            runNullComparison,
}

func runNullComparison(pass *Pass) {
	for _, output := range pass.File.RNode {
		for _, call := range file.ListResolvedAstNode[*googlesql.ResolvedFunctionCall](output) {
			checkNullComparison(pass, call)
		}
	}
}

func checkNullComparison(pass *Pass, call *googlesql.ResolvedFunctionCall) {
	fn, err := call.Function()
	if err != nil || fn == nil {
		return
	}
	name, err := fn.Name()
	if err != nil {
		return
	}
	var op, replacement string
	switch name {
	case "$equal":
		op, replacement = "=", "IS NULL"
	case "$not_equal":
		op, replacement = "!=", "IS NOT NULL"
	default:
		return
	}

	args, err := call.ArgumentList()
	if err != nil || len(args) != 2 {
		return
	}
	nullOnRight := isNullLiteral(args[1])
	if !nullOnRight && !isNullLiteral(args[0]) {
		return
	}
	nullArg := args[1]
	if !nullOnRight {
		nullArg = args[0]
	}

	// Implicit comparisons synthesized by the analyzer have no source location
	// and are not something the user can fix.
	callLoc, err := call.GetParseLocationRangeOrNULL()
	if err != nil || callLoc == nil {
		return
	}
	callRange, ok := pass.File.ToLspRange(callLoc)
	if !ok {
		return
	}
	callSrc, ok := pass.File.ExtractSQL(callLoc)
	if !ok {
		return
	}
	nullLoc, err := nullArg.GetParseLocationRangeOrNULL()
	if err != nil || nullLoc == nil {
		return
	}
	nullSrc, ok := pass.File.ExtractSQL(nullLoc)
	// Declared variables are analyzed as substituted literals; only flag NULLs
	// the user actually wrote.
	if !ok || !strings.Contains(strings.ToUpper(nullSrc), "NULL") {
		return
	}

	d := Diagnostic{
		Range:    callRange,
		Message:  fmt.Sprintf("Comparison with NULL using %s always yields NULL; use %s", op, replacement),
		Severity: SeverityWarning,
	}
	if otherSrc, ok := otherOperandSource(callSrc, nullSrc, nullOnRight); ok {
		d.SuggestedFixes = []SuggestedFix{{
			Title: "Replace with " + replacement,
			Edits: []lsp.TextEdit{{Range: callRange, NewText: otherSrc + " " + replacement}},
		}}
	}
	pass.Report(d)
}

// otherOperandSource slices the non-NULL operand out of the comparison's own
// source text rather than using the operand's resolved location, because
// resolved locations exclude redundant parentheses the user wrote.
func otherOperandSource(callSrc, nullSrc string, nullOnRight bool) (string, bool) {
	var rest string
	var ok bool
	if nullOnRight {
		rest, ok = strings.CutSuffix(callSrc, nullSrc)
		rest = strings.TrimRight(rest, " \t\r\n")
		rest = strings.TrimRight(rest, "=!<>")
	} else {
		rest, ok = strings.CutPrefix(callSrc, nullSrc)
		rest = strings.TrimLeft(rest, " \t\r\n")
		rest = strings.TrimLeft(rest, "=!<>")
	}
	rest = strings.TrimSpace(rest)
	if !ok || rest == "" {
		return "", false
	}
	return rest, true
}

func isNullLiteral(expr googlesql.ResolvedExprNode) bool {
	lit, ok := expr.(*googlesql.ResolvedLiteral)
	if !ok {
		return false
	}
	v, err := lit.Value()
	if err != nil || v == nil {
		return false
	}
	isNull, err := v.IsNull()
	return err == nil && isNull
}
