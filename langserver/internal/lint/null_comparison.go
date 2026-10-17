package lint

import (
	"fmt"
	"regexp"
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
	name, ok := functionName(call)
	if !ok {
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
	if !ok || !nullSourcePattern.MatchString(strings.TrimSpace(nullSrc)) {
		return
	}

	d := Diagnostic{
		Range:    callRange,
		Message:  fmt.Sprintf("Comparison with NULL using %s always yields NULL; use %s", op, replacement),
		Severity: SeverityWarning,
	}
	if cmp, ok := SplitNullComparison(callSrc, nullSrc, nullOnRight); ok {
		d.Message = fmt.Sprintf("Comparison with NULL using %s always yields NULL; use %s", cmp.Op, replacement)
		d.SuggestedFixes = []SuggestedFix{{
			Title: "Replace with " + replacement,
			Edits: []lsp.TextEdit{{Range: callRange, NewText: cmp.Other + " " + replacement}},
		}}
	}
	pass.Report(d)
}

var nullSourcePattern = regexp.MustCompile(`(?i)^\(*\s*(NULL\b|(SAFE_)?CAST\s*\(\s*NULL\s+AS\b)`)

// NullComparisonParts is a `<other> <op> <null>` comparison split from its source text.
type NullComparisonParts struct {
	Other string
	Op    string
}

// SplitNullComparison slices the non-NULL operand and the operator out of the
// comparison's own source text rather than using the operand's location,
// because locations exclude parentheses the user wrote around either side.
func SplitNullComparison(cmpSrc, nullSrc string, nullOnRight bool) (NullComparisonParts, bool) {
	var rest, between string
	if nullOnRight {
		i := strings.LastIndex(cmpSrc, nullSrc)
		if i < 0 || strings.Trim(cmpSrc[i+len(nullSrc):], " \t\r\n)") != "" {
			return NullComparisonParts{}, false
		}
		rest = strings.TrimRight(cmpSrc[:i], " \t\r\n(")
		other := strings.TrimRight(rest, "=!<>")
		between, rest = rest[len(other):], other
	} else {
		i := strings.Index(cmpSrc, nullSrc)
		if i < 0 || strings.Trim(cmpSrc[:i], " \t\r\n(") != "" {
			return NullComparisonParts{}, false
		}
		rest = strings.TrimLeft(cmpSrc[i+len(nullSrc):], " \t\r\n)")
		other := strings.TrimLeft(rest, "=!<>")
		between, rest = rest[:len(rest)-len(other)], other
	}
	rest = strings.TrimSpace(rest)
	if rest == "" || between == "" {
		return NullComparisonParts{}, false
	}
	return NullComparisonParts{Other: rest, Op: between}, true
}

// NullReplacement returns the IS [NOT] NULL form for a written comparison operator.
func NullReplacement(op string) (string, bool) {
	switch op {
	case "=":
		return "IS NULL", true
	case "!=", "<>":
		return "IS NOT NULL", true
	}
	return "", false
}

func functionName(call *googlesql.ResolvedFunctionCall) (string, bool) {
	fn, err := call.Function()
	if err != nil || fn == nil {
		return "", false
	}
	name, err := fn.Name()
	return name, err == nil
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
