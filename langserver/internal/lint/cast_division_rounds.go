package lint

import (
	"regexp"
	"slices"
	"strings"

	googlesql "github.com/goccy/go-googlesql"
	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/kitagry/bqls/langserver/internal/source/file"
)

var CastDivisionRounds = &Analyzer{
	Name:           "cast-division-rounds",
	Doc:            "reports CAST(a / b AS INT64) of integers, which rounds the FLOAT64 quotient instead of truncating it",
	DefaultEnabled: true,
	Run:            runCastDivisionRounds,
}

const castDivisionRoundsMessage = "CAST to INT64 rounds the FLOAT64 result of an integer division to the nearest integer, halfway cases away from zero (CAST(7 / 2 AS INT64) is 4); use DIV for integer division that truncates toward zero, or ROUND / FLOOR / CEIL to state the rounding"

// integerDivisions maps the divisions of two INT64 values, which return
// FLOAT64, to their integer counterparts.
var integerDivisions = map[string]string{
	"$divide":     "DIV",
	"safe_divide": "SAFE.DIV",
}

// integerLiteral matches an INT64 literal, which the analyzer turns into a
// FLOAT64 literal when it is divided.
var integerLiteral = regexp.MustCompile(`^-?\s*(?:\d+|0[xX][0-9a-fA-F]+)$`)

func runCastDivisionRounds(pass *Pass) {
	// The resolved CAST is located at its type name, so the whole
	// expression comes from the parse tree.
	casts := make(map[offsets]*googlesql.ASTCastExpression)
	for _, c := range file.ListAstNode[*googlesql.ASTCastExpression](pass.File.Node) {
		typ, err := c.Type()
		if err != nil || typ == nil {
			continue
		}
		casts[astOffsets(typ)] = c
	}
	var groupBys []offsets
	for _, g := range file.ListAstNode[*googlesql.ASTGroupBy](pass.File.Node) {
		groupBys = append(groupBys, astOffsets(g))
	}
	for _, output := range pass.File.RNode {
		for _, cast := range file.ListResolvedAstNode[*googlesql.ResolvedCast](output) {
			astCast, ok := casts[resolvedOffsets(cast)]
			if !ok {
				continue
			}
			if to, ok := exprTypeName(cast); !ok || to != "INT64" {
				continue
			}
			inner, err := cast.Expr()
			if err != nil {
				continue
			}
			call, ok := inner.(*googlesql.ResolvedFunctionCall)
			if !ok {
				continue
			}
			name, ok := functionName(call)
			if !ok || integerDivisions[name] == "" || !dividesIntegers(pass.File.FixedSrc, call) {
				continue
			}
			reportCastDivisionRounds(pass, astCast, integerDivisions[name], groupBys)
		}
	}
}

func reportCastDivisionRounds(pass *Pass, cast *googlesql.ASTCastExpression, div string, groupBys []offsets) {
	o := astOffsets(cast)
	r, ok := pass.File.OffsetsToLspRange(o.start, o.end)
	if !ok {
		return
	}
	d := Diagnostic{
		Range:    r,
		Message:  castDivisionRoundsMessage,
		Severity: SeverityWarning,
	}
	// A SELECT list expression equal to a GROUP BY expression is resolved as
	// a reference to it, so rewriting the GROUP BY one alone would leave the
	// SELECT list ungrouped.
	inGroupBy := slices.ContainsFunc(groupBys, func(g offsets) bool { return g.start <= o.start && o.end <= g.end })
	if operands, ok := divisionOperandTexts(pass, cast); ok && !inGroupBy {
		replacement := div + "(" + operands[0] + ", " + operands[1] + ")"
		d.SuggestedFixes = []SuggestedFix{{
			Title:         "Replace with " + replacement,
			Edits:         []lsp.TextEdit{{Range: r, NewText: replacement}},
			ChangesResult: true,
		}}
	}
	pass.Report(d)
}

// dividesIntegers reports whether both arguments of the division were INT64
// before the analyzer coerced them to FLOAT64.
func dividesIntegers(src string, call *googlesql.ResolvedFunctionCall) bool {
	args, err := call.ArgumentList()
	if err != nil || len(args) != 2 {
		return false
	}
	for _, arg := range args {
		if !isIntegerOperand(src, arg) {
			return false
		}
	}
	return true
}

func isIntegerOperand(src string, arg googlesql.ResolvedExprNode) bool {
	if inner, ok := implicitCastOperand(arg); ok {
		from, ok := exprTypeName(inner)
		return ok && from == "INT64"
	}
	// The analyzer turns an INT64 literal into a FLOAT64 literal when it is
	// divided, so only the source text tells them apart.
	lit, ok := arg.(*googlesql.ResolvedLiteral)
	if !ok {
		return false
	}
	if explicit, err := lit.HasExplicitType(); err != nil || explicit {
		return false
	}
	o := resolvedOffsets(lit)
	return inBounds(src, o) && integerLiteral.MatchString(src[o.start:o.end])
}

// divisionOperandTexts returns the written text of the two operands of the
// division inside cast. Resolved locations do not cover a whole operand (an
// explicit CAST is located at its type name, a function call at its name),
// so the text comes from the parse tree.
func divisionOperandTexts(pass *Pass, cast *googlesql.ASTCastExpression) ([2]string, bool) {
	expr, err := cast.Expr()
	if err != nil || isNilNode(expr) {
		return [2]string{}, false
	}
	var operands [2]googlesql.ASTExpressionNode
	switch e := expr.(type) {
	case *googlesql.ASTBinaryExpression:
		if op, err := e.Op(); err != nil || op != googlesql.ASTBinaryExpressionEnums_OpDivide {
			return [2]string{}, false
		}
		lhs, lerr := e.Lhs()
		rhs, rerr := e.Rhs()
		if lerr != nil || rerr != nil {
			return [2]string{}, false
		}
		operands = [2]googlesql.ASTExpressionNode{lhs, rhs}
	case *googlesql.ASTFunctionCall:
		for i := range operands {
			arg, err := e.Arguments(int32(i))
			if err != nil {
				return [2]string{}, false
			}
			operands[i] = arg
		}
	default:
		return [2]string{}, false
	}
	var texts [2]string
	for i, operand := range operands {
		if isNilNode(operand) {
			return [2]string{}, false
		}
		text, ok := writtenText(pass, operand)
		if !ok {
			return [2]string{}, false
		}
		// A scalar subquery is located without its parentheses, which it
		// needs as a function argument.
		if _, ok := operand.(*googlesql.ASTExpressionSubquery); ok && !strings.HasPrefix(text, "(") {
			text = "(" + text + ")"
		}
		texts[i] = text
	}
	return texts, true
}
