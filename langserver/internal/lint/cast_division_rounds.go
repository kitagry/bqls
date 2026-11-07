package lint

import (
	"regexp"

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
			if !ok || integerDivisions[name] == "" {
				continue
			}
			reportCastDivisionRounds(pass, astCast, call, integerDivisions[name])
		}
	}
}

func reportCastDivisionRounds(pass *Pass, cast *googlesql.ASTCastExpression, call *googlesql.ResolvedFunctionCall, div string) {
	args, err := call.ArgumentList()
	if err != nil || len(args) != 2 {
		return
	}
	operands := make([]string, len(args))
	for i, arg := range args {
		text, ok := integerOperandText(pass.File.FixedSrc, arg)
		if !ok {
			return
		}
		operands[i] = text
	}
	o := astOffsets(cast)
	r, ok := pass.File.OffsetsToLspRange(o.start, o.end)
	if !ok {
		return
	}
	replacement := div + "(" + operands[0] + ", " + operands[1] + ")"
	pass.Report(Diagnostic{
		Range:    r,
		Message:  castDivisionRoundsMessage,
		Severity: SeverityWarning,
		SuggestedFixes: []SuggestedFix{{
			Title:         "Replace with " + replacement,
			Edits:         []lsp.TextEdit{{Range: r, NewText: replacement}},
			ChangesResult: true,
		}},
	})
}

// integerOperandText returns the source text of a division operand that was
// an INT64 before the analyzer coerced it to FLOAT64.
func integerOperandText(src string, arg googlesql.ResolvedExprNode) (string, bool) {
	var o offsets
	if inner, ok := implicitCastOperand(arg); ok {
		if from, ok := exprTypeName(inner); !ok || from != "INT64" {
			return "", false
		}
		o = resolvedOffsets(inner)
	} else if lit, ok := arg.(*googlesql.ResolvedLiteral); ok {
		if explicit, err := lit.HasExplicitType(); err != nil || explicit {
			return "", false
		}
		o = resolvedOffsets(lit)
		if o.start < 0 || o.end > len(src) || !integerLiteral.MatchString(src[o.start:o.end]) {
			return "", false
		}
	} else {
		return "", false
	}
	if o.start < 0 || o.end > len(src) {
		return "", false
	}
	return src[o.start:o.end], true
}
