package lint

import (
	"regexp"

	googlesql "github.com/goccy/go-googlesql"
	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/kitagry/bqls/langserver/internal/source/file"
)

var literalNullComparisonError = regexp.MustCompile(`^Operands of (=|!=|<>) cannot be literal NULL`)

// LiteralNullComparisonFix builds an IS [NOT] NULL rewrite for GoogleSQL's
// "Operands of = cannot be literal NULL" analysis error. Lint never runs on
// files with errors, so this fix hangs off the error diagnostic instead.
func LiteralNullComparisonFix(f file.ParsedFile, e file.Error) (SuggestedFix, bool) {
	if !literalNullComparisonError.MatchString(e.Msg) || f.Node == nil {
		return SuggestedFix{}, false
	}
	for _, bin := range file.ListAstNode[*googlesql.ASTBinaryExpression](f.Node) {
		if fix, ok := literalNullComparisonFixFor(f, bin, e.Position); ok {
			return fix, true
		}
	}
	return SuggestedFix{}, false
}

func literalNullComparisonFixFor(f file.ParsedFile, bin *googlesql.ASTBinaryExpression, errPos lsp.Position) (SuggestedFix, bool) {
	op, err := bin.Op()
	if err != nil || (op != googlesql.ASTBinaryExpressionEnums_OpEq && op != googlesql.ASTBinaryExpressionEnums_OpNe && op != googlesql.ASTBinaryExpressionEnums_OpNe2) {
		return SuggestedFix{}, false
	}
	lhs, err := bin.Lhs()
	if err != nil {
		return SuggestedFix{}, false
	}
	rhs, err := bin.Rhs()
	if err != nil {
		return SuggestedFix{}, false
	}

	// GoogleSQL points the error at the offending NULL operand.
	for _, side := range []struct {
		node        googlesql.ASTExpressionNode
		nullOnRight bool
	}{{rhs, true}, {lhs, false}} {
		if _, ok := side.node.(*googlesql.ASTNullLiteral); !ok {
			continue
		}
		nullLoc, err := side.node.GetParseLocationRange()
		if err != nil {
			continue
		}
		nullRange, ok := f.ToLspRange(nullLoc)
		if !ok || nullRange.Start != errPos {
			continue
		}
		return nullComparisonFix(f, bin, nullLoc, side.nullOnRight)
	}
	return SuggestedFix{}, false
}

func nullComparisonFix(f file.ParsedFile, bin *googlesql.ASTBinaryExpression, nullLoc *googlesql.ParseLocationRange, nullOnRight bool) (SuggestedFix, bool) {
	binLoc, err := bin.GetParseLocationRange()
	if err != nil {
		return SuggestedFix{}, false
	}
	binRange, ok := f.ToLspRange(binLoc)
	if !ok {
		return SuggestedFix{}, false
	}
	binSrc, ok := f.ExtractSQL(binLoc)
	if !ok {
		return SuggestedFix{}, false
	}
	nullSrc, ok := f.ExtractSQL(nullLoc)
	if !ok {
		return SuggestedFix{}, false
	}
	parts, ok := SplitNullComparison(binSrc, nullSrc, nullOnRight)
	if !ok {
		return SuggestedFix{}, false
	}
	replacement, ok := NullReplacement(parts.Op)
	if !ok {
		return SuggestedFix{}, false
	}
	edits := []lsp.TextEdit{{Range: binRange, NewText: parts.Other + " " + replacement}}
	return SuggestedFix{Title: "Replace with " + replacement, Edits: edits}, true
}
