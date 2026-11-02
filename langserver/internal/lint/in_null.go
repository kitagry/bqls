package lint

import (
	"regexp"
	"strings"

	googlesql "github.com/goccy/go-googlesql"
	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/kitagry/bqls/langserver/internal/source/file"
)

var InNull = &Analyzer{
	Name:           "in-null",
	Doc:            "reports `IN (NULL)` / `NOT IN (NULL)` lists of only NULLs, which always yield NULL, and `NOT IN` lists with a NULL, which never yield TRUE",
	DefaultEnabled: true,
	Run:            runInNull,
}

func runInNull(pass *Pass) {
	inExprs := make(map[offsets]*googlesql.ASTInExpression)
	for _, in := range file.ListAstNode[*googlesql.ASTInExpression](pass.File.Node) {
		inExprs[astOffsets(in)] = in
	}
	for _, output := range pass.File.RNode {
		for _, call := range file.ListResolvedAstNode[*googlesql.ResolvedFunctionCall](output) {
			checkInNull(pass, inExprs, call)
		}
	}
}

func checkInNull(pass *Pass, inExprs map[offsets]*googlesql.ASTInExpression, call *googlesql.ResolvedFunctionCall) {
	name, ok := functionName(call)
	if !ok {
		return
	}
	in, negated := call, false
	switch name {
	case "$in":
	case "$not":
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
		in, negated = inner, true
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
	// `x NOT IN (...)` resolves to $not($in(...)) where only $not carries the
	// location; a location on the inner $in means the user wrote NOT (x IN ...).
	inLoc, err := in.GetParseLocationRangeOrNULL()
	if err != nil {
		return
	}
	wroteNotIn := inLoc == nil
	if wroteNotIn {
		inLoc = callLoc
	}

	// The first argument is the tested value; the rest is the IN list.
	args, err := in.ArgumentList()
	if err != nil || len(args) < 2 {
		return
	}
	isNull := make([]bool, len(args)-1)
	nulls := 0
	for i, elem := range args[1:] {
		if isWrittenNull(pass, elem) {
			isNull[i] = true
			nulls++
		}
	}
	astIn := inExprs[offsets{file.ParseLocStart(inLoc), file.ParseLocEnd(inLoc)}]

	var d Diagnostic
	switch {
	// NOT (x IN (NULL)) is left to the $in itself, which is IN with only NULLs.
	case nulls == len(isNull) && (!negated || wroteNotIn):
		op, replacement := "IN", "IS NULL"
		if negated {
			op, replacement = "NOT IN", "IS NOT NULL"
		}
		d = Diagnostic{Range: callRange, Message: op + " with only NULL values always yields NULL"}
		if fix, ok := inNullCheckFix(pass, astIn, replacement); ok {
			d.SuggestedFixes = []SuggestedFix{fix}
		}
	case 0 < nulls && nulls < len(isNull) && negated:
		d = Diagnostic{Range: callRange, Message: "NOT IN with a NULL value never yields TRUE"}
		if fix, ok := removeNullsFix(pass, astIn, isNull); ok {
			d.SuggestedFixes = []SuggestedFix{fix}
		}
	default:
		return
	}
	d.Severity = SeverityWarning
	pass.Report(d)
}

var inKeywordPattern = regexp.MustCompile(`(?i)^\s*(NOT\s+)?IN\s*\(\s*$`)

// inNullCheckFix rewrites `x [NOT] IN (NULL, ...)` to `x IS [NOT] NULL`.
// The list is dropped, so it gives up when a comment would go with it.
func inNullCheckFix(pass *Pass, in *googlesql.ASTInExpression, replacement string) (SuggestedFix, bool) {
	parts, ok := splitInExpression(pass, in)
	if !ok {
		return SuggestedFix{}, false
	}
	src := pass.File.FixedSrc
	// Locations of the tested value leave out the parentheses around it, which
	// open at the start of the IN expression and close before IN.
	opening := strings.Count(src[parts.whole.start:parts.lhs.start], "(")
	if strings.Trim(src[parts.whole.start:parts.lhs.start], "( \t\r\n") != "" {
		return SuggestedFix{}, false
	}
	end := parts.lhs.end
	for range opening {
		for end < parts.list.start && isSQLSpace(src[end]) {
			end++
		}
		if end == parts.list.start || src[end] != ')' {
			return SuggestedFix{}, false
		}
		end++
	}
	if !inKeywordPattern.MatchString(src[end:parts.list.start]) || containsComment(src[parts.list.start:parts.whole.end]) {
		return SuggestedFix{}, false
	}
	r, ok := pass.File.OffsetsToLspRange(parts.whole.start, parts.whole.end)
	if !ok {
		return SuggestedFix{}, false
	}
	return SuggestedFix{
		Title: "Replace with " + replacement,
		Edits: []lsp.TextEdit{{Range: r, NewText: src[parts.whole.start:end] + " " + replacement}},
	}, true
}

// removeNullsFix deletes each NULL in the list together with the comma that
// separates it from a kept element, so the list stays well formed.
func removeNullsFix(pass *Pass, in *googlesql.ASTInExpression, isNull []bool) (SuggestedFix, bool) {
	parts, ok := splitInExpression(pass, in)
	if !ok || len(parts.elems) != len(isNull) {
		return SuggestedFix{}, false
	}
	src := pass.File.FixedSrc
	elems := make([]offsets, len(parts.elems))
	for i, e := range parts.elems {
		elems[i] = widenParens(src, e, parts.list)
	}
	var edits []lsp.TextEdit
	keptBefore := false
	for i, e := range elems {
		if !isNull[i] {
			keptBefore = true
			continue
		}
		del := offsets{elems[max(i-1, 0)].end, e.end}
		if !keptBefore {
			del = offsets{e.start, elems[i+1].start}
		}
		if containsComment(src[del.start:del.end]) {
			return SuggestedFix{}, false
		}
		r, ok := pass.File.OffsetsToLspRange(del.start, del.end)
		if !ok {
			return SuggestedFix{}, false
		}
		edits = append(edits, lsp.TextEdit{Range: r, NewText: ""})
	}
	return SuggestedFix{Title: "Remove NULL from the NOT IN list", Edits: edits}, true
}

type inExpressionParts struct {
	whole, lhs, list offsets
	elems            []offsets
}

// splitInExpression locates the parts of `x [NOT] IN (a, b)`, where lhs and
// the elements leave out their own parentheses and list is what is between
// the list's parentheses. It fails unless the expression's text in FixedSrc
// is what the user wrote.
func splitInExpression(pass *Pass, in *googlesql.ASTInExpression) (inExpressionParts, bool) {
	if in == nil {
		return inExpressionParts{}, false
	}
	if _, ok := writtenText(pass, in); !ok {
		return inExpressionParts{}, false
	}
	parts := inExpressionParts{whole: astOffsets(in)}
	src := pass.File.FixedSrc
	lhs, err := in.Lhs()
	if err != nil || lhs == nil {
		return inExpressionParts{}, false
	}
	list, err := in.InList()
	if err != nil || list == nil {
		return inExpressionParts{}, false
	}
	parts.lhs, parts.list = astOffsets(lhs), astOffsets(list)
	n, err := list.NumChildren()
	if err != nil {
		return inExpressionParts{}, false
	}
	for i := range int32(n) {
		e, err := list.List(i)
		if err != nil || e == nil {
			return inExpressionParts{}, false
		}
		parts.elems = append(parts.elems, astOffsets(e))
	}
	all := append([]offsets{parts.lhs, parts.list}, parts.elems...)
	for _, o := range all {
		if !inBounds(src, o) || o.start < parts.whole.start || o.end > parts.whole.end {
			return inExpressionParts{}, false
		}
	}
	return parts, true
}

// widenParens extends o over the parentheses written around it, staying
// inside within.
func widenParens(src string, o, within offsets) offsets {
	for {
		start, end := o.start, o.end
		for start > within.start && isSQLSpace(src[start-1]) {
			start--
		}
		for end < within.end && isSQLSpace(src[end]) {
			end++
		}
		if start == within.start || end == within.end || src[start-1] != '(' || src[end] != ')' {
			return o
		}
		o = offsets{start - 1, end + 1}
	}
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

// writtenText returns n's text in FixedSrc, failing unless it is exactly
// what the user wrote, so that it can be copied into an edit of Src.
func writtenText(pass *Pass, n googlesql.ASTNode) (string, bool) {
	loc, err := n.GetParseLocationRange()
	if err != nil || loc == nil {
		return "", false
	}
	written, ok := pass.File.ExtractSQL(loc)
	if !ok {
		return "", false
	}
	o := astOffsets(n)
	if !inBounds(pass.File.FixedSrc, o) || pass.File.FixedSrc[o.start:o.end] != written {
		return "", false
	}
	return written, true
}
