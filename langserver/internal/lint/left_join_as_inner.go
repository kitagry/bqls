package lint

import (
	"regexp"
	"strings"

	googlesql "github.com/goccy/go-googlesql"
	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/kitagry/bqls/langserver/internal/source/file"
)

var LeftJoinAsInner = &Analyzer{
	Name:           "left-join-as-inner",
	Doc:            "reports LEFT / RIGHT JOINs whose WHERE clause rejects the NULL rows of the optional side, so they behave as INNER JOINs",
	DefaultEnabled: true,
	Run:            runLeftJoinAsInner,
}

func leftJoinAsInnerMessage(join string) string {
	return "this " + join + " behaves as an INNER JOIN: the WHERE clause rejects the NULL columns of its unmatched rows; move the condition into the ON clause to keep those rows, or write INNER JOIN"
}

// A WHERE condition is checked only against joins reachable from its
// FilterScan through the preserved side of each join: there, a row whose
// optional-side columns are all NULL reaches the filter unchanged. Below the
// optional side, or across subqueries, the NULLs may already have been
// replaced or filtered, so those joins are left alone.
func runLeftJoinAsInner(pass *Pass) {
	joins := astJoinsByLocation(pass.File.Node)
	for _, output := range pass.File.RNode {
		for _, filter := range file.ListResolvedAstNode[*googlesql.ResolvedFilterScan](output) {
			cond, err := filter.FilterExpr()
			if err != nil || cond == nil {
				continue
			}
			input, err := filter.InputScan()
			if err != nil {
				continue
			}
			for _, join := range joinsUnderFilter(input) {
				if optional := optionalColumns(join); len(optional) > 0 && rejectsNullRow(cond, optional) {
					reportLeftJoinAsInner(pass, join, joins[resolvedOffsets(join)])
				}
			}
		}
	}
}

// joinsUnderFilter returns the outer joins whose unmatched rows reach a
// filter on top of scan.
func joinsUnderFilter(scan googlesql.ResolvedScanNode) []*googlesql.ResolvedJoinScan {
	join, ok := scan.(*googlesql.ResolvedJoinScan)
	if !ok {
		return nil
	}
	typ, err := join.JoinType()
	if err != nil {
		return nil
	}
	left, err := join.LeftScan()
	if err != nil {
		return nil
	}
	right, err := join.RightScan()
	if err != nil {
		return nil
	}
	switch typ {
	case googlesql.ResolvedJoinScanEnums_JoinTypeInner:
		return append(joinsUnderFilter(left), joinsUnderFilter(right)...)
	case googlesql.ResolvedJoinScanEnums_JoinTypeLeft:
		return append(joinsUnderFilter(left), join)
	case googlesql.ResolvedJoinScanEnums_JoinTypeRight:
		return append(joinsUnderFilter(right), join)
	}
	return nil
}

// optionalColumns returns the ids of the columns that are NULL in the rows
// an outer join adds for unmatched rows.
func optionalColumns(join *googlesql.ResolvedJoinScan) map[int32]bool {
	side, ok := optionalSide(join)
	if !ok {
		return nil
	}
	ids, _ := scanColumnIDs(side)
	return ids
}

// rejectsNullRow reports whether cond is certainly NULL or FALSE when every
// column in optional is NULL. Only top-level AND terms are inspected, since
// an OR may still pass the row through its other branch.
func rejectsNullRow(cond googlesql.ResolvedExprNode, optional map[int32]bool) bool {
	call, ok := cond.(*googlesql.ResolvedFunctionCall)
	if !ok {
		return yieldsNull(cond, optional)
	}
	name, ok := functionName(call)
	if !ok {
		return false
	}
	args, err := call.ArgumentList()
	if err != nil {
		return false
	}
	switch name {
	case "$and":
		for _, arg := range args {
			if rejectsNullRow(arg, optional) {
				return true
			}
		}
		return false
	case "$is_true", "$is_false":
		// NULL IS TRUE and NULL IS FALSE are both FALSE.
		return len(args) == 1 && yieldsNull(args[0], optional)
	case "$not":
		// x IS NOT NULL resolves to $not($is_null(x)).
		if len(args) == 1 {
			if inner, ok := args[0].(*googlesql.ResolvedFunctionCall); ok {
				if innerName, ok := functionName(inner); ok && innerName == "$is_null" {
					innerArgs, err := inner.ArgumentList()
					return err == nil && len(innerArgs) == 1 && yieldsNull(innerArgs[0], optional)
				}
			}
		}
	}
	return yieldsNull(cond, optional)
}

// nullPropagatingArgs lists, per function, which arguments make the result
// NULL when they are NULL; -1 means any argument. Functions that can turn
// NULL into a value (COALESCE, IF, IS NULL, OR, ...) are deliberately absent.
var nullPropagatingArgs = map[string]int{
	"$equal":            -1,
	"$not_equal":        -1,
	"$less":             -1,
	"$less_or_equal":    -1,
	"$greater":          -1,
	"$greater_or_equal": -1,
	"$like":             -1,
	"$add":              -1,
	"$subtract":         -1,
	"$multiply":         -1,
	"$divide":           -1,
	"$unary_minus":      -1,
	"$not":              -1,
	// Only the tested value: `NULL IN (...)` is NULL, but `x IN (NULL, 1)` may be TRUE.
	"$in": 0,
	// Only the tested value: `x BETWEEN NULL AND 0` is FALSE for x > 0, which
	// also rejects the row, but trusting the bounds is not worth the reasoning.
	"$between": 0,
}

// yieldsNull reports whether expr is certainly NULL when every column in
// optional is NULL.
func yieldsNull(expr googlesql.ResolvedExprNode, optional map[int32]bool) bool {
	switch e := expr.(type) {
	case *googlesql.ResolvedColumnRef:
		c, err := e.Column()
		if err != nil {
			return false
		}
		id, err := c.ColumnId()
		return err == nil && optional[id]
	case *googlesql.ResolvedCast:
		inner, err := e.Expr()
		return err == nil && yieldsNull(inner, optional)
	case *googlesql.ResolvedGetStructField:
		inner, err := e.Expr()
		return err == nil && yieldsNull(inner, optional)
	case *googlesql.ResolvedFunctionCall:
		name, ok := functionName(e)
		if !ok {
			return false
		}
		idx, ok := nullPropagatingArgs[name]
		if !ok {
			return false
		}
		args, err := e.ArgumentList()
		if err != nil {
			return false
		}
		if idx >= 0 {
			return idx < len(args) && yieldsNull(args[idx], optional)
		}
		for _, arg := range args {
			if yieldsNull(arg, optional) {
				return true
			}
		}
	}
	return false
}

func reportLeftJoinAsInner(pass *Pass, join *googlesql.ResolvedJoinScan, ast *googlesql.ASTJoin) {
	loc, err := join.GetParseLocationRangeOrNULL()
	if err != nil || loc == nil {
		return
	}
	r, ok := pass.File.OffsetsToLspRange(file.ParseLocStart(loc), file.ParseLocEnd(loc))
	if !ok {
		return
	}
	typ, err := join.JoinType()
	if err != nil {
		return
	}
	keyword := "LEFT JOIN"
	if typ == googlesql.ResolvedJoinScanEnums_JoinTypeRight {
		keyword = "RIGHT JOIN"
	}
	d := Diagnostic{
		Range:    r,
		Message:  leftJoinAsInnerMessage(keyword),
		Severity: SeverityWarning,
	}
	if ast != nil {
		if edit, ok := innerJoinEdit(pass.File, ast); ok {
			d.SuggestedFixes = []SuggestedFix{{
				Title: "Replace with INNER JOIN",
				Edits: []lsp.TextEdit{edit},
			}}
		}
	}
	pass.Report(d)
}

// outerJoinKeyword matches the join keywords of an outer join; the first
// group is the part replaced by INNER, so that a join hint is kept.
var outerJoinKeyword = regexp.MustCompile(`(?i)^((?:LEFT|RIGHT)(?:\s+OUTER)?)\s+(?:(?:HASH|LOOKUP)\s+)?JOIN$`)

// innerJoinEdit rewrites the outer join keywords to INNER. A RIGHT JOIN with
// USING is left alone: its USING columns come from the right side, while an
// INNER JOIN's come from the left, which may differ in type or value.
func innerJoinEdit(f file.ParsedFile, j *googlesql.ASTJoin) (lsp.TextEdit, bool) {
	typ, err := j.JoinType()
	if err != nil {
		return lsp.TextEdit{}, false
	}
	if using, err := j.UsingClause(); err != nil || (using != nil && typ == googlesql.ASTJoinEnums_JoinTypeRight) {
		return lsp.TextEdit{}, false
	}
	loc, err := j.JoinLocation()
	if err != nil || loc == nil {
		return lsp.TextEdit{}, false
	}
	at := astOffsets(loc)
	if !inBounds(f.FixedSrc, at) {
		return lsp.TextEdit{}, false
	}
	m := outerJoinKeyword.FindStringSubmatchIndex(f.FixedSrc[at.start:at.end])
	if m == nil {
		return lsp.TextEdit{}, false
	}
	outer := offsets{at.start + m[2], at.start + m[3]}
	r, ok := writtenSpan(f, outer)
	if !ok {
		return lsp.TextEdit{}, false
	}
	return lsp.TextEdit{Range: r, NewText: matchCase("INNER", f.FixedSrc[outer.start:outer.end])}, true
}

// matchCase returns keyword in lower case when the text it replaces is.
func matchCase(keyword, replaced string) string {
	if replaced == strings.ToLower(replaced) {
		return strings.ToLower(keyword)
	}
	return keyword
}
