package lint

import (
	googlesql "github.com/goccy/go-googlesql"
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
					reportLeftJoinAsInner(pass, join)
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

func reportLeftJoinAsInner(pass *Pass, join *googlesql.ResolvedJoinScan) {
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
	pass.Report(Diagnostic{
		Range:    r,
		Message:  leftJoinAsInnerMessage(keyword),
		Severity: SeverityWarning,
	})
}
