package lint

import (
	"errors"
	"slices"

	googlesql "github.com/goccy/go-googlesql"
	"github.com/kitagry/bqls/langserver/internal/source/file"
)

var SetOperationImplicitCast = &Analyzer{
	Name:           "set-operation-implicit-cast",
	Doc:            "reports set operation columns that are implicitly cast to FLOAT64 because another input has a FLOAT64 there",
	DefaultEnabled: true,
	Run:            runSetOperationImplicitCast,
}

func setOperationImplicitCastMessage(from string) string {
	return "this column is " + from + " here but FLOAT64 in another input of the set operation, so it is implicitly cast to FLOAT64, which cannot represent every " + from + " value exactly (e.g. INT64 values above 2^53); give the inputs the same type, or write an explicit CAST"
}

// Widening to NUMERIC or BIGNUMERIC is exact, so only casts to FLOAT64 are
// reported.
var lossyToFloat64 = map[string]bool{
	"INT64":      true,
	"NUMERIC":    true,
	"BIGNUMERIC": true,
}

// The analyzer coerces an input by wrapping its scan in a ProjectScan of
// casts, which carry no location, so the select list column is found by
// position in the parse tree.
func runSetOperationImplicitCast(pass *Pass) {
	astSetOps := file.ListAstNode[*googlesql.ASTSetOperation](pass.File.Node)
	for _, output := range pass.File.RNode {
		for _, scan := range file.ListResolvedAstNode[*googlesql.ResolvedSetOperationScan](output) {
			items, err := scan.InputItemList()
			if err != nil {
				continue
			}
			inputs, ok := astSetOperationInputs(astSetOps, items)
			if !ok {
				continue
			}
			reported := make(map[setOperationReport]bool)
			for i, item := range items {
				checkSetOperationItem(pass, item, inputs[i], reported)
			}
		}
	}
}

type setOperationReport struct {
	at   offsets
	from string
}

// astSetOperationInputs returns the inputs of the set operation that items
// were resolved from. A set operation scan has no location under ORDER BY,
// LIMIT or a WITH clause, so it is matched through a located node of each
// input instead: the innermost set operation whose inputs hold them in order.
func astSetOperationInputs(setOps []*googlesql.ASTSetOperation, items []*googlesql.ResolvedSetOperationItem) ([]googlesql.ASTQueryExpressionNode, bool) {
	locs := make([]int, len(items))
	for i, item := range items {
		scan, err := item.Scan()
		if err != nil {
			return nil, false
		}
		loc, ok := firstLocation(scan)
		if !ok {
			return nil, false
		}
		locs[i] = loc
	}
	var found []googlesql.ASTQueryExpressionNode
	var span offsets
	for _, s := range setOps {
		inputs := setOperationInputs(s)
		if len(inputs) != len(items) {
			continue
		}
		matches := true
		for i, in := range inputs {
			if o := astOffsets(in); locs[i] < o.start || locs[i] >= o.end {
				matches = false
				break
			}
		}
		if !matches {
			continue
		}
		if o := astOffsets(s); found == nil || o.end-o.start < span.end-span.start {
			found, span = inputs, o
		}
	}
	return found, found != nil
}

// firstLocation returns the start of the first node under scan that has a
// location.
func firstLocation(scan googlesql.ResolvedNode) (int, bool) {
	start := -1
	file.WalkResolved(scan, func(n googlesql.ResolvedNode) error { //nolint
		if o := resolvedOffsets(n); o.start >= 0 {
			start = o.start
			return errStopWalk
		}
		return nil
	})
	return start, start >= 0
}

var errStopWalk = errors.New("stop walk")

func checkSetOperationItem(pass *Pass, item *googlesql.ResolvedSetOperationItem, input googlesql.ASTQueryExpressionNode, reported map[setOperationReport]bool) {
	itemScan, err := item.Scan()
	if err != nil {
		return
	}
	project, ok := itemScan.(*googlesql.ResolvedProjectScan)
	if !ok {
		return
	}
	// CORRESPONDING and BY NAME put a ProjectScan that reorders the columns
	// to the output order over the one that casts them.
	if src, err := project.NodeSource(); err == nil && src == "resolver_set_operation_corresponding" {
		in, err := project.InputScan()
		if err != nil {
			return
		}
		if project, ok = in.(*googlesql.ResolvedProjectScan); !ok {
			return
		}
	}
	exprs, err := project.ExprList()
	if err != nil {
		return
	}
	// The casting ProjectScan lists its columns in select list order.
	cols, err := project.ColumnList()
	if err != nil {
		return
	}
	for _, computed := range exprs {
		expr, err := computed.Expr()
		if err != nil {
			continue
		}
		inner, ok := implicitCastOperand(expr)
		if !ok {
			continue
		}
		if _, ok := inner.(*googlesql.ResolvedLiteral); ok {
			continue
		}
		from, ok := exprTypeName(inner)
		if !ok || !lossyToFloat64[from] {
			continue
		}
		if to, ok := exprTypeName(expr); !ok || to != "FLOAT64" {
			continue
		}
		id, ok := columnID(computed.Column())
		if !ok {
			continue
		}
		pos := slices.Index(columnIDs(cols), id)
		at, ok := selectColumnOffsets(input, pos)
		if !ok {
			at = astOffsets(input)
		}
		// Columns not written out, as in SELECT *, all fall back to the input.
		key := setOperationReport{at, from}
		if reported[key] {
			continue
		}
		reported[key] = true
		r, ok := pass.File.OffsetsToLspRange(at.start, at.end)
		if !ok {
			continue
		}
		pass.Report(Diagnostic{Range: r, Message: setOperationImplicitCastMessage(from), Severity: SeverityWarning})
	}
}

// selectColumnOffsets returns the location of the i-th select list column of
// input, when input is a SELECT whose parse tree shows it. The columns of a
// nested set operation come from all of its inputs, so none of their select
// lists stands for them.
func selectColumnOffsets(input googlesql.ASTQueryExpressionNode, i int) (offsets, bool) {
	if q, ok := input.(*googlesql.ASTQuery); ok {
		if hasPipeOperator(q) {
			return offsets{}, false
		}
		expr, err := q.QueryExpr()
		if err != nil || isNilNode(expr) {
			return offsets{}, false
		}
		input = expr
	}
	sel, ok := input.(*googlesql.ASTSelect)
	if !ok {
		return offsets{}, false
	}
	cols, ok := selectListColumns(sel)
	if !ok || i < 0 || i >= len(cols.names) {
		return offsets{}, false
	}
	col, err := cols.list.Columns(int32(i))
	if err != nil || col == nil {
		return offsets{}, false
	}
	return astOffsets(col), true
}
