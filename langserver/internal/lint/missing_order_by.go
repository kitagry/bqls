package lint

import (
	"strings"

	googlesql "github.com/goccy/go-googlesql"
	"github.com/kitagry/bqls/langserver/internal/source/file"
)

var MissingOrderBy = &Analyzer{
	Name: "missing-order-by",
	Doc:  "reports ROW_NUMBER filters, ARRAY_AGG / STRING_AGG with LIMIT, and indexed ARRAY_AGG results that have no ORDER BY, so they pick arbitrary rows",
	Run:  runMissingOrderBy,
}

const (
	rowNumberWithoutOrderMessage  = "ROW_NUMBER() has no ORDER BY in its window, so it numbers rows in an arbitrary order and the rows a filter on it keeps can change from run to run; add ORDER BY to the window"
	arrayIndexWithoutOrderMessage = "ARRAY_AGG without ORDER BY returns its elements in an arbitrary order, so indexing the result picks an arbitrary element; add ORDER BY inside the call"
)

func aggLimitWithoutOrderMessage(fn string) string {
	return fn + " with LIMIT but no ORDER BY keeps arbitrary elements, which can change from run to run; add ORDER BY inside the call"
}

// googlesql rejects a window without ORDER BY for every other numbering and
// navigation function (RANK, LAG, FIRST_VALUE, ...), so only ROW_NUMBER and
// the aggregates that keep an order are left to check.
var limitedAggregates = map[string]bool{"array_agg": true, "string_agg": true, "array_concat_agg": true}

var arrayIndexFunctions = map[string]bool{
	"$array_at_offset": true, "$array_at_ordinal": true,
	"$safe_array_at_offset": true, "$safe_array_at_ordinal": true,
}

// A false positive here is worse than a miss. An arbitrary order matters only
// when it decides which rows or elements are kept, so ROW_NUMBER is reported
// only when a condition reads it, and ARRAY_AGG without LIMIT only when its
// result is indexed. Rows that look the same to the rest of the statement, as
// when exact duplicates are removed with `PARTITION BY TO_JSON_STRING(t)`,
// make any order give the same result and are not reported either.
func runMissingOrderBy(pass *Pass) {
	flows := pass.columnFlows()
	for i, output := range pass.File.RNode {
		flow := flows[i]
		if flow == nil {
			continue
		}
		stmt, err := output.ResolvedStatement()
		if err != nil {
			continue
		}
		indexed := indexedArrays(stmt)
		walkLive(stmt, func(n googlesql.ResolvedNode) {
			switch n := n.(type) {
			case *googlesql.ResolvedAnalyticScan:
				checkAnalyticScan(pass, flow, indexed, n)
			case *googlesql.ResolvedAggregateScan:
				checkAggregateScan(pass, flow, indexed, n)
			}
		})
	}
}

func checkAnalyticScan(pass *Pass, flow *columnFlow, indexed map[int32]bool, scan *googlesql.ResolvedAnalyticScan) {
	groups, err := scan.FunctionGroupList()
	if err != nil {
		return
	}
	input, err := scan.InputScan()
	if err != nil || input == nil {
		return
	}
	for _, g := range groups {
		if order, err := g.OrderBy(); err != nil || order != nil {
			continue
		}
		keys := partitionKeys(g, input)
		calls, err := g.AnalyticFunctionList()
		if err != nil {
			continue
		}
		for _, c := range calls {
			id, expr, ok := computed(c)
			if !ok {
				continue
			}
			call, ok := expr.(*googlesql.ResolvedAnalyticFunctionCall)
			if !ok {
				continue
			}
			name, ok := functionName(call)
			if !ok {
				continue
			}
			switch {
			case name == "row_number" && flow.aliasReaches(id, flow.filtered) && !sameRows(flow, input, keys):
				reportMissingOrderBy(pass, call, rowNumberWithoutOrderMessage)
			case name == "array_agg" && flow.aliasReaches(id, indexed) && !covers(keys, argumentColumns(call)):
				reportMissingOrderBy(pass, call, arrayIndexWithoutOrderMessage)
			}
		}
	}
}

func checkAggregateScan(pass *Pass, flow *columnFlow, indexed map[int32]bool, scan *googlesql.ResolvedAggregateScan) {
	aggs, err := scan.AggregateList()
	if err != nil {
		return
	}
	groupBy, err := scan.GroupByList()
	if err != nil {
		return
	}
	keys := make(map[int32]bool)
	for _, g := range groupBy {
		id, expr, ok := computed(g)
		if !ok {
			return
		}
		keys[id] = true
		for _, ref := range referencedColumnIDs(expr) {
			keys[ref] = true
		}
	}
	for _, a := range aggs {
		id, expr, ok := computed(a)
		if !ok {
			continue
		}
		call, ok := expr.(*googlesql.ResolvedAggregateFunctionCall)
		if !ok {
			continue
		}
		if order, err := call.OrderByItemList(); err != nil || len(order) > 0 {
			continue
		}
		name, ok := functionName(call)
		if !ok || !limitedAggregates[name] || covers(keys, argumentColumns(call)) {
			continue
		}
		limit, err := call.Limit()
		switch {
		case err == nil && !isNilNode(limit):
			reportMissingOrderBy(pass, call, aggLimitWithoutOrderMessage(strings.ToUpper(name)))
		case name == "array_agg" && flow.aliasReaches(id, indexed):
			reportMissingOrderBy(pass, call, arrayIndexWithoutOrderMessage)
		}
	}
}

// indexedArrays returns the columns read by [OFFSET(n)] and its variants.
func indexedArrays(stmt googlesql.ResolvedNode) map[int32]bool {
	indexed := make(map[int32]bool)
	_ = file.WalkResolved(stmt, func(n googlesql.ResolvedNode) error {
		call, ok := n.(*googlesql.ResolvedFunctionCall)
		if !ok {
			return nil
		}
		if name, ok := functionName(call); !ok || !arrayIndexFunctions[name] {
			return nil
		}
		args, err := call.ArgumentList()
		if err != nil || len(args) == 0 {
			return nil
		}
		if id, ok := columnRefID(args[0]); ok {
			indexed[id] = true
		}
		return nil
	})
	return indexed
}

// partitionKeys returns the partitioning columns together with the columns
// the expressions computed for them read, such as `t`'s columns for
// `PARTITION BY TO_JSON_STRING(t)`.
func partitionKeys(g *googlesql.ResolvedAnalyticFunctionGroup, input googlesql.ResolvedScanNode) map[int32]bool {
	keys := make(map[int32]bool)
	p, err := g.PartitionBy()
	if err != nil || p == nil {
		return keys
	}
	refs, err := p.PartitionByList()
	if err != nil {
		return keys
	}
	for _, r := range refs {
		if id, ok := columnRefID(r); ok {
			keys[id] = true
		}
	}
	project, ok := input.(*googlesql.ResolvedProjectScan)
	if !ok {
		return keys
	}
	exprs, err := project.ExprList()
	if err != nil {
		return keys
	}
	for _, cc := range exprs {
		if id, expr, ok := computed(cc); ok && keys[id] {
			for _, ref := range referencedColumnIDs(expr) {
				keys[ref] = true
			}
		}
	}
	return keys
}

// sameRows reports whether the rows of a partition can differ only in
// columns whose values the statement never uses.
func sameRows(flow *columnFlow, input googlesql.ResolvedScanNode, keys map[int32]bool) bool {
	cols, err := input.MutableColumnList()
	if err != nil {
		return false
	}
	for _, id := range columnIDs(cols) {
		if keys[id] {
			continue
		}
		for d := range flow.derivedFrom([]int32{id}) {
			if flow.used[d] {
				return false
			}
		}
	}
	return true
}

// aliasReaches reports whether id, or any plain copy of it, is in targets.
func (f *columnFlow) aliasReaches(id int32, targets map[int32]bool) bool {
	for a := range f.aliasedFrom([]int32{id}) {
		if targets[a] {
			return true
		}
	}
	return false
}

// argumentColumns returns the columns call's arguments read, leaving out
// options such as LIMIT.
func argumentColumns(call interface {
	ArgumentList() ([]googlesql.ResolvedExprNode, error)
}) []int32 {
	args, err := call.ArgumentList()
	if err != nil {
		return nil
	}
	var ids []int32
	for _, a := range args {
		ids = append(ids, referencedColumnIDs(a)...)
	}
	return ids
}

func covers(keys map[int32]bool, ids []int32) bool {
	for _, id := range ids {
		if !keys[id] {
			return false
		}
	}
	return true
}

func computed(n googlesql.ResolvedNode) (int32, googlesql.ResolvedExprNode, bool) {
	cc, ok := n.(computedColumn)
	if !ok {
		return 0, nil, false
	}
	col, err := cc.Column()
	if err != nil || col == nil {
		return 0, nil, false
	}
	id, err := col.ColumnId()
	if err != nil {
		return 0, nil, false
	}
	expr, err := cc.Expr()
	if err != nil || isNilNode(expr) {
		return 0, nil, false
	}
	return id, expr, true
}

func reportMissingOrderBy(pass *Pass, call googlesql.ResolvedNode, message string) {
	at := resolvedOffsets(call)
	r, ok := pass.File.OffsetsToLspRange(at.start, at.end)
	if !ok {
		return
	}
	pass.Report(Diagnostic{
		Range:    r,
		Message:  message,
		Severity: SeverityWarning,
	})
}
