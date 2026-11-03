package lint

import (
	"fmt"
	"reflect"
	"strings"

	bq "cloud.google.com/go/bigquery"
	googlesql "github.com/goccy/go-googlesql"
	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/kitagry/bqls/langserver/internal/source/file"
)

var ArrayNullElement = &Analyzer{
	Name: "array-null-element",
	Doc:  "reports arrays that may hold a NULL element and reach the result, which BigQuery fails to write",
	// Columns are NULLABLE unless declared otherwise, so many that never
	// hold a NULL would be reported, while a real NULL fails the query
	// loudly rather than returning a wrong result.
	DefaultEnabled: false,
	Run:            runArrayNullElement,
}

// BigQuery only rejects such an array when it is written out, so only arrays
// that reach the statement's output unchanged are checked; one consumed by
// UNNEST, ARRAY_LENGTH and the like inside the query is fine.
func runArrayNullElement(pass *Pass) {
	for _, output := range pass.File.RNode {
		stmt, err := output.ResolvedStatement()
		if err != nil || stmt == nil {
			continue
		}
		outCols, values := writtenArrays(stmt)
		if len(outCols) == 0 && len(values) == 0 {
			continue
		}
		c := newArrayNullChecker(pass, stmt)
		for _, id := range outCols {
			if expr, ok := c.computedArray(id); ok {
				c.check(expr)
			}
		}
		for _, v := range values {
			c.check(v)
		}
	}
}

// writtenArrays returns the output columns a statement writes out, and the
// values an INSERT ... VALUES writes. A view is not run when it is created.
func writtenArrays(stmt googlesql.ResolvedStatementNode) ([]int32, []googlesql.ResolvedExprNode) {
	switch s := stmt.(type) {
	case *googlesql.ResolvedQueryStmt:
		return columnIDs(resolvedOutputColumns(s.OutputColumnList())), nil
	case *googlesql.ResolvedCreateTableAsSelectStmt:
		return columnIDs(resolvedOutputColumns(s.OutputColumnList())), nil
	case *googlesql.ResolvedInsertStmt:
		cols, _ := s.QueryOutputColumnList()
		return columnIDs(cols), insertedValues(s)
	}
	return nil, nil
}

func insertedValues(stmt *googlesql.ResolvedInsertStmt) []googlesql.ResolvedExprNode {
	rows, err := stmt.RowList()
	if err != nil {
		return nil
	}
	var values []googlesql.ResolvedExprNode
	for _, row := range rows {
		list, err := row.ValueList()
		if err != nil {
			continue
		}
		for _, v := range list {
			if expr, err := v.Value(); err == nil && expr != nil {
				values = append(values, expr)
			}
		}
	}
	return values
}

func resolvedOutputColumns(list []*googlesql.ResolvedOutputColumn, err error) []*googlesql.ResolvedColumn {
	if err != nil {
		return nil
	}
	var cols []*googlesql.ResolvedColumn
	for _, o := range list {
		if c, err := o.Column(); err == nil && c != nil {
			cols = append(cols, c)
		}
	}
	return cols
}

type arrayNullChecker struct {
	pass    *Pass
	lineage *columnLineage
	// computed maps a column id to the expression that computes it.
	computed map[int32]googlesql.ResolvedExprNode
	// filtered is every column id any condition mentions, subqueries
	// included, since e.g. `NOT EXISTS (SELECT 1 FROM UNNEST(arr) AS e
	// WHERE e IS NULL)` drops the rows whose array holds a NULL.
	filtered map[int32]bool
}

func newArrayNullChecker(pass *Pass, stmt googlesql.ResolvedStatementNode) *arrayNullChecker {
	c := &arrayNullChecker{
		pass:     pass,
		lineage:  traceColumnLineage(stmt),
		computed: make(map[int32]googlesql.ResolvedExprNode),
		filtered: make(map[int32]bool),
	}
	addCondition := func(expr googlesql.ResolvedExprNode, err error) {
		if err != nil || expr == nil {
			return
		}
		for _, id := range referencedColumnIDs(expr) {
			c.filtered[id] = true
		}
	}
	_ = file.WalkResolved(stmt, func(n googlesql.ResolvedNode) error {
		switch n := n.(type) {
		case *googlesql.ResolvedComputedColumn:
			col, err := n.Column()
			expr, err2 := n.Expr()
			if err != nil || err2 != nil || col == nil || expr == nil {
				break
			}
			if id, err := col.ColumnId(); err == nil {
				c.computed[id] = expr
			}
		case *googlesql.ResolvedFilterScan:
			addCondition(n.FilterExpr())
		case *googlesql.ResolvedJoinScan:
			addCondition(n.JoinExpr())
		case *googlesql.ResolvedArrayScan:
			addCondition(n.JoinExpr())
		}
		return nil
	})
	return c
}

// computedArray follows id through unchanged copies to the expression that
// computes it, failing when a condition mentions any column on the way.
func (c *arrayNullChecker) computedArray(id int32) (googlesql.ResolvedExprNode, bool) {
	seen := make(map[int32]bool)
	for !seen[id] {
		seen[id] = true
		if c.filtered[id] {
			return nil, false
		}
		if next, ok := c.lineage.copied[id]; ok {
			id = next
			continue
		}
		expr, ok := c.computed[id]
		return expr, ok
	}
	return nil, false
}

func (c *arrayNullChecker) check(expr googlesql.ResolvedExprNode) {
	switch e := expr.(type) {
	case *googlesql.ResolvedAggregateFunctionCall:
		c.checkArrayAgg(e, e.ResolvedNonScalarFunctionCallBase, true)
	case *googlesql.ResolvedAnalyticFunctionCall:
		// BigQuery rejects IGNORE NULLS on analytic ARRAY_AGG.
		c.checkArrayAgg(e, e.ResolvedNonScalarFunctionCallBase, false)
	case *googlesql.ResolvedSubqueryExpr:
		if typ, err := e.SubqueryType(); err != nil || typ != googlesql.ResolvedSubqueryExprEnums_SubqueryTypeArray {
			return
		}
		scan, err := e.Subquery()
		if err != nil || scan == nil {
			return
		}
		cols, err := scan.MutableColumnList()
		if err != nil || len(cols) != 1 {
			return
		}
		id, err := cols[0].ColumnId()
		if err != nil {
			return
		}
		if reason, ok := c.nullableColumn(id); ok {
			c.report(arraySubqueryOffsets(c.pass, resolvedOffsets(e)), reason, false, nil)
		}
	case *googlesql.ResolvedLiteral:
		if literalHasNullElement(e) {
			c.report(resolvedOffsets(e), "it has a NULL element", false, nil)
		}
	case *googlesql.ResolvedFunctionCall:
		if name, ok := functionName(e); !ok || name != "$make_array" {
			return
		}
		args, err := e.ArgumentList()
		if err != nil {
			return
		}
		for _, arg := range args {
			if reason, ok := c.nullableElement(arg); ok {
				c.report(resolvedOffsets(e), reason, false, nil)
				return
			}
		}
	}
}

func (c *arrayNullChecker) checkArrayAgg(call googlesql.ResolvedNode, base *googlesql.ResolvedNonScalarFunctionCallBase, fixable bool) {
	if name, ok := functionName(base); !ok || name != "array_agg" {
		return
	}
	modifier, err := base.NullHandlingModifier()
	if err != nil || modifier == googlesql.ResolvedNonScalarFunctionCallBaseEnums_NullHandlingModifierIgnoreNulls {
		return
	}
	args, err := base.ArgumentList()
	if err != nil || len(args) != 1 {
		return
	}
	reason, ok := c.nullableElement(args[0])
	if !ok {
		return
	}
	at := resolvedOffsets(call)
	ast, analytic := arrayAggCall(c.pass, at.start)
	if ast == nil {
		return
	}
	span := astOffsets(ast)
	if analytic != nil {
		span = astOffsets(analytic)
	}
	var fix *SuggestedFix
	if fixable && modifier == googlesql.ResolvedNonScalarFunctionCallBaseEnums_NullHandlingModifierDefaultNullHandling {
		fix = ignoreNullsFix(c.pass, ast)
	}
	c.report(span, reason, fixable, fix)
}

// nullableElement reports why expr may be NULL: a NULL literal, or a copy of
// a NULLABLE table column that no condition mentions.
func (c *arrayNullChecker) nullableElement(expr googlesql.ResolvedExprNode) (string, bool) {
	if isNullLiteral(expr) {
		return "it has a NULL element", true
	}
	ref, ok := expr.(*googlesql.ResolvedColumnRef)
	if !ok {
		return "", false
	}
	col, err := ref.Column()
	if err != nil || col == nil {
		return "", false
	}
	id, err := col.ColumnId()
	if err != nil {
		return "", false
	}
	return c.nullableColumn(id)
}

func (c *arrayNullChecker) nullableColumn(id int32) (string, bool) {
	src, ok := c.lineage.source(id)
	if !ok {
		return "", false
	}
	table, err := src.scan.Table()
	if err != nil || table == nil {
		return "", false
	}
	name, err := table.Name()
	if err != nil {
		return "", false
	}
	meta, err := c.pass.LookupTable(c.pass.Context, name)
	// Only a regular table declares its modes; e.g. a view derives them from its query.
	if err != nil || meta == nil || meta.Type != bq.RegularTable || !nullableField(meta.Schema, src.name) {
		return "", false
	}
	return fmt.Sprintf("`%s` of `%s` is NULLABLE", src.name, name), true
}

func literalHasNullElement(lit *googlesql.ResolvedLiteral) bool {
	v, err := lit.Value()
	if err != nil || v == nil {
		return false
	}
	if isNull, err := v.IsNull(); err != nil || isNull {
		return false
	}
	if kind, err := v.TypeKind(); err != nil || kind != googlesql.TypeKindTypeArray {
		return false
	}
	elems, err := v.Elements()
	if err != nil {
		return false
	}
	for _, e := range elems {
		if isNull, err := e.IsNull(); err == nil && isNull {
			return true
		}
	}
	return false
}

// arrayAggCall returns the ARRAY_AGG call written at start, and the analytic
// call around it if it has an OVER clause.
func arrayAggCall(pass *Pass, start int) (*googlesql.ASTFunctionCall, *googlesql.ASTAnalyticFunctionCall) {
	for _, a := range file.ListAstNode[*googlesql.ASTAnalyticFunctionCall](pass.File.Node) {
		if astOffsets(a).start != start {
			continue
		}
		if f, err := a.Function(); err == nil && f != nil {
			return f, a
		}
	}
	for _, f := range file.ListAstNode[*googlesql.ASTFunctionCall](pass.File.Node) {
		if astOffsets(f).start == start {
			return f, nil
		}
	}
	return nil, nil
}

// arraySubqueryOffsets widens the subquery of an ARRAY(...) to the whole
// expression.
func arraySubqueryOffsets(pass *Pass, sub offsets) offsets {
	found := sub
	for _, e := range file.ListAstNode[*googlesql.ASTExpressionSubquery](pass.File.Node) {
		o := astOffsets(e)
		if o.start <= sub.start && sub.end <= o.end && (found == sub || o.end-o.start < found.end-found.start) {
			found = o
		}
	}
	return found
}

// ignoreNullsFix writes IGNORE NULLS right after the argument, where it goes
// before any ORDER BY or LIMIT.
func ignoreNullsFix(pass *Pass, call *googlesql.ASTFunctionCall) *SuggestedFix {
	if m, err := call.NullHandlingModifier(); err != nil || m != googlesql.ASTFunctionCallEnums_NullHandlingModifierDefaultNullHandling {
		return nil
	}
	arg, err := call.Arguments(0)
	if err != nil || arg == nil || reflect.ValueOf(arg).IsNil() {
		return nil
	}
	fn, err := call.Function()
	if err != nil || fn == nil {
		return nil
	}
	src := pass.File.FixedSrc
	whole := astOffsets(call)
	open := strings.IndexByte(src[astOffsets(fn).end:whole.end], '(')
	if open < 0 {
		return nil
	}
	// The parentheses of `ARRAY_AGG((x))` are not part of the argument's
	// span, and IGNORE NULLS has to follow them.
	inner := offsets{astOffsets(fn).end + open + 1, whole.end - 1}
	end := widenParens(src, astOffsets(arg), inner).end
	r, ok := pass.File.OffsetsToLspRange(end, end)
	if !ok {
		return nil
	}
	return &SuggestedFix{
		Title: "Add IGNORE NULLS, which drops the NULLs and so leaves the array shorter",
		Edits: []lsp.TextEdit{{Range: r, NewText: " IGNORE NULLS"}},
	}
}

func (c *arrayNullChecker) report(at offsets, reason string, arrayAgg bool, fix *SuggestedFix) {
	r, ok := c.pass.File.OffsetsToLspRange(at.start, at.end)
	if !ok {
		return
	}
	remedy := "filter out the NULLs before building the array"
	if arrayAgg {
		remedy = "add IGNORE NULLS, which drops the NULLs and so leaves the array shorter, or filter them out"
	}
	d := Diagnostic{
		Range: r,
		Message: fmt.Sprintf("this array may hold a NULL element, as %s, and BigQuery fails with \"Array cannot have a null element\" once one reaches the result; %s",
			reason, remedy),
		Severity: SeverityWarning,
	}
	if fix != nil {
		d.SuggestedFixes = []SuggestedFix{*fix}
	}
	c.pass.Report(d)
}
