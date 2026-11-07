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

var NotInNullable = &Analyzer{
	Name:           "not-in-nullable",
	Doc:            "reports `x NOT IN (subquery)` whose subquery returns a NULLABLE table column, which yields no rows once that column holds a NULL",
	DefaultEnabled: true,
	Run:            runNotInNullable,
}

// A false positive here pushes users to disable the rule, so the subquery's
// column is only traced through plain pass-through (projections, joins,
// GROUP BY keys and CTE references) down to a table column the schema marks
// NULLABLE; any expression such as IFNULL or an aggregate stops the trace.
// A condition anywhere in the statement that mentions a traced column is
// taken as filtering NULLs out, since `IS NOT NULL`, `=` and `>` all do.
func runNotInNullable(pass *Pass) {
	for _, output := range pass.File.RNode {
		stmt, err := output.ResolvedStatement()
		if err != nil || stmt == nil {
			continue
		}
		var lineage *columnLineage
		for _, call := range file.ListResolvedAstNode[*googlesql.ResolvedFunctionCall](output) {
			col, ok := notInSubqueryColumn(call)
			if !ok {
				continue
			}
			if lineage == nil {
				lineage = traceColumnLineage(stmt)
			}
			checkNotInNullable(pass, lineage, call, col)
		}
	}
}

// notInSubqueryColumn returns the column a `NOT IN (subquery)` compares
// against. Both `x NOT IN (...)` and `NOT (x IN (...))` resolve to $not
// over an IN subquery.
func notInSubqueryColumn(call *googlesql.ResolvedFunctionCall) (*googlesql.ResolvedColumn, bool) {
	if name, ok := functionName(call); !ok || name != "$not" {
		return nil, false
	}
	args, err := call.ArgumentList()
	if err != nil || len(args) != 1 {
		return nil, false
	}
	sub, ok := args[0].(*googlesql.ResolvedSubqueryExpr)
	if !ok {
		return nil, false
	}
	if typ, err := sub.SubqueryType(); err != nil || typ != googlesql.ResolvedSubqueryExprEnums_SubqueryTypeIn {
		return nil, false
	}
	scan, err := sub.Subquery()
	if err != nil || scan == nil {
		return nil, false
	}
	cols, err := scan.MutableColumnList()
	if err != nil || len(cols) != 1 {
		return nil, false
	}
	return cols[0], true
}

type tableColumn struct {
	scan *googlesql.ResolvedTableScan
	name string
}

// columnLineage records, for one statement, where each column's value comes
// from when it is an unchanged copy of another column.
type columnLineage struct {
	tables map[int32]tableColumn
	// copied maps a column id to the column it copies unchanged.
	copied map[int32]int32
	// conditioned is every column id a WHERE / HAVING / QUALIFY / ON
	// condition mentions.
	conditioned map[int32]bool
}

func traceColumnLineage(stmt googlesql.ResolvedStatementNode) *columnLineage {
	l := &columnLineage{
		tables:      make(map[int32]tableColumn),
		copied:      make(map[int32]int32),
		conditioned: make(map[int32]bool),
	}
	withSubqueries := make(map[string][]googlesql.ResolvedScanNode)
	var withRefs []*googlesql.ResolvedWithRefScan
	addCondition := func(expr googlesql.ResolvedExprNode, err error) {
		if err != nil || expr == nil {
			return
		}
		for _, id := range conditionColumnIDs(expr) {
			l.conditioned[id] = true
		}
	}
	_ = file.WalkResolved(stmt, func(n googlesql.ResolvedNode) error {
		switch n := n.(type) {
		case *googlesql.ResolvedTableScan:
			cols, err := n.ColumnList()
			if err != nil {
				break
			}
			for _, c := range cols {
				id, err := c.ColumnId()
				name, err2 := c.Name()
				if err == nil && err2 == nil {
					l.tables[id] = tableColumn{scan: n, name: name}
				}
			}
		case *googlesql.ResolvedComputedColumn:
			col, err := n.Column()
			expr, err2 := n.Expr()
			if err != nil || err2 != nil {
				break
			}
			ref, ok := expr.(*googlesql.ResolvedColumnRef)
			if !ok {
				break
			}
			src, err := ref.Column()
			if err != nil || src == nil {
				break
			}
			id, err := col.ColumnId()
			srcID, err2 := src.ColumnId()
			if err == nil && err2 == nil {
				l.copied[id] = srcID
			}
		case *googlesql.ResolvedWithEntry:
			name, err := n.WithQueryName()
			sub, err2 := n.WithSubquery()
			if err == nil && err2 == nil {
				withSubqueries[name] = append(withSubqueries[name], sub)
			}
		case *googlesql.ResolvedWithRefScan:
			withRefs = append(withRefs, n)
		case *googlesql.ResolvedFilterScan:
			addCondition(n.FilterExpr())
		case *googlesql.ResolvedJoinScan:
			addCondition(n.JoinExpr())
		case *googlesql.ResolvedArrayScan:
			addCondition(n.JoinExpr())
		}
		return nil
	})

	for _, ref := range withRefs {
		name, err := ref.WithQueryName()
		// Nested WITH clauses may reuse a name; such a reference is ambiguous.
		if err != nil || len(withSubqueries[name]) != 1 {
			continue
		}
		out, err := ref.ColumnList()
		in, err2 := withSubqueries[name][0].MutableColumnList()
		if err != nil || err2 != nil || len(in) != len(out) {
			continue
		}
		for i, c := range out {
			id, err := c.ColumnId()
			srcID, err2 := in[i].ColumnId()
			if err == nil && err2 == nil {
				l.copied[id] = srcID
			}
		}
	}
	return l
}

// source follows id through unchanged copies to a table column. It fails
// when the value is computed somewhere on the way, or when a condition
// mentions any column on the way.
func (l *columnLineage) source(id int32) (tableColumn, bool) {
	return l.follow(id, func(id int32) bool { return l.conditioned[id] })
}

// follow follows id through unchanged copies to a table column, failing when
// the value is computed on the way or stop holds for a column on the way.
func (l *columnLineage) follow(id int32, stop func(int32) bool) (tableColumn, bool) {
	seen := make(map[int32]bool)
	for !seen[id] {
		seen[id] = true
		if stop(id) {
			return tableColumn{}, false
		}
		if t, ok := l.tables[id]; ok {
			return t, true
		}
		next, ok := l.copied[id]
		if !ok {
			return tableColumn{}, false
		}
		id = next
	}
	return tableColumn{}, false
}

// conditionColumnIDs returns the columns expr references itself, leaving out
// the bodies of its subqueries, whose own conditions are visited separately
// and whose computed columns would otherwise look like conditions here.
func conditionColumnIDs(expr googlesql.ResolvedNode) []int32 {
	var ids []int32
	var walk func(googlesql.ResolvedNode)
	walk = func(n googlesql.ResolvedNode) {
		if n == nil || reflect.ValueOf(n).IsNil() {
			return
		}
		switch n := n.(type) {
		case *googlesql.ResolvedColumnRef:
			if col, err := n.Column(); err == nil && col != nil {
				if id, err := col.ColumnId(); err == nil {
					ids = append(ids, id)
				}
			}
		case *googlesql.ResolvedSubqueryExpr:
			if in, err := n.InExpr(); err == nil {
				walk(in)
			}
			return
		}
		children, err := n.GetChildNodes()
		if err != nil {
			return
		}
		for _, c := range children {
			walk(c)
		}
	}
	walk(expr)
	return ids
}

func checkNotInNullable(pass *Pass, lineage *columnLineage, call *googlesql.ResolvedFunctionCall, col *googlesql.ResolvedColumn) {
	id, err := col.ColumnId()
	if err != nil {
		return
	}
	src, ok := lineage.source(id)
	if !ok {
		return
	}
	table, err := src.scan.Table()
	if err != nil || table == nil {
		return
	}
	name, err := table.Name()
	if err != nil {
		return
	}
	meta, err := pass.LookupTable(pass.Context, name)
	// Only a regular table declares its modes; e.g. a view derives them from its query.
	if err != nil || meta == nil || meta.Type != bq.RegularTable {
		return
	}
	if !nullableField(meta.Schema, src.name) {
		return
	}

	loc, err := call.GetParseLocationRangeOrNULL()
	if err != nil || loc == nil {
		return
	}
	r, ok := pass.File.ToLspRange(loc)
	if !ok {
		return
	}
	d := Diagnostic{
		Range: r,
		Message: fmt.Sprintf("NOT IN yields NULL, never TRUE, once the subquery returns a NULL, and `%s` of `%s` is NULLABLE; rewrite it with NOT EXISTS, or filter the subquery with `%s IS NOT NULL`",
			src.name, name, src.name),
		Severity: SeverityWarning,
	}
	if fix, ok := notNullFilterFix(pass, notInSubquery(pass, loc)); ok {
		d.SuggestedFixes = []SuggestedFix{fix}
	}
	pass.Report(d)
}

// notInSubquery returns the subquery of the outermost `IN (subquery)` written
// within loc, which is the one the reported NOT negates.
func notInSubquery(pass *Pass, loc *googlesql.ParseLocationRange) *googlesql.ASTQuery {
	start, end := file.ParseLocStart(loc), file.ParseLocEnd(loc)
	var found *googlesql.ASTQuery
	var span offsets
	for _, in := range file.ListAstNode[*googlesql.ASTInExpression](pass.File.Node) {
		o := astOffsets(in)
		if o.start < start || o.end > end || (found != nil && o.end-o.start <= span.end-span.start) {
			continue
		}
		q, err := in.Query()
		if err != nil || q == nil {
			continue
		}
		found, span = q, o
	}
	return found
}

// notNullFilterFix adds `col IS NOT NULL` to the subquery's WHERE, which drops
// only the NULLs and keeps every other row the subquery returns. A new filter
// would change which rows LIMIT and QUALIFY keep, so it gives up on those.
func notNullFilterFix(pass *Pass, q *googlesql.ASTQuery) (SuggestedFix, bool) {
	if q == nil {
		return SuggestedFix{}, false
	}
	if lo, err := q.LimitOffset(); err != nil || lo != nil {
		return SuggestedFix{}, false
	}
	if op, err := q.PipeOperatorList(0); err == nil && op != nil {
		return SuggestedFix{}, false
	}
	expr, err := q.QueryExpr()
	if err != nil {
		return SuggestedFix{}, false
	}
	sel, ok := expr.(*googlesql.ASTSelect)
	if !ok {
		return SuggestedFix{}, false
	}
	if qualify, err := sel.Qualify(); err != nil || qualify != nil {
		return SuggestedFix{}, false
	}
	if as, err := sel.SelectAs(); err != nil || as != nil {
		return SuggestedFix{}, false
	}
	// ROLLUP, CUBE and GROUPING SETS add rows whose key is NULL, which no
	// WHERE can drop.
	groupBy, err := sel.GroupBy()
	if err != nil {
		return SuggestedFix{}, false
	}
	if groupBy != nil {
		multiLevel := false
		_ = file.Walk(groupBy, func(n googlesql.ASTNode) error {
			switch n.(type) {
			case *googlesql.ASTRollup, *googlesql.ASTCube, *googlesql.ASTGroupingSetList:
				multiLevel = true
			}
			return nil
		})
		if multiLevel {
			return SuggestedFix{}, false
		}
	}
	// The selected column is written as a path that resolves in the FROM
	// scope, so the same text names it in WHERE; an alias would not.
	list, err := sel.SelectList()
	if err != nil || list == nil {
		return SuggestedFix{}, false
	}
	if n, err := list.NumChildren(); err != nil || n != 1 {
		return SuggestedFix{}, false
	}
	selCol, err := list.Columns(0)
	if err != nil || selCol == nil {
		return SuggestedFix{}, false
	}
	colExpr, err := selCol.Expression()
	if err != nil {
		return SuggestedFix{}, false
	}
	path, ok := colExpr.(*googlesql.ASTPathExpression)
	if !ok {
		return SuggestedFix{}, false
	}
	colText, ok := writtenText(pass, path)
	if !ok {
		return SuggestedFix{}, false
	}
	cond := colText + " IS NOT NULL"

	insert := func(pos int, text string) (lsp.TextEdit, bool) {
		r, ok := pass.File.OffsetsToLspRange(pos, pos)
		return lsp.TextEdit{Range: r, NewText: text}, ok
	}
	var edits []lsp.TextEdit
	where, err := sel.WhereClause()
	if err != nil {
		return SuggestedFix{}, false
	}
	if where == nil {
		from, err := sel.FromClause()
		if err != nil || from == nil {
			return SuggestedFix{}, false
		}
		e, ok := insert(astOffsets(from).end, " WHERE "+cond)
		if !ok {
			return SuggestedFix{}, false
		}
		edits = append(edits, e)
	} else {
		filter, err := where.Expression()
		if err != nil || filter == nil {
			return SuggestedFix{}, false
		}
		o := astOffsets(filter)
		suffix := " AND " + cond
		// AND binds tighter than OR.
		if _, isOr := filter.(*googlesql.ASTOrExpr); isOr {
			open, ok := insert(o.start, "(")
			if !ok {
				return SuggestedFix{}, false
			}
			edits = append(edits, open)
			suffix = ")" + suffix
		}
		e, ok := insert(o.end, suffix)
		if !ok {
			return SuggestedFix{}, false
		}
		edits = append(edits, e)
	}
	return SuggestedFix{Title: "Filter the subquery with " + cond, Edits: edits}, true
}

func nullableField(schema bq.Schema, name string) bool {
	for _, f := range schema {
		if strings.EqualFold(f.Name, name) {
			return !f.Required && !f.Repeated
		}
	}
	return false
}
