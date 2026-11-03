package lint

import (
	"fmt"
	"slices"
	"strings"

	bq "cloud.google.com/go/bigquery"
	googlesql "github.com/goccy/go-googlesql"
	"github.com/kitagry/bqls/langserver/internal/source/file"
)

var InsertNullIntoRequired = &Analyzer{
	Name: "insert-null-into-required",
	Doc:  "reports INSERT, UPDATE and MERGE statements that write a value that may be NULL into a REQUIRED column, which BigQuery rejects only at run time",
	Run:  runInsertNullIntoRequired,
}

// As in not-in-nullable, a false positive pushes users to disable the rule,
// so a value counts as possibly NULL only when it is a NULL literal, an
// unchanged copy of a NULLABLE table column that no condition mentions, or an
// unchanged copy of a column from the optional side of an outer join whose
// side no condition outside the join reads. Anything computed is skipped.
func runInsertNullIntoRequired(pass *Pass) {
	inserts := insertStatementsByTarget(pass)

	for _, output := range pass.File.RNode {
		stmt, err := output.ResolvedStatement()
		if err != nil || stmt == nil {
			continue
		}
		switch stmt := stmt.(type) {
		case *googlesql.ResolvedInsertStmt:
			checkInsertNull(pass, stmt, inserts)
		case *googlesql.ResolvedUpdateStmt:
			checkUpdateNull(pass, stmt)
		case *googlesql.ResolvedMergeStmt:
			checkMergeNull(pass, stmt)
		}
	}
}

func checkInsertNull(pass *Pass, ins *googlesql.ResolvedInsertStmt, inserts map[offsets]*googlesql.ASTInsertStatement) {
	target, ok := lookupDMLTarget(pass, ins.TableScan)
	if !ok {
		return
	}
	cols, err := ins.InsertColumnList()
	if err != nil {
		return
	}
	n := traceNullability(pass, ins)
	rows, err := ins.RowList()
	if err != nil {
		return
	}
	for _, row := range rows {
		checkInsertRow(pass, n, target, cols, row)
	}
	checkInsertQuery(pass, n, target, ins, cols, inserts[target.offsets])
}

func checkInsertRow(pass *Pass, n *nullability, target dmlTarget, cols []*googlesql.ResolvedColumn, row *googlesql.ResolvedInsertRow) {
	values, err := row.ValueList()
	if err != nil || len(values) != len(cols) {
		return
	}
	for i, v := range values {
		checkDMLValue(pass, n, target, cols[i], v)
	}
}

func checkDMLValue(pass *Pass, n *nullability, target dmlTarget, col *googlesql.ResolvedColumn, v *googlesql.ResolvedDMLValue) {
	if v == nil {
		return
	}
	expr, err := v.Value()
	if err != nil || isNilNode(expr) {
		return
	}
	target.check(pass, n, col, dmlValue{expr: expr, at: resolvedOffsets(expr)})
}

func checkUpdateNull(pass *Pass, upd *googlesql.ResolvedUpdateStmt) {
	target, ok := lookupDMLTarget(pass, upd.TableScan)
	if !ok {
		return
	}
	items, err := upd.UpdateItemList()
	if err != nil {
		return
	}
	checkUpdateItems(pass, traceNullability(pass, upd), target, items)
}

func checkMergeNull(pass *Pass, merge *googlesql.ResolvedMergeStmt) {
	target, ok := lookupDMLTarget(pass, merge.TableScan)
	if !ok {
		return
	}
	whens, err := merge.WhenClauseList()
	if err != nil {
		return
	}
	n := traceNullability(pass, merge)
	for _, when := range whens {
		if items, err := when.UpdateItemList(); err == nil {
			checkUpdateItems(pass, n, target, items)
		}
		cols, err := when.InsertColumnList()
		if err != nil {
			continue
		}
		if row, err := when.InsertRow(); err == nil && row != nil {
			checkInsertRow(pass, n, target, cols, row)
		}
	}
}

// checkUpdateItems checks `SET col = value` items; a nested target such as
// `SET s.f = value` is left out.
func checkUpdateItems(pass *Pass, n *nullability, target dmlTarget, items []*googlesql.ResolvedUpdateItem) {
	for _, item := range items {
		t, err := item.Target()
		if err != nil {
			continue
		}
		ref, ok := t.(*googlesql.ResolvedColumnRef)
		if !ok {
			continue
		}
		col, err := ref.Column()
		if err != nil || col == nil {
			continue
		}
		v, err := item.SetValue()
		if err != nil {
			continue
		}
		checkDMLValue(pass, n, target, col, v)
	}
}

// checkInsertQuery reports the query's columns that may be NULL, each on its
// select list item when the query has one per column, and otherwise on the
// target table.
func checkInsertQuery(pass *Pass, n *nullability, target dmlTarget, ins *googlesql.ResolvedInsertStmt, cols []*googlesql.ResolvedColumn, astIns *googlesql.ASTInsertStatement) {
	outs, err := ins.QueryOutputColumnList()
	if err != nil || len(outs) != len(cols) {
		return
	}
	var items []googlesql.ASTNode
	if astIns != nil {
		if q, err := astIns.Query(); err == nil && q != nil {
			if sel, ok := outputColumns(q); ok && len(sel.names) == len(outs) {
				items = children(sel.list)
			}
		}
	}
	for i, out := range outs {
		id, err := out.ColumnId()
		if err != nil {
			continue
		}
		at := target.offsets
		if len(items) == len(outs) {
			at = astOffsets(items[i])
		}
		target.check(pass, n, cols[i], dmlValue{id: id, at: at})
	}
}

// dmlValue is a value a DML statement writes to a column.
type dmlValue struct {
	// expr is the written expression, or nil for a column of the query of
	// `INSERT ... SELECT`, whose id is then set.
	expr googlesql.ResolvedExprNode
	id   int32
	// at is where to report the value.
	at offsets
}

// insertStatementsByTarget maps the AST INSERT statements by the offsets of
// their target path, which match those of the resolved statement's table scan.
func insertStatementsByTarget(pass *Pass) map[offsets]*googlesql.ASTInsertStatement {
	inserts := make(map[offsets]*googlesql.ASTInsertStatement)
	for _, s := range file.ListAstNode[*googlesql.ASTInsertStatement](pass.File.Node) {
		path, err := s.GetTargetPathForNonNested()
		if err != nil || path == nil {
			continue
		}
		inserts[astOffsets(path)] = s
	}
	return inserts
}

// dmlTarget is the table a DML statement writes to.
type dmlTarget struct {
	name    string
	schema  bq.Schema
	offsets offsets
}

// lookupDMLTarget looks up the schema of the table a DML statement writes to.
// Only a regular table declares its modes.
func lookupDMLTarget(pass *Pass, scan func() (*googlesql.ResolvedTableScan, error)) (dmlTarget, bool) {
	ts, err := scan()
	if err != nil || ts == nil {
		return dmlTarget{}, false
	}
	name, ok := tableName(ts)
	if !ok {
		return dmlTarget{}, false
	}
	meta, err := pass.LookupTable(pass.Context, name)
	if err != nil || meta == nil || meta.Type != bq.RegularTable {
		return dmlTarget{}, false
	}
	return dmlTarget{name: name, schema: meta.Schema, offsets: resolvedOffsets(ts)}, true
}

func (t dmlTarget) check(pass *Pass, n *nullability, col *googlesql.ResolvedColumn, v dmlValue) {
	name, err := col.Name()
	if err != nil {
		return
	}
	for _, f := range t.schema {
		if strings.EqualFold(f.Name, name) {
			t.checkField(pass, n, f, f.Name, v)
			return
		}
	}
}

// checkField checks v against field, and the fields of a struct v builds
// against the subfields of a RECORD field.
func (t dmlTarget) checkField(pass *Pass, n *nullability, field *bq.FieldSchema, path string, v dmlValue) {
	if field.Repeated {
		return
	}
	if field.Required {
		if because, ok := n.mayBeNull(v); ok {
			t.report(pass, path, v.at, because)
			return
		}
	}
	if field.Type != bq.RecordFieldType {
		return
	}
	values, ok := n.structFields(v)
	if !ok || len(values) != len(field.Schema) {
		return
	}
	for i, expr := range values {
		at := resolvedOffsets(expr)
		if at.start < 0 {
			at = v.at
		}
		sub := field.Schema[i]
		t.checkField(pass, n, sub, path+"."+sub.Name, dmlValue{expr: expr, at: at})
	}
}

func (t dmlTarget) report(pass *Pass, path string, at offsets, because string) {
	r, ok := pass.File.OffsetsToLspRange(at.start, at.end)
	if !ok {
		return
	}
	pass.Report(Diagnostic{
		Range:    r,
		Message:  fmt.Sprintf("`%s` of `%s` is REQUIRED, but %s; BigQuery fails the statement at run time once it writes a NULL there", path, t.name, because),
		Severity: SeverityWarning,
	})
}

// nullability tells, for the columns of one statement, whether their values
// may be NULL.
type nullability struct {
	pass    *Pass
	lineage *columnLineage
	// nulls holds the columns computed as a NULL literal.
	nulls map[int32]bool
	// exprs maps a computed column id to its expression, and derived to every
	// column that expression reads.
	exprs   map[int32]googlesql.ResolvedExprNode
	derived map[int32][]int32
	// joins holds the outer joins, and extendedBy maps a column id to the
	// outer joins that output it from their optional side.
	joins      []outerJoin
	extendedBy map[int32][]int
	conditions []nullCondition
}

type outerJoin struct {
	kind string
	// filtered is set when a condition outside the join drops its
	// NULL-extended rows.
	filtered bool
}

type nullCondition struct {
	ids []int32
	// within holds the outer joins whose optional side or ON clause the
	// condition is part of, where it cannot drop their NULL-extended rows.
	within map[int]bool
}

func traceNullability(pass *Pass, stmt googlesql.ResolvedStatementNode) *nullability {
	n := &nullability{
		pass:       pass,
		lineage:    traceColumnLineage(stmt),
		nulls:      make(map[int32]bool),
		exprs:      make(map[int32]googlesql.ResolvedExprNode),
		derived:    make(map[int32][]int32),
		extendedBy: make(map[int32][]int),
	}
	n.walk(stmt, nil)
	n.markFilteredJoins()
	return n
}

func (n *nullability) walk(node googlesql.ResolvedNode, within []int) {
	if isNilNode(node) {
		return
	}
	addCondition := func(expr googlesql.ResolvedExprNode, err error) {
		if err == nil {
			n.addCondition(expr, within)
		}
	}
	switch node := node.(type) {
	case *googlesql.ResolvedJoinScan:
		if n.walkOuterJoin(node, within) {
			return
		}
		addCondition(node.JoinExpr())
	case *googlesql.ResolvedFilterScan:
		addCondition(node.FilterExpr())
	case *googlesql.ResolvedArrayScan:
		addCondition(node.JoinExpr())
	case *googlesql.ResolvedComputedColumn:
		n.addComputedColumn(node)
	case *googlesql.ResolvedUpdateStmt:
		n.addStatementCondition(node.WhereExpr())
	// The merge condition is taken as a condition even for WHEN NOT MATCHED,
	// where it does not drop NULLs; that only misses reports.
	case *googlesql.ResolvedMergeStmt:
		n.addStatementCondition(node.MergeExpr())
	case *googlesql.ResolvedMergeWhen:
		n.addStatementCondition(node.MatchExpr())
	}
	children, err := node.GetChildNodes()
	if err != nil {
		return
	}
	for _, c := range children {
		n.walk(c, within)
	}
}

// walkOuterJoin records an outer join and walks its inputs, or returns false
// for an inner join.
func (n *nullability) walkOuterJoin(join *googlesql.ResolvedJoinScan, within []int) bool {
	typ, err := join.JoinType()
	if err != nil {
		return false
	}
	var leftOptional, rightOptional bool
	var kind string
	switch typ {
	case googlesql.ResolvedJoinScanEnums_JoinTypeLeft:
		kind, rightOptional = "LEFT", true
	case googlesql.ResolvedJoinScanEnums_JoinTypeRight:
		kind, leftOptional = "RIGHT", true
	case googlesql.ResolvedJoinScanEnums_JoinTypeFull:
		kind, leftOptional, rightOptional = "FULL", true, true
	default:
		return false
	}
	j := len(n.joins)
	n.joins = append(n.joins, outerJoin{kind: kind})
	inJoin := append(slices.Clone(within), j)
	walkSide := func(side func() (googlesql.ResolvedScanNode, error), optional bool) {
		scan, err := side()
		if err != nil || isNilNode(scan) {
			return
		}
		if !optional {
			n.walk(scan, within)
			return
		}
		if cols, err := scan.MutableColumnList(); err == nil {
			for _, c := range cols {
				if id, err := c.ColumnId(); err == nil {
					n.extendedBy[id] = append(n.extendedBy[id], j)
				}
			}
		}
		n.walk(scan, inJoin)
	}
	walkSide(join.LeftScan, leftOptional)
	walkSide(join.RightScan, rightOptional)
	// An outer join's ON keeps every row of the preserved side, so it drops
	// no NULL-extended rows; it is walked only for its subqueries' conditions.
	if expr, err := join.JoinExpr(); err == nil {
		n.walk(expr, inJoin)
	}
	return true
}

func (n *nullability) addCondition(expr googlesql.ResolvedExprNode, within []int) {
	if isNilNode(expr) {
		return
	}
	c := nullCondition{ids: referencedColumnIDs(expr), within: make(map[int]bool)}
	for _, j := range within {
		c.within[j] = true
	}
	n.conditions = append(n.conditions, c)
}

// addStatementCondition records a condition of a DML statement, which the
// column lineage does not visit.
func (n *nullability) addStatementCondition(expr googlesql.ResolvedExprNode, err error) {
	if err != nil || isNilNode(expr) {
		return
	}
	n.addCondition(expr, nil)
	for _, id := range conditionColumnIDs(expr) {
		n.lineage.conditioned[id] = true
	}
}

func (n *nullability) addComputedColumn(cc *googlesql.ResolvedComputedColumn) {
	col, err := cc.Column()
	expr, err2 := cc.Expr()
	if err != nil || err2 != nil || col == nil || isNilNode(expr) {
		return
	}
	id, err := col.ColumnId()
	if err != nil {
		return
	}
	n.exprs[id] = expr
	n.derived[id] = referencedColumnIDs(expr)
	switch e := unwrapCast(expr).(type) {
	case *googlesql.ResolvedLiteral:
		n.nulls[id] = isNullLiteral(e)
	// A cast keeps NULL as NULL and any other value as non-NULL, as the
	// implicit cast `INSERT` adds to match the target type does.
	case *googlesql.ResolvedColumnRef:
		if srcID, ok := columnRefID(e); ok {
			n.lineage.copied[id] = srcID
		}
	}
}

// markFilteredJoins marks the outer joins whose optional side a condition
// outside the join reads, possibly through copies and computed columns. Such
// a condition may drop the NULL-extended rows, e.g. `b.w > 0`.
func (n *nullability) markFilteredJoins() {
	for _, c := range n.conditions {
		seen := make(map[int32]bool)
		queue := slices.Clone(c.ids)
		for len(queue) > 0 {
			id := queue[0]
			queue = queue[1:]
			if seen[id] {
				continue
			}
			seen[id] = true
			for _, j := range n.extendedBy[id] {
				if !c.within[j] {
					n.joins[j].filtered = true
				}
			}
			if next, ok := n.lineage.copied[id]; ok {
				queue = append(queue, next)
			}
			queue = append(queue, n.derived[id]...)
		}
	}
}

func (n *nullability) mayBeNull(v dmlValue) (string, bool) {
	if v.expr == nil {
		return n.columnMayBeNull(v.id)
	}
	switch e := unwrapCast(v.expr).(type) {
	case *googlesql.ResolvedLiteral:
		if isNullLiteral(e) {
			return "this value is NULL", true
		}
	case *googlesql.ResolvedColumnRef:
		if id, ok := columnRefID(e); ok {
			return n.columnMayBeNull(id)
		}
	}
	return "", false
}

// structFields returns the field values of the struct v builds with
// `STRUCT(...)`, following v through unchanged copies and casts.
func (n *nullability) structFields(v dmlValue) ([]googlesql.ResolvedExprNode, bool) {
	expr, id := v.expr, v.id
	seen := make(map[int32]bool)
	for {
		if expr != nil {
			switch e := unwrapCast(expr).(type) {
			case *googlesql.ResolvedMakeStruct:
				fields, err := e.FieldList()
				return fields, err == nil
			case *googlesql.ResolvedColumnRef:
				var ok bool
				if id, ok = columnRefID(e); !ok {
					return nil, false
				}
				expr = nil
			default:
				return nil, false
			}
		}
		if seen[id] || n.lineage.conditioned[id] {
			return nil, false
		}
		seen[id] = true
		if e, ok := n.exprs[id]; ok {
			expr = e
			continue
		}
		next, ok := n.lineage.copied[id]
		if !ok {
			return nil, false
		}
		id = next
	}
}

func unwrapCast(expr googlesql.ResolvedExprNode) googlesql.ResolvedExprNode {
	for {
		c, ok := expr.(*googlesql.ResolvedCast)
		if !ok {
			return expr
		}
		inner, err := c.Expr()
		if err != nil || isNilNode(inner) {
			return expr
		}
		expr = inner
	}
}

// columnMayBeNull follows id through unchanged copies, giving up once a
// condition mentions a column on the way.
func (n *nullability) columnMayBeNull(id int32) (string, bool) {
	if kind, ok := n.nullExtended(id); ok {
		return fmt.Sprintf("this value comes from the side of a %s JOIN that is NULL when no row matches", kind), true
	}
	l := n.lineage
	seen := make(map[int32]bool)
	for !seen[id] {
		seen[id] = true
		if l.conditioned[id] {
			return "", false
		}
		if n.nulls[id] {
			return "this value is NULL", true
		}
		if t, ok := l.tables[id]; ok {
			return n.nullableTableColumn(t)
		}
		next, ok := l.copied[id]
		if !ok {
			return "", false
		}
		id = next
	}
	return "", false
}

// nullExtended returns the kind of an outer join whose NULL-extended rows
// reach id through unchanged copies and are not filtered out.
func (n *nullability) nullExtended(id int32) (string, bool) {
	seen := make(map[int32]bool)
	for !seen[id] {
		seen[id] = true
		for _, j := range n.extendedBy[id] {
			if !n.joins[j].filtered {
				return n.joins[j].kind, true
			}
		}
		next, ok := n.lineage.copied[id]
		if !ok {
			break
		}
		id = next
	}
	return "", false
}

func (n *nullability) nullableTableColumn(t tableColumn) (string, bool) {
	name, ok := tableName(t.scan)
	if !ok {
		return "", false
	}
	meta, err := n.pass.LookupTable(n.pass.Context, name)
	if err != nil || meta == nil || meta.Type != bq.RegularTable || !nullableField(meta.Schema, t.name) {
		return "", false
	}
	return fmt.Sprintf("this value comes from `%s` of `%s`, which is NULLABLE", t.name, name), true
}
