package lint

import (
	"fmt"
	"slices"
	"strings"

	googlesql "github.com/goccy/go-googlesql"
	"github.com/kitagry/bqls/langserver/internal/source/file"
)

// PartitionFilterNotPrunable is off by default because its patterns come from
// the BigQuery documentation rather than from the optimizer itself, which may
// learn to prune some of them, and bqls cannot confirm pruning per table.
var PartitionFilterNotPrunable = &Analyzer{
	Name:           "partition-filter-not-prunable",
	Doc:            "reports partition column filters written in a form BigQuery documents as unable to prune partitions",
	DefaultEnabled: false,
	Run:            runPartitionFilterNotPrunable,
}

// Only forms that the BigQuery documentation explicitly lists as unable to
// prune are reported, and only when no other term on the partition column
// might prune, since BigQuery prunes with any one qualifying AND term.
func runPartitionFilterNotPrunable(pass *Pass) {
	for _, output := range pass.File.RNode {
		stmt, err := output.ResolvedStatement()
		if err != nil || stmt == nil {
			continue
		}
		flow, ok := analyzeColumnFlow(stmt)
		if !ok {
			continue
		}
		reported := make(map[offsets]bool)
		for _, scan := range flow.scans {
			checkPartitionFilterPrunable(pass, flow, scan, reported)
		}
	}
}

func checkPartitionFilterPrunable(pass *Pass, flow *columnFlow, scan *googlesql.ResolvedTableScan, reported map[offsets]bool) {
	name, meta, ok := partitionedTable(pass, flow, scan)
	if !ok {
		return
	}
	cols, err := scan.ColumnList()
	if err != nil {
		return
	}
	partitionColumns := partitionColumnNames(meta)
	j, ok := newPrunableJudge(flow, cols, func(name string) bool {
		return slices.ContainsFunc(partitionColumns, func(p string) bool { return strings.EqualFold(p, name) })
	})
	if !ok {
		return
	}
	j.rangePartition = meta.RangePartitioning != nil

	findings, ok := j.findings(flow)
	if !ok {
		return
	}
	consequence := "it may scan all partitions"
	if requiresPartitionFilter(meta) {
		consequence = "BigQuery will reject the query because the table requires a partition filter"
	}
	for _, f := range findings {
		reportOnce(pass, f.expr, reported,
			fmt.Sprintf("Filter on partition column `%s` of `%s` cannot prune partitions because %s; %s", f.column, name, f.reason, consequence))
	}
}

type unprunableTerm struct {
	expr   googlesql.ResolvedExprNode
	column string
	reason string
}

// findings returns every filter term on the judged column, or false when
// any of them might limit the scan, since one such AND term is enough.
func (j prunableJudge) findings(flow *columnFlow) ([]unprunableTerm, bool) {
	var terms []unprunableTerm
	for i, expr := range flow.conjunctExprs {
		ids := flow.conjuncts[i]
		if !slices.ContainsFunc(ids, func(id int32) bool { return j.derived[id] }) {
			continue
		}
		// A value computed from the column may still limit the scan, e.g.
		// DATE_ADD in a CTE, and this rule does not follow such computations.
		if slices.ContainsFunc(ids, func(id int32) bool { return j.derived[id] && j.judged[id] == "" }) {
			return nil, false
		}
		column, reason, ok := j.unprunable(expr, ids)
		if !ok {
			return nil, false
		}
		terms = append(terms, unprunableTerm{expr, column, reason})
	}
	return terms, true
}

// reportOnce reports at expr unless a diagnostic is already there, which
// happens when one term filters several scans.
func reportOnce(pass *Pass, expr googlesql.ResolvedExprNode, reported map[offsets]bool, message string) {
	at := resolvedOffsets(expr)
	if at.start < 0 || reported[at] {
		return
	}
	r, ok := pass.File.OffsetsToLspRange(at.start, at.end)
	if !ok {
		return
	}
	reported[at] = true
	pass.Report(Diagnostic{Range: r, Message: message, Severity: SeverityWarning})
}

// prunableJudge judges filter terms on a column that BigQuery uses to limit
// the data a scan reads: a partition column or _TABLE_SUFFIX.
type prunableJudge struct {
	// judged maps the judged column and its plain copies to the judged
	// column's name.
	judged map[int32]string
	// derived is every column computed from the judged column.
	derived map[int32]bool
	// other is every other column of the same scan and its plain copies.
	other          map[int32]bool
	rangePartition bool
}

// newPrunableJudge judges the scan columns cols for which judged is true,
// or returns false when the scan reads none of them.
func newPrunableJudge(flow *columnFlow, cols []*googlesql.ResolvedColumn, judged func(name string) bool) (prunableJudge, bool) {
	names := make(map[int32]string)
	var judgedIDs, otherIDs []int32
	for _, c := range cols {
		name, err := c.Name()
		id, err2 := c.ColumnId()
		if err != nil || err2 != nil {
			return prunableJudge{}, false
		}
		names[id] = name
		if judged(name) {
			judgedIDs = append(judgedIDs, id)
		} else {
			otherIDs = append(otherIDs, id)
		}
	}
	if len(judgedIDs) == 0 {
		return prunableJudge{}, false
	}
	j := prunableJudge{
		judged:  make(map[int32]string),
		derived: flow.derivedFrom(judgedIDs),
		other:   flow.aliasedFrom(otherIDs),
	}
	for _, id := range judgedIDs {
		for a := range flow.aliasedFrom([]int32{id}) {
			j.judged[a] = names[id]
		}
	}
	return j, true
}

var prunableComparisons = map[string]bool{
	"$equal": true, "$less": true, "$less_or_equal": true,
	"$greater": true, "$greater_or_equal": true, "$between": true,
}

// prunableFormats are the only formats the BigQuery documentation lists as
// supporting pruning with FORMAT_TIMESTAMP.
var prunableFormats = map[string]bool{"%F": true, "%Y-%m-%d": true, "%Y%m%d": true}

// unprunable reports the column and the documented reason why a filter term
// on the judged column cannot limit the scan, or false when it might.
func (j prunableJudge) unprunable(expr googlesql.ResolvedExprNode, ids []int32) (string, string, bool) {
	call, ok := expr.(*googlesql.ResolvedFunctionCall)
	if !ok {
		return "", "", false
	}
	fn, ok := functionName(call)
	if !ok {
		return "", "", false
	}
	args, err := call.ArgumentList()
	if err != nil {
		return "", "", false
	}
	column := j.firstJudgedColumn(ids)

	if fn == "$or" {
		for _, d := range splitOperands(expr, "$or") {
			refs := referencedColumnIDs(d)
			if len(refs) > 0 && !slices.ContainsFunc(refs, func(id int32) bool { return j.derived[id] }) {
				return column, "a branch of OR does not filter on it", true
			}
		}
		return "", "", false
	}
	if !prunableComparisons[fn] {
		return "", "", false
	}
	var side googlesql.ResolvedExprNode
	subquery := false
	for _, a := range args {
		if slices.ContainsFunc(referencedColumnIDs(a), func(id int32) bool { return j.judged[id] != "" }) {
			if side != nil {
				return "", "", false
			}
			side = a
			continue
		}
		subquery = subquery || containsSubquery(a)
	}
	// Checked first because a correlated subquery may read other columns.
	if subquery {
		return column, "it is compared with a subquery result, which is not a constant expression", true
	}
	if slices.ContainsFunc(ids, func(id int32) bool { return j.other[id] }) {
		return column, "it is compared with another column of the table", true
	}
	return j.unprunableSide(side, column)
}

// unprunableSide judges the comparison operand that holds the judged column.
func (j prunableJudge) unprunableSide(side googlesql.ResolvedExprNode, column string) (string, string, bool) {
	call, ok := side.(*googlesql.ResolvedFunctionCall)
	if !ok {
		return "", "", false
	}
	if j.rangePartition {
		return column, "functions over an integer-range partitioning column are not supported for pruning", true
	}
	fn, ok := functionName(call)
	if !ok {
		return "", "", false
	}
	args, err := call.ArgumentList()
	if err != nil || !slices.ContainsFunc(args, j.isJudgedRef) {
		return "", "", false
	}
	switch fn {
	case "$add", "$subtract":
		return column, "it is not isolated on one side of the comparison", true
	case "$extract":
		if len(args) < 2 || !j.isJudgedRef(args[0]) {
			return "", "", false
		}
		part, ok := literalString(args[1])
		if !ok || part == "YEAR" || part == "ISOYEAR" {
			return "", "", false
		}
		return column, fmt.Sprintf("EXTRACT(%s) is not supported for pruning", part), true
	case "format_date", "format_datetime", "format_timestamp":
		if len(args) < 2 || !j.isJudgedRef(args[1]) {
			return "", "", false
		}
		format, ok := args[0].(*googlesql.ResolvedLiteral)
		if !ok {
			return "", "", false
		}
		v, err := format.Value()
		if err != nil || v == nil {
			return "", "", false
		}
		s, err := v.StringValue()
		if err != nil || prunableFormats[s] {
			return "", "", false
		}
		return column, fmt.Sprintf("%s with format '%s' is not supported for pruning", strings.ToUpper(fn), s), true
	}
	return "", "", false
}

func (j prunableJudge) isJudgedRef(e googlesql.ResolvedExprNode) bool {
	if _, ok := e.(*googlesql.ResolvedColumnRef); !ok {
		return false
	}
	ids := referencedColumnIDs(e)
	return len(ids) == 1 && j.judged[ids[0]] != ""
}

func (j prunableJudge) firstJudgedColumn(ids []int32) string {
	for _, id := range ids {
		if name := j.judged[id]; name != "" {
			return name
		}
	}
	return ""
}

func containsSubquery(expr googlesql.ResolvedNode) bool {
	found := false
	_ = file.WalkResolved(expr, func(n googlesql.ResolvedNode) error {
		if _, ok := n.(*googlesql.ResolvedSubqueryExpr); ok {
			found = true
		}
		return nil
	})
	return found
}

func literalString(e googlesql.ResolvedExprNode) (string, bool) {
	lit, ok := e.(*googlesql.ResolvedLiteral)
	if !ok {
		return "", false
	}
	v, err := lit.Value()
	if err != nil || v == nil {
		return "", false
	}
	s, err := v.ShortDebugString()
	return s, err == nil
}
