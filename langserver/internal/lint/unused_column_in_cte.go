package lint

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	googlesql "github.com/goccy/go-googlesql"
	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/kitagry/bqls/langserver/internal/source/file"
)

var UnusedColumnInCTE = &Analyzer{
	Name:           "unused-column-in-cte",
	Doc:            "reports CTE output columns that nothing downstream reads",
	DefaultEnabled: true,
	Run:            runUnusedColumnInCTE,
}

// cteColumnUsage holds what one statement does with the columns of its CTE references.
// The analyzer's prune_unused_columns option cannot provide this: it prunes
// table scans only, and leaves every column of a ResolvedWithRefScan.
type cteColumnUsage struct {
	// used holds the column ids that an expression reads or a SELECT list outputs.
	used map[int32]bool
	refs map[string][]*googlesql.ResolvedWithRefScan
}

func runUnusedColumnInCTE(pass *Pass) {
	usages := make(map[*googlesql.AnalyzerOutput]*cteColumnUsage)
	for _, wc := range resolvedWithClauses(pass) {
		// A recursive CTE's columns feed its own next iteration.
		if recursive, err := wc.ws.Recursive(); err != nil || recursive {
			continue
		}
		usage, ok := usages[wc.output]
		if !ok {
			usage = newCTEColumnUsage(wc.output)
			usages[wc.output] = usage
		}
		if usage == nil {
			continue
		}
		// A non-recursive WITH keeps its entries in source order.
		resolved, err := wc.ws.WithEntryList()
		if err != nil || len(resolved) != len(wc.entries) {
			continue
		}
		for i, e := range wc.entries {
			checkCTEColumns(pass, e, resolved[i], usage.refs[wc.names[i]], usage.used)
		}
	}
}

// errUnfollowedScan stops the walk in newCTEColumnUsage.
var errUnfollowedScan = errors.New("unfollowed scan")

// newCTEColumnUsage returns nil when the statement passes columns somewhere
// bqls does not follow.
func newCTEColumnUsage(output *googlesql.AnalyzerOutput) *cteColumnUsage {
	stmt, err := output.ResolvedStatement()
	if err != nil || stmt == nil {
		return nil
	}
	outputs, ok := statementOutputColumns(stmt)
	if !ok {
		return nil
	}
	u := &cteColumnUsage{used: make(map[int32]bool), refs: make(map[string][]*googlesql.ResolvedWithRefScan)}
	u.markColumns(outputs)
	// A column that a query outputs counts as used even when nothing reads
	// the query's result. Without a SELECT list to compute, the output is the
	// column list of an ORDER BY or LIMIT scan; every such scan counts, which
	// may hide an unused column behind e.g. `|> LIMIT` but never reports a
	// used one.
	// One walk, since every node access crosses into the analyzer.
	err = file.WalkResolved(stmt, func(n googlesql.ResolvedNode) error {
		if scan, ok := n.(googlesql.ResolvedScanNode); ok && !followedScan(scan) {
			return errUnfollowedScan
		}
		switch n := n.(type) {
		case *googlesql.ResolvedColumnRef:
			if col, err := n.Column(); err == nil && col != nil {
				u.markColumns([]*googlesql.ResolvedColumn{col})
			}
		case *googlesql.ResolvedProjectScan, *googlesql.ResolvedOrderByScan, *googlesql.ResolvedLimitOffsetScan:
			u.markScan(n.(googlesql.ResolvedScanNode))
		case *googlesql.ResolvedWithEntry:
			if body, err := n.WithSubquery(); err == nil && body != nil {
				u.markScan(body)
			}
		case *googlesql.ResolvedSetOperationItem:
			if cols, err := n.OutputColumnList(); err == nil {
				u.markColumns(cols)
			}
		case *googlesql.ResolvedSubqueryExpr:
			if q, err := n.Subquery(); err == nil && q != nil {
				u.markScan(q)
			}
		case *googlesql.ResolvedWithRefScan:
			if name, err := n.WithQueryName(); err == nil {
				u.refs[name] = append(u.refs[name], n)
			}
		}
		return nil
	})
	if err != nil {
		return nil
	}
	return u
}

// followedScan reports whether every column the scan reads from its input is
// either passed on in its column list or read through a ResolvedColumnRef.
// PIVOT, for example, groups by the columns it does not mention.
func followedScan(scan googlesql.ResolvedScanNode) bool {
	switch scan.(type) {
	case *googlesql.ResolvedProjectScan, *googlesql.ResolvedFilterScan, *googlesql.ResolvedJoinScan,
		*googlesql.ResolvedAggregateScan, *googlesql.ResolvedOrderByScan, *googlesql.ResolvedLimitOffsetScan,
		*googlesql.ResolvedWithScan, *googlesql.ResolvedWithRefScan, *googlesql.ResolvedTableScan,
		*googlesql.ResolvedSingleRowScan, *googlesql.ResolvedArrayScan, *googlesql.ResolvedAnalyticScan,
		*googlesql.ResolvedSetOperationScan, *googlesql.ResolvedSampleScan:
		return true
	}
	return false
}

func statementOutputColumns(stmt googlesql.ResolvedStatementNode) ([]*googlesql.ResolvedColumn, bool) {
	switch s := stmt.(type) {
	case *googlesql.ResolvedInsertStmt:
		cols, err := s.QueryOutputColumnList()
		return cols, err == nil
	case interface {
		OutputColumnList() ([]*googlesql.ResolvedOutputColumn, error)
	}:
		list, err := s.OutputColumnList()
		return resolvedOutputColumns(list, err), err == nil
	}
	return nil, false
}

func (u *cteColumnUsage) markScan(scan googlesql.ResolvedScanNode) {
	if cols, ok := scanColumns(scan); ok {
		u.markColumns(cols)
	}
}

func (u *cteColumnUsage) markColumns(cols []*googlesql.ResolvedColumn) {
	for _, id := range columnIDs(cols) {
		u.used[id] = true
	}
}

// orderedColumnIDs returns nil when an id is unknown, since callers match columns by position.
func orderedColumnIDs(scan googlesql.ResolvedScanNode) []int32 {
	cols, ok := scanColumns(scan)
	if !ok {
		return nil
	}
	ids := make([]int32, 0, len(cols))
	for _, c := range cols {
		id, err := c.ColumnId()
		if err != nil {
			return nil
		}
		ids = append(ids, id)
	}
	return ids
}

func checkCTEColumns(pass *Pass, e cteEntry, entry *googlesql.ResolvedWithEntry, refs []*googlesql.ResolvedWithRefScan, used map[int32]bool) {
	if len(refs) == 0 {
		return
	}
	body, err := entry.WithSubquery()
	if err != nil || body == nil {
		return
	}
	outputs, err := body.MutableColumnList()
	if err != nil {
		return
	}
	unused := make([]bool, len(outputs))
	for j := range unused {
		unused[j] = true
	}
	// Each reference lists the CTE's output columns in order.
	for _, ref := range refs {
		ids := orderedColumnIDs(ref)
		if len(ids) != len(outputs) {
			return
		}
		for j, id := range ids {
			if used[id] {
				unused[j] = false
			}
		}
	}

	if !slices.Contains(unused, false) {
		// Nothing reads a column, e.g. `SELECT COUNT(*) FROM a`, so any one
		// column would do and none is worth reporting.
		return
	}

	q, sel, ok := cteSelect(e)
	if !ok {
		return
	}
	// Under DISTINCT every column decides which rows are kept.
	if distinct, err := sel.Distinct(); err != nil || distinct {
		return
	}
	// Likewise under GROUP BY ALL every non-aggregate column is a grouping key.
	if groupByAll(sel) {
		return
	}
	cols, ok := selectListColumns(sel)
	if !ok || len(cols.names) != len(outputs) {
		return
	}
	columns := make([]*googlesql.ASTSelectColumn, len(outputs))
	spans := make([]offsets, len(outputs))
	for j := range outputs {
		col, err := cols.list.Columns(int32(j))
		if err != nil || col == nil {
			return
		}
		columns[j], spans[j] = col, astOffsets(col)
	}
	// Removing the only aggregate of a query without GROUP BY would turn its
	// single row into one row per input row.
	fixable := !referencesOrdinalOrAlias(q, sel, cols.names, unused) && !aggregatesWithoutGroupBy(body)
	for j, isUnused := range unused {
		if !isUnused {
			continue
		}
		name := cols.names[j]
		if name == "" {
			// The resolved name of an anonymous column is internal, e.g. `$col2`.
			if !inBounds(pass.File.FixedSrc, spans[j]) {
				continue
			}
			name = strings.Join(strings.Fields(pass.File.FixedSrc[spans[j].start:spans[j].end]), " ")
		}
		at := spans[j]
		if alias, err := columns[j].Alias(); err == nil && alias != nil {
			at = astOffsets(alias)
		}
		r, ok := pass.File.OffsetsToLspRange(at.start, at.end)
		if !ok {
			continue
		}
		d := Diagnostic{
			Range:    r,
			Message:  fmt.Sprintf("Column `%s` of CTE `%s` is never used", name, e.name),
			Severity: SeverityInformation,
		}
		if fixable {
			if del, ok := unusedSelectColumnDeletion(pass.File.FixedSrc, spans, unused, j); ok {
				if r, ok := pass.File.OffsetsToLspRange(del.start, del.end); ok {
					d.SuggestedFixes = []SuggestedFix{{
						Title: fmt.Sprintf("Remove unused column `%s`", name),
						Edits: []lsp.TextEdit{{Range: r, NewText: ""}},
					}}
				}
			}
		}
		pass.Report(d)
	}
}

func cteSelect(e cteEntry) (*googlesql.ASTQuery, *googlesql.ASTSelect, bool) {
	q, err := e.query.Query()
	if err != nil || q == nil || hasPipeOperator(q) {
		return nil, nil, false
	}
	expr, err := q.QueryExpr()
	if err != nil {
		return nil, nil, false
	}
	sel, ok := expr.(*googlesql.ASTSelect)
	return q, sel, ok
}

func groupByAll(sel *googlesql.ASTSelect) bool {
	g, err := sel.GroupBy()
	if err != nil || g == nil {
		return false
	}
	all, err := g.All()
	return err != nil || all != nil
}

// aggregatesWithoutGroupBy reports whether the CTE body is a SELECT that
// aggregates without grouping keys. It follows only the scans that a single
// SELECT stacks on its aggregation, so a FROM subquery's aggregation is not
// mistaken for the body's.
func aggregatesWithoutGroupBy(body googlesql.ResolvedScanNode) bool {
	scan, seenProject := body, false
	for !isNilNode(scan) {
		var next googlesql.ResolvedScanNode
		var err error
		switch s := scan.(type) {
		case *googlesql.ResolvedAggregateScan:
			n, err := s.GroupByListSize()
			return err != nil || n == 0
		case *googlesql.ResolvedWithScan:
			next, err = s.Query()
		case *googlesql.ResolvedOrderByScan:
			next, err = s.InputScan()
		case *googlesql.ResolvedLimitOffsetScan:
			next, err = s.InputScan()
		case *googlesql.ResolvedProjectScan:
			if seenProject {
				return false
			}
			seenProject = true
			next, err = s.InputScan()
		case *googlesql.ResolvedAnalyticScan:
			next, err = s.InputScan()
		case *googlesql.ResolvedFilterScan:
			next, err = s.InputScan()
		default:
			return false
		}
		if err != nil {
			return true
		}
		scan = next
	}
	return false
}

// referencesOrdinalOrAlias reports whether removing the unused columns from
// the SELECT list could break the clauses that refer to it: an ordinal such as
// `GROUP BY 1` would shift, and an alias would disappear.
func referencesOrdinalOrAlias(q *googlesql.ASTQuery, sel *googlesql.ASTSelect, names []string, unused []bool) bool {
	removed := make(map[string]bool)
	for j, isUnused := range unused {
		if isUnused && names[j] != "" {
			removed[strings.ToLower(names[j])] = true
		}
	}
	type clause struct {
		node googlesql.ASTNode
		// ordinals is whether the clause accepts ordinals; elsewhere an
		// integer literal such as `HAVING COUNT(*) > 1` is just a value.
		ordinals bool
	}
	var clauses []clause
	if n, err := sel.GroupBy(); err == nil && n != nil {
		clauses = append(clauses, clause{n, true})
	}
	if n, err := sel.Having(); err == nil && n != nil {
		clauses = append(clauses, clause{n, false})
	}
	if n, err := sel.Qualify(); err == nil && n != nil {
		clauses = append(clauses, clause{n, false})
	}
	if n, err := sel.WindowClause(); err == nil && n != nil {
		clauses = append(clauses, clause{n, false})
	}
	if n, err := q.OrderBy(); err == nil && n != nil {
		clauses = append(clauses, clause{n, true})
	}
	found := false
	for _, c := range clauses {
		_ = file.Walk(c.node, func(n googlesql.ASTNode) error {
			switch n := n.(type) {
			case *googlesql.ASTIntLiteral:
				if c.ordinals {
					found = true
				}
			case *googlesql.ASTIdentifier:
				if name, err := n.GetAsString(); err == nil && removed[strings.ToLower(name)] {
					found = true
				}
			}
			return nil
		})
	}
	return found
}

// unusedSelectColumnDeletion returns the span that removes column j together
// with one separating comma. A column with a kept column before it takes the
// comma before it, any other the comma after it, so that the fixes for several
// columns never overlap. It gives up whenever a comment would be deleted or
// left describing a different column.
func unusedSelectColumnDeletion(src string, spans []offsets, unused []bool, j int) (offsets, bool) {
	cur := spans[j]
	if !inBounds(src, cur) || hasTrailingComment(src, cur.end) {
		return offsets{}, false
	}
	if slices.Contains(unused[:j], false) {
		del := offsets{spans[j-1].end, cur.end}
		return del, isSeparator(src, del.start, cur.start)
	}
	if j+1 >= len(spans) || hasCommentLineAbove(src, cur.start) {
		return offsets{}, false
	}
	del := offsets{cur.start, spans[j+1].start}
	return del, isSeparator(src, cur.end, del.end)
}
