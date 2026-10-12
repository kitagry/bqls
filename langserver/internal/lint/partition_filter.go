package lint

import (
	"fmt"
	"reflect"
	"slices"
	"strings"

	bq "cloud.google.com/go/bigquery"
	googlesql "github.com/goccy/go-googlesql"
	"github.com/kitagry/bqls/langserver/internal/source/file"
)

var PartitionFilter = &Analyzer{
	Name:           "partition-filter",
	Doc:            "reports queries on partitioned tables that never filter on the partition column",
	DefaultEnabled: true,
	Run:            runPartitionFilter,
}

// A false positive here pushes users to disable the rule, so a statement is
// skipped whenever column flow through it cannot be followed, and any filter
// that merely mentions a value derived from the partition column counts,
// whether or not BigQuery could prune with it.
func runPartitionFilter(pass *Pass) {
	for _, output := range pass.File.RNode {
		stmt, err := output.ResolvedStatement()
		if err != nil || stmt == nil {
			continue
		}
		flow, ok := analyzeColumnFlow(stmt)
		if !ok {
			continue
		}
		for _, scan := range flow.scans {
			checkPartitionFilter(pass, flow, scan)
		}
	}
}

type columnFlow struct {
	scans []*googlesql.ResolvedTableScan
	// dmlTargets are DML target scans keyed by location; BigQuery prunes them
	// through the statement's own conditions, which this rule does not model.
	dmlTargets map[offsets]bool
	// derived maps a column id to the column ids computed from its value.
	derived map[int32][]int32
	// filtered is every column id referenced by a filtering predicate.
	filtered map[int32]bool
	// used is every column id whose value the statement consumes.
	used map[int32]bool
}

func analyzeColumnFlow(stmt googlesql.ResolvedStatementNode) (*columnFlow, bool) {
	switch stmt.(type) {
	case *googlesql.ResolvedQueryStmt, *googlesql.ResolvedCreateTableAsSelectStmt,
		*googlesql.ResolvedInsertStmt, *googlesql.ResolvedUpdateStmt,
		*googlesql.ResolvedDeleteStmt, *googlesql.ResolvedMergeStmt,
		*googlesql.ResolvedExportDataStmt:
	default:
		// e.g. CREATE VIEW: callers of the view supply the partition filter.
		return nil, false
	}

	flow := &columnFlow{
		dmlTargets: make(map[offsets]bool),
		derived:    make(map[int32][]int32),
		filtered:   make(map[int32]bool),
		used:       make(map[int32]bool),
	}
	withSubqueries := make(map[string]googlesql.ResolvedScanNode)
	var withRefs []*googlesql.ResolvedWithRefScan
	ok := true
	addFilter := func(expr googlesql.ResolvedExprNode, err error) {
		if err != nil {
			ok = false
			return
		}
		for _, id := range referencedColumnIDs(expr) {
			flow.filtered[id] = true
		}
	}
	addTarget := func(scan *googlesql.ResolvedTableScan, err error) {
		if err != nil {
			ok = false
			return
		}
		if scan != nil {
			flow.dmlTargets[resolvedOffsets(scan)] = true
		}
	}
	derive := func(col *googlesql.ResolvedColumn, from []int32) {
		id, err := col.ColumnId()
		if err != nil {
			ok = false
			return
		}
		for _, f := range from {
			flow.derived[f] = append(flow.derived[f], id)
		}
	}
	deriveComputed := func(n computedColumn) {
		col, err := n.Column()
		expr, err2 := n.Expr()
		if err != nil || err2 != nil {
			ok = false
			return
		}
		derive(col, referencedColumnIDs(expr))
	}

	walkLive(stmt, func(n googlesql.ResolvedNode) {
		switch n := n.(type) {
		case *googlesql.ResolvedColumnRef:
			for _, id := range referencedColumnIDs(n) {
				flow.used[id] = true
			}
		case *googlesql.ResolvedSubqueryExpr:
			// Every subquery kind but EXISTS consumes the subquery's columns.
			typ, err := n.SubqueryType()
			sub, err2 := n.Subquery()
			if err != nil || err2 != nil || sub == nil {
				ok = false
				break
			}
			if typ == googlesql.ResolvedSubqueryExprEnums_SubqueryTypeExists {
				break
			}
			cols, err := sub.MutableColumnList()
			if err != nil {
				ok = false
				break
			}
			for _, id := range columnIDs(cols) {
				flow.used[id] = true
			}
		case *googlesql.ResolvedOutputColumn:
			if c, err := n.Column(); err == nil && c != nil {
				for _, id := range columnIDs([]*googlesql.ResolvedColumn{c}) {
					flow.used[id] = true
				}
			} else {
				ok = false
			}
		case *googlesql.ResolvedTableScan:
			flow.scans = append(flow.scans, n)
		case *googlesql.ResolvedFilterScan:
			addFilter(n.FilterExpr())
		case *googlesql.ResolvedJoinScan:
			addFilter(n.JoinExpr())
		case *googlesql.ResolvedArrayScan:
			addFilter(n.JoinExpr())
			exprs, err := n.ArrayExprList()
			elems, err2 := n.ElementColumnList()
			if err != nil || err2 != nil {
				ok = false
				break
			}
			var from []int32
			for _, e := range exprs {
				from = append(from, referencedColumnIDs(e)...)
			}
			for _, c := range elems {
				derive(c, from)
			}
			if holder, err := n.ArrayOffsetColumn(); err == nil && holder != nil {
				if c, err := holder.Column(); err == nil && c != nil {
					derive(c, from)
				}
			}
		case *googlesql.ResolvedComputedColumn:
			deriveComputed(n)
		case *googlesql.ResolvedDeferredComputedColumn:
			deriveComputed(n)
		case *googlesql.ResolvedWithEntry:
			name, err := n.WithQueryName()
			sub, err2 := n.WithSubquery()
			if err != nil || err2 != nil {
				ok = false
				break
			}
			withSubqueries[name] = sub
		case *googlesql.ResolvedWithRefScan:
			withRefs = append(withRefs, n)
		case *googlesql.ResolvedSetOperationScan:
			items, err := n.InputItemList()
			out, err2 := n.ColumnList()
			if err != nil || err2 != nil {
				ok = false
				break
			}
			for _, item := range items {
				in, err := item.OutputColumnList()
				if err != nil || len(in) != len(out) {
					ok = false
					break
				}
				for i, c := range out {
					derive(c, columnIDs(in[i:i+1]))
				}
			}
		case *googlesql.ResolvedUpdateStmt:
			addTarget(n.TableScan())
			addFilter(n.WhereExpr())
		case *googlesql.ResolvedDeleteStmt:
			addTarget(n.TableScan())
			addFilter(n.WhereExpr())
		case *googlesql.ResolvedMergeStmt:
			addTarget(n.TableScan())
			addFilter(n.MergeExpr())
		case *googlesql.ResolvedMergeWhen:
			addFilter(n.MatchExpr())
		case *googlesql.ResolvedInsertStmt:
			addTarget(n.TableScan())
			if cols, err := n.QueryOutputColumnList(); err == nil {
				for _, id := range columnIDs(cols) {
					flow.used[id] = true
				}
			} else {
				ok = false
			}
		case googlesql.ResolvedScanNode:
			if !columnPreservingScan(n) {
				ok = false
			}
		}
	})

	for _, ref := range withRefs {
		name, err := ref.WithQueryName()
		sub, found := withSubqueries[name]
		if err != nil || !found {
			return nil, false
		}
		out, err := ref.ColumnList()
		in, err2 := sub.MutableColumnList()
		if err != nil || err2 != nil || len(in) != len(out) {
			return nil, false
		}
		for i, c := range out {
			derive(c, columnIDs(in[i:i+1]))
		}
	}
	return flow, ok
}

// walkLive is file.WalkResolved except that it skips WITH entries that are
// never referenced, because BigQuery does not evaluate them. Entries can only
// reference earlier ones, so visiting them last-to-first after the main query
// sees every reference to an entry before deciding on it.
func walkLive(node googlesql.ResolvedNode, fn func(googlesql.ResolvedNode)) {
	referenced := make(map[string]bool)
	var walk func(googlesql.ResolvedNode)
	walk = func(n googlesql.ResolvedNode) {
		if n == nil || reflect.ValueOf(n).IsNil() {
			return
		}
		fn(n)
		switch n := n.(type) {
		case *googlesql.ResolvedWithRefScan:
			if name, err := n.WithQueryName(); err == nil {
				referenced[name] = true
			}
		case *googlesql.ResolvedWithScan:
			entries, err := n.WithEntryList()
			query, err2 := n.Query()
			if err == nil && err2 == nil {
				walk(query)
				for _, e := range slices.Backward(entries) {
					if name, err := e.WithQueryName(); err != nil || referenced[name] {
						walk(e)
					}
				}
				return
			}
		}
		children, err := n.GetChildNodes()
		if err != nil {
			return
		}
		for _, c := range children {
			walk(c)
		}
	}
	walk(node)
}

// columnPreservingScan reports whether a scan only passes through input
// columns or creates new ones via ResolvedComputedColumn, so that the walk
// in analyzeColumnFlow sees every derivation it introduces.
func columnPreservingScan(n googlesql.ResolvedScanNode) bool {
	switch n.(type) {
	case *googlesql.ResolvedProjectScan, *googlesql.ResolvedAggregateScan,
		*googlesql.ResolvedAnalyticScan, *googlesql.ResolvedOrderByScan,
		*googlesql.ResolvedLimitOffsetScan, *googlesql.ResolvedSampleScan,
		*googlesql.ResolvedSingleRowScan, *googlesql.ResolvedWithScan:
		return true
	}
	return false
}

type computedColumn interface {
	Column() (*googlesql.ResolvedColumn, error)
	Expr() (googlesql.ResolvedExprNode, error)
}

func referencedColumnIDs(expr googlesql.ResolvedNode) []int32 {
	var ids []int32
	_ = file.WalkResolved(expr, func(n googlesql.ResolvedNode) error {
		if ref, ok := n.(*googlesql.ResolvedColumnRef); ok {
			if col, err := ref.Column(); err == nil && col != nil {
				if id, err := col.ColumnId(); err == nil {
					ids = append(ids, id)
				}
			}
		}
		return nil
	})
	return ids
}

func columnIDs(cols []*googlesql.ResolvedColumn) []int32 {
	ids := make([]int32, 0, len(cols))
	for _, c := range cols {
		if id, err := c.ColumnId(); err == nil {
			ids = append(ids, id)
		}
	}
	return ids
}

// reaches reports whether id, or any column computed from it, is in targets.
func (f *columnFlow) reaches(id int32, targets map[int32]bool) bool {
	seen := map[int32]bool{id: true}
	queue := []int32{id}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		if targets[cur] {
			return true
		}
		for _, next := range f.derived[cur] {
			if !seen[next] {
				seen[next] = true
				queue = append(queue, next)
			}
		}
	}
	return false
}

func checkPartitionFilter(pass *Pass, flow *columnFlow, scan *googlesql.ResolvedTableScan) {
	loc, err := scan.GetParseLocationRangeOrNULL()
	if err != nil || loc == nil {
		return
	}
	if flow.dmlTargets[offsets{file.ParseLocStart(loc), file.ParseLocEnd(loc)}] {
		return
	}
	table, err := scan.Table()
	if err != nil || table == nil {
		return
	}
	name, err := table.Name()
	if err != nil || strings.HasSuffix(name, "*") {
		return
	}
	meta, err := pass.LookupTable(pass.Context, name)
	if err != nil || meta == nil || meta.Type != bq.RegularTable {
		return
	}
	partitionColumns := partitionColumnNames(meta)
	if len(partitionColumns) == 0 {
		return
	}

	cols, err := scan.ColumnList()
	if err != nil {
		return
	}
	required := meta.RequirePartitionFilter || meta.TimePartitioning != nil && meta.TimePartitioning.RequirePartitionFilter
	// BigQuery reads no partition, e.g. for COUNT(*), when no column is
	// consumed, yet still rejects such a query on a table requiring a filter.
	if !required && !slices.ContainsFunc(columnIDs(cols), func(id int32) bool { return flow.reaches(id, flow.used) }) {
		return
	}
	for _, c := range cols {
		colName, err := c.Name()
		if err != nil {
			return
		}
		for _, p := range partitionColumns {
			if !strings.EqualFold(colName, p) {
				continue
			}
			id, err := c.ColumnId()
			if err != nil || flow.reaches(id, flow.filtered) {
				return
			}
		}
	}

	r, ok := pass.File.ToLspRange(loc)
	if !ok {
		return
	}
	quoted := make([]string, len(partitionColumns))
	for i, p := range partitionColumns {
		quoted[i] = "`" + p + "`"
	}
	consequence := "it may scan all partitions"
	if required {
		consequence = "BigQuery will reject the query because the table requires a partition filter"
	}
	pass.Report(Diagnostic{
		Range:    r,
		Message:  fmt.Sprintf("Query on partitioned table `%s` has no filter on partition column %s; %s", name, strings.Join(quoted, " or "), consequence),
		Severity: SeverityWarning,
	})
}

func partitionColumnNames(meta *bq.TableMetadata) []string {
	switch {
	case meta.TimePartitioning != nil && meta.TimePartitioning.Field != "":
		return []string{meta.TimePartitioning.Field}
	case file.HasPartitionDate(meta):
		return []string{"_PARTITIONTIME", "_PARTITIONDATE"}
	case meta.TimePartitioning != nil:
		return []string{"_PARTITIONTIME"}
	case meta.RangePartitioning != nil && meta.RangePartitioning.Field != "":
		return []string{meta.RangePartitioning.Field}
	}
	return nil
}

func resolvedOffsets(n googlesql.ResolvedNode) offsets {
	loc, err := n.GetParseLocationRangeOrNULL()
	if err != nil || loc == nil {
		return offsets{-1, -1}
	}
	return offsets{file.ParseLocStart(loc), file.ParseLocEnd(loc)}
}
