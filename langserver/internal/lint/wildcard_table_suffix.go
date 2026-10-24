package lint

import (
	"fmt"
	"strings"

	googlesql "github.com/goccy/go-googlesql"
	"github.com/kitagry/bqls/langserver/internal/source/file"
)

var WildcardTableSuffix = &Analyzer{
	Name:           "wildcard-table-suffix",
	Doc:            "reports wildcard table queries that never filter on _TABLE_SUFFIX",
	DefaultEnabled: true,
	Run:            runWildcardTableSuffix,
}

// Filtering is judged exactly as partition-filter does, so a condition that
// merely mentions a value derived from _TABLE_SUFFIX (even on one side of an
// OR) counts: a false positive costs more trust than a missed report.
func runWildcardTableSuffix(pass *Pass) {
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
			checkWildcardTableSuffix(pass, flow, scan)
		}
	}
}

func checkWildcardTableSuffix(pass *Pass, flow *columnFlow, scan *googlesql.ResolvedTableScan) {
	table, err := scan.Table()
	if err != nil || table == nil {
		return
	}
	name, err := table.Name()
	if err != nil || !strings.HasSuffix(name, "*") {
		return
	}
	// The pseudo column is only in the scan's column list when the query
	// references it, so its absence means the table is not filtered.
	cols, err := scan.ColumnList()
	if err != nil {
		return
	}
	for _, c := range cols {
		colName, err := c.Name()
		if err != nil {
			return
		}
		if !strings.EqualFold(colName, "_TABLE_SUFFIX") {
			continue
		}
		id, err := c.ColumnId()
		if err != nil || flow.reaches(id, flow.filtered) {
			return
		}
	}

	loc, err := scan.GetParseLocationRangeOrNULL()
	if err != nil || loc == nil {
		return
	}
	at := tablePathOffsets(pass.File.Node, offsets{file.ParseLocStart(loc), file.ParseLocEnd(loc)})
	r, ok := pass.File.OffsetsToLspRange(at.start, at.end)
	if !ok {
		return
	}
	pass.Report(Diagnostic{
		Range:    r,
		Message:  fmt.Sprintf("Query on wildcard table `%s` has no filter on `_TABLE_SUFFIX`; it scans every table matching the wildcard", name),
		Severity: SeverityWarning,
	})
}
