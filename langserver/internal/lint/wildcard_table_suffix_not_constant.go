package lint

import (
	"fmt"
	"strings"

	googlesql "github.com/goccy/go-googlesql"
)

// WildcardTableSuffixNotConstant is off by default for the same reason as
// PartitionFilterNotPrunable: its patterns come from the documentation.
var WildcardTableSuffixNotConstant = &Analyzer{
	Name:           "wildcard-table-suffix-not-constant",
	Doc:            "reports _TABLE_SUFFIX filters that BigQuery documents as unable to limit the tables a wildcard table query scans",
	DefaultEnabled: false,
	Run:            runWildcardTableSuffixNotConstant,
}

func runWildcardTableSuffixNotConstant(pass *Pass) {
	for _, flow := range pass.columnFlows() {
		if flow == nil {
			continue
		}
		reported := make(map[offsets]bool)
		for _, scan := range flow.scans {
			checkWildcardTableSuffixConstant(pass, flow, scan, reported)
		}
	}
}

func checkWildcardTableSuffixConstant(pass *Pass, flow *columnFlow, scan *googlesql.ResolvedTableScan, reported map[offsets]bool) {
	table, err := scan.Table()
	if err != nil || table == nil {
		return
	}
	name, err := table.Name()
	if err != nil || !strings.HasSuffix(name, "*") {
		return
	}
	cols, err := scan.ColumnList()
	if err != nil {
		return
	}
	j, ok := newPrunableJudge(flow, cols, func(n string) bool { return strings.EqualFold(n, "_TABLE_SUFFIX") })
	if !ok {
		return
	}
	findings, ok := j.findings(flow)
	if !ok {
		return
	}
	for _, f := range findings {
		reportOnce(pass, f.expr, reported,
			fmt.Sprintf("Filter on `_TABLE_SUFFIX` of wildcard table `%s` does not limit the tables scanned because %s; it scans every table matching the wildcard", name, f.reason))
	}
}
