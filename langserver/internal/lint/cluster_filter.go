package lint

import (
	"fmt"
	"slices"
	"strings"

	bq "cloud.google.com/go/bigquery"
	googlesql "github.com/goccy/go-googlesql"
)

// Both clustering rules are off by default: clustering only lowers the cost
// under on-demand pricing, BigQuery does not show the saving in a dry run,
// and reading a clustered table in full is often intended.
var ClusterFilter = &Analyzer{
	Name:           "cluster-filter",
	Doc:            "reports clustered tables read without a filter on the first clustering column, or with a clustering column skipped",
	DefaultEnabled: false,
	Run:            runClusterFilter,
}

var ClusterFilterNotPrunable = &Analyzer{
	Name:           "cluster-filter-not-prunable",
	Doc:            "reports clustering column filters written in a form BigQuery documents as unable to reduce the data read",
	DefaultEnabled: false,
	Run:            runClusterFilterNotPrunable,
}

const clusterFilterAdvice = "filter on the clustering columns in order, starting with the first"

func runClusterFilter(pass *Pass) {
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
			checkClusterFilter(pass, flow, scan)
		}
	}
}

// clusteredTable returns the table a scan reads when it is a clustered table
// whose filters the clustering rules judge.
func clusteredTable(pass *Pass, flow *columnFlow, scan *googlesql.ResolvedTableScan) (string, *bq.TableMetadata, bool) {
	name, meta, ok := filteredRegularTable(pass, flow, scan)
	if !ok || meta.Clustering == nil || len(meta.Clustering.Fields) == 0 {
		return "", nil, false
	}
	return name, meta, true
}

// BigQuery documents that a filter helps only on the clustering columns in
// their order, starting with the first, so the first unfiltered column ends
// the useful prefix.
func checkClusterFilter(pass *Pass, flow *columnFlow, scan *googlesql.ResolvedTableScan) {
	name, meta, ok := clusteredTable(pass, flow, scan)
	if !ok {
		return
	}
	cols, err := scan.ColumnList()
	if err != nil {
		return
	}
	// Nothing is read, e.g. for COUNT(*).
	if !slices.ContainsFunc(columnIDs(cols), func(id int32) bool { return flow.reaches(id, flow.used) }) {
		return
	}
	filtered := func(field string) bool {
		for _, c := range cols {
			colName, err := c.Name()
			id, err2 := c.ColumnId()
			if err == nil && err2 == nil && strings.EqualFold(colName, field) && flow.reaches(id, flow.filtered) {
				return true
			}
		}
		return false
	}
	fields := meta.Clustering.Fields
	var msg string
	if !filtered(fields[0]) {
		msg = fmt.Sprintf("Query on clustered table `%s` has no filter on its first clustering column `%s`, so the clustering cannot reduce the data read; %s", name, fields[0], clusterFilterAdvice)
	} else {
		missing := ""
		for _, f := range fields[1:] {
			if !filtered(f) {
				if missing == "" {
					missing = f
				}
				continue
			}
			if missing != "" {
				msg = fmt.Sprintf("Query on clustered table `%s` filters clustering column `%s` but not `%s` before it, so that filter cannot reduce the data read; %s", name, f, missing, clusterFilterAdvice)
				break
			}
		}
	}
	if msg == "" {
		return
	}
	at := tablePathOffsets(pass.File.Node, resolvedOffsets(scan))
	r, ok := pass.File.OffsetsToLspRange(at.start, at.end)
	if !ok {
		return
	}
	pass.Report(Diagnostic{Range: r, Message: msg, Severity: SeverityWarning})
}

func runClusterFilterNotPrunable(pass *Pass) {
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
			checkClusterFilterPrunable(pass, flow, scan, reported)
		}
	}
}

// As in partition-filter-not-prunable, a term is reported only when no other
// term on the clustering columns might reduce the data read.
func checkClusterFilterPrunable(pass *Pass, flow *columnFlow, scan *googlesql.ResolvedTableScan, reported map[offsets]bool) {
	name, meta, ok := clusteredTable(pass, flow, scan)
	if !ok {
		return
	}
	cols, err := scan.ColumnList()
	if err != nil {
		return
	}
	fields := meta.Clustering.Fields
	j, ok := newPrunableJudge(flow, cols, func(name string) bool {
		return slices.ContainsFunc(fields, func(f string) bool { return strings.EqualFold(f, name) })
	})
	if !ok {
		return
	}
	j.anyExpression = true
	findings, ok := j.findings(flow)
	if !ok {
		return
	}
	for _, f := range findings {
		reportOnce(pass, f.expr, reported,
			fmt.Sprintf("Filter on clustering column `%s` of `%s` cannot reduce the data read because %s", f.column, name, f.reason))
	}
}
