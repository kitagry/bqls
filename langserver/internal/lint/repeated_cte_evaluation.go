package lint

import (
	"fmt"

	googlesql "github.com/goccy/go-googlesql"
	"github.com/kitagry/bqls/langserver/internal/source/file"
)

var RepeatedCTEEvaluation = &Analyzer{
	Name:           "repeated-cte-evaluation",
	Doc:            "reports CTEs that join, aggregate or use window functions over tables and are evaluated more than once",
	DefaultEnabled: false,
	Run:            runRepeatedCTEEvaluation,
}

func repeatedCTEEvaluationMessage(name string, n int) string {
	return fmt.Sprintf("CTE `%s` is evaluated %d times, once per reference, because BigQuery does not materialize non-recursive CTEs, so its table reads and its joins, aggregations or window functions run again each time; if it is expensive, store its result in a temporary table", name, n)
}

// Reading a table twice is the usual price of a readable query, so a CTE is
// reported only when its own body also joins, aggregates or computes window
// functions. A CTE that only filters another one is not reported; the CTE it
// reads is, with the evaluations it causes counted in.
func runRepeatedCTEEvaluation(pass *Pass) {
	graphs := make(map[*googlesql.AnalyzerOutput]repeatedCTEGraph)
	for _, wc := range resolvedWithClauses(pass) {
		g, ok := graphs[wc.output]
		if !ok {
			g, ok = newRepeatedCTEGraph(wc.output)
			if !ok {
				continue
			}
			graphs[wc.output] = g
		}
		for i, name := range wc.names {
			node, ok := g.ctes[name]
			if !ok || node.recursive {
				continue
			}
			n := g.evals[node]
			if n < 2 || !g.bodies[name].heavy || !g.readsTable(name, map[string]bool{}) {
				continue
			}
			ident := wc.entries[i].ident
			r, ok := pass.File.OffsetsToLspRange(ident.start, ident.end)
			if !ok {
				continue
			}
			pass.Report(Diagnostic{Range: r, Message: repeatedCTEEvaluationMessage(wc.entries[i].name, n), Severity: SeverityWarning})
		}
	}
}

type repeatedCTEGraph struct {
	ctes   map[string]*cteNode
	evals  map[*cteNode]int
	bodies map[string]cteBody
}

// cteBody describes what a CTE's own body does, leaving out the WITH entries
// nested in it, which are CTEs of their own.
type cteBody struct {
	heavy      bool
	readsTable bool
	refs       []string
}

func newRepeatedCTEGraph(output *googlesql.AnalyzerOutput) (repeatedCTEGraph, bool) {
	stmt, err := output.ResolvedStatement()
	if err != nil || stmt == nil {
		return repeatedCTEGraph{}, false
	}
	g := &cteGraph{ctes: make(map[string]*cteNode), groupKeys: make(map[int32]bool)}
	g.walk(stmt, walkCtx{})
	bodies := make(map[string]cteBody)
	for _, entry := range file.ListResolvedAstNode[*googlesql.ResolvedWithEntry](output) {
		name, err := entry.WithQueryName()
		if err != nil {
			continue
		}
		sub, err := entry.WithSubquery()
		if err != nil || isNilNode(sub) {
			continue
		}
		var body cteBody
		body.add(sub)
		bodies[name] = body
	}
	return repeatedCTEGraph{ctes: g.ctes, evals: g.evaluations(), bodies: bodies}, true
}

func (b *cteBody) add(n googlesql.ResolvedNode) {
	switch n := n.(type) {
	case *googlesql.ResolvedWithEntry:
		return
	case *googlesql.ResolvedTableScan:
		b.readsTable = true
	case *googlesql.ResolvedJoinScan, *googlesql.ResolvedAggregateScan, *googlesql.ResolvedAnalyticScan:
		b.heavy = true
	case *googlesql.ResolvedWithRefScan:
		if name, err := n.WithQueryName(); err == nil {
			b.refs = append(b.refs, name)
		}
	}
	children, err := n.GetChildNodes()
	if err != nil {
		return
	}
	for _, c := range children {
		if !isNilNode(c) {
			b.add(c)
		}
	}
}

// readsTable reports whether the CTE reads a table, directly or through the
// CTEs it references.
func (g repeatedCTEGraph) readsTable(name string, seen map[string]bool) bool {
	if seen[name] {
		return false
	}
	seen[name] = true
	body := g.bodies[name]
	if body.readsTable {
		return true
	}
	for _, ref := range body.refs {
		if g.readsTable(ref, seen) {
			return true
		}
	}
	return false
}
