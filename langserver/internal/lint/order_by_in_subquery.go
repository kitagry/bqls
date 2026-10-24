package lint

import (
	"strings"

	googlesql "github.com/goccy/go-googlesql"
	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/kitagry/bqls/langserver/internal/source/file"
)

var OrderByInSubquery = &Analyzer{
	Name:           "order-by-in-subquery",
	Doc:            "reports ORDER BY in a subquery, CTE or derived table whose order cannot reach the result",
	DefaultEnabled: true,
	Run:            runOrderByInSubquery,
}

const orderByInSubqueryMessage = "ORDER BY in a subquery has no effect on the result because BigQuery does not preserve row order; it only adds sorting cost"

// A false positive here is worse than a miss, so an ORDER BY is reported only
// when no consumer above it could observe the order. Anything that might
// (LIMIT, aggregates such as ARRAY_AGG, analytic functions, set operations,
// ARRAY and scalar subqueries) marks everything beneath it as order sensitive.
// Only queries are checked: DML and CREATE TABLE AS SELECT are left alone.
func runOrderByInSubquery(pass *Pass) {
	var scans []*googlesql.ResolvedOrderByScan
	for _, output := range pass.File.RNode {
		stmt, err := output.ResolvedStatement()
		if err != nil || stmt == nil {
			continue
		}
		q, ok := stmt.(*googlesql.ResolvedQueryStmt)
		if !ok {
			continue
		}
		scan, err := q.Query()
		if err != nil || scan == nil {
			continue
		}
		w := &orderByWalker{refSeen: map[string]bool{}, refSensitive: map[string]bool{}}
		w.visit(scan, false, true)
		scans = append(scans, w.found...)
	}
	if len(scans) == 0 {
		return
	}

	// Resolved ORDER BY scans of subqueries carry the location of their whole
	// query; the clause itself comes from the parser AST. Top level and pipe
	// ORDER BYs have no location, so they are never reported.
	queries := make(map[offsets]*googlesql.ASTQuery)
	for _, q := range file.ListAstNode[*googlesql.ASTQuery](pass.File.Node) {
		if ob, err := q.OrderBy(); err != nil || ob == nil {
			continue
		}
		queries[astOffsets(q)] = q
	}
	for _, scan := range scans {
		loc, err := scan.GetParseLocationRangeOrNULL()
		if err != nil || loc == nil {
			continue
		}
		q, ok := queries[offsets{file.ParseLocStart(loc), file.ParseLocEnd(loc)}]
		if !ok {
			continue
		}
		if lo, err := q.LimitOffset(); err != nil || lo != nil {
			continue
		}
		if n, err := q.NumChildren(); err != nil || n == 0 {
			continue
		}
		reportOrderByInSubquery(pass, q)
	}
}

type orderByWalker struct {
	found []*googlesql.ResolvedOrderByScan
	// refSeen and refSensitive are keyed by resolved WITH name, which is unique
	// within a statement.
	refSeen      map[string]bool
	refSensitive map[string]bool
}

// visit walks n. sensitive means a consumer above may depend on the row order
// n produces; top means n is the outermost query, whose ORDER BY is the result order.
func (w *orderByWalker) visit(n googlesql.ResolvedNode, sensitive, top bool) {
	if n == nil {
		return
	}
	switch n := n.(type) {
	case *googlesql.ResolvedOrderByScan:
		if !sensitive && !top {
			w.found = append(w.found, n)
		}
		// Whatever feeds an ORDER BY is re-sorted.
		w.visitChildren(n, false)
	case *googlesql.ResolvedLimitOffsetScan, *googlesql.ResolvedAggregateScan,
		*googlesql.ResolvedAnalyticScan, *googlesql.ResolvedSetOperationScan:
		w.visitChildren(n, true)
	case *googlesql.ResolvedSubqueryExpr:
		typ, err := n.SubqueryType()
		inert := err == nil && (typ == googlesql.ResolvedSubqueryExprEnums_SubqueryTypeExists ||
			typ == googlesql.ResolvedSubqueryExprEnums_SubqueryTypeIn)
		w.visitChildren(n, !inert)
	case *googlesql.ResolvedWithRefScan:
		if name, err := n.WithQueryName(); err == nil {
			w.refSeen[name] = true
			if sensitive {
				w.refSensitive[name] = true
			}
		}
	case *googlesql.ResolvedWithScan:
		w.visitWith(n, sensitive, top)
	default:
		w.visitChildren(n, sensitive)
	}
}

func (w *orderByWalker) visitChildren(n googlesql.ResolvedNode, sensitive bool) {
	children, err := n.GetChildNodes()
	if err != nil {
		return
	}
	for _, c := range children {
		w.visit(c, sensitive, false)
	}
}

// visitWith visits the main query first so that every reference to an entry
// is known; entries are then visited last to first, because an entry can only
// refer to the ones before it.
func (w *orderByWalker) visitWith(ws *googlesql.ResolvedWithScan, sensitive, top bool) {
	recursive, err := ws.Recursive()
	if err != nil || recursive {
		w.visitChildren(ws, true)
		return
	}
	query, err := ws.Query()
	if err != nil {
		return
	}
	w.visit(query, sensitive, top)

	entries, err := ws.WithEntryList()
	if err != nil {
		return
	}
	for i := len(entries) - 1; i >= 0; i-- {
		name, err := entries[i].WithQueryName()
		if err != nil {
			continue
		}
		sub, err := entries[i].WithSubquery()
		if err != nil {
			continue
		}
		// An entry that is never referenced is the unused-cte rule's concern.
		w.visit(sub, !w.refSeen[name] || w.refSensitive[name], false)
	}
}

func reportOrderByInSubquery(pass *Pass, q *googlesql.ASTQuery) {
	ob, err := q.OrderBy()
	if err != nil || ob == nil {
		return
	}
	span := astOffsets(ob)
	r, ok := pass.File.OffsetsToLspRange(span.start, span.end)
	if !ok {
		return
	}
	d := Diagnostic{
		Range:    r,
		Message:  orderByInSubqueryMessage,
		Severity: SeverityWarning,
	}
	if del, ok := orderByDeletion(pass.File.FixedSrc, span); ok {
		if r, ok := pass.File.OffsetsToLspRange(del.start, del.end); ok {
			d.SuggestedFixes = []SuggestedFix{{
				Title: "Remove ORDER BY",
				Edits: []lsp.TextEdit{{Range: r, NewText: ""}},
			}}
		}
	}
	pass.Report(d)
}

// orderByDeletion returns the clause together with the whitespace before it.
// It gives up when a comment is involved, since deleting the line break after
// a line comment would comment out whatever follows.
func orderByDeletion(src string, clause offsets) (offsets, bool) {
	if !inBounds(src, clause) || containsComment(src[clause.start:clause.end]) {
		return offsets{}, false
	}
	start := clause.start
	for start > 0 && isSQLSpace(src[start-1]) {
		start--
	}
	lineStart := strings.LastIndex(src[:start], "\n") + 1
	if containsComment(src[lineStart:start]) || hasTrailingComment(src, clause.end) {
		return offsets{}, false
	}
	return offsets{start, clause.end}, true
}

func containsComment(s string) bool {
	return strings.Contains(s, "--") || strings.Contains(s, "#") || strings.Contains(s, "/*")
}
