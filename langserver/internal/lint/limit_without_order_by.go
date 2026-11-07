package lint

import (
	googlesql "github.com/goccy/go-googlesql"
	"github.com/kitagry/bqls/langserver/internal/source/file"
)

var LimitWithoutOrderBy = &Analyzer{
	Name:           "limit-without-order-by",
	Doc:            "reports LIMIT without ORDER BY in subqueries, CTEs and DML, and LIMIT ... OFFSET without ORDER BY, which keep arbitrary rows",
	DefaultEnabled: false,
	Run:            runLimitWithoutOrderBy,
}

const (
	limitWithoutOrderByMessage  = "LIMIT without ORDER BY keeps an arbitrary set of rows, which can differ between runs; add ORDER BY to decide which rows are kept"
	offsetWithoutOrderByMessage = "LIMIT ... OFFSET without ORDER BY skips an arbitrary set of rows, so pages can overlap or miss rows between runs; add ORDER BY to page through a fixed order"
)

// A LIMIT is left alone where the arbitrary rows cannot matter: at the top of
// a query without OFFSET, which is how a result is previewed; at the top of an
// EXISTS subquery, whose result depends only on the number of rows; and over
// input of at most one row.
func runLimitWithoutOrderBy(pass *Pass) {
	// A resolved limit expression carries the location of the expression
	// only, so the clause comes from the parse tree.
	var clauses []limitClause
	for _, lo := range file.ListAstNode[*googlesql.ASTLimitOffset](pass.File.Node) {
		limit, err := lo.LimitExpression()
		if err != nil || isNilNode(limit) {
			continue
		}
		clauses = append(clauses, limitClause{limit: astOffsets(limit), clause: lo})
	}

	for _, output := range pass.File.RNode {
		stmt, err := output.ResolvedStatement()
		if err != nil || stmt == nil {
			continue
		}
		exempt := make(map[offsets]bool)
		if q, ok := stmt.(*googlesql.ResolvedQueryStmt); ok {
			if scan, err := q.Query(); err == nil {
				if lo, ok := queryRoot(scan).(*googlesql.ResolvedLimitOffsetScan); ok {
					if offset, err := lo.Offset(); err == nil && isNilNode(offset) {
						exempt[resolvedOffsets(lo)] = true
					}
				}
			}
		}
		withSubqueries := make(map[string]googlesql.ResolvedScanNode)
		var scans []*googlesql.ResolvedLimitOffsetScan
		walkLive(stmt, func(n googlesql.ResolvedNode) {
			switch n := n.(type) {
			case *googlesql.ResolvedLimitOffsetScan:
				scans = append(scans, n)
			case *googlesql.ResolvedWithEntry:
				name, err := n.WithQueryName()
				sub, err2 := n.WithSubquery()
				if err == nil && err2 == nil {
					withSubqueries[name] = sub
				}
			case *googlesql.ResolvedSubqueryExpr:
				typ, err := n.SubqueryType()
				if err != nil || typ != googlesql.ResolvedSubqueryExprEnums_SubqueryTypeExists {
					return
				}
				if sub, err := n.Subquery(); err == nil {
					if lo, ok := queryRoot(sub).(*googlesql.ResolvedLimitOffsetScan); ok {
						exempt[resolvedOffsets(lo)] = true
					}
				}
			}
		})
		for _, scan := range scans {
			if exempt[resolvedOffsets(scan)] {
				continue
			}
			checkLimitWithoutOrderBy(pass, clauses, withSubqueries, scan)
		}
	}
}

type limitClause struct {
	limit  offsets
	clause *googlesql.ASTLimitOffset
}

// findLimitClause matches by containment rather than equality because the
// resolved expression can be located on only part of the written one, as for
// LIMIT CAST(@n AS INT64).
func findLimitClause(clauses []limitClause, at offsets) (*googlesql.ASTLimitOffset, bool) {
	if at.start < 0 {
		return nil, false
	}
	for _, c := range clauses {
		if c.limit.start <= at.start && at.end <= c.limit.end {
			return c.clause, true
		}
	}
	return nil, false
}

// queryRoot skips the WITH clause of a query.
func queryRoot(scan googlesql.ResolvedScanNode) googlesql.ResolvedScanNode {
	for {
		ws, ok := scan.(*googlesql.ResolvedWithScan)
		if !ok {
			return scan
		}
		q, err := ws.Query()
		if err != nil || q == nil {
			return scan
		}
		scan = q
	}
}

func checkLimitWithoutOrderBy(pass *Pass, clauses []limitClause, withSubqueries map[string]googlesql.ResolvedScanNode, scan *googlesql.ResolvedLimitOffsetScan) {
	limit, err := scan.Limit()
	if err != nil || isNilNode(limit) {
		return
	}
	if n, ok := int64Literal(limit); ok && n == 0 {
		return
	}
	input, err := scan.InputScan()
	if err != nil || isNilNode(input) || orderedInput(input) || atMostOneRow(input, withSubqueries) {
		return
	}
	clause, ok := findLimitClause(clauses, resolvedOffsets(limit))
	if !ok {
		return
	}
	at := astOffsets(clause)
	r, ok := pass.File.OffsetsToLspRange(at.start, at.end)
	if !ok {
		return
	}
	msg := limitWithoutOrderByMessage
	if offset, err := scan.Offset(); err == nil && !isNilNode(offset) {
		msg = offsetWithoutOrderByMessage
	}
	pass.Report(Diagnostic{Range: r, Message: msg, Severity: SeverityWarning})
}

// orderedInput reports whether scan is sorted. Pipe operators such as
// |> WHERE between |> ORDER BY and |> LIMIT keep the order but do not mark
// their scan as ordered.
func orderedInput(scan googlesql.ResolvedScanNode) bool {
	for {
		if ordered, err := scan.IsOrdered(); err == nil && ordered {
			return true
		}
		switch scan.(type) {
		case *googlesql.ResolvedProjectScan, *googlesql.ResolvedFilterScan:
		default:
			return false
		}
		in, ok := scan.(interface {
			InputScan() (googlesql.ResolvedScanNode, error)
		})
		if !ok {
			return false
		}
		input, err := in.InputScan()
		if err != nil || isNilNode(input) {
			return false
		}
		scan = input
	}
}
