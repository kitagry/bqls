package lint

import (
	"slices"

	googlesql "github.com/goccy/go-googlesql"
	"github.com/kitagry/bqls/langserver/internal/source/file"
)

var CrossJoin = &Analyzer{
	Name:           "cross-join",
	Doc:            "reports joins without a join condition, which return the Cartesian product of both sides",
	DefaultEnabled: true,
	Run:            runCrossJoin,
}

const crossJoinMessage = "Join has no join condition, so it returns every combination of rows from both sides; add a condition, or write CROSS JOIN if this is intended"

// A false positive here is worse than a miss, so a join is reported only when
// nothing suggests the product is intended: an explicit CROSS JOIN, an outer
// or lateral join, a side known to yield at most one row, or a filter that
// relates the two sides all keep the rule silent.
func runCrossJoin(pass *Pass) {
	joins := make(map[offsets]*googlesql.ASTJoin)
	for _, j := range file.ListAstNode[*googlesql.ASTJoin](pass.File.Node) {
		loc, err := j.JoinLocation()
		if err != nil || loc == nil {
			continue
		}
		joins[astOffsets(loc)] = j
	}

	for _, output := range pass.File.RNode {
		stmt, err := output.ResolvedStatement()
		if err != nil || stmt == nil {
			continue
		}
		flow, ok := analyzeColumnFlow(stmt)
		if !ok {
			continue
		}
		withSubqueries := make(map[string]googlesql.ResolvedScanNode)
		var scans []*googlesql.ResolvedJoinScan
		walkLive(stmt, func(n googlesql.ResolvedNode) {
			switch n := n.(type) {
			case *googlesql.ResolvedJoinScan:
				scans = append(scans, n)
			case *googlesql.ResolvedWithEntry:
				name, err := n.WithQueryName()
				sub, err2 := n.WithSubquery()
				if err == nil && err2 == nil {
					withSubqueries[name] = sub
				}
			}
		})
		for _, scan := range scans {
			// A join scan is located at its join keyword, except for pipe
			// joins, which are located at the whole query and so not found.
			j, ok := joins[resolvedOffsets(scan)]
			if !ok || !unconditionedJoin(flow, withSubqueries, scan, j) {
				continue
			}
			reportCrossJoin(pass, j)
		}
	}
}

func unconditionedJoin(flow *columnFlow, withSubqueries map[string]googlesql.ResolvedScanNode, scan *googlesql.ResolvedJoinScan, j *googlesql.ASTJoin) bool {
	if typ, err := j.JoinType(); err != nil || typ == googlesql.ASTJoinEnums_JoinTypeCross {
		return false
	}
	if typ, err := scan.JoinType(); err != nil || typ != googlesql.ResolvedJoinScanEnums_JoinTypeInner {
		return false
	}
	if lateral, err := scan.IsLateral(); err != nil || lateral {
		return false
	}
	expr, err := scan.JoinExpr()
	if err != nil || expr != nil && !isTrueLiteral(expr) {
		return false
	}
	left, err := scan.LeftScan()
	right, err2 := scan.RightScan()
	if err != nil || err2 != nil {
		return false
	}
	if atMostOneRow(left, withSubqueries) || atMostOneRow(right, withSubqueries) {
		return false
	}
	leftCols, err := left.MutableColumnList()
	rightCols, err2 := right.MutableColumnList()
	if err != nil || err2 != nil {
		return false
	}
	return !conditioned(flow, columnIDs(leftCols), columnIDs(rightCols))
}

// conditioned reports whether the two sides are related by some filter term:
// either one term mentions both, or each side is compared with something
// outside itself, as in `FROM a, b, c WHERE a.id = c.a_id AND b.id = c.b_id`.
// The latter may still pair sides with unrelated tables, but staying silent
// then is the cheaper mistake.
func conditioned(flow *columnFlow, left, right []int32) bool {
	fromLeft, fromRight := flow.derivedFrom(left), flow.derivedFrom(right)
	var leftRelated, rightRelated bool
	for _, c := range flow.conjuncts {
		inLeft := slices.ContainsFunc(c, func(id int32) bool { return fromLeft[id] })
		inRight := slices.ContainsFunc(c, func(id int32) bool { return fromRight[id] })
		if inLeft && inRight {
			return true
		}
		leftRelated = leftRelated || inLeft && slices.ContainsFunc(c, func(id int32) bool { return !fromLeft[id] })
		rightRelated = rightRelated || inRight && slices.ContainsFunc(c, func(id int32) bool { return !fromRight[id] })
	}
	return leftRelated && rightRelated
}

func isTrueLiteral(expr googlesql.ResolvedExprNode) bool {
	lit, ok := expr.(*googlesql.ResolvedLiteral)
	if !ok {
		return false
	}
	v, err := lit.Value()
	if err != nil || v == nil {
		return false
	}
	b, err := v.BoolValue()
	return err == nil && b
}

// atMostOneRow reports whether scan is known to produce no more than one row,
// e.g. a global aggregate, which is commonly joined without a condition to
// attach its values to every row.
func atMostOneRow(scan googlesql.ResolvedScanNode, withSubqueries map[string]googlesql.ResolvedScanNode) bool {
	switch s := scan.(type) {
	case *googlesql.ResolvedSingleRowScan:
		return true
	case *googlesql.ResolvedAggregateScan:
		n, err := s.GroupByListSize()
		return err == nil && n == 0
	case *googlesql.ResolvedLimitOffsetScan:
		if limit, err := s.Limit(); err == nil && isLimitAtMostOne(limit) {
			return true
		}
	case *googlesql.ResolvedWithRefScan:
		name, err := s.WithQueryName()
		sub, ok := withSubqueries[name]
		return err == nil && ok && atMostOneRow(sub, withSubqueries)
	case *googlesql.ResolvedWithScan:
		query, err := s.Query()
		return err == nil && atMostOneRow(query, withSubqueries)
	}
	// Each remaining scan below yields no more rows than its input.
	switch scan.(type) {
	case *googlesql.ResolvedProjectScan, *googlesql.ResolvedFilterScan,
		*googlesql.ResolvedOrderByScan, *googlesql.ResolvedAnalyticScan,
		*googlesql.ResolvedSampleScan, *googlesql.ResolvedLimitOffsetScan:
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
	return err == nil && input != nil && atMostOneRow(input, withSubqueries)
}

func isLimitAtMostOne(expr googlesql.ResolvedExprNode) bool {
	lit, ok := expr.(*googlesql.ResolvedLiteral)
	if !ok {
		return false
	}
	v, err := lit.Value()
	if err != nil || v == nil {
		return false
	}
	n, err := v.ToInt64()
	return err == nil && n <= 1
}

func reportCrossJoin(pass *Pass, j *googlesql.ASTJoin) {
	rhs, err := j.Rhs()
	if err != nil || rhs == nil {
		return
	}
	at := astOffsets(rhs)
	r, ok := pass.File.OffsetsToLspRange(at.start, at.end)
	if !ok {
		return
	}
	pass.Report(Diagnostic{
		Range:    r,
		Message:  crossJoinMessage,
		Severity: SeverityWarning,
	})
}
