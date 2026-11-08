package lint

import (
	"regexp"
	"slices"

	googlesql "github.com/goccy/go-googlesql"
	"github.com/kitagry/bqls/langserver/internal/lsp"
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
	joins := astJoinsByLocation(pass.File.Node)

	flows := pass.columnFlows()
	for i, output := range pass.File.RNode {
		flow := flows[i]
		if flow == nil {
			continue
		}
		stmt, err := output.ResolvedStatement()
		if err != nil {
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

// astJoinsByLocation indexes joins by their join keyword, which is where a
// resolved join scan is located.
func astJoinsByLocation(node *googlesql.ASTScript) map[offsets]*googlesql.ASTJoin {
	joins := make(map[offsets]*googlesql.ASTJoin)
	for _, j := range file.ListAstNode[*googlesql.ASTJoin](node) {
		loc, err := j.JoinLocation()
		if err != nil || loc == nil {
			continue
		}
		joins[astOffsets(loc)] = j
	}
	return joins
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
	n, ok := int64Literal(expr)
	return ok && n <= 1
}

func int64Literal(expr googlesql.ResolvedExprNode) (int64, bool) {
	lit, ok := expr.(*googlesql.ResolvedLiteral)
	if !ok {
		return 0, false
	}
	v, err := lit.Value()
	if err != nil || v == nil {
		return 0, false
	}
	n, err := v.ToInt64()
	return n, err == nil
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
	d := Diagnostic{
		Range:    r,
		Message:  crossJoinMessage,
		Severity: SeverityWarning,
	}
	if edits, ok := crossJoinEdits(pass.File, j); ok {
		d.SuggestedFixes = []SuggestedFix{{Title: "Replace with CROSS JOIN", Edits: edits}}
	}
	pass.Report(d)
}

var (
	innerJoinKeyword = regexp.MustCompile(`(?i)^(?:INNER\s+)?JOIN$`)
	// onTrue is matched from the end of the right side, so that a comment
	// before ON is not deleted with it.
	onTrue = regexp.MustCompile(`(?i)^\s*ON(?:\s+TRUE|\s*\(\s*TRUE\s*\))$`)
)

// crossJoinEdits rewrites a comma join or a JOIN ... ON TRUE to CROSS JOIN.
// A join taking part in consecutive ON clauses (`a JOIN b JOIN c ON x ON y`)
// is left alone, since dropping one ON would pair the other with another join.
func crossJoinEdits(f file.ParsedFile, j *googlesql.ASTJoin) ([]lsp.TextEdit, bool) {
	if inConsecutiveOn(j) {
		return nil, false
	}
	loc, err := j.JoinLocation()
	if err != nil || loc == nil {
		return nil, false
	}
	keyword := astOffsets(loc)
	src := f.FixedSrc
	if !inBounds(src, keyword) {
		return nil, false
	}
	typ, err := j.JoinType()
	if err != nil {
		return nil, false
	}
	if typ == googlesql.ASTJoinEnums_JoinTypeComma {
		r, ok := writtenSpan(f, keyword)
		if !ok || src[keyword.start:keyword.end] != "," {
			return nil, false
		}
		text := matchCase("CROSS JOIN", fromKeyword(src, j))
		if keyword.start > 0 && !isSQLSpace(src[keyword.start-1]) {
			text = " " + text
		}
		if keyword.end < len(src) && !isSQLSpace(src[keyword.end]) {
			text += " "
		}
		return []lsp.TextEdit{{Range: r, NewText: text}}, true
	}

	written := src[keyword.start:keyword.end]
	if !innerJoinKeyword.MatchString(written) {
		return nil, false
	}
	rhs, err := j.Rhs()
	if err != nil || rhs == nil {
		return nil, false
	}
	on, err := j.OnClause()
	if err != nil || on == nil {
		return nil, false
	}
	del := offsets{astOffsets(rhs).end, astOffsets(on).end}
	if !inBounds(src, del) || !onTrue.MatchString(src[del.start:del.end]) {
		return nil, false
	}
	keywordRange, ok := writtenSpan(f, keyword)
	if !ok {
		return nil, false
	}
	delRange, ok := writtenSpan(f, del)
	if !ok {
		return nil, false
	}
	return []lsp.TextEdit{
		{Range: keywordRange, NewText: matchCase("CROSS JOIN", written)},
		{Range: delRange, NewText: ""},
	}, true
}

// writtenSpan returns the range of o, a span of FixedSrc, when the user wrote
// it as is; declared variables, for one, are substituted in FixedSrc.
func writtenSpan(f file.ParsedFile, o offsets) (lsp.Range, bool) {
	r, ok := f.OffsetsToLspRange(o.start, o.end)
	if !ok || f.Src[f.SrcOffset(r.Start):f.SrcOffset(r.End)] != f.FixedSrc[o.start:o.end] {
		return lsp.Range{}, false
	}
	return r, true
}

// inConsecutiveOn reports whether j is the right side of another join without
// parentheses, or has such a join as its right side; the parser only builds
// that shape from consecutive ON clauses.
func inConsecutiveOn(j *googlesql.ASTJoin) bool {
	if rhs, err := j.Rhs(); err != nil || isJoin(rhs) {
		return true
	}
	parent, err := j.Parent()
	if err != nil {
		return true
	}
	p, ok := parent.(*googlesql.ASTJoin)
	if !ok {
		return false
	}
	rhs, err := p.Rhs()
	return err != nil || rhs != nil && astOffsets(rhs) == astOffsets(j)
}

func isJoin(n googlesql.ASTTableExpressionNode) bool {
	_, ok := n.(*googlesql.ASTJoin)
	return ok
}

// fromKeyword returns the FROM keyword of the clause holding j, so that a
// comma is replaced in the case the query is written in.
func fromKeyword(src string, j *googlesql.ASTJoin) string {
	var n googlesql.ASTNode = j
	for n != nil {
		if _, ok := n.(*googlesql.ASTFromClause); ok {
			at := astOffsets(n)
			if inBounds(src, at) && at.end-at.start >= len("FROM") {
				return src[at.start : at.start+len("FROM")]
			}
			break
		}
		parent, err := n.Parent()
		if err != nil {
			break
		}
		n = parent
	}
	return "FROM"
}
