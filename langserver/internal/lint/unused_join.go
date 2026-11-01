package lint

import (
	googlesql "github.com/goccy/go-googlesql"
	"github.com/kitagry/bqls/langserver/internal/source/file"
)

var UnusedJoin = &Analyzer{
	Name:           "unused-join",
	Doc:            "reports LEFT / RIGHT JOINs whose optional side is never used outside the join condition",
	DefaultEnabled: true,
	Run:            runUnusedJoin,
}

const unusedJoinMessage = "no column of the joined table is used outside its join condition; the join only adds cost (or duplicates rows when several rows match), so it can likely be removed"

// Only the optional side of an outer join is checked: an INNER or CROSS JOIN
// filters or multiplies rows, and each side of a FULL JOIN contributes its
// unmatched rows, so those change the result even when no column is read.
//
// A column counts as used wherever the resolved AST reads it outside the
// join's own condition and the joined side itself. Besides column references
// that includes every ProjectScan column list and the statement's output
// columns, because `SELECT *` passes columns through without a reference.
// A subquery that selects the column therefore uses it even if nothing above
// reads it; that only misses reports.
func runUnusedJoin(pass *Pass) {
	for _, output := range pass.File.RNode {
		stmt, err := output.ResolvedStatement()
		if err != nil || stmt == nil {
			continue
		}
		uses := columnUses(stmt)
		for _, join := range file.ListResolvedAstNode[*googlesql.ResolvedJoinScan](output) {
			if isUnusedJoin(join, uses) {
				reportUnusedJoin(pass, join)
			}
		}
	}
}

func isUnusedJoin(join *googlesql.ResolvedJoinScan, uses map[int32]int) bool {
	side, ok := optionalSide(join)
	if !ok {
		return false
	}
	cols, ok := scanColumns(side)
	if !ok {
		return false
	}

	local := columnUses(side)
	if cond, err := join.JoinExpr(); err == nil && cond != nil {
		for id, n := range columnUses(cond) {
			local[id] += n
		}
	}
	for _, c := range cols {
		id, err := c.ColumnId()
		if err != nil || uses[id] > local[id] {
			return false
		}
	}
	return true
}

// columnUses counts, per column id, how often n and its descendants read it.
func columnUses(n googlesql.ResolvedNode) map[int32]int {
	uses := make(map[int32]int)
	file.WalkResolved(n, func(n googlesql.ResolvedNode) error { //nolint
		var cols []*googlesql.ResolvedColumn
		switch n := n.(type) {
		case *googlesql.ResolvedColumnRef:
			if c, err := n.Column(); err == nil {
				cols = append(cols, c)
			}
		case *googlesql.ResolvedOutputColumn:
			if c, err := n.Column(); err == nil {
				cols = append(cols, c)
			}
		case *googlesql.ResolvedProjectScan:
			cols, _ = n.ColumnList()
		}
		for _, c := range cols {
			if id, err := c.ColumnId(); err == nil {
				uses[id]++
			}
		}
		return nil
	})
	return uses
}

// optionalSide returns the side of a LEFT / RIGHT JOIN whose columns are NULL
// in the rows added for unmatched rows of the other side.
func optionalSide(join *googlesql.ResolvedJoinScan) (googlesql.ResolvedScanNode, bool) {
	typ, err := join.JoinType()
	if err != nil {
		return nil, false
	}
	var side googlesql.ResolvedScanNode
	switch typ {
	case googlesql.ResolvedJoinScanEnums_JoinTypeLeft:
		side, err = join.RightScan()
	case googlesql.ResolvedJoinScanEnums_JoinTypeRight:
		side, err = join.LeftScan()
	default:
		return nil, false
	}
	return side, err == nil && side != nil
}

func scanColumns(scan googlesql.ResolvedScanNode) ([]*googlesql.ResolvedColumn, bool) {
	// ColumnList is defined on each concrete scan type, not on ResolvedScanNode.
	s, ok := scan.(interface {
		ColumnList() ([]*googlesql.ResolvedColumn, error)
	})
	if !ok {
		return nil, false
	}
	cols, err := s.ColumnList()
	return cols, err == nil
}

func reportUnusedJoin(pass *Pass, join *googlesql.ResolvedJoinScan) {
	loc, err := join.GetParseLocationRangeOrNULL()
	if err != nil || loc == nil {
		return
	}
	r, ok := pass.File.OffsetsToLspRange(file.ParseLocStart(loc), file.ParseLocEnd(loc))
	if !ok {
		return
	}
	pass.Report(Diagnostic{
		Range:    r,
		Message:  unusedJoinMessage,
		Severity: SeverityInformation,
	})
}
