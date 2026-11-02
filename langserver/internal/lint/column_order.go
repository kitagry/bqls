package lint

import (
	"reflect"
	"slices"
	"strings"

	googlesql "github.com/goccy/go-googlesql"
	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/kitagry/bqls/langserver/internal/source/file"
)

// Helpers shared by union-column-order and insert-column-order, which both
// compare the output names of a query with columns it is matched to by position.

// children is used instead of indexed accessors such as Inputs(i), which
// read past the end without checking.
func children(n googlesql.ASTNode) []googlesql.ASTNode {
	num, err := n.NumChildren()
	if err != nil {
		return nil
	}
	var nodes []googlesql.ASTNode
	for i := range num {
		c, err := n.Child(i)
		if err != nil || isNilNode(c) {
			continue
		}
		nodes = append(nodes, c)
	}
	return nodes
}

func setOperationInputs(setOp *googlesql.ASTSetOperation) []googlesql.ASTQueryExpressionNode {
	var inputs []googlesql.ASTQueryExpressionNode
	for _, c := range children(setOp) {
		if in, ok := c.(googlesql.ASTQueryExpressionNode); ok {
			inputs = append(inputs, in)
		}
	}
	return inputs
}

type selectColumns struct {
	// names holds "" where the name is not known, e.g. for `SELECT x + 1`.
	names []string
	list  *googlesql.ASTSelectList
}

// outputColumns returns the names of the columns n outputs, or false when
// their positions are not known from the parse tree alone.
func outputColumns(n googlesql.ASTQueryExpressionNode) (selectColumns, bool) {
	switch n := n.(type) {
	case *googlesql.ASTSelect:
		return selectListColumns(n)
	case *googlesql.ASTQuery:
		if hasPipeOperator(n) {
			return selectColumns{}, false
		}
		expr, err := n.QueryExpr()
		if err != nil || isNilNode(expr) {
			return selectColumns{}, false
		}
		return outputColumns(expr)
	case *googlesql.ASTSetOperation:
		// A nested set operation that matches by name may drop or reorder
		// its first input's columns.
		if matchesByName(n) {
			return selectColumns{}, false
		}
		inputs := setOperationInputs(n)
		if len(inputs) == 0 {
			return selectColumns{}, false
		}
		return outputColumns(inputs[0])
	}
	return selectColumns{}, false
}

func hasPipeOperator(q *googlesql.ASTQuery) bool {
	return slices.ContainsFunc(children(q), func(c googlesql.ASTNode) bool {
		_, ok := c.(googlesql.ASTPipeOperatorNode)
		return ok
	})
}

func selectListColumns(sel *googlesql.ASTSelect) (selectColumns, bool) {
	if as, err := sel.SelectAs(); err != nil || as != nil {
		return selectColumns{}, false
	}
	list, err := sel.SelectList()
	if err != nil || list == nil {
		return selectColumns{}, false
	}
	n, err := list.NumChildren()
	if err != nil {
		return selectColumns{}, false
	}
	cols := selectColumns{list: list}
	for i := range n {
		col, err := list.Columns(int32(i))
		if err != nil || col == nil {
			return selectColumns{}, false
		}
		name, ok := selectColumnName(col)
		if !ok {
			return selectColumns{}, false
		}
		cols.names = append(cols.names, name)
	}
	return cols, true
}

// selectColumnName returns the column's explicit or implicit alias, "" when
// it has none, and false when it may expand to several columns.
func selectColumnName(col *googlesql.ASTSelectColumn) (string, bool) {
	expr, err := col.Expression()
	if err != nil {
		return "", false
	}
	switch expr.(type) {
	case *googlesql.ASTStar, *googlesql.ASTDotStar,
		*googlesql.ASTStarWithModifiers, *googlesql.ASTDotStarWithModifiers:
		return "", false
	}
	if alias, err := col.Alias(); err != nil {
		return "", false
	} else if alias != nil {
		name, err := alias.GetAsString()
		return name, err == nil
	}
	path, ok := expr.(*googlesql.ASTPathExpression)
	if !ok {
		return "", true
	}
	id, err := path.LastName()
	if err != nil || id == nil {
		return "", true
	}
	name, err := id.GetAsString()
	if err != nil {
		return "", true
	}
	return name, true
}

func matchesByName(setOp *googlesql.ASTSetOperation) bool {
	list, err := setOp.Metadata()
	if err != nil || list == nil {
		return true
	}
	for _, c := range children(list) {
		md, ok := c.(*googlesql.ASTSetOperationMetadata)
		if !ok {
			return true
		}
		if mode, err := md.ColumnMatchMode(); err != nil || mode != nil {
			return true
		}
	}
	return false
}

// misplaced returns the names at the positions where the two inputs differ,
// when those names are the same columns in another order. Positions without a
// known name on either side are left out, since they cannot tell a different
// column from a moved one.
func misplaced(first, other []string) (want, got []string) {
	for i := range first {
		if !misplacedAt(first, other, i) {
			continue
		}
		want = append(want, first[i])
		got = append(got, other[i])
	}
	if !slices.Equal(sortedLower(want), sortedLower(got)) {
		return nil, nil
	}
	return want, got
}

// misplacedAt reports whether position i holds a known name on both sides
// that differs.
func misplacedAt(first, other []string, i int) bool {
	return first[i] != "" && other[i] != "" && !strings.EqualFold(first[i], other[i])
}

func sortedLower(names []string) []string {
	lower := make([]string, len(names))
	for i, n := range names {
		lower[i] = strings.ToLower(n)
	}
	slices.Sort(lower)
	return lower
}

// scannedSources returns the sorted, deduplicated tables and WITH entries
// read by n.
func scannedSources(n googlesql.ResolvedNode) []string {
	var sources []string
	file.WalkResolved(n, func(n googlesql.ResolvedNode) error { //nolint
		switch n := n.(type) {
		case *googlesql.ResolvedTableScan:
			if name, ok := tableName(n); ok {
				sources = append(sources, "table:"+name)
			}
		case *googlesql.ResolvedWithRefScan:
			if name, err := n.WithQueryName(); err == nil {
				sources = append(sources, "with:"+name)
			}
		}
		return nil
	})
	slices.Sort(sources)
	return slices.Compact(sources)
}

func tableName(scan *googlesql.ResolvedTableScan) (string, bool) {
	table, err := scan.Table()
	if err != nil || table == nil {
		return "", false
	}
	name, err := table.FullName()
	return name, err == nil
}

func isNilNode(n any) bool {
	return n == nil || reflect.ValueOf(n).IsNil()
}

func quoteNames(names []string) string {
	quoted := make([]string, len(names))
	for i, n := range names {
		quoted[i] = "`" + n + "`"
	}
	return strings.Join(quoted, ", ")
}

// reordering returns, for each position, the position whose column belongs
// there so that other follows first's names, or false when that is ambiguous.
// As in misplaced, positions without a known name on either side stay.
func reordering(first, other []string) ([]int, bool) {
	from := make([]int, len(first))
	used := make(map[int]bool)
	for i := range first {
		from[i] = i
		if !misplacedAt(first, other, i) {
			continue
		}
		found := -1
		for j := range other {
			if !misplacedAt(first, other, j) || !strings.EqualFold(first[i], other[j]) {
				continue
			}
			if found >= 0 {
				return nil, false
			}
			found = j
		}
		if found < 0 || used[found] {
			return nil, false
		}
		used[found] = true
		from[i] = found
	}
	return from, true
}

// plainSelect returns the SELECT that n consists of, together with the
// queries wrapping it, or false when n is a set operation or pipe query whose
// columns cannot be reordered in one select list.
func plainSelect(n googlesql.ASTQueryExpressionNode) (*googlesql.ASTSelect, []*googlesql.ASTQuery, bool) {
	var queries []*googlesql.ASTQuery
	for {
		switch q := n.(type) {
		case *googlesql.ASTSelect:
			return q, queries, true
		case *googlesql.ASTQuery:
			if hasPipeOperator(q) {
				return nil, nil, false
			}
			expr, err := q.QueryExpr()
			if err != nil || isNilNode(expr) {
				return nil, nil, false
			}
			queries = append(queries, q)
			n = expr
		default:
			return nil, nil, false
		}
	}
}

// refersByOrdinal reports whether GROUP BY or ORDER BY may refer to the
// select list by position, as in `GROUP BY 1`. Any integer literal there
// counts, which also rejects harmless ones such as `GROUP BY x + 1`.
func refersByOrdinal(sel *googlesql.ASTSelect, queries []*googlesql.ASTQuery) bool {
	var clauses []googlesql.ASTNode
	if gb, err := sel.GroupBy(); err != nil {
		return true
	} else if gb != nil {
		clauses = append(clauses, gb)
	}
	for _, q := range queries {
		if ob, err := q.OrderBy(); err != nil {
			return true
		} else if ob != nil {
			clauses = append(clauses, ob)
		}
	}
	for _, c := range clauses {
		found := false
		file.Walk(c, func(n googlesql.ASTNode) error { //nolint
			if _, ok := n.(*googlesql.ASTIntLiteral); ok {
				found = true
			}
			return nil
		})
		if found {
			return true
		}
	}
	return false
}

// permutedEdits moves the text of the node at from[i] to position i. It gives
// up when a comment is inside span, since a comment between two items cannot
// be told to belong to either.
func permutedEdits(f file.ParsedFile, span offsets, nodes []googlesql.ASTNode, from []int) ([]lsp.TextEdit, bool) {
	src := f.FixedSrc
	if !inBounds(src, span) || containsComment(src[span.start:span.end]) {
		return nil, false
	}
	var edits []lsp.TextEdit
	for i, j := range from {
		if i == j {
			continue
		}
		at, text := astOffsets(nodes[i]), astOffsets(nodes[j])
		if !inBounds(src, at) || !inBounds(src, text) {
			return nil, false
		}
		r, ok := f.OffsetsToLspRange(at.start, at.end)
		if !ok {
			return nil, false
		}
		edits = append(edits, lsp.TextEdit{Range: r, NewText: src[text.start:text.end]})
	}
	return edits, len(edits) > 0
}
