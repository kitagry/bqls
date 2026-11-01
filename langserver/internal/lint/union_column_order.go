package lint

import (
	"fmt"
	"reflect"
	"slices"
	"strings"

	googlesql "github.com/goccy/go-googlesql"
	"github.com/kitagry/bqls/langserver/internal/source/file"
)

var UnionColumnOrder = &Analyzer{
	Name:           "union-column-order",
	Doc:            "reports set operation inputs whose columns are the first input's columns in a different order",
	DefaultEnabled: true,
	Run:            runUnionColumnOrder,
}

var setOperationNames = map[googlesql.ResolvedSetOperationScanEnums_SetOperationType]string{
	googlesql.ResolvedSetOperationScanEnums_SetOperationTypeUnionAll:          "UNION ALL",
	googlesql.ResolvedSetOperationScanEnums_SetOperationTypeUnionDistinct:     "UNION DISTINCT",
	googlesql.ResolvedSetOperationScanEnums_SetOperationTypeIntersectAll:      "INTERSECT ALL",
	googlesql.ResolvedSetOperationScanEnums_SetOperationTypeIntersectDistinct: "INTERSECT DISTINCT",
	googlesql.ResolvedSetOperationScanEnums_SetOperationTypeExceptAll:         "EXCEPT ALL",
	googlesql.ResolvedSetOperationScanEnums_SetOperationTypeExceptDistinct:    "EXCEPT DISTINCT",
}

// The resolved AST finds the set operations that match by position, but
// output names come from the parse tree: a column reference keeps its source
// name in the resolved AST even when the select list renames it, so
// `SELECT x AS a, y AS b ... UNION ALL SELECT y AS a, x AS b` would look
// reordered there.
func runUnionColumnOrder(pass *Pass) {
	setOps := make(map[offsets]*googlesql.ASTSetOperation)
	for _, s := range file.ListAstNode[*googlesql.ASTSetOperation](pass.File.Node) {
		setOps[astOffsets(s)] = s
	}

	for _, output := range pass.File.RNode {
		stmt, err := output.ResolvedStatement()
		if err != nil || stmt == nil {
			continue
		}
		walkLive(stmt, func(n googlesql.ResolvedNode) {
			scan, ok := n.(*googlesql.ResolvedSetOperationScan)
			if !ok {
				return
			}
			setOp, ok := findSetOperation(setOps, pass.File.Src, resolvedOffsets(scan))
			if !ok {
				return
			}
			checkSetOperation(pass, scan, setOp)
		})
	}
}

// findSetOperation returns the AST node at a set operation's resolved
// location, which includes the parentheses around a parenthesized query such
// as a CTE body. Pipe set operations are located at the whole query, so they
// find no node.
func findSetOperation(setOps map[offsets]*googlesql.ASTSetOperation, src string, at offsets) (*googlesql.ASTSetOperation, bool) {
	for at.start >= 0 && at.start <= at.end && at.end <= len(src) {
		if s, ok := setOps[at]; ok {
			return s, true
		}
		text := src[at.start:at.end]
		if !strings.HasPrefix(text, "(") || !strings.HasSuffix(text, ")") {
			break
		}
		inner := text[1 : len(text)-1]
		trimmed := strings.TrimLeft(inner, " \t\r\n")
		at.start += 1 + len(inner) - len(trimmed)
		at.end = at.start + len(strings.TrimRight(trimmed, " \t\r\n"))
	}
	return nil, false
}

func checkSetOperation(pass *Pass, scan *googlesql.ResolvedSetOperationScan, setOp *googlesql.ASTSetOperation) {
	if mode, err := scan.ColumnMatchMode(); err != nil || mode != googlesql.ResolvedSetOperationScanEnums_SetOperationColumnMatchModeByPosition {
		return
	}
	opType, err := scan.OpType()
	if err != nil {
		return
	}
	op, ok := setOperationNames[opType]
	if !ok {
		return
	}
	items, err := scan.InputItemList()
	if err != nil || len(items) < 2 {
		return
	}
	inputs := setOperationInputs(setOp)
	if len(inputs) != len(items) {
		return
	}
	first, ok := outputColumns(inputs[0])
	if !ok {
		return
	}
	firstSources := scannedSources(items[0])
	for i, in := range inputs[1:] {
		cols, ok := outputColumns(in)
		if !ok || len(cols.names) != len(first.names) {
			continue
		}
		want, got := misplaced(first.names, cols.names)
		if len(got) == 0 {
			continue
		}
		// Reading the same sources in another order is how a relation is
		// made symmetric, as in `SELECT src, dst FROM edges UNION ALL
		// SELECT dst, src FROM edges`.
		if sources := scannedSources(items[i+1]); len(sources) > 0 && slices.Equal(sources, firstSources) {
			continue
		}
		reportUnionColumnOrder(pass, cols.list, op, got, want)
	}
}

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
		if slices.ContainsFunc(children(n), func(c googlesql.ASTNode) bool {
			_, ok := c.(googlesql.ASTPipeOperatorNode)
			return ok
		}) {
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
		if first[i] == "" || other[i] == "" || strings.EqualFold(first[i], other[i]) {
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

func sortedLower(names []string) []string {
	lower := make([]string, len(names))
	for i, n := range names {
		lower[i] = strings.ToLower(n)
	}
	slices.Sort(lower)
	return lower
}

// scannedSources returns the sorted, deduplicated tables and WITH entries
// read by item.
func scannedSources(item *googlesql.ResolvedSetOperationItem) []string {
	scan, err := item.Scan()
	if err != nil {
		return nil
	}
	var sources []string
	file.WalkResolved(scan, func(n googlesql.ResolvedNode) error { //nolint
		switch n := n.(type) {
		case *googlesql.ResolvedTableScan:
			if table, err := n.Table(); err == nil && table != nil {
				if name, err := table.FullName(); err == nil {
					sources = append(sources, "table:"+name)
				}
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

func isNilNode(n any) bool {
	return n == nil || reflect.ValueOf(n).IsNil()
}

func reportUnionColumnOrder(pass *Pass, list *googlesql.ASTSelectList, op string, got, want []string) {
	at := astOffsets(list)
	r, ok := pass.File.OffsetsToLspRange(at.start, at.end)
	if !ok {
		return
	}
	pass.Report(Diagnostic{
		Range:    r,
		Message:  fmt.Sprintf("%s matches columns by position, not by name, but this input has %s where the first input has %s; reorder the columns or use %s BY NAME", op, quoteNames(got), quoteNames(want), op),
		Severity: SeverityWarning,
	})
}

func quoteNames(names []string) string {
	quoted := make([]string, len(names))
	for i, n := range names {
		quoted[i] = "`" + n + "`"
	}
	return strings.Join(quoted, ", ")
}
