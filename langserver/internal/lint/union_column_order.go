package lint

import (
	"fmt"
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
	firstSources := itemSources(items[0])
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
		if sources := itemSources(items[i+1]); len(sources) > 0 && slices.Equal(sources, firstSources) {
			continue
		}
		reportUnionColumnOrder(pass, cols.list, op, got, want)
	}
}

func itemSources(item *googlesql.ResolvedSetOperationItem) []string {
	scan, err := item.Scan()
	if err != nil {
		return nil
	}
	return scannedSources(scan)
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
