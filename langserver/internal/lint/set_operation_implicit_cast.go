package lint

import (
	googlesql "github.com/goccy/go-googlesql"
	"github.com/kitagry/bqls/langserver/internal/source/file"
)

var SetOperationImplicitCast = &Analyzer{
	Name:           "set-operation-implicit-cast",
	Doc:            "reports set operation columns that are implicitly cast to FLOAT64 because another input has a FLOAT64 there",
	DefaultEnabled: true,
	Run:            runSetOperationImplicitCast,
}

func setOperationImplicitCastMessage(from string) string {
	return "this column is " + from + " here but FLOAT64 in another input of the set operation, so it is implicitly cast to FLOAT64, which cannot represent every " + from + " value exactly (e.g. INT64 values above 2^53); give the inputs the same type, or write an explicit CAST"
}

// Widening to NUMERIC or BIGNUMERIC is exact, so only casts to FLOAT64 are
// reported.
var lossyToFloat64 = map[string]bool{
	"INT64":      true,
	"NUMERIC":    true,
	"BIGNUMERIC": true,
}

// The analyzer coerces an input by wrapping its scan in a ProjectScan of
// casts, which carry no location, so the select list column is found by
// position in the parse tree.
func runSetOperationImplicitCast(pass *Pass) {
	astSetOps := file.ListAstNode[*googlesql.ASTSetOperation](pass.File.Node)
	for _, output := range pass.File.RNode {
		for _, scan := range file.ListResolvedAstNode[*googlesql.ResolvedSetOperationScan](output) {
			astSetOp, ok := outermostSetOperation(astSetOps, resolvedOffsets(scan))
			if !ok {
				continue
			}
			items, err := scan.InputItemList()
			if err != nil {
				continue
			}
			inputs := setOperationInputs(astSetOp)
			if len(inputs) != len(items) {
				continue
			}
			for i, item := range items {
				checkSetOperationItem(pass, item, inputs[i])
			}
		}
	}
}

// outermostSetOperation returns the set operation that at spans. A resolved
// set operation in parentheses, such as a CTE body, also spans them.
func outermostSetOperation(setOps []*googlesql.ASTSetOperation, at offsets) (*googlesql.ASTSetOperation, bool) {
	var found *googlesql.ASTSetOperation
	var span offsets
	for _, s := range setOps {
		o := astOffsets(s)
		if o.start < at.start || o.end > at.end {
			continue
		}
		if found == nil || o.end-o.start > span.end-span.start {
			found, span = s, o
		}
	}
	return found, found != nil
}

func checkSetOperationItem(pass *Pass, item *googlesql.ResolvedSetOperationItem, input googlesql.ASTQueryExpressionNode) {
	itemScan, err := item.Scan()
	if err != nil {
		return
	}
	project, ok := itemScan.(*googlesql.ResolvedProjectScan)
	if !ok {
		return
	}
	exprs, err := project.ExprList()
	if err != nil {
		return
	}
	outCols, err := item.OutputColumnList()
	if err != nil {
		return
	}
	for _, computed := range exprs {
		expr, err := computed.Expr()
		if err != nil {
			continue
		}
		inner, ok := implicitCastOperand(expr)
		if !ok {
			continue
		}
		if _, ok := inner.(*googlesql.ResolvedLiteral); ok {
			continue
		}
		from, ok := exprTypeName(inner)
		if !ok || !lossyToFloat64[from] {
			continue
		}
		if to, ok := exprTypeName(expr); !ok || to != "FLOAT64" {
			continue
		}
		id, ok := columnID(computed.Column())
		if !ok {
			continue
		}
		pos := -1
		for j, c := range outCols {
			if cid, err := c.ColumnId(); err == nil && cid == id {
				pos = j
			}
		}
		at, ok := selectColumnOffsets(input, pos)
		if !ok {
			at = astOffsets(input)
		}
		r, ok := pass.File.OffsetsToLspRange(at.start, at.end)
		if !ok {
			continue
		}
		pass.Report(Diagnostic{Range: r, Message: setOperationImplicitCastMessage(from), Severity: SeverityWarning})
	}
}

// selectColumnOffsets returns the location of the i-th select list column of
// input, when the parse tree shows it.
func selectColumnOffsets(input googlesql.ASTQueryExpressionNode, i int) (offsets, bool) {
	cols, ok := outputColumns(input)
	if !ok || i < 0 || i >= len(cols.names) {
		return offsets{}, false
	}
	col, err := cols.list.Columns(int32(i))
	if err != nil || col == nil {
		return offsets{}, false
	}
	return astOffsets(col), true
}
