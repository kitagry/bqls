package lint

import (
	"strings"

	googlesql "github.com/goccy/go-googlesql"
	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/kitagry/bqls/langserver/internal/source/file"
)

var WindowDefaultFrame = &Analyzer{
	Name:           "window-default-frame",
	Doc:            "reports LAST_VALUE / NTH_VALUE whose window has ORDER BY but no frame, so the default frame ends at the current row instead of the end of the partition",
	DefaultEnabled: true,
	Run:            runWindowDefaultFrame,
}

const (
	fullPartitionFrame           = "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING"
	lastValueDefaultFrameMessage = "LAST_VALUE has no window frame, so it uses the default RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW and returns the value of the current row (or of its last peer), not of the last row of the partition; add " + fullPartitionFrame + " to the window"
	nthValueDefaultFrameMessage  = "NTH_VALUE has no window frame, so it uses the default RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW and returns NULL for the rows before the Nth row of the partition; add " + fullPartitionFrame + " to the window"
)

// FIRST_VALUE is left out: the default frame starts at the start of the
// partition, so it already returns the first row.
func runWindowDefaultFrame(pass *Pass) {
	specs := windowSpecsByCall(pass.File.Node)
	for _, output := range pass.File.RNode {
		for _, scan := range file.ListResolvedAstNode[*googlesql.ResolvedAnalyticScan](output) {
			groups, err := scan.FunctionGroupList()
			if err != nil {
				continue
			}
			for _, g := range groups {
				checkWindowDefaultFrame(pass, specs, g)
			}
		}
	}
}

func checkWindowDefaultFrame(pass *Pass, specs map[int]*googlesql.ASTWindowSpecification, g *googlesql.ResolvedAnalyticFunctionGroup) {
	order, err := g.OrderBy()
	if err != nil || order == nil || orderedByPartitionKeys(g, order) {
		return
	}
	calls, err := g.AnalyticFunctionList()
	if err != nil {
		return
	}
	for _, c := range calls {
		_, expr, ok := computed(c)
		if !ok {
			continue
		}
		call, ok := expr.(*googlesql.ResolvedAnalyticFunctionCall)
		if !ok || !hasDefaultFrame(call) {
			continue
		}
		name, ok := functionName(call)
		if !ok {
			continue
		}
		var message string
		switch {
		// LAST_VALUE(x IGNORE NULLS) over the default frame is the usual way
		// to carry the last non-NULL value forward.
		case name == "last_value" && !ignoresNulls(call):
			message = lastValueDefaultFrameMessage
		// NTH_VALUE(x, 1) is FIRST_VALUE.
		case name == "nth_value" && !nthIsFirst(call):
			message = nthValueDefaultFrameMessage
		default:
			continue
		}
		at := resolvedOffsets(call)
		r, ok := pass.File.OffsetsToLspRange(at.start, at.end)
		if !ok {
			continue
		}
		var fixes []SuggestedFix
		if edit, ok := fullPartitionFrameEdit(pass, specs[at.start]); ok {
			fixes = []SuggestedFix{{Title: "Add " + fullPartitionFrame, Edits: []lsp.TextEdit{edit}}}
		}
		pass.Report(Diagnostic{
			Range:          r,
			Message:        message,
			Severity:       SeverityWarning,
			SuggestedFixes: fixes,
		})
	}
}

// hasDefaultFrame reports whether the frame was filled in by the analyzer:
// a written frame, also one taken from a named window, has a parse location.
func hasDefaultFrame(call *googlesql.ResolvedAnalyticFunctionCall) bool {
	frame, err := call.WindowFrame()
	if err != nil || frame == nil {
		return false
	}
	loc, err := frame.GetParseLocationRangeOrNULL()
	return err == nil && loc == nil
}

func ignoresNulls(call *googlesql.ResolvedAnalyticFunctionCall) bool {
	m, err := call.NullHandlingModifier()
	return err != nil || m == googlesql.ResolvedNonScalarFunctionCallBaseEnums_NullHandlingModifierIgnoreNulls
}

func nthIsFirst(call *googlesql.ResolvedAnalyticFunctionCall) bool {
	args, err := call.ArgumentList()
	if err != nil || len(args) < 2 {
		return true
	}
	lit, ok := args[1].(*googlesql.ResolvedLiteral)
	if !ok {
		return false
	}
	v, err := lit.Value()
	if err != nil || v == nil {
		return true
	}
	n, err := v.ToInt64()
	return err != nil || n <= 1
}

// orderedByPartitionKeys reports whether every row of a partition is a peer
// of every other, which makes the default frame the whole partition.
func orderedByPartitionKeys(g *googlesql.ResolvedAnalyticFunctionGroup, order *googlesql.ResolvedWindowOrdering) bool {
	items, err := order.OrderByItemList()
	if err != nil {
		return false
	}
	keys := partitionKeys(g, nil)
	for _, item := range items {
		ref, err := item.ColumnRef()
		if err != nil || ref == nil {
			return false
		}
		if id, ok := columnRefID(ref); !ok || !keys[id] {
			return false
		}
	}
	return true
}

// windowSpecsByCall maps the start offset of each analytic function call to
// its OVER clause.
func windowSpecsByCall(script *googlesql.ASTScript) map[int]*googlesql.ASTWindowSpecification {
	specs := make(map[int]*googlesql.ASTWindowSpecification)
	for _, a := range file.ListAstNode[*googlesql.ASTAnalyticFunctionCall](script) {
		fn, err := a.Function()
		if err != nil || fn == nil {
			continue
		}
		spec, err := a.WindowSpec()
		if err != nil || spec == nil {
			continue
		}
		specs[astOffsets(fn).start] = spec
	}
	return specs
}

// fullPartitionFrameEdit adds the frame after the last clause of the OVER
// clause, so a comment before its closing parenthesis stays where it is. A
// bare window name gets parentheses: `OVER w` becomes `OVER (w ROWS ...)`.
func fullPartitionFrameEdit(pass *Pass, spec *googlesql.ASTWindowSpecification) (lsp.TextEdit, bool) {
	if spec == nil {
		return lsp.TextEdit{}, false
	}
	at := astOffsets(spec)
	if at.start < 0 || at.end > len(pass.File.FixedSrc) {
		return lsp.TextEdit{}, false
	}
	src := pass.File.FixedSrc[at.start:at.end]
	if !strings.HasPrefix(src, "(") {
		r, ok := pass.File.OffsetsToLspRange(at.start, at.end)
		return lsp.TextEdit{Range: r, NewText: "(" + src + " " + fullPartitionFrame + ")"}, ok
	}
	end := -1
	if name, err := spec.BaseWindowName(); err == nil && name != nil {
		end = max(end, astOffsets(name).end)
	}
	if p, err := spec.PartitionBy(); err == nil && p != nil {
		end = max(end, astOffsets(p).end)
	}
	if o, err := spec.OrderBy(); err == nil && o != nil {
		end = max(end, astOffsets(o).end)
	}
	if end < 0 {
		return lsp.TextEdit{}, false
	}
	r, ok := pass.File.OffsetsToLspRange(end, end)
	return lsp.TextEdit{Range: r, NewText: " " + fullPartitionFrame}, ok
}
