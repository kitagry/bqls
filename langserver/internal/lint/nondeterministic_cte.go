package lint

import (
	"fmt"
	"slices"

	googlesql "github.com/goccy/go-googlesql"
)

var NondeterministicCTE = &Analyzer{
	Name:           "nondeterministic-cte",
	Doc:            "reports RAND() / GENERATE_UUID() in a CTE that is evaluated more than once",
	DefaultEnabled: true,
	Run:            runNondeterministicCTE,
}

// nondeterministicFunctions return a new value on every call. CURRENT_DATE and
// the other current-time functions are fixed for the whole statement, so a CTE
// calling them yields the same value on every evaluation.
var nondeterministicFunctions = map[string]string{
	"rand":          "RAND",
	"generate_uuid": "GENERATE_UUID",
}

// BigQuery evaluates a non-recursive CTE once per reference, so a random call
// in it can disagree between references. To avoid noise, a call is reported
// only when two evaluations can observe it: it decides which rows the CTE
// returns, or it flows into an output column that two evaluations read.
// Column flows the walk does not model are treated as not observed.
func runNondeterministicCTE(pass *Pass) {
	for _, output := range pass.File.RNode {
		stmt, err := output.ResolvedStatement()
		if err != nil || stmt == nil {
			continue
		}
		g := &cteGraph{ctes: make(map[string]*cteNode), groupKeys: make(map[int32]bool)}
		g.walk(stmt, walkCtx{})
		evals := g.evaluations()
		consumed := g.consumed()
		for _, c := range g.calls {
			if c.in == nil || c.in.recursive {
				continue
			}
			if n := g.observations(c, evals, consumed); n >= 2 {
				reportNondeterministicCTE(pass, c, n)
			}
		}
	}
}

// cteGraph records one statement. Resolved WITH names are unique within a
// statement because the analyzer renames shadowed CTEs, and column ids are
// unique within a statement too.
type cteGraph struct {
	ctes  map[string]*cteNode
	refs  []cteRef
	calls []nondeterministicCall
	// uses are the places a column value is read.
	uses      []columnUse
	setOps    []setOp
	groupKeys map[int32]bool
	// outputs are columns the statement itself returns or writes.
	outputs []int32
}

type cteNode struct {
	name      string
	recursive bool
	cols      []int32
}

type cteRef struct {
	target string
	// in is the CTE whose body holds the reference, or nil for the main query.
	in   *cteNode
	cols []int32
}

type nondeterministicCall struct {
	fn       string
	call     *googlesql.ResolvedFunctionCall
	in       *cteNode
	computed []int32
}

// columnUse is a read of col. computed lists the enclosing computed columns,
// innermost last; when it is empty the read decides rows (a filter, a join
// condition, an ORDER BY with LIMIT, ...) rather than a value.
type columnUse struct {
	col      int32
	in       *cteNode
	computed []int32
}

type setOp struct {
	in    *cteNode
	cols  []int32
	items [][]int32
}

type walkCtx struct {
	in       *cteNode
	computed []int32
	// limited is set for the direct input of a LIMIT, where ORDER BY matters.
	limited bool
}

func (g *cteGraph) walk(n googlesql.ResolvedNode, ctx walkCtx) {
	limited := ctx.limited
	ctx.limited = false
	switch n := n.(type) {
	case *googlesql.ResolvedWithEntry:
		name, err := n.WithQueryName()
		if err != nil {
			return
		}
		sub, err := n.WithSubquery()
		if err != nil || sub == nil {
			return
		}
		_, recursive := sub.(*googlesql.ResolvedRecursiveScan)
		ctx = walkCtx{in: &cteNode{name: name, recursive: recursive, cols: orderedScanColumnIDs(sub)}}
		g.ctes[name] = ctx.in
	case *googlesql.ResolvedWithRefScan:
		if name, err := n.WithQueryName(); err == nil {
			g.refs = append(g.refs, cteRef{target: name, in: ctx.in, cols: orderedScanColumnIDs(n)})
		}
	case *googlesql.ResolvedFunctionCall:
		if name, ok := functionName(n); ok {
			if fn, ok := nondeterministicFunctions[name]; ok {
				g.calls = append(g.calls, nondeterministicCall{fn: fn, call: n, in: ctx.in, computed: ctx.computed})
			}
		}
	case googlesql.ResolvedComputedColumnBaseNode:
		if id, ok := columnID(n.Column()); ok {
			ctx.computed = append(slices.Clip(ctx.computed), id)
		}
	case *googlesql.ResolvedColumnRef:
		if id, ok := columnID(n.Column()); ok {
			g.uses = append(g.uses, columnUse{col: id, in: ctx.in, computed: ctx.computed})
		}
	case *googlesql.ResolvedOutputColumn:
		if id, ok := columnID(n.Column()); ok {
			g.outputs = append(g.outputs, id)
		}
	case *googlesql.ResolvedInsertStmt:
		if cols, err := n.QueryOutputColumnList(); err == nil {
			g.outputs = append(g.outputs, columnIDs(cols)...)
		}
	case *googlesql.ResolvedSubqueryExpr:
		g.walkSubqueryExpr(n, ctx)
	case *googlesql.ResolvedSetOperationScan:
		g.recordSetOp(n, ctx)
	case *googlesql.ResolvedAggregateScan:
		if keys, err := n.GroupByList(); err == nil {
			for _, k := range keys {
				if id, ok := columnID(k.Column()); ok {
					g.groupKeys[id] = true
				}
			}
		}
	case *googlesql.ResolvedLimitOffsetScan:
		if input, err := n.InputScan(); err == nil && input != nil {
			g.walk(input, walkCtx{in: ctx.in, computed: ctx.computed, limited: true})
		}
		for _, e := range []func() (googlesql.ResolvedExprNode, error){n.Limit, n.Offset} {
			if expr, err := e(); err == nil && expr != nil {
				g.walk(expr, ctx)
			}
		}
		return
	case *googlesql.ResolvedOrderByScan:
		// Inside a CTE, the order only reaches the result through a LIMIT.
		if ctx.in != nil && !limited {
			if input, err := n.InputScan(); err == nil && input != nil {
				g.walk(input, ctx)
			}
			return
		}
	}
	children, err := n.GetChildNodes()
	if err != nil {
		return
	}
	for _, child := range children {
		if child != nil {
			g.walk(child, ctx)
		}
	}
}

// walkSubqueryExpr records the subquery's output as read wherever its value
// is used; EXISTS only checks for rows.
func (g *cteGraph) walkSubqueryExpr(n *googlesql.ResolvedSubqueryExpr, ctx walkCtx) {
	typ, err := n.SubqueryType()
	if err != nil || typ == googlesql.ResolvedSubqueryExprEnums_SubqueryTypeExists {
		return
	}
	sub, err := n.Subquery()
	if err != nil || sub == nil {
		return
	}
	for _, id := range orderedScanColumnIDs(sub) {
		g.uses = append(g.uses, columnUse{col: id, in: ctx.in, computed: ctx.computed})
	}
}

func (g *cteGraph) recordSetOp(n *googlesql.ResolvedSetOperationScan, ctx walkCtx) {
	items, err := n.InputItemList()
	if err != nil {
		return
	}
	op := setOp{in: ctx.in, cols: orderedScanColumnIDs(n)}
	for _, item := range items {
		cols, err := item.OutputColumnList()
		if err != nil || len(cols) != len(op.cols) {
			return
		}
		op.items = append(op.items, columnIDs(cols))
	}
	g.setOps = append(g.setOps, op)
}

// evaluations counts how many times each CTE body runs: once per reference,
// times the evaluations of the CTE holding that reference. A recursive CTE is
// materialized, so it counts as running once.
func (g *cteGraph) evaluations() map[*cteNode]int {
	evals := make(map[*cteNode]int, len(g.ctes))
	var count func(c *cteNode) int
	count = func(c *cteNode) int {
		if n, ok := evals[c]; ok {
			return n
		}
		// Guards against a cycle; non-recursive WITH cannot form one.
		evals[c] = 0
		n := 0
		for _, r := range g.refs {
			if g.ctes[r.target] == c {
				n += g.weight(r, count)
			}
		}
		evals[c] = n
		return n
	}
	for _, c := range g.ctes {
		count(c)
	}
	return evals
}

func (g *cteGraph) weight(r cteRef, count func(*cteNode) int) int {
	if r.in == nil || r.in.recursive {
		return 1
	}
	return count(r.in)
}

// consumed returns the columns whose values reach the statement's result,
// a row-deciding expression, or a subquery value.
func (g *cteGraph) consumed() map[int32]bool {
	consumed := make(map[int32]bool)
	for _, id := range g.outputs {
		consumed[id] = true
	}
	for _, u := range g.uses {
		if len(u.computed) == 0 {
			consumed[u.col] = true
		}
	}
	for changed := true; changed; {
		changed = false
		mark := func(from, to int32) {
			if consumed[from] && !consumed[to] {
				consumed[to] = true
				changed = true
			}
		}
		for _, u := range g.uses {
			if len(u.computed) > 0 {
				mark(u.computed[len(u.computed)-1], u.col)
			}
		}
		for _, op := range g.setOps {
			for _, item := range op.items {
				for j, id := range item {
					mark(op.cols[j], id)
				}
			}
		}
		for _, r := range g.refs {
			c := g.ctes[r.target]
			if c == nil || len(c.cols) != len(r.cols) {
				continue
			}
			for j, id := range r.cols {
				mark(id, c.cols[j])
			}
		}
	}
	return consumed
}

// observations returns how many evaluations of the call's CTE observe its
// value, or 0 when the value never reaches anything they read.
func (g *cteGraph) observations(c nondeterministicCall, evals map[*cteNode]int, consumed map[int32]bool) int {
	tainted := make(map[int32]bool)
	decidesRows := taint(c.computed, tainted)
	seen := make([]bool, len(g.uses))
	for changed := true; changed; {
		changed = false
		for i, u := range g.uses {
			if !seen[i] && u.in == c.in && tainted[u.col] {
				seen[i] = true
				decidesRows = taint(u.computed, tainted) || decidesRows
				changed = true
			}
		}
		for _, op := range g.setOps {
			if op.in != c.in {
				continue
			}
			for _, item := range op.items {
				for j, id := range item {
					if tainted[id] && !tainted[op.cols[j]] {
						tainted[op.cols[j]] = true
						changed = true
					}
				}
			}
		}
	}
	for id := range tainted {
		decidesRows = decidesRows || g.groupKeys[id]
	}
	if decidesRows {
		return evals[c.in]
	}

	count := func(n *cteNode) int { return evals[n] }
	best := 0
	for j, id := range c.in.cols {
		if !tainted[id] {
			continue
		}
		n := 0
		for _, r := range g.refs {
			if g.ctes[r.target] == c.in && j < len(r.cols) && consumed[r.cols[j]] {
				n += g.weight(r, count)
			}
		}
		best = max(best, n)
	}
	return best
}

// taint marks the computed columns and reports whether there were none, i.e.
// the value decides rows instead.
func taint(computed []int32, tainted map[int32]bool) bool {
	for _, id := range computed {
		tainted[id] = true
	}
	return len(computed) == 0
}

func reportNondeterministicCTE(pass *Pass, c nondeterministicCall, n int) {
	loc, err := c.call.GetParseLocationRangeOrNULL()
	if err != nil || loc == nil {
		return
	}
	r, ok := pass.File.ToLspRange(loc)
	if !ok {
		return
	}
	pass.Report(Diagnostic{
		Range:   r,
		Message: fmt.Sprintf("%s() in CTE `%s` returns a different value for each of its %d evaluations, because BigQuery evaluates a non-recursive CTE once per reference", c.fn, c.in.name, n),
	})
}

// orderedScanColumnIDs keeps column order, which WITH references and set
// operations match by position.
func orderedScanColumnIDs(scan googlesql.ResolvedScanNode) []int32 {
	cols, ok := scanColumns(scan)
	if !ok {
		return nil
	}
	return columnIDs(cols)
}

func columnID(c *googlesql.ResolvedColumn, err error) (int32, bool) {
	if err != nil || c == nil {
		return 0, false
	}
	id, err := c.ColumnId()
	return id, err == nil
}
