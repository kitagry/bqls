package lint

import (
	"regexp"
	"slices"
	"strings"

	googlesql "github.com/goccy/go-googlesql"
	"github.com/kitagry/bqls/langserver/internal/lsp"
)

var ImplicitOuterReference = &Analyzer{
	Name:           "implicit-outer-reference",
	Doc:            "reports unqualified column names in a subquery that refer to a column of the outer query",
	DefaultEnabled: true,
	Run:            runImplicitOuterReference,
}

func implicitOuterReferenceMessage(name string) string {
	return "`" + name + "` refers to a column of the outer query, since no table of this subquery has it; qualify it with the outer table if that is intended, otherwise the subquery may compare the outer row with itself (e.g. `x IN (SELECT x FROM t)` is TRUE for every non-NULL x)"
}

// bareColumnName matches a column written without a qualifier.
var bareColumnName = regexp.MustCompile("^(?:[A-Za-z_][A-Za-z0-9_]*|`[^`.]+`)$")

// An unqualified name resolves to the innermost scope that has it, so a
// correlated one means the subquery's own tables lack the column: often a
// typo or a column of the wrong table, which silently turns the subquery
// into a comparison of the outer row with itself.
func runImplicitOuterReference(pass *Pass) {
	for _, output := range pass.File.RNode {
		stmt, err := output.ResolvedStatement()
		if err != nil || stmt == nil {
			continue
		}
		w := &outerRefWalker{src: pass.File.FixedSrc, tables: make(map[int32]outerTable), blocks: []int{0}}
		w.walk(stmt)
		reported := make(map[offsets]bool)
		for _, r := range w.refs {
			at := resolvedOffsets(r.ref)
			if reported[at] {
				continue
			}
			reported[at] = true
			w.report(pass, r)
		}
	}
}

// subqueryScope is a subquery expression together with what a qualifier
// written inside it could resolve to.
type subqueryScope struct {
	// outer holds the IDs of the outer columns the subquery references.
	outer map[int32]bool
	// hasTable is false for a subquery that only reads UNNEST of outer
	// arrays or has no FROM, where an outer name is the whole point.
	hasTable bool
	// columns holds the lower-cased names of the columns its scans produce.
	columns map[string]bool
	tables  []googlesql.TableNode
	text    string
}

type outerRef struct {
	ref *googlesql.ResolvedColumnRef
	// scopes are the subqueries enclosing ref, outermost first.
	scopes []*subqueryScope
	// blocks are the query blocks enclosing ref.
	blocks []int
}

type outerTable struct {
	name string
	// block is the query block that reads the table, where its name is
	// visible.
	block int
}

type outerRefWalker struct {
	src    string
	tables map[int32]outerTable
	refs   []outerRef
	scopes []*subqueryScope
	// blocks numbers each ProjectScan visited, since node wrappers are not
	// stable enough to compare.
	blocks    []int
	numBlocks int
}

func (w *outerRefWalker) walk(n googlesql.ResolvedNode) {
	if isNilNode(n) {
		return
	}
	var scope *subqueryScope
	if len(w.scopes) > 0 {
		scope = w.scopes[len(w.scopes)-1]
	}
	if scan, ok := n.(googlesql.ResolvedScanNode); ok && scope != nil {
		if cols, ok := scanColumns(scan); ok {
			for _, c := range cols {
				if name, err := c.Name(); err == nil {
					scope.columns[strings.ToLower(name)] = true
				}
			}
		}
	}
	switch n := n.(type) {
	case *googlesql.ResolvedProjectScan:
		w.numBlocks++
		w.blocks = append(w.blocks, w.numBlocks)
		w.walkChildren(n)
		w.blocks = w.blocks[:len(w.blocks)-1]
		return
	case *googlesql.ResolvedTableScan:
		if scope != nil {
			scope.hasTable = true
			if table, err := n.Table(); err == nil && !isNilNode(table) {
				scope.tables = append(scope.tables, table)
			}
		}
		if name, ok := rangeName(n); ok {
			if cols, err := n.ColumnList(); err == nil {
				for _, id := range columnIDs(cols) {
					w.tables[id] = outerTable{name: name, block: w.blocks[len(w.blocks)-1]}
				}
			}
		}
	case *googlesql.ResolvedWithRefScan:
		if scope != nil {
			scope.hasTable = true
		}
	case *googlesql.ResolvedColumnRef:
		if correlated, err := n.IsCorrelated(); err == nil && correlated && len(w.scopes) > 0 {
			w.refs = append(w.refs, outerRef{ref: n, scopes: slices.Clone(w.scopes), blocks: slices.Clone(w.blocks)})
		}
	case *googlesql.ResolvedSubqueryExpr:
		// The IN operand belongs to the enclosing query, and the parameter
		// list only repeats the outer columns referenced inside.
		if in, err := n.InExpr(); err == nil {
			w.walk(in)
		}
		query, err := n.Subquery()
		if err != nil {
			return
		}
		s := &subqueryScope{outer: make(map[int32]bool), columns: make(map[string]bool)}
		if params, err := n.ParameterList(); err == nil {
			for _, p := range params {
				if id, ok := columnRefID(p); ok {
					s.outer[id] = true
				}
			}
		}
		if at := resolvedOffsets(n); inBounds(w.src, at) {
			s.text = w.src[at.start:at.end]
		}
		w.scopes = append(w.scopes, s)
		w.walk(query)
		w.scopes = w.scopes[:len(w.scopes)-1]
		return
	}
	w.walkChildren(n)
}

func (w *outerRefWalker) walkChildren(n googlesql.ResolvedNode) {
	children, err := n.GetChildNodes()
	if err != nil {
		return
	}
	for _, c := range children {
		w.walk(c)
	}
}

func (w *outerRefWalker) report(pass *Pass, r outerRef) {
	if !r.scopes[len(r.scopes)-1].hasTable {
		return
	}
	at := resolvedOffsets(r.ref)
	if !inBounds(pass.File.FixedSrc, at) {
		return
	}
	text := pass.File.FixedSrc[at.start:at.end]
	if !bareColumnName.MatchString(text) {
		return
	}
	rng, ok := pass.File.OffsetsToLspRange(at.start, at.end)
	if !ok {
		return
	}
	d := Diagnostic{Range: rng, Message: implicitOuterReferenceMessage(strings.Trim(text, "`")), Severity: SeverityWarning}
	if q, ok := w.qualifier(r); ok {
		q = quoteIdentifier(q)
		d.SuggestedFixes = []SuggestedFix{{
			Title: "Qualify with " + q,
			Edits: []lsp.TextEdit{{Range: lsp.Range{Start: rng.Start, End: rng.Start}, NewText: q + "."}},
		}}
	}
	pass.Report(d)
}

// qualifier returns the name of the outer table r reads, when writing it
// before r keeps the meaning.
func (w *outerRefWalker) qualifier(r outerRef) (string, bool) {
	id, ok := columnRefID(r.ref)
	if !ok {
		return "", false
	}
	table, ok := w.tables[id]
	// A table read inside a derived table is not visible outside it, even
	// though the derived table passes its columns through unchanged.
	if !ok || !slices.Contains(r.blocks, table.block) {
		return "", false
	}
	// Every subquery the column is outer to sees its own names first, so
	// none of them may have a range variable or column of that name.
	word := regexp.MustCompile("(?i)(?:^|[^A-Za-z0-9_])" + regexp.QuoteMeta(table.name) + "(?:$|[^A-Za-z0-9_])")
	for _, s := range r.scopes {
		if !s.outer[id] {
			continue
		}
		if s.text == "" || word.MatchString(s.text) || s.columns[strings.ToLower(table.name)] {
			return "", false
		}
		for _, t := range s.tables {
			if c, err := t.FindColumnByName(table.name); err == nil && !isNilNode(c) {
				return "", false
			}
		}
	}
	return table.name, true
}

func rangeName(scan *googlesql.ResolvedTableScan) (string, bool) {
	if alias, err := scan.Alias(); err == nil && alias != "" {
		return alias, true
	}
	table, ok := tableName(scan)
	if !ok {
		return "", false
	}
	name := table[strings.LastIndex(table, ".")+1:]
	return name, simpleIdentifier.MatchString(name)
}
