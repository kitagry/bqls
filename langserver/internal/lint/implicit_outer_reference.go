package lint

import (
	"regexp"
	"strings"

	googlesql "github.com/goccy/go-googlesql"
	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/kitagry/bqls/langserver/internal/source/file"
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
		rangeNames := tableRangeNames(output)
		// innerNames holds, for each correlated reference, the range
		// variable names of every subquery it is in.
		innerNames := make(map[offsets]map[string]bool)
		var refs []*googlesql.ResolvedColumnRef
		for _, sub := range file.ListResolvedAstNode[*googlesql.ResolvedSubqueryExpr](output) {
			names := make(map[string]bool)
			var inner []*googlesql.ResolvedColumnRef
			walkResolved(sub, func(n googlesql.ResolvedNode) {
				switch n := n.(type) {
				case *googlesql.ResolvedTableScan:
					if name, ok := rangeName(n); ok {
						names[strings.ToLower(name)] = true
					}
				case *googlesql.ResolvedWithRefScan:
					if name, err := n.WithQueryName(); err == nil {
						names[strings.ToLower(name)] = true
					}
				case *googlesql.ResolvedColumnRef:
					if correlated, err := n.IsCorrelated(); err == nil && correlated {
						inner = append(inner, n)
					}
				}
			})
			for _, ref := range inner {
				at := resolvedOffsets(ref)
				if innerNames[at] == nil {
					innerNames[at] = make(map[string]bool)
					refs = append(refs, ref)
				}
				for n := range names {
					innerNames[at][n] = true
				}
			}
		}
		for _, ref := range refs {
			reportImplicitOuterReference(pass, ref, rangeNames, innerNames[resolvedOffsets(ref)])
		}
	}
}

func reportImplicitOuterReference(pass *Pass, ref *googlesql.ResolvedColumnRef, rangeNames map[int32]string, inner map[string]bool) {
	at := resolvedOffsets(ref)
	if !inBounds(pass.File.FixedSrc, at) {
		return
	}
	text := pass.File.FixedSrc[at.start:at.end]
	if !bareColumnName.MatchString(text) {
		return
	}
	r, ok := pass.File.OffsetsToLspRange(at.start, at.end)
	if !ok {
		return
	}
	d := Diagnostic{Range: r, Message: implicitOuterReferenceMessage(strings.Trim(text, "`")), Severity: SeverityWarning}
	// The qualifier must not name a range variable of the subquery, which
	// would capture it.
	if id, ok := columnRefID(ref); ok {
		if q, ok := rangeNames[id]; ok && !inner[strings.ToLower(q)] {
			d.SuggestedFixes = []SuggestedFix{{
				Title: "Qualify with " + q,
				Edits: []lsp.TextEdit{{Range: lsp.Range{Start: r.Start, End: r.Start}, NewText: q + "."}},
			}}
		}
	}
	pass.Report(d)
}

// tableRangeNames maps the columns of each table scan to the name it is
// referred to by: its alias, or the last part of the table name.
func tableRangeNames(output *googlesql.AnalyzerOutput) map[int32]string {
	names := make(map[int32]string)
	for _, scan := range file.ListResolvedAstNode[*googlesql.ResolvedTableScan](output) {
		name, ok := rangeName(scan)
		if !ok {
			continue
		}
		cols, err := scan.ColumnList()
		if err != nil {
			continue
		}
		for _, id := range columnIDs(cols) {
			names[id] = name
		}
	}
	return names
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

func walkResolved(n googlesql.ResolvedNode, fn func(googlesql.ResolvedNode)) {
	if isNilNode(n) {
		return
	}
	fn(n)
	children, err := n.GetChildNodes()
	if err != nil {
		return
	}
	for _, c := range children {
		walkResolved(c, fn)
	}
}
