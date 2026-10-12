package lint

import (
	"fmt"
	"strings"

	googlesql "github.com/goccy/go-googlesql"
	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/kitagry/bqls/langserver/internal/source/file"
)

var UnusedCTE = &Analyzer{
	Name:           "unused-cte",
	Doc:            "reports WITH entries that are never referenced",
	DefaultEnabled: true,
	Run:            runUnusedCTE,
}

type offsets struct{ start, end int }

// Usage comes from the resolved AST because the analyzer has already bound
// every table reference: shadowed CTEs get unique names (e.g. `a_1`), and a
// recursive self reference is a ResolvedRecursiveRefScan rather than a
// ResolvedWithRefScan. Resolved WITH entries carry no location, so each one is
// paired with its parser-AST entry through the enclosing query's location.
func runUnusedCTE(pass *Pass) {
	type scope struct {
		ws *googlesql.ResolvedWithScan
		// used is per statement because resolved names are only unique within one.
		used map[string]bool
	}
	withScans := make(map[offsets]scope)
	for _, output := range pass.File.RNode {
		used := make(map[string]bool)
		for _, ref := range file.ListResolvedAstNode[*googlesql.ResolvedWithRefScan](output) {
			if name, err := ref.WithQueryName(); err == nil {
				used[name] = true
			}
		}
		for _, ws := range file.ListResolvedAstNode[*googlesql.ResolvedWithScan](output) {
			loc, err := ws.GetParseLocationRangeOrNULL()
			if err != nil || loc == nil {
				continue
			}
			withScans[offsets{file.ParseLocStart(loc), file.ParseLocEnd(loc)}] = scope{ws: ws, used: used}
		}
	}

	for _, clause := range file.ListAstNode[*googlesql.ASTWithClause](pass.File.Node) {
		parent, err := clause.Parent()
		if err != nil || parent == nil {
			continue
		}
		sc, ok := withScans[astOffsets(parent)]
		if !ok {
			continue
		}
		entries := withClauseEntries(clause)
		names, ok := resolvedEntryNames(sc.ws, entries)
		if !ok {
			continue
		}
		for i := range entries {
			if !sc.used[names[i]] {
				reportUnusedCTE(pass, clause, entries, i)
			}
		}
	}
}

type cteEntry struct {
	name string
	// ident is the CTE name; span is `name AS (...)`.
	ident, span offsets
}

func withClauseEntries(clause *googlesql.ASTWithClause) []cteEntry {
	var entries []cteEntry
	n, err := clause.NumChildren()
	if err != nil {
		return nil
	}
	for i := int32(0); i < n; i++ {
		entry, err := clause.Entries(i)
		if err != nil || entry == nil {
			return nil
		}
		aq, err := entry.AliasedQuery()
		if err != nil || aq == nil {
			return nil
		}
		alias, err := aq.Alias()
		if err != nil || alias == nil {
			return nil
		}
		name, err := alias.GetAsString()
		if err != nil {
			return nil
		}
		entries = append(entries, cteEntry{name: name, ident: astOffsets(alias), span: astOffsets(aq)})
	}
	return entries
}

// resolvedEntryNames returns the resolved name of each parser-AST entry.
// Non-recursive WITH keeps source order, possibly renaming shadowed entries to
// `<name>_<n>`. Recursive WITH reorders entries by dependency, so they are
// matched by name instead, and the clause is skipped when a rename makes that
// ambiguous.
func resolvedEntryNames(ws *googlesql.ResolvedWithScan, entries []cteEntry) ([]string, bool) {
	resolved, err := ws.WithEntryList()
	if err != nil || len(resolved) != len(entries) {
		return nil, false
	}
	recursive, err := ws.Recursive()
	if err != nil {
		return nil, false
	}

	names := make([]string, len(entries))
	if !recursive {
		for i, r := range resolved {
			name, err := r.WithQueryName()
			if err != nil || (name != entries[i].name && !strings.HasPrefix(name, entries[i].name+"_")) {
				return nil, false
			}
			names[i] = name
		}
		return names, true
	}

	resolvedNames := make(map[string]bool, len(resolved))
	for _, r := range resolved {
		name, err := r.WithQueryName()
		if err != nil {
			return nil, false
		}
		resolvedNames[name] = true
	}
	for i, e := range entries {
		if !resolvedNames[e.name] {
			return nil, false
		}
		names[i] = e.name
	}
	return names, true
}

func reportUnusedCTE(pass *Pass, clause *googlesql.ASTWithClause, entries []cteEntry, i int) {
	e := entries[i]
	r, ok := pass.File.OffsetsToLspRange(e.ident.start, e.ident.end)
	if !ok {
		return
	}
	d := Diagnostic{
		Range:    r,
		Message:  fmt.Sprintf("CTE `%s` is defined but never used", e.name),
		Severity: SeverityInformation,
	}
	if del, ok := unusedCTEDeletion(pass.File.FixedSrc, astOffsets(clause), entries, i); ok {
		if r, ok := pass.File.OffsetsToLspRange(del.start, del.end); ok {
			d.SuggestedFixes = []SuggestedFix{{
				Title: fmt.Sprintf("Remove unused CTE `%s`", e.name),
				Edits: []lsp.TextEdit{{Range: r, NewText: ""}},
			}}
		}
	}
	pass.Report(d)
}

// unusedCTEDeletion returns the span to delete so that the remaining WITH
// clause stays valid. It gives up when the deleted separator holds anything
// but whitespace and the comma, since that is most likely a user comment.
func unusedCTEDeletion(src string, clause offsets, entries []cteEntry, i int) (offsets, bool) {
	var del offsets
	switch {
	case len(entries) == 1:
		del = offsets{clause.start, clause.end}
		for del.end < len(src) && isSQLSpace(src[del.end]) {
			del.end++
		}
		return del, clause.start >= 0 && clause.end <= len(src)
	case i == 0:
		del = offsets{entries[0].span.start, entries[1].span.start}
		return del, isSeparator(src, entries[0].span.end, del.end)
	default:
		del = offsets{entries[i-1].span.end, entries[i].span.end}
		return del, isSeparator(src, del.start, entries[i].span.start)
	}
}

func isSeparator(src string, start, end int) bool {
	if start < 0 || start > end || end > len(src) {
		return false
	}
	return strings.TrimSpace(src[start:end]) == ","
}

func isSQLSpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\r' || c == '\n'
}

func astOffsets(n googlesql.ASTNode) offsets {
	loc, err := n.GetParseLocationRange()
	if err != nil || loc == nil {
		return offsets{-1, -1}
	}
	return offsets{file.ParseLocStart(loc), file.ParseLocEnd(loc)}
}
