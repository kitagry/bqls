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
// clause stays valid. It gives up whenever a user comment would be deleted or
// left describing a different entry.
func unusedCTEDeletion(src string, clause offsets, entries []cteEntry, i int) (offsets, bool) {
	e := entries[i]
	if !inBounds(src, clause) || !inBounds(src, e.span) || hasTrailingComment(src, e.span.end) {
		return offsets{}, false
	}
	if i == 0 && (!onlyWithKeywords(src[clause.start:e.span.start]) || hasCommentLineAbove(src, e.span.start)) {
		return offsets{}, false
	}

	switch {
	case len(entries) == 1:
		if strings.TrimSpace(src[e.span.end:clause.end]) != "" {
			return offsets{}, false
		}
		del := offsets{clause.start, clause.end}
		for del.end < len(src) && isSQLSpace(src[del.end]) {
			del.end++
		}
		return del, true
	case i == 0:
		del := offsets{e.span.start, entries[1].span.start}
		return del, isSeparator(src, e.span.end, del.end)
	default:
		del := offsets{entries[i-1].span.end, e.span.end}
		return del, isSeparator(src, del.start, e.span.start)
	}
}

func inBounds(src string, o offsets) bool {
	return o.start >= 0 && o.start <= o.end && o.end <= len(src)
}

func onlyWithKeywords(s string) bool {
	for _, f := range strings.Fields(s) {
		if !strings.EqualFold(f, "WITH") && !strings.EqualFold(f, "RECURSIVE") {
			return false
		}
	}
	return true
}

// hasTrailingComment reports whether a comment follows pos (after an optional
// comma) on the same line, where it most likely describes the entry ending at pos.
func hasTrailingComment(src string, pos int) bool {
	rest, _, _ := strings.Cut(src[pos:], "\n")
	rest = strings.TrimSpace(rest)
	rest = strings.TrimSpace(strings.TrimPrefix(rest, ","))
	return startsWithComment(rest)
}

// hasCommentLineAbove reports whether pos starts its line and the previous
// line is a comment, which most likely describes the entry starting at pos.
func hasCommentLineAbove(src string, pos int) bool {
	lineStart := strings.LastIndex(src[:pos], "\n") + 1
	if lineStart == 0 || strings.TrimSpace(src[lineStart:pos]) != "" {
		return false
	}
	prevStart := strings.LastIndex(src[:lineStart-1], "\n") + 1
	prev := strings.TrimSpace(src[prevStart : lineStart-1])
	return startsWithComment(prev) || strings.HasSuffix(prev, "*/")
}

func startsWithComment(s string) bool {
	return strings.HasPrefix(s, "--") || strings.HasPrefix(s, "#") || strings.HasPrefix(s, "/*")
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
