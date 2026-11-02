package lint

import (
	"fmt"
	"strings"

	googlesql "github.com/goccy/go-googlesql"
	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/kitagry/bqls/langserver/internal/source/file"
	"github.com/kitagry/bqls/langserver/internal/source/helper"
)

// SelectStar is off by default because whether reading every column is
// wasteful depends on intent (ad-hoc exploration, a deliberate full-row dump),
// and so does a sensible min_columns.
var SelectStar = &Analyzer{
	Name:           "select-star",
	Doc:            "reports SELECT * that reads at least min_columns columns of a table",
	DefaultEnabled: false,
	NewOptions:     func() any { return &SelectStarOptions{MinColumns: 50} },
	Run:            runSelectStar,
}

type SelectStarOptions struct {
	MinColumns int `json:"min_columns"`
}

func (o *SelectStarOptions) Validate() error {
	if o.MinColumns < 1 {
		return fmt.Errorf("min_columns must be positive, got %d", o.MinColumns)
	}
	return nil
}

// A false positive here pushes users to disable the rule, so only a bare `*`
// or `t.*` that expands a table read in the same SELECT is reported, and only
// by the number of its columns the statement actually consumes: BigQuery
// prunes the rest, e.g. in `SELECT a FROM (SELECT * FROM t)`. `* EXCEPT` and
// `* REPLACE` show the author has already looked at the columns, and a `*`
// over a CTE or subquery is left to the `*` inside it that reads the table.
// Only queries are checked, since DML, CREATE TABLE AS SELECT and EXPORT DATA
// usually copy whole rows on purpose.
func runSelectStar(pass *Pass) {
	opts := pass.Options.(*SelectStarOptions)
	reads := consumedTableReads(pass.File)
	if len(reads) == 0 {
		return
	}

	var estimate string
	if pass.DryRun != nil && pass.DryRun.Statistics != nil {
		estimate = fmt.Sprintf("; the dry-run estimates the file processes %s", helper.ByteSize(pass.DryRun.Statistics.TotalBytesProcessed))
	}
	for _, sel := range file.ListAstNode[*googlesql.ASTSelect](pass.File.Node) {
		tables, onlyTables := fromTables(sel)
		for _, star := range selectStars(sel) {
			span := astOffsets(star.node)
			r, ok := pass.File.OffsetsToLspRange(span.start, span.end)
			if !ok {
				continue
			}
			// A self join expands the same table more than once, which is
			// still a single finding about it.
			var names []string
			consumed := make(map[string]int)
			for _, tp := range tables {
				if !star.expands(tp) {
					continue
				}
				path, err := tp.PathExpr()
				if err != nil || path == nil {
					continue
				}
				read, ok := reads[astOffsets(path)]
				if !ok {
					continue
				}
				if _, seen := consumed[read.name]; !seen {
					names = append(names, read.name)
				}
				consumed[read.name] = max(consumed[read.name], read.consumed)
			}
			fixed := false
			for _, name := range names {
				if consumed[name] < opts.MinColumns {
					continue
				}
				d := Diagnostic{
					Range: r,
					Message: fmt.Sprintf("SELECT * reads %d columns of `%s`; list only the columns you need to reduce the bytes processed%s",
						consumed[name], name, estimate),
					Severity: SeverityWarning,
				}
				// Every finding of a `*` would carry the same edit, which
				// lint --fix would then skip as overlapping.
				if !fixed {
					if fix, ok := selectStarFix(pass.File, sel, star, tables, onlyTables, reads); ok {
						d.SuggestedFixes = []SuggestedFix{fix}
						fixed = true
					}
				}
				pass.Report(d)
			}
		}
	}
}

type tableRead struct {
	name string
	// consumed is how many of the scanned columns the statement uses.
	consumed int
	// columns are what `*` expands the table to, or nil when unknown.
	columns []string
}

// consumedTableReads returns the table scans of query statements, keyed by
// the location of their table path.
func consumedTableReads(f file.ParsedFile) map[offsets]tableRead {
	reads := make(map[offsets]tableRead)
	for _, output := range f.RNode {
		stmt, err := output.ResolvedStatement()
		if err != nil || stmt == nil {
			continue
		}
		if _, ok := stmt.(*googlesql.ResolvedQueryStmt); !ok {
			continue
		}
		flow, ok := analyzeColumnFlow(stmt)
		if !ok {
			continue
		}
		for _, scan := range flow.scans {
			table, err := scan.Table()
			if err != nil || table == nil {
				continue
			}
			name, err := table.Name()
			if err != nil {
				continue
			}
			cols, err := scan.ColumnList()
			if err != nil {
				continue
			}
			consumed := 0
			for _, id := range columnIDs(cols) {
				if flow.reaches(id, flow.used) {
					consumed++
				}
			}
			reads[resolvedOffsets(scan)] = tableRead{name: name, consumed: consumed, columns: starColumns(table)}
		}
	}
	return reads
}

type selectStar struct {
	node googlesql.ASTNode
	// qualifier is the `t` of `t.*`, or empty for a bare `*`.
	qualifier string
}

func (s selectStar) expands(tp *googlesql.ASTTablePathExpression) bool {
	if s.qualifier == "" {
		return true
	}
	name, ok := tableRangeName(tp)
	return ok && strings.EqualFold(name, s.qualifier)
}

// selectStars returns the bare `*` and `t.*` items of sel's select list,
// leaving out those with EXCEPT or REPLACE.
func selectStars(sel *googlesql.ASTSelect) []selectStar {
	list, err := sel.SelectList()
	if err != nil || list == nil {
		return nil
	}
	n, err := list.NumChildren()
	if err != nil {
		return nil
	}
	var stars []selectStar
	for i := range n {
		col, err := list.Columns(int32(i))
		if err != nil || col == nil {
			continue
		}
		expr, err := col.Expression()
		if err != nil {
			continue
		}
		switch e := expr.(type) {
		case *googlesql.ASTStar:
			stars = append(stars, selectStar{node: e})
		case *googlesql.ASTDotStar:
			q, err := e.Expr()
			if err != nil {
				continue
			}
			path, ok := q.(*googlesql.ASTPathExpression)
			if !ok {
				continue
			}
			if n, err := path.NumNames(); err != nil || n != 1 {
				continue
			}
			id, err := path.FirstName()
			if err != nil || id == nil {
				continue
			}
			name, err := id.GetAsString()
			if err != nil {
				continue
			}
			stars = append(stars, selectStar{node: e, qualifier: name})
		}
	}
	return stars
}

// fromTables returns the table paths joined in sel's FROM clause itself,
// without descending into subqueries. onlyTables is false when the FROM clause
// also has other items, such as UNNEST or subqueries, or a USING join, so that
// a bare `*` produces more than the columns of the tables side by side.
func fromTables(sel *googlesql.ASTSelect) (tables []*googlesql.ASTTablePathExpression, onlyTables bool) {
	from, err := sel.FromClause()
	if err != nil || from == nil {
		return nil, false
	}
	expr, err := from.TableExpression()
	if err != nil {
		return nil, false
	}
	onlyTables = true
	var visit func(googlesql.ASTTableExpressionNode)
	visit = func(e googlesql.ASTTableExpressionNode) {
		switch e := e.(type) {
		case *googlesql.ASTTablePathExpression:
			tables = append(tables, e)
			if path, err := e.PathExpr(); err != nil || path == nil {
				onlyTables = false
			}
		case *googlesql.ASTJoin:
			if using, err := e.UsingClause(); err != nil || using != nil {
				onlyTables = false
			}
			if natural, err := e.Natural(); err != nil || natural {
				onlyTables = false
			}
			if lhs, err := e.Lhs(); err == nil {
				visit(lhs)
			}
			if rhs, err := e.Rhs(); err == nil {
				visit(rhs)
			}
		case *googlesql.ASTParenthesizedJoin:
			if j, err := e.Join(); err == nil && j != nil {
				visit(j)
			}
		default:
			onlyTables = false
		}
	}
	visit(expr)
	return tables, onlyTables
}

// tableRangeName is the name a `t.*` refers to tp by: its alias, or else the
// last component of its path.
func tableRangeName(tp *googlesql.ASTTablePathExpression) (string, bool) {
	id, ok := tableRangeIdentifier(tp)
	if !ok {
		return "", false
	}
	name, err := id.GetAsString()
	return name, err == nil
}

func tableRangeIdentifier(tp *googlesql.ASTTablePathExpression) (*googlesql.ASTIdentifier, bool) {
	var id *googlesql.ASTIdentifier
	if alias, err := tp.Alias(); err == nil && alias != nil {
		id, err = alias.Identifier()
		if err != nil {
			return nil, false
		}
	} else {
		path, err := tp.PathExpr()
		if err != nil || path == nil {
			return nil, false
		}
		if id, err = path.LastName(); err != nil {
			return nil, false
		}
	}
	return id, id != nil
}

// starColumns returns the columns `*` expands table to: all of them in
// order, except pseudo-columns such as _PARTITIONTIME.
func starColumns(table googlesql.TableNode) []string {
	if value, err := table.IsValueTable(); err != nil || value {
		return nil
	}
	n, err := table.NumColumns()
	if err != nil {
		return nil
	}
	var cols []string
	for i := range n {
		col, err := table.GetColumn(i)
		if err != nil || col == nil {
			return nil
		}
		if pseudo, err := col.IsPseudoColumn(); err != nil || pseudo {
			continue
		}
		name, err := col.Name()
		if err != nil {
			return nil
		}
		cols = append(cols, name)
	}
	return cols
}

// selectStarFix replaces star with the columns it expands to. It gives up whenever the explicit list could
// mean something else than the `*`.
func selectStarFix(f file.ParsedFile, sel *googlesql.ASTSelect, star selectStar, tables []*googlesql.ASTTablePathExpression, onlyTables bool, reads map[offsets]tableRead) (SuggestedFix, bool) {
	items, ok := starItems(star, tables, onlyTables, reads)
	if !ok {
		return SuggestedFix{}, false
	}

	src := f.FixedSrc
	span := astOffsets(star.node)
	loc, err := star.node.GetParseLocationRange()
	if err != nil || !inBounds(src, span) {
		return SuggestedFix{}, false
	}
	// A fix-up may have rewritten the star, and a comment inside `t.*`
	// would be deleted with it.
	text := src[span.start:span.end]
	if written, ok := f.ExtractSQL(loc); !ok || written != text || containsComment(text) {
		return SuggestedFix{}, false
	}

	start, newText := starLayout(src, astOffsets(sel), span, items)
	r, ok := f.OffsetsToLspRange(start, span.end)
	if !ok {
		return SuggestedFix{}, false
	}
	return SuggestedFix{
		Title: fmt.Sprintf("Expand %s into explicit columns", text),
		Edits: []lsp.TextEdit{{Range: r, NewText: newText}},
	}, true
}

// starLayout returns where the edit replacing star starts and its text, laid
// out like the select list: inline in a one-line SELECT, one item per line
// when the star starts its line, and otherwise one per line one level deeper
// when the star ends its line in a multi-line SELECT.
func starLayout(src string, sel, star offsets, items []string) (int, string) {
	lineStart := strings.LastIndex(src[:star.start], "\n") + 1
	before := src[lineStart:star.start]
	indent := before[:len(before)-len(strings.TrimLeft(before, " \t"))]
	if indent == before {
		return star.start, strings.Join(items, ",\n"+indent)
	}
	rest, _, _ := strings.Cut(src[star.end:], "\n")
	rest = strings.TrimSpace(rest)
	multiline := inBounds(src, sel) && strings.Contains(src[sel.start:sel.end], "\n")
	if !multiline || (rest != "" && rest != ",") {
		return star.start, strings.Join(items, ", ")
	}
	inner := indent + "  "
	if strings.Contains(indent, "\t") {
		inner = indent + "\t"
	}
	return lineStart + len(strings.TrimRight(before, " \t")), "\n" + inner + strings.Join(items, ",\n"+inner)
}

// starItems returns the select items star expands to. A bare `*` over
// several tables qualifies every column, and is left alone when two columns
// share a name. A bare `*` over one table needs no qualifier unless a column
// is named like the table, since range variables win over columns.
func starItems(star selectStar, tables []*googlesql.ASTTablePathExpression, onlyTables bool, reads map[offsets]tableRead) ([]string, bool) {
	type source struct {
		rangeName string
		columns   []string
	}
	var sources []source
	for _, tp := range tables {
		if !star.expands(tp) {
			continue
		}
		path, err := tp.PathExpr()
		if err != nil || path == nil {
			return nil, false
		}
		read, ok := reads[astOffsets(path)]
		if !ok || read.columns == nil {
			return nil, false
		}
		name, ok := tableRangeName(tp)
		if !ok {
			return nil, false
		}
		sources = append(sources, source{rangeName: name, columns: read.columns})
	}

	var items []string
	switch {
	case star.qualifier != "":
		if len(sources) != 1 {
			return nil, false
		}
		for _, c := range sources[0].columns {
			items = append(items, quoteIdentifier(star.qualifier)+"."+quoteIdentifier(c))
		}
	case !onlyTables || len(sources) == 0:
		return nil, false
	case len(sources) == 1:
		for _, c := range sources[0].columns {
			if strings.EqualFold(c, sources[0].rangeName) {
				return nil, false
			}
			items = append(items, quoteIdentifier(c))
		}
	default:
		seen := make(map[string]bool)
		for _, s := range sources {
			// The implicit alias of a quoted `p.d.t` path differs between
			// BigQuery and the analyzer.
			if strings.Contains(s.rangeName, ".") {
				return nil, false
			}
			for _, c := range s.columns {
				if seen[strings.ToLower(c)] {
					return nil, false
				}
				seen[strings.ToLower(c)] = true
				items = append(items, quoteIdentifier(s.rangeName)+"."+quoteIdentifier(c))
			}
		}
	}
	return items, len(items) > 0
}
