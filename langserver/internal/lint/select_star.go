package lint

import (
	"fmt"
	"strings"

	googlesql "github.com/goccy/go-googlesql"
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
		tables := fromTables(sel)
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
			for _, name := range names {
				if consumed[name] < opts.MinColumns {
					continue
				}
				pass.Report(Diagnostic{
					Range: r,
					Message: fmt.Sprintf("SELECT * reads %d columns of `%s`; list only the columns you need to reduce the bytes processed%s",
						consumed[name], name, estimate),
					Severity: SeverityWarning,
				})
			}
		}
	}
}

type tableRead struct {
	name string
	// consumed is how many of the scanned columns the statement uses.
	consumed int
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
			reads[resolvedOffsets(scan)] = tableRead{name: name, consumed: consumed}
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
// without descending into subqueries.
func fromTables(sel *googlesql.ASTSelect) []*googlesql.ASTTablePathExpression {
	from, err := sel.FromClause()
	if err != nil || from == nil {
		return nil
	}
	expr, err := from.TableExpression()
	if err != nil {
		return nil
	}
	var tables []*googlesql.ASTTablePathExpression
	var visit func(googlesql.ASTTableExpressionNode)
	visit = func(e googlesql.ASTTableExpressionNode) {
		switch e := e.(type) {
		case *googlesql.ASTTablePathExpression:
			tables = append(tables, e)
		case *googlesql.ASTJoin:
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
		}
	}
	visit(expr)
	return tables
}

// tableRangeName is the name a `t.*` refers to tp by: its alias, or else the
// last component of its path.
func tableRangeName(tp *googlesql.ASTTablePathExpression) (string, bool) {
	var id *googlesql.ASTIdentifier
	if alias, err := tp.Alias(); err == nil && alias != nil {
		id, err = alias.Identifier()
		if err != nil {
			return "", false
		}
	} else {
		path, err := tp.PathExpr()
		if err != nil || path == nil {
			return "", false
		}
		if id, err = path.LastName(); err != nil {
			return "", false
		}
	}
	if id == nil {
		return "", false
	}
	name, err := id.GetAsString()
	return name, err == nil
}
