package lint

import (
	"fmt"
	"regexp"
	"strings"
	"sync"

	googlesql "github.com/goccy/go-googlesql"
	"github.com/kitagry/bqls/langserver/internal/lsp"
)

var InsertColumnOrder = &Analyzer{
	Name:           "insert-column-order",
	Doc:            "reports INSERT queries whose columns are the target columns in a different order",
	DefaultEnabled: true,
	Run:            runInsertColumnOrder,
}

// As in union-column-order, the query's output names come from the parse tree,
// since the resolved AST keeps a column reference's source name even when the
// select list renames it. The resolved insert_column_list holds the target
// columns in order whether or not the statement lists them.
func runInsertColumnOrder(pass *Pass) {
	inserts := insertStatementsByTarget(pass)
	for _, output := range pass.File.RNode {
		stmt, err := output.ResolvedStatement()
		if err != nil || stmt == nil {
			continue
		}
		ins, ok := stmt.(*googlesql.ResolvedInsertStmt)
		if !ok {
			continue
		}
		target, err := ins.TableScan()
		if err != nil || target == nil {
			continue
		}
		astIns, ok := inserts[resolvedOffsets(target)]
		if !ok {
			continue
		}
		checkInsert(pass, ins, target, astIns)
	}
}

func checkInsert(pass *Pass, ins *googlesql.ResolvedInsertStmt, target *googlesql.ResolvedTableScan, astIns *googlesql.ASTInsertStatement) {
	query, err := astIns.Query()
	if err != nil || query == nil {
		return
	}
	cols, ok := outputColumns(query)
	if !ok {
		return
	}
	insertCols, err := ins.InsertColumnList()
	if err != nil || len(insertCols) != len(cols.names) {
		return
	}
	targetNames := make([]string, len(insertCols))
	for i, c := range insertCols {
		name, err := c.Name()
		if err != nil {
			return
		}
		targetNames[i] = name
	}
	want, got := misplaced(targetNames, cols.names)
	if len(got) == 0 {
		return
	}
	table, ok := tableName(target)
	if !ok {
		return
	}
	// Reading the target table in another order is how a relation is made
	// symmetric, as in `INSERT INTO edges SELECT dst, src FROM edges`.
	if readsTable(ins, table) {
		return
	}
	columnList, err := astIns.ColumnList()
	if err != nil {
		return
	}
	at := astOffsets(cols.list)
	r, ok := pass.File.OffsetsToLspRange(at.start, at.end)
	if !ok {
		return
	}
	msg := fmt.Sprintf("INSERT matches columns to the column list by position, not by name, but this query has %s where the column list has %s; reorder the columns", quoteNames(got), quoteNames(want))
	if columnList == nil {
		msg = fmt.Sprintf("INSERT without a column list matches columns to `%s` by position, not by name, but this query has %s where the table has %s; reorder the columns or add a column list", table, quoteNames(got), quoteNames(want))
	}
	d := Diagnostic{Range: r, Message: msg, Severity: SeverityWarning}
	if fix, ok := insertColumnOrderFix(pass, astIns, columnList, targetNames, cols.names); ok {
		d.SuggestedFixes = []SuggestedFix{fix}
	}
	pass.Report(d)
}

// insertColumnOrderFix makes the column list follow the query's names: an
// existing list is reordered, and otherwise one is added after the target.
func insertColumnOrderFix(pass *Pass, astIns *googlesql.ASTInsertStatement, columnList *googlesql.ASTColumnList, targets, names []string) (SuggestedFix, bool) {
	from, ok := reordering(targets, names)
	if !ok {
		return SuggestedFix{}, false
	}
	if columnList != nil {
		edits, ok := permutedEdits(pass.File, astOffsets(columnList), children(columnList), from)
		if !ok {
			return SuggestedFix{}, false
		}
		return SuggestedFix{Title: "Reorder the column list to match the query", Edits: edits}, true
	}
	path, err := astIns.GetTargetPathForNonNested()
	if err != nil || path == nil {
		return SuggestedFix{}, false
	}
	end := astOffsets(path).end
	r, ok := pass.File.OffsetsToLspRange(end, end)
	if !ok {
		return SuggestedFix{}, false
	}
	list := make([]string, len(from))
	for i, j := range from {
		list[i] = quoteIdentifier(targets[j])
	}
	return SuggestedFix{
		Title: "Add a column list matching the query",
		Edits: []lsp.TextEdit{{Range: r, NewText: " (" + strings.Join(list, ", ") + ")"}},
	}, true
}

var simpleIdentifier = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

var reservedKeywords = sync.OnceValues(googlesql.NewLanguageOptions)

// quoteIdentifier quotes name unless it can be written bare. It quotes when
// the reserved keywords cannot be looked up.
func quoteIdentifier(name string) string {
	if simpleIdentifier.MatchString(name) {
		if opts, err := reservedKeywords(); err == nil {
			if reserved, err := opts.IsReservedKeyword(strings.ToUpper(name)); err == nil && !reserved {
				return name
			}
		}
	}
	return "`" + strings.NewReplacer(`\`, `\\`, "`", "\\`").Replace(name) + "`"
}
