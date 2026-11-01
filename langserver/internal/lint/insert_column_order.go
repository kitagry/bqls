package lint

import (
	"fmt"
	"slices"

	googlesql "github.com/goccy/go-googlesql"
	"github.com/kitagry/bqls/langserver/internal/source/file"
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
	inserts := make(map[offsets]*googlesql.ASTInsertStatement)
	for _, s := range file.ListAstNode[*googlesql.ASTInsertStatement](pass.File.Node) {
		path, err := s.GetTargetPathForNonNested()
		if err != nil || path == nil {
			continue
		}
		inserts[astOffsets(path)] = s
	}

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
	if q, err := ins.Query(); err == nil && slices.Contains(scannedSources(q), "table:"+table) {
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
	pass.Report(Diagnostic{Range: r, Message: msg, Severity: SeverityWarning})
}
