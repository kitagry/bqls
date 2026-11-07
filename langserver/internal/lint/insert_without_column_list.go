package lint

import (
	"strings"

	googlesql "github.com/goccy/go-googlesql"
	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/kitagry/bqls/langserver/internal/source/file"
)

var InsertWithoutColumnList = &Analyzer{
	Name:           "insert-without-column-list",
	Doc:            "reports INSERT statements without a column list, whose values are matched to the table's columns by position",
	DefaultEnabled: true,
	Run:            runInsertWithoutColumnList,
}

func insertWithoutColumnListMessage(table string) string {
	return "INSERT without a column list writes the values to the columns of `" + table + "` by position, so a column added to or reordered in the table silently shifts them; list the target columns"
}

func runInsertWithoutColumnList(pass *Pass) {
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
		if columnList, err := astIns.ColumnList(); err != nil || columnList != nil {
			continue
		}
		reportInsertWithoutColumnList(pass, ins, target, astIns)
	}
}

func reportInsertWithoutColumnList(pass *Pass, ins *googlesql.ResolvedInsertStmt, target *googlesql.ResolvedTableScan, astIns *googlesql.ASTInsertStatement) {
	table, ok := tableName(target)
	if !ok {
		return
	}
	at := resolvedOffsets(target)
	r, ok := pass.File.OffsetsToLspRange(at.start, at.end)
	if !ok {
		return
	}
	d := Diagnostic{Range: r, Message: insertWithoutColumnListMessage(table), Severity: SeverityWarning}
	if fix, ok := insertColumnListFix(pass, ins, astIns, at.end); ok {
		d.SuggestedFixes = []SuggestedFix{fix}
	}
	pass.Report(d)
}

// insertColumnListFix adds the table's columns in schema order, which is
// what the statement writes today. The resolved insert_column_list holds
// them whether or not the statement lists them.
func insertColumnListFix(pass *Pass, ins *googlesql.ResolvedInsertStmt, astIns *googlesql.ASTInsertStatement, at int) (SuggestedFix, bool) {
	insertCols, err := ins.InsertColumnList()
	if err != nil || len(insertCols) == 0 {
		return SuggestedFix{}, false
	}
	names := make([]string, len(insertCols))
	for i, c := range insertCols {
		name, err := c.Name()
		if err != nil {
			return SuggestedFix{}, false
		}
		names[i] = name
	}
	// Listing the columns in schema order would state as intended the swap
	// that insert-column-order reports.
	if query, err := astIns.Query(); err == nil && query != nil {
		if cols, ok := outputColumns(query); ok && len(cols.names) == len(names) {
			if _, got := misplaced(names, cols.names); len(got) > 0 {
				return SuggestedFix{}, false
			}
		}
	}
	r, ok := pass.File.OffsetsToLspRange(at, at)
	if !ok {
		return SuggestedFix{}, false
	}
	quoted := make([]string, len(names))
	for i, n := range names {
		quoted[i] = quoteIdentifier(n)
	}
	return SuggestedFix{
		Title: "Add the column list",
		Edits: []lsp.TextEdit{{Range: r, NewText: " (" + strings.Join(quoted, ", ") + ")"}},
	}, true
}
