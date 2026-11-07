package lint

import (
	"fmt"
	"strings"

	bq "cloud.google.com/go/bigquery"
	googlesql "github.com/goccy/go-googlesql"
	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/kitagry/bqls/langserver/internal/source/file"
)

var RepeatedNullCheck = &Analyzer{
	Name:           "repeated-null-check",
	Doc:            "reports IS [NOT] NULL on a REPEATED table column, which BigQuery never stores as NULL",
	DefaultEnabled: true,
	Run:            runRepeatedNullCheck,
}

func repeatedNullCheckMessage(column, table string, negated bool) string {
	if negated {
		return fmt.Sprintf("`%s` is a REPEATED column of `%s`, which BigQuery stores as an empty array, never NULL, so IS NOT NULL is always TRUE; to check for a non-empty array, use ARRAY_LENGTH(%s) > 0", column, table, column)
	}
	return fmt.Sprintf("`%s` is a REPEATED column of `%s`, which BigQuery stores as an empty array, never NULL, so IS NULL is always FALSE; to check for an empty array, use ARRAY_LENGTH(%s) = 0", column, table, column)
}

// BigQuery writes a NULL array to a REPEATED column as an empty array, so a
// table's REPEATED column read unchanged is never NULL. It can still be NULL
// on the optional side of an outer join, or as a field of a NULL RECORD,
// which is a field access rather than a column reference.
func runRepeatedNullCheck(pass *Pass) {
	for _, output := range pass.File.RNode {
		stmt, err := output.ResolvedStatement()
		if err != nil || stmt == nil {
			continue
		}
		lineage := traceColumnLineage(stmt)
		nullExtended := outerJoinNullableColumns(output)
		for _, call := range file.ListResolvedAstNode[*googlesql.ResolvedFunctionCall](output) {
			check, negated, ok := nullCheck(call)
			if !ok {
				continue
			}
			checkRepeatedNullCheck(pass, lineage, nullExtended, check, negated)
		}
	}
}

// nullCheck returns the $is_null call of `x IS NULL`, or of `x IS NOT NULL`,
// which is located at the enclosing $not.
func nullCheck(call *googlesql.ResolvedFunctionCall) (located *googlesql.ResolvedFunctionCall, negated, ok bool) {
	name, ok := functionName(call)
	if !ok {
		return nil, false, false
	}
	switch name {
	case "$is_null":
		return call, false, true
	case "$not":
		args, err := call.ArgumentList()
		if err != nil || len(args) != 1 {
			return nil, false, false
		}
		inner, ok := args[0].(*googlesql.ResolvedFunctionCall)
		if !ok {
			return nil, false, false
		}
		if n, ok := functionName(inner); !ok || n != "$is_null" {
			return nil, false, false
		}
		// `NOT (x IS NULL)` keeps its own location on the inner call and
		// is reported there.
		if loc := resolvedOffsets(inner); loc.start >= 0 {
			return nil, false, false
		}
		return call, true, true
	}
	return nil, false, false
}

func checkRepeatedNullCheck(pass *Pass, lineage *columnLineage, nullExtended map[int32]bool, call *googlesql.ResolvedFunctionCall, negated bool) {
	loc := resolvedOffsets(call)
	if loc.start < 0 {
		return
	}
	isNull := call
	if negated {
		args, err := call.ArgumentList()
		if err != nil || len(args) != 1 {
			return
		}
		isNull = args[0].(*googlesql.ResolvedFunctionCall)
	}
	args, err := isNull.ArgumentList()
	if err != nil || len(args) != 1 {
		return
	}
	ref, ok := args[0].(*googlesql.ResolvedColumnRef)
	if !ok {
		return
	}
	col, err := ref.Column()
	if err != nil || col == nil {
		return
	}
	id, err := col.ColumnId()
	if err != nil {
		return
	}
	src, ok := lineage.unchangedSource(id, nullExtended)
	if !ok {
		return
	}
	table, err := src.scan.Table()
	if err != nil || table == nil {
		return
	}
	tableName, err := table.Name()
	if err != nil {
		return
	}
	meta, err := pass.LookupTable(pass.Context, tableName)
	// A view computes its columns, so they may be NULL.
	if err != nil || meta == nil || meta.Type != bq.RegularTable || !repeatedField(meta.Schema, src.name) {
		return
	}
	at := resolvedOffsets(ref)
	if !inBounds(pass.File.FixedSrc, at) {
		return
	}
	r, ok := pass.File.OffsetsToLspRange(loc.start, loc.end)
	if !ok {
		return
	}
	replacement := "ARRAY_LENGTH(" + pass.File.FixedSrc[at.start:at.end] + ") = 0"
	if negated {
		replacement = "ARRAY_LENGTH(" + pass.File.FixedSrc[at.start:at.end] + ") > 0"
	}
	pass.Report(Diagnostic{
		Range:    r,
		Message:  repeatedNullCheckMessage(src.name, tableName, negated),
		Severity: SeverityWarning,
		SuggestedFixes: []SuggestedFix{{
			Title:         "Replace with " + replacement,
			Edits:         []lsp.TextEdit{{Range: r, NewText: replacement}},
			ChangesResult: true,
		}},
	})
}

// unchangedSource follows id through unchanged copies to a table column,
// failing when the value is computed on the way or may be NULL-extended by
// an outer join. Unlike source, conditions do not matter: a condition
// cannot make an array NULL.
func (l *columnLineage) unchangedSource(id int32, nullExtended map[int32]bool) (tableColumn, bool) {
	seen := make(map[int32]bool)
	for !seen[id] {
		seen[id] = true
		if nullExtended[id] {
			return tableColumn{}, false
		}
		if t, ok := l.tables[id]; ok {
			return t, true
		}
		next, ok := l.copied[id]
		if !ok {
			return tableColumn{}, false
		}
		id = next
	}
	return tableColumn{}, false
}

// outerJoinNullableColumns returns the columns of the sides of outer joins
// that are padded with NULLs for unmatched rows.
func outerJoinNullableColumns(output *googlesql.AnalyzerOutput) map[int32]bool {
	ids := make(map[int32]bool)
	add := func(scan googlesql.ResolvedScanNode, err error) {
		if err != nil || isNilNode(scan) {
			return
		}
		cols, err := scan.MutableColumnList()
		if err != nil {
			return
		}
		for _, id := range columnIDs(cols) {
			ids[id] = true
		}
	}
	for _, join := range file.ListResolvedAstNode[*googlesql.ResolvedJoinScan](output) {
		typ, err := join.JoinType()
		if err != nil {
			continue
		}
		switch typ {
		case googlesql.ResolvedJoinScanEnums_JoinTypeLeft:
			add(join.RightScan())
		case googlesql.ResolvedJoinScanEnums_JoinTypeRight:
			add(join.LeftScan())
		case googlesql.ResolvedJoinScanEnums_JoinTypeFull:
			add(join.LeftScan())
			add(join.RightScan())
		}
	}
	return ids
}

func repeatedField(schema bq.Schema, name string) bool {
	for _, f := range schema {
		if strings.EqualFold(f.Name, name) {
			return f.Repeated
		}
	}
	return false
}
