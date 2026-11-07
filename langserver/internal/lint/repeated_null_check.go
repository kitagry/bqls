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

func repeatedNullCheckMessage(column, table, expr string, negated bool) string {
	if negated {
		return fmt.Sprintf("`%s` is a REPEATED column of `%s`, which BigQuery stores as an empty array, never NULL, so IS NOT NULL is always TRUE; to check for a non-empty array, use ARRAY_LENGTH(%s) > 0", column, table, expr)
	}
	return fmt.Sprintf("`%s` is a REPEATED column of `%s`, which BigQuery stores as an empty array, never NULL, so IS NULL is always FALSE; to check for an empty array, use ARRAY_LENGTH(%s) = 0", column, table, expr)
}

// BigQuery writes a NULL array to a REPEATED column as an empty array, so a
// table's REPEATED column read unchanged is never NULL. It can still be NULL
// on the optional side of an outer join, or as a field of a NULL RECORD,
// which is a field access rather than a column reference.
func runRepeatedNullCheck(pass *Pass) {
	for _, output := range pass.File.RNode {
		var checks []repeatedNullCheckCandidate
		for _, call := range file.ListResolvedAstNode[*googlesql.ResolvedFunctionCall](output) {
			if c, ok := nullCheck(call); ok {
				checks = append(checks, c)
			}
		}
		if len(checks) == 0 {
			continue
		}
		stmt, err := output.ResolvedStatement()
		if err != nil || stmt == nil {
			continue
		}
		lineage := traceColumnLineage(stmt)
		nullExtended := outerJoinNullableColumns(output)
		for _, c := range checks {
			checkRepeatedNullCheck(pass, lineage, nullExtended, c)
		}
	}
}

type repeatedNullCheckCandidate struct {
	// call is where the check is located: the $is_null call, or the $not
	// around it for IS NOT NULL.
	call    *googlesql.ResolvedFunctionCall
	ref     *googlesql.ResolvedColumnRef
	negated bool
}

// nullCheck matches `x IS NULL`, or `x IS NOT NULL`, which is located at the
// enclosing $not, where x is a column reference.
func nullCheck(call *googlesql.ResolvedFunctionCall) (repeatedNullCheckCandidate, bool) {
	name, ok := functionName(call)
	if !ok {
		return repeatedNullCheckCandidate{}, false
	}
	isNull, negated := call, false
	switch name {
	case "$is_null":
	case "$not":
		inner, ok := singleArgument(call).(*googlesql.ResolvedFunctionCall)
		if !ok {
			return repeatedNullCheckCandidate{}, false
		}
		if n, ok := functionName(inner); !ok || n != "$is_null" {
			return repeatedNullCheckCandidate{}, false
		}
		// `NOT (x IS NULL)` keeps its own location on the inner call and
		// is reported there.
		if loc := resolvedOffsets(inner); loc.start >= 0 {
			return repeatedNullCheckCandidate{}, false
		}
		isNull, negated = inner, true
	default:
		return repeatedNullCheckCandidate{}, false
	}
	ref, ok := singleArgument(isNull).(*googlesql.ResolvedColumnRef)
	if !ok {
		return repeatedNullCheckCandidate{}, false
	}
	return repeatedNullCheckCandidate{call: call, ref: ref, negated: negated}, true
}

func singleArgument(call *googlesql.ResolvedFunctionCall) googlesql.ResolvedExprNode {
	args, err := call.ArgumentList()
	if err != nil || len(args) != 1 {
		return nil
	}
	return args[0]
}

func checkRepeatedNullCheck(pass *Pass, lineage *columnLineage, nullExtended map[int32]bool, c repeatedNullCheckCandidate) {
	loc := resolvedOffsets(c.call)
	if loc.start < 0 {
		return
	}
	col, err := c.ref.Column()
	if err != nil || col == nil {
		return
	}
	id, err := col.ColumnId()
	if err != nil {
		return
	}
	// Unlike lineage.source, conditions do not matter: a condition cannot
	// make an array NULL, but an outer join can.
	src, ok := lineage.follow(id, func(id int32) bool { return nullExtended[id] })
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
	at := resolvedOffsets(c.ref)
	if !inBounds(pass.File.FixedSrc, at) {
		return
	}
	r, ok := pass.File.OffsetsToLspRange(loc.start, loc.end)
	if !ok {
		return
	}
	expr := pass.File.FixedSrc[at.start:at.end]
	replacement := "ARRAY_LENGTH(" + expr + ") = 0"
	if c.negated {
		replacement = "ARRAY_LENGTH(" + expr + ") > 0"
	}
	pass.Report(Diagnostic{
		Range:    r,
		Message:  repeatedNullCheckMessage(src.name, tableName, expr, c.negated),
		Severity: SeverityWarning,
		SuggestedFixes: []SuggestedFix{{
			Title:         "Replace with " + replacement,
			Edits:         []lsp.TextEdit{{Range: r, NewText: replacement}},
			ChangesResult: true,
		}},
	})
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
