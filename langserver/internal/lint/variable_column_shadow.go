package lint

import (
	"fmt"
	"strings"

	googlesql "github.com/goccy/go-googlesql"
	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/kitagry/bqls/langserver/internal/source/file"
)

var VariableColumnShadow = &Analyzer{
	Name:           "variable-column-shadow",
	Doc:            "reports a column compared with itself because its bare name was meant as a same-named DECLARE variable",
	DefaultEnabled: true,
	Run:            runVariableColumnShadow,
}

var selfComparisonFunctions = map[string]bool{
	"$equal":                true,
	"$not_equal":            true,
	"$less":                 true,
	"$less_or_equal":        true,
	"$greater":              true,
	"$greater_or_equal":     true,
	"$is_distinct_from":     true,
	"$is_not_distinct_from": true,
}

// A column takes precedence over a variable of the same name, so only a
// comparison of a column with itself is reported: it is meaningless whatever
// the author intended, whereas any other use of the name may well mean the
// column.
func runVariableColumnShadow(pass *Pass) {
	scopes := variableScopes(pass.File.Node)
	if len(scopes) == 0 {
		return
	}
	for _, st := range pass.File.Statements {
		if st.Output == nil {
			continue
		}
		vars := variablesInScope(scopes, astOffsets(st.Stmt))
		if len(vars) == 0 {
			continue
		}
		var tables map[offsets][]rangeTable
		for _, call := range file.ListResolvedAstNode[*googlesql.ResolvedFunctionCall](st.Output) {
			if tables == nil {
				tables = visibleTables(pass, st.Output)
			}
			checkVariableColumnShadow(pass, call, vars, tables)
		}
	}
}

type variableScope struct {
	name string
	// span is where the variable is visible: from its declaration to the end
	// of the enclosing block or script.
	span offsets
}

func variableScopes(script *googlesql.ASTScript) []variableScope {
	var scopes []variableScope
	for _, decl := range file.ListAstNode[*googlesql.ASTVariableDeclaration](script) {
		block, ok := enclosingBlock(decl)
		if !ok {
			continue
		}
		span := offsets{astOffsets(decl).end, astOffsets(block).end}
		for _, v := range declaredVariables(decl) {
			scopes = append(scopes, variableScope{name: strings.ToLower(v.name), span: span})
		}
	}
	return scopes
}

// enclosingBlock returns the node that owns the statement list holding decl.
func enclosingBlock(decl *googlesql.ASTVariableDeclaration) (googlesql.ASTNode, bool) {
	list, err := decl.Parent()
	if err != nil || list == nil {
		return nil, false
	}
	block, err := list.Parent()
	if err != nil || block == nil {
		return nil, false
	}
	return block, true
}

func variablesInScope(scopes []variableScope, stmt offsets) map[string]bool {
	vars := make(map[string]bool)
	for _, s := range scopes {
		if s.span.start <= stmt.start && stmt.end <= s.span.end {
			vars[s.name] = true
		}
	}
	return vars
}

func checkVariableColumnShadow(pass *Pass, call *googlesql.ResolvedFunctionCall, vars map[string]bool, tables map[offsets][]rangeTable) {
	args, ok := selfComparisonOperands(call)
	if !ok {
		return
	}
	operands := shadowingOperands(pass, args, vars)
	if len(operands) == 0 {
		return
	}
	ident := operands[0].ident
	r := operands[0].rng
	d := Diagnostic{
		Range: r,
		Message: fmt.Sprintf("`%[1]s` refers to the column, not the variable `%[1]s`, because a column takes precedence over a variable of the same name; this compares the column with itself. Rename the variable (e.g. `v_%[1]s`) to use it here",
			ident),
		Severity: SeverityWarning,
	}
	if qualifier, ok := columnQualifier(args[0], tables); ok {
		edits := make([]lsp.TextEdit, 0, len(operands))
		// Edits follow the source order of the operands.
		for i := len(operands) - 1; i >= 0; i-- {
			at := operands[i].rng.Start
			edits = append(edits, lsp.TextEdit{Range: lsp.Range{Start: at, End: at}, NewText: qualifier + "."})
		}
		d.SuggestedFixes = []SuggestedFix{{
			Title: fmt.Sprintf("Qualify `%[1]s` as the column `%[2]s.%[1]s`", ident, qualifier),
			Edits: edits,
		}}
	}
	pass.Report(d)
}

// rangeTable is a table read in a FROM clause, by the range variable it is
// referred to as.
type rangeTable struct {
	// name is written as in the source, so that quoting is kept.
	name    string
	columns map[int32]bool
}

// visibleTables maps the location of each column reference to the tables
// whose range variables are visible to it. A table read through a subquery,
// a CTE or a pipe operator that redefines the columns is left out, since its
// range variable is not visible there.
func visibleTables(pass *Pass, output *googlesql.AnalyzerOutput) map[offsets][]rangeTable {
	names := make(map[offsets]string)
	for _, tp := range file.ListAstNode[*googlesql.ASTTablePathExpression](pass.File.Node) {
		path, err := tp.PathExpr()
		if err != nil || path == nil {
			continue
		}
		if name, ok := writtenRangeName(pass, tp); ok {
			names[astOffsets(path)] = name
		}
	}

	result := make(map[offsets][]rangeTable)
	addRefs := func(expr googlesql.ResolvedNode, tables []rangeTable) {
		if expr == nil || len(tables) == 0 {
			return
		}
		// The walk is pre-order, so a reference in a nested subquery is later
		// overwritten by the subquery's own tables.
		file.WalkResolved(expr, func(n googlesql.ResolvedNode) error { //nolint
			if ref, ok := n.(*googlesql.ResolvedColumnRef); ok {
				result[resolvedOffsets(ref)] = tables
			}
			return nil
		})
	}
	stmt, err := output.ResolvedStatement()
	if err != nil || stmt == nil {
		return result
	}
	file.WalkResolved(stmt, func(n googlesql.ResolvedNode) error { //nolint
		switch n := n.(type) {
		case *googlesql.ResolvedFilterScan:
			input, err := n.InputScan()
			expr, err2 := n.FilterExpr()
			if err == nil && err2 == nil {
				addRefs(expr, fromClauseTables(pass, names, input))
			}
		case *googlesql.ResolvedJoinScan:
			expr, err := n.JoinExpr()
			if err == nil {
				addRefs(expr, fromClauseTables(pass, names, n))
			}
		case *googlesql.ResolvedProjectScan:
			input, err := n.InputScan()
			exprs, err2 := n.ExprList()
			if err != nil || err2 != nil {
				break
			}
			tables := fromClauseTables(pass, names, input)
			for _, c := range exprs {
				addRefs(c, tables)
			}
		}
		return nil
	})
	return result
}

// fromClauseTables returns the tables read directly in the FROM clause that scan
// evaluates.
func fromClauseTables(pass *Pass, names map[offsets]string, scan googlesql.ResolvedScanNode) []rangeTable {
	switch s := scan.(type) {
	case *googlesql.ResolvedTableScan:
		name, ok := names[tablePathOffsets(pass.File.Node, resolvedOffsets(s))]
		if !ok {
			return nil
		}
		cols, err := s.ColumnList()
		if err != nil {
			return nil
		}
		t := rangeTable{name: name, columns: make(map[int32]bool, len(cols))}
		for _, c := range cols {
			if id, err := c.ColumnId(); err == nil {
				t.columns[id] = true
			}
		}
		return []rangeTable{t}
	case *googlesql.ResolvedJoinScan:
		left, err := s.LeftScan()
		right, err2 := s.RightScan()
		if err != nil || err2 != nil {
			return nil
		}
		return append(fromClauseTables(pass, names, left), fromClauseTables(pass, names, right)...)
	case *googlesql.ResolvedFilterScan:
		input, err := s.InputScan()
		if err != nil {
			return nil
		}
		return fromClauseTables(pass, names, input)
	case *googlesql.ResolvedArrayScan:
		input, err := s.InputScan()
		if err != nil || input == nil {
			return nil
		}
		return fromClauseTables(pass, names, input)
	}
	return nil
}

// writtenRangeName returns the range variable of tp as written. An implicit
// alias that holds a dot (the last component of the backquoted `p.d.t`) is
// rejected, because BigQuery and the analyzer disagree on it.
func writtenRangeName(pass *Pass, tp *googlesql.ASTTablePathExpression) (string, bool) {
	id, ok := tableRangeIdentifier(tp)
	if !ok {
		return "", false
	}
	if name, err := id.GetAsString(); err != nil || strings.Contains(name, ".") {
		return "", false
	}
	loc, err := id.GetParseLocationRange()
	if err != nil {
		return "", false
	}
	return pass.File.ExtractSQL(loc)
}

// columnQualifier returns the range variable that refers to the table of the
// column ref reads. A correlated reference is rejected, since the range
// variable could be shadowed by one of the inner query.
func columnQualifier(expr googlesql.ResolvedExprNode, tables map[offsets][]rangeTable) (string, bool) {
	ref, ok := expr.(*googlesql.ResolvedColumnRef)
	if !ok {
		return "", false
	}
	if correlated, err := ref.IsCorrelated(); err != nil || correlated {
		return "", false
	}
	id, ok := columnRefID(ref)
	if !ok {
		return "", false
	}
	for _, t := range tables[resolvedOffsets(ref)] {
		if t.columns[id] {
			return t.name, true
		}
	}
	return "", false
}

// selfComparisonOperands returns the operands of a comparison of a column
// with itself.
func selfComparisonOperands(call *googlesql.ResolvedFunctionCall) ([]googlesql.ResolvedExprNode, bool) {
	name, ok := functionName(call)
	if !ok || !selfComparisonFunctions[name] {
		return nil, false
	}
	args, err := call.ArgumentList()
	if err != nil || len(args) != 2 {
		return nil, false
	}
	left, ok := columnRefID(args[0])
	if !ok {
		return nil, false
	}
	right, ok := columnRefID(args[1])
	if !ok || left != right {
		return nil, false
	}
	return args, true
}

type shadowingName struct {
	ident string
	rng   lsp.Range
}

// shadowingOperands returns the operands written as the bare name of a
// variable in vars. The right operand comes first since `column = variable` is
// the usual way to write the filter.
func shadowingOperands(pass *Pass, args []googlesql.ResolvedExprNode, vars map[string]bool) []shadowingName {
	var result []shadowingName
	for _, arg := range []googlesql.ResolvedExprNode{args[1], args[0]} {
		loc, err := arg.GetParseLocationRangeOrNULL()
		if err != nil || loc == nil {
			continue
		}
		written, ok := pass.File.ExtractSQL(loc)
		if !ok {
			continue
		}
		ident, ok := bareIdentifier(written)
		if !ok || !vars[strings.ToLower(ident)] {
			continue
		}
		r, ok := pass.File.ToLspRange(loc)
		if !ok {
			continue
		}
		result = append(result, shadowingName{ident: ident, rng: r})
	}
	return result
}

func columnRefID(expr googlesql.ResolvedExprNode) (int32, bool) {
	ref, ok := expr.(*googlesql.ResolvedColumnRef)
	if !ok {
		return 0, false
	}
	col, err := ref.Column()
	if err != nil || col == nil {
		return 0, false
	}
	id, err := col.ColumnId()
	return id, err == nil
}

// bareIdentifier returns the name of an unqualified identifier, which is the
// only way a script variable can be written.
func bareIdentifier(s string) (string, bool) {
	s = strings.TrimSpace(s)
	if len(s) >= 2 && s[0] == '`' && s[len(s)-1] == '`' {
		s = s[1 : len(s)-1]
	}
	if s == "" || strings.ContainsAny(s, ".` \t\n") {
		return "", false
	}
	return s, true
}
