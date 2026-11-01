package lint

import (
	"fmt"
	"strings"

	googlesql "github.com/goccy/go-googlesql"
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
		for _, call := range file.ListResolvedAstNode[*googlesql.ResolvedFunctionCall](st.Output) {
			checkVariableColumnShadow(pass, call, vars)
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

func checkVariableColumnShadow(pass *Pass, call *googlesql.ResolvedFunctionCall, vars map[string]bool) {
	args, ok := selfComparisonOperands(call)
	if !ok {
		return
	}
	arg, ident, ok := shadowingOperand(pass, args, vars)
	if !ok {
		return
	}
	loc, err := arg.GetParseLocationRangeOrNULL()
	if err != nil || loc == nil {
		return
	}
	r, ok := pass.File.ToLspRange(loc)
	if !ok {
		return
	}
	pass.Report(Diagnostic{
		Range: r,
		Message: fmt.Sprintf("`%[1]s` refers to the column, not the variable `%[1]s`, because a column takes precedence over a variable of the same name; this compares the column with itself. Rename the variable (e.g. `v_%[1]s`) to use it here",
			ident),
		Severity: SeverityWarning,
	})
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

// shadowingOperand returns the operand written as the bare name of a variable
// in vars, and that name.
func shadowingOperand(pass *Pass, args []googlesql.ResolvedExprNode, vars map[string]bool) (googlesql.ResolvedExprNode, string, bool) {
	// The right operand is checked first since `column = variable` is the
	// usual way to write the filter.
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
		if ok && vars[strings.ToLower(ident)] {
			return arg, ident, true
		}
	}
	return nil, "", false
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
