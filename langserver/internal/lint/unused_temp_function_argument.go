package lint

import (
	"fmt"
	"strings"

	googlesql "github.com/goccy/go-googlesql"
	"github.com/kitagry/bqls/langserver/internal/source/file"
)

var UnusedTempFunctionArgument = &Analyzer{
	Name:           "unused-temp-function-argument",
	Doc:            "reports arguments of SQL temp functions that the body never references",
	DefaultEnabled: true,
	Run:            runUnusedTempFunctionArgument,
}

// Any identifier in the body with the argument's name counts as a reference,
// even a same-named column or lambda argument, so that a case this rule cannot
// tell apart is never reported. Only SQL bodies are looked at: the body of a
// JavaScript or other non-SQL function is opaque.
func runUnusedTempFunctionArgument(pass *Pass) {
	for _, fn := range tempFunctions(pass.File.Node) {
		body, err := fn.stmt.SqlFunctionBody()
		if err != nil || body == nil {
			continue
		}
		mentioned := make(map[string]bool)
		file.Walk(body, func(n googlesql.ASTNode) error { //nolint
			if ident, ok := n.(*googlesql.ASTIdentifier); ok {
				if name, err := ident.GetAsString(); err == nil {
					mentioned[strings.ToLower(name)] = true
				}
			}
			return nil
		})
		for _, arg := range functionArguments(fn.stmt) {
			if !mentioned[strings.ToLower(arg.name)] {
				reportUnusedTempFunctionArgument(pass, fn, arg)
			}
		}
	}
}

type functionArgument struct {
	name  string
	ident offsets
}

func functionArguments(stmt *googlesql.ASTCreateFunctionStatement) []functionArgument {
	decl, err := stmt.FunctionDeclaration()
	if err != nil || decl == nil {
		return nil
	}
	params, err := decl.Parameters()
	if err != nil || params == nil {
		return nil
	}
	n, err := params.NumChildren()
	if err != nil {
		return nil
	}
	var args []functionArgument
	for i := int32(0); i < n; i++ {
		param, err := params.ParameterEntries(i)
		if err != nil || param == nil {
			return nil
		}
		ident, err := param.Name()
		if err != nil || ident == nil {
			return nil
		}
		name, err := ident.GetAsString()
		if err != nil {
			return nil
		}
		args = append(args, functionArgument{name: name, ident: astOffsets(ident)})
	}
	return args
}

func reportUnusedTempFunctionArgument(pass *Pass, fn tempFunction, arg functionArgument) {
	r, ok := pass.File.OffsetsToLspRange(arg.ident.start, arg.ident.end)
	if !ok {
		return
	}
	pass.Report(Diagnostic{
		Range:    r,
		Message:  fmt.Sprintf("Argument `%s` of function `%s` is never used", arg.name, fn.name),
		Severity: SeverityInformation,
	})
}
