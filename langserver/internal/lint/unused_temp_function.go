package lint

import (
	"fmt"
	"strings"

	googlesql "github.com/goccy/go-googlesql"
	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/kitagry/bqls/langserver/internal/source/file"
)

var UnusedTempFunction = &Analyzer{
	Name:           "unused-temp-function",
	Doc:            "reports CREATE TEMP FUNCTION definitions that are never called",
	DefaultEnabled: true,
	Run:            runUnusedTempFunction,
}

// As in unused-declare, references are matched by name on a fresh parse of the
// source as written, so that a case this rule cannot tell apart is never
// reported: any identifier with the function's name counts, and so does the
// name as a word of a string literal, which may be a query run by EXECUTE
// IMMEDIATE.
func runUnusedTempFunction(pass *Pass) {
	fns := tempFunctions(pass.File.Node)
	if len(fns) == 0 || onlyFunctionDefinitions(pass.File.Statements) {
		return
	}
	script, err := file.ParseScript(pass.File.Src)
	if err != nil {
		return
	}
	mentioned := mentionedFunctionNames(script)

	for _, fn := range fns {
		if !mentioned[strings.ToLower(fn.name)] {
			reportUnusedTempFunction(pass, fn)
		}
	}
}

type tempFunction struct {
	stmt *googlesql.ASTCreateFunctionStatement
	name string
	// ident is the function name; span is the whole statement.
	ident, span offsets
}

func tempFunctions(script *googlesql.ASTScript) []tempFunction {
	var fns []tempFunction
	for _, stmt := range file.ListAstNode[*googlesql.ASTCreateFunctionStatement](script) {
		if temp, err := stmt.IsTemp(); err != nil || !temp {
			continue
		}
		decl, err := stmt.FunctionDeclaration()
		if err != nil || decl == nil {
			continue
		}
		path, err := decl.Name()
		if err != nil || path == nil {
			continue
		}
		names, err := path.ToIdentifierVector()
		if err != nil || len(names) != 1 {
			continue
		}
		fns = append(fns, tempFunction{stmt: stmt, name: names[0], ident: astOffsets(path), span: astOffsets(stmt)})
	}
	return fns
}

// onlyFunctionDefinitions reports whether the file defines functions and does
// nothing else, as a library of functions for later queries of the same
// session would.
func onlyFunctionDefinitions(stmts []file.StatementResult) bool {
	for _, s := range stmts {
		if _, ok := s.Stmt.(*googlesql.ASTCreateFunctionStatement); !ok {
			return false
		}
	}
	return true
}

// mentionedFunctionNames returns the lower-cased identifiers of the script,
// other than the names of defined functions and their arguments, together with
// the words of its string literals.
func mentionedFunctionNames(script *googlesql.ASTScript) map[string]bool {
	names := make(map[string]bool)
	for _, ident := range file.ListAstNode[*googlesql.ASTIdentifier](script) {
		if isFunctionDefinitionName(ident) {
			continue
		}
		if name, err := ident.GetAsString(); err == nil {
			names[strings.ToLower(name)] = true
		}
	}
	for _, lit := range file.ListAstNode[*googlesql.ASTStringLiteral](script) {
		s, err := lit.StringValue()
		if err != nil {
			continue
		}
		for _, w := range strings.FieldsFunc(s, func(r rune) bool { return !isIdentifierRune(r) }) {
			names[strings.ToLower(w)] = true
		}
	}
	return names
}

func isFunctionDefinitionName(ident *googlesql.ASTIdentifier) bool {
	parent, err := ident.Parent()
	if err != nil || parent == nil {
		return false
	}
	if _, ok := parent.(*googlesql.ASTFunctionParameter); ok {
		return true
	}
	if _, ok := parent.(*googlesql.ASTPathExpression); !ok {
		return false
	}
	decl, err := parent.Parent()
	if err != nil || decl == nil {
		return false
	}
	_, ok := decl.(*googlesql.ASTFunctionDeclaration)
	return ok
}

func isIdentifierRune(r rune) bool {
	return r == '_' || 'a' <= r && r <= 'z' || 'A' <= r && r <= 'Z' || '0' <= r && r <= '9'
}

func reportUnusedTempFunction(pass *Pass, fn tempFunction) {
	r, ok := pass.File.OffsetsToLspRange(fn.ident.start, fn.ident.end)
	if !ok {
		return
	}
	d := Diagnostic{
		Range:    r,
		Message:  fmt.Sprintf("Function `%s` is defined but never used", fn.name),
		Severity: SeverityInformation,
	}
	if del, ok := statementDeletion(pass.File.FixedSrc, fn.span); ok {
		if r, ok := pass.File.OffsetsToLspRange(del.start, del.end); ok {
			d.SuggestedFixes = []SuggestedFix{{
				Title: fmt.Sprintf("Remove unused function `%s`", fn.name),
				Edits: []lsp.TextEdit{{Range: r, NewText: ""}},
			}}
		}
	}
	pass.Report(d)
}
