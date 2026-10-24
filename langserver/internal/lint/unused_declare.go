package lint

import (
	"fmt"
	"strings"

	googlesql "github.com/goccy/go-googlesql"
	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/kitagry/bqls/langserver/internal/source/file"
)

var UnusedDeclare = &Analyzer{
	Name:           "unused-declare",
	Doc:            "reports DECLARE variables that are never referenced",
	DefaultEnabled: true,
	Run:            runUnusedDeclare,
}

// References are collected from a fresh parse of the source as written, because
// the analyzed AST has every use of a declared variable replaced by a dummy
// value. Any identifier with the variable's name counts as a reference, even
// a `SET` target or a same-named alias or column, so that a case this rule
// cannot tell apart is never reported.
func runUnusedDeclare(pass *Pass) {
	decls := file.ListAstNode[*googlesql.ASTVariableDeclaration](pass.File.Node)
	if len(decls) == 0 {
		return
	}
	script, err := file.ParseScript(pass.File.Src)
	if err != nil {
		return
	}
	mentioned := mentionedNames(script)

	for _, decl := range decls {
		vars := declaredVariables(decl)
		if vars == nil {
			continue
		}
		var unused []int
		for i, v := range vars {
			if !mentioned[strings.ToLower(v.name)] {
				unused = append(unused, i)
			}
		}
		for _, i := range unused {
			reportUnusedDeclare(pass, decl, vars, i, len(unused) < len(vars))
		}
	}
}

// mentionedNames returns the lower-cased identifiers of the script outside of
// the variable lists of DECLARE statements.
func mentionedNames(script *googlesql.ASTScript) map[string]bool {
	names := make(map[string]bool)
	for _, ident := range file.ListAstNode[*googlesql.ASTIdentifier](script) {
		if inDeclaredList(ident) {
			continue
		}
		if name, err := ident.GetAsString(); err == nil {
			names[strings.ToLower(name)] = true
		}
	}
	return names
}

func inDeclaredList(ident *googlesql.ASTIdentifier) bool {
	list, err := ident.Parent()
	if err != nil || list == nil {
		return false
	}
	if _, ok := list.(*googlesql.ASTIdentifierList); !ok {
		return false
	}
	decl, err := list.Parent()
	if err != nil || decl == nil {
		return false
	}
	_, ok := decl.(*googlesql.ASTVariableDeclaration)
	return ok
}

type declaredVariable struct {
	name  string
	ident offsets
}

// declaredVariables returns nil when any variable cannot be read, so that a
// declaration is never half reported.
func declaredVariables(decl *googlesql.ASTVariableDeclaration) []declaredVariable {
	list, err := decl.VariableList()
	if err != nil || list == nil {
		return nil
	}
	n, err := list.NumChildren()
	if err != nil {
		return nil
	}
	vars := make([]declaredVariable, 0, n)
	for i := int32(0); i < n; i++ {
		ident, err := list.IdentifierList(i)
		if err != nil || ident == nil {
			return nil
		}
		name, err := ident.GetAsString()
		if err != nil {
			return nil
		}
		vars = append(vars, declaredVariable{name: name, ident: astOffsets(ident)})
	}
	return vars
}

func reportUnusedDeclare(pass *Pass, decl *googlesql.ASTVariableDeclaration, vars []declaredVariable, i int, othersUsed bool) {
	v := vars[i]
	r, ok := pass.File.OffsetsToLspRange(v.ident.start, v.ident.end)
	if !ok {
		return
	}
	d := Diagnostic{
		Range:    r,
		Message:  fmt.Sprintf("Variable `%s` is declared but never used", v.name),
		Severity: SeverityInformation,
	}
	if del, ok := unusedDeclareDeletion(pass.File.FixedSrc, astOffsets(decl), vars, i, othersUsed); ok {
		if r, ok := pass.File.OffsetsToLspRange(del.start, del.end); ok {
			d.SuggestedFixes = []SuggestedFix{{
				Title: fmt.Sprintf("Remove unused variable `%s`", v.name),
				Edits: []lsp.TextEdit{{Range: r, NewText: ""}},
			}}
		}
	}
	pass.Report(d)
}

// unusedDeclareDeletion returns the span to delete. With several variables it
// gives up unless another one stays, because the fixes of the others would
// overlap, and it gives up whenever a user comment would be deleted or left
// describing something else.
func unusedDeclareDeletion(src string, decl offsets, vars []declaredVariable, i int, othersUsed bool) (offsets, bool) {
	v := vars[i]
	if !inBounds(src, decl) || !inBounds(src, v.ident) {
		return offsets{}, false
	}

	if len(vars) > 1 {
		if !othersUsed {
			return offsets{}, false
		}
		if i == 0 {
			del := offsets{v.ident.start, vars[1].ident.start}
			return del, isSeparator(src, v.ident.end, del.end)
		}
		del := offsets{vars[i-1].ident.end, v.ident.end}
		return del, isSeparator(src, del.start, v.ident.start)
	}

	end := decl.end
	semi := end
	for semi < len(src) && (src[semi] == ' ' || src[semi] == '\t') {
		semi++
	}
	if semi < len(src) && src[semi] == ';' {
		end = semi + 1
	}
	if hasTrailingComment(src, end) || hasCommentLineAbove(src, decl.start) {
		return offsets{}, false
	}
	for end < len(src) && isSQLSpace(src[end]) {
		end++
	}
	return offsets{decl.start, end}, true
}
