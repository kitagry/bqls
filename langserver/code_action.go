package langserver

import (
	"context"
	"fmt"
	"strings"

	"github.com/kitagry/bqls/langserver/internal/lint"
	"github.com/kitagry/bqls/langserver/internal/lsp"
)

// lintCodeActions recomputes lint diagnostics instead of trusting the
// client-supplied context, so actions always match the current text and
// settings.
func (h *Handler) lintCodeActions(ctx context.Context, uri lsp.DocumentURI, rng lsp.Range) []any {
	if h.project == nil || !h.supportsCodeActionLiterals() {
		return nil
	}
	diags, ok := h.project.Diagnose(ctx, uri, h.lintConfig())
	if !ok {
		return nil
	}

	var result []any
	for i, fixes := range diags.ErrorFixes {
		diag := convertErrorsToDiagnostics(diags.Errors[i : i+1])[0]
		result = append(result, fixCodeActions(uri, diag, fixes, rng)...)
	}
	if len(diags.Lint) == 0 {
		return result
	}
	src, _ := h.project.GetFile(uri)
	lines := strings.Split(src, "\n")

	type ruleLine struct {
		rule string
		line int
	}
	ignored := make(map[ruleLine]bool)
	for _, d := range diags.Lint {
		if !rangesOverlap(d.Range, rng) {
			continue
		}
		lspDiag := convertLintDiagnostics([]lint.Diagnostic{d})[0]
		result = append(result, fixCodeActions(uri, lspDiag, d.SuggestedFixes, rng)...)

		key := ruleLine{rule: d.Rule, line: d.Range.Start.Line}
		if ignored[key] {
			continue
		}
		ignored[key] = true
		if action, ok := ignoreCodeAction(uri, src, lines, d.Rule, d.Range.Start.Line, lspDiag); ok {
			result = append(result, action)
		}
	}
	return result
}

// fixCodeActions turns fixes into quick fixes when rng touches either the
// diagnostic or the text a fix rewrites; error diagnostics can be zero-width
// while the fix covers the whole expression.
func fixCodeActions(uri lsp.DocumentURI, diag lsp.Diagnostic, fixes []lint.SuggestedFix, rng lsp.Range) []any {
	var result []any
	for i, fix := range fixes {
		touched := rangesOverlap(diag.Range, rng)
		for _, e := range fix.Edits {
			touched = touched || rangesOverlap(e.Range, rng)
		}
		if !touched {
			continue
		}
		result = append(result, lsp.CodeAction{
			Title:       fix.Title,
			Kind:        lsp.CAKQuickFix,
			Diagnostics: []lsp.Diagnostic{diag},
			IsPreferred: i == 0,
			Edit:        &lsp.WorkspaceEdit{Changes: map[string][]lsp.TextEdit{string(uri): fix.Edits}},
		})
	}
	return result
}

// supportsCodeActionLiterals reports whether the client accepts CodeAction
// objects; older clients only understand bare Command results.
func (h *Handler) supportsCodeActionLiterals() bool {
	h.settingsMu.RLock()
	defer h.settingsMu.RUnlock()
	return len(h.initializeParams.Capabilities.TextDocument.CodeAction.CodeActionLiteralSupport.CodeActionKind.ValueSet) > 0
}

// ignoreCodeAction inserts a standalone directive above the line rather than
// a trailing one, so it never lands inside a multi-line expression's tail.
// When the line above already is a standalone directive, the rule is appended
// to it instead, because a new line would push that directive off its target.
func ignoreCodeAction(uri lsp.DocumentURI, src string, lines []string, rule string, line int, diag lsp.Diagnostic) (lsp.CodeAction, bool) {
	var edit lsp.TextEdit
	if prev, ok := previousIgnoreLine(lines, line); ok {
		if len(prev.Rules) == 0 {
			return lsp.CodeAction{}, false
		}
		pos := lsp.Position{Line: line - 1, Character: prev.RulesEnd}
		edit = lsp.TextEdit{Range: lsp.Range{Start: pos, End: pos}, NewText: ", " + rule}
		if !lint.EditsASCIISafe(src, []lsp.TextEdit{edit}) {
			return lsp.CodeAction{}, false
		}
	} else {
		var indent string
		if line < len(lines) {
			text := lines[line]
			indent = text[:len(text)-len(strings.TrimLeft(text, " \t"))]
		}
		pos := lsp.Position{Line: line}
		edit = lsp.TextEdit{Range: lsp.Range{Start: pos, End: pos}, NewText: fmt.Sprintf("%s-- bqls:ignore %s\n", indent, rule)}
	}
	return lsp.CodeAction{
		Title:       fmt.Sprintf("Ignore %s on this line", rule),
		Kind:        lsp.CAKQuickFix,
		Diagnostics: []lsp.Diagnostic{diag},
		Edit:        &lsp.WorkspaceEdit{Changes: map[string][]lsp.TextEdit{string(uri): {edit}}},
	}, true
}

func previousIgnoreLine(lines []string, line int) (lint.IgnoreLine, bool) {
	if line == 0 || line > len(lines) {
		return lint.IgnoreLine{}, false
	}
	return lint.ParseIgnoreLine(lines[line-1])
}

func rangesOverlap(a, b lsp.Range) bool {
	return !positionLess(a.End, b.Start) && !positionLess(b.End, a.Start)
}

func positionLess(a, b lsp.Position) bool {
	if a.Line != b.Line {
		return a.Line < b.Line
	}
	return a.Character < b.Character
}
