package langserver

import (
	"context"
	"fmt"
	"strings"

	"github.com/kitagry/bqls/langserver/internal/lint"
	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/kitagry/bqls/langserver/internal/source/helper"
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

	src, _ := h.project.GetFile(uri)
	// Diagnose works in byte columns while rng and everything sent back are
	// in UTF-16 units, so all comparisons happen after conversion.
	conv := helper.NewPositionConverter(src)
	var result []any
	for i, fixes := range diags.ErrorFixes {
		diag := errorsToLspDiagnostics(conv, diags.Errors[i:i+1])[0]
		result = append(result, fixCodeActions(uri, diag, fixesToUTF16(conv, fixes), rng)...)
	}
	if len(diags.Lint) == 0 {
		return result
	}
	lines := strings.Split(src, "\n")
	states := lint.LineStates(src)

	type ruleLine struct {
		rule string
		line int
	}
	ignored := make(map[ruleLine]bool)
	for _, d := range diags.Lint {
		lspDiag := lintDiagnosticsToLsp(conv, []lint.Diagnostic{d})[0]
		if !rangesOverlap(lspDiag.Range, rng) {
			continue
		}
		result = append(result, fixCodeActions(uri, lspDiag, fixesToUTF16(conv, d.SuggestedFixes), rng)...)

		key := ruleLine{rule: d.Rule, line: d.Range.Start.Line}
		if ignored[key] {
			continue
		}
		ignored[key] = true
		if action, ok := ignoreCodeAction(uri, conv, lines, states, d.Rule, d.Range.Start.Line, lspDiag); ok {
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

func fixesToUTF16(conv helper.PositionConverter, fixes []lint.SuggestedFix) []lint.SuggestedFix {
	result := make([]lint.SuggestedFix, len(fixes))
	for i, fix := range fixes {
		edits := make([]lsp.TextEdit, len(fix.Edits))
		for j, e := range fix.Edits {
			e.Range = conv.RangeToUTF16(e.Range)
			edits[j] = e
		}
		// Copy the whole fix so fields added to SuggestedFix later survive.
		result[i] = fix
		result[i].Edits = edits
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

// ignoreCodeAction prefers a standalone directive above the line to a
// trailing one, so it never lands inside a multi-line expression's tail.
// When the line above already is a standalone directive, the rule is appended
// to it instead, because a new line would push that directive off its target.
// A line that begins inside a multi-line string or comment cannot take a
// directive above it, so it falls back to a trailing one.
func ignoreCodeAction(uri lsp.DocumentURI, conv helper.PositionConverter, lines []string, states []lint.LineState, rule string, line int, diag lsp.Diagnostic) (lsp.CodeAction, bool) {
	var edit lsp.TextEdit
	if line < len(states) && !states[line].StartsInCode {
		// A trailing comment or an unclosed string would swallow the directive.
		if !states[line].EndsInCode {
			return lsp.CodeAction{}, false
		}
		text := strings.TrimSuffix(lines[line], "\r")
		pos := conv.ToUTF16(lsp.Position{Line: line, Character: len(text)})
		edit = lsp.TextEdit{Range: lsp.Range{Start: pos, End: pos}, NewText: " -- bqls:ignore " + rule}
	} else if prev, ok := previousIgnoreLine(lines, states, line); ok {
		if len(prev.Rules) == 0 {
			return lsp.CodeAction{}, false
		}
		// ParseIgnoreLine only allows spaces and tabs before the directive, so
		// RulesEnd is ASCII-only today; convert anyway so the edit stays right
		// if the directive syntax ever admits non-ASCII text before the rules.
		pos := conv.ToUTF16(lsp.Position{Line: line - 1, Character: prev.RulesEnd})
		edit = lsp.TextEdit{Range: lsp.Range{Start: pos, End: pos}, NewText: ", " + rule}
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

// previousIgnoreLine skips a line that begins inside a string or comment,
// where directive-like text is not a directive.
func previousIgnoreLine(lines []string, states []lint.LineState, line int) (lint.IgnoreLine, bool) {
	if line == 0 || line > len(lines) || !states[line-1].StartsInCode {
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
