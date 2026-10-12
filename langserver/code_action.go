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
	if !ok || len(diags.Lint) == 0 {
		return nil
	}
	src, _ := h.project.GetFile(uri)
	lines := strings.Split(src, "\n")

	var result []any
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
		for i, fix := range d.SuggestedFixes {
			result = append(result, lsp.CodeAction{
				Title:       fix.Title,
				Kind:        lsp.CAKQuickFix,
				Diagnostics: []lsp.Diagnostic{lspDiag},
				IsPreferred: i == 0,
				Edit:        &lsp.WorkspaceEdit{Changes: map[string][]lsp.TextEdit{string(uri): fix.Edits}},
			})
		}

		key := ruleLine{rule: d.Rule, line: d.Range.Start.Line}
		if ignored[key] {
			continue
		}
		ignored[key] = true
		result = append(result, ignoreCodeAction(uri, lines, d.Rule, d.Range.Start.Line, lspDiag))
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
func ignoreCodeAction(uri lsp.DocumentURI, lines []string, rule string, line int, diag lsp.Diagnostic) lsp.CodeAction {
	var indent string
	if line < len(lines) {
		text := lines[line]
		indent = text[:len(text)-len(strings.TrimLeft(text, " \t"))]
	}
	pos := lsp.Position{Line: line}
	return lsp.CodeAction{
		Title:       fmt.Sprintf("Ignore %s on this line", rule),
		Kind:        lsp.CAKQuickFix,
		Diagnostics: []lsp.Diagnostic{diag},
		Edit: &lsp.WorkspaceEdit{Changes: map[string][]lsp.TextEdit{
			string(uri): {{
				Range:   lsp.Range{Start: pos, End: pos},
				NewText: fmt.Sprintf("%s-- bqls:ignore %s\n", indent, rule),
			}},
		}},
	}
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
