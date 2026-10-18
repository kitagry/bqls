package langserver

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"
	"unicode/utf16"

	"github.com/google/go-cmp/cmp"
	"github.com/kitagry/bqls/langserver/internal/lint"
	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/sourcegraph/jsonrpc2"
)

func TestHandler_handleTextDocumentCodeAction_Lint(t *testing.T) {
	const uri lsp.DocumentURI = "file:///a.sql"
	const src = "SELECT x\nFROM UNNEST([1]) AS x\n  WHERE x = CAST(NULL AS INT64)"
	diagRange := lsp.Range{Start: lsp.Position{Line: 2, Character: 8}, End: lsp.Position{Line: 2, Character: 31}}
	lintDiag := lsp.Diagnostic{
		Range:    diagRange,
		Severity: lsp.Warning,
		Code:     "null-comparison",
		Source:   "bqls-lint",
		Message:  "Comparison with NULL using = always yields NULL; use IS NULL",
	}
	commands := []any{
		lsp.Command{Title: "Execute Query", Command: CommandExecuteQuery, Arguments: []any{uri}},
		lsp.Command{Title: "List Personal Job Histories", Command: CommandListJobHistories},
		lsp.Command{Title: "List Project Job Histories", Command: CommandListJobHistories, Arguments: []any{"--all-user"}},
	}

	tests := map[string]struct {
		rng            lsp.Range
		literalSupport bool
		want           []any
	}{
		"cursor on lint diagnostic offers fix and ignore": {
			literalSupport: true,
			rng:            lsp.Range{Start: lsp.Position{Line: 2, Character: 10}, End: lsp.Position{Line: 2, Character: 10}},
			want: append([]any{
				lsp.CodeAction{
					Title:       "Replace with IS NULL",
					Kind:        lsp.CAKQuickFix,
					Diagnostics: []lsp.Diagnostic{lintDiag},
					IsPreferred: true,
					Edit: &lsp.WorkspaceEdit{Changes: map[string][]lsp.TextEdit{
						string(uri): {{Range: diagRange, NewText: "x IS NULL"}},
					}},
				},
				// The ignore comment goes on its own line above, keeping the
				// diagnostic line's indentation.
				lsp.CodeAction{
					Title:       "Ignore null-comparison on this line",
					Kind:        lsp.CAKQuickFix,
					Diagnostics: []lsp.Diagnostic{lintDiag},
					Edit: &lsp.WorkspaceEdit{Changes: map[string][]lsp.TextEdit{
						string(uri): {{
							Range:   lsp.Range{Start: lsp.Position{Line: 2}, End: lsp.Position{Line: 2}},
							NewText: "  -- bqls:ignore null-comparison\n",
						}},
					}},
				},
			}, commands...),
		},
		"range without lint diagnostic offers only commands": {
			literalSupport: true,
			rng:            lsp.Range{Start: lsp.Position{Line: 0, Character: 0}, End: lsp.Position{Line: 0, Character: 3}},
			want:           commands,
		},
		"client without code action literal support gets only commands": {
			rng:  lsp.Range{Start: lsp.Position{Line: 2, Character: 10}, End: lsp.Position{Line: 2, Character: 10}},
			want: commands,
		},
	}

	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			h := newTestHandlerWithFile(t, uri, src)
			if tt.literalSupport {
				h.initializeParams.Capabilities.TextDocument.CodeAction.CodeActionLiteralSupport.CodeActionKind.ValueSet = []lsp.CodeActionKind{lsp.CAKQuickFix}
			}
			params, err := json.Marshal(lsp.CodeActionParams{TextDocument: lsp.TextDocumentIdentifier{URI: uri}, Range: tt.rng})
			if err != nil {
				t.Fatal(err)
			}
			raw := json.RawMessage(params)

			got, err := h.handleTextDocumentCodeAction(t.Context(), nil, &jsonrpc2.Request{Params: &raw})
			if err != nil {
				t.Fatalf("handleTextDocumentCodeAction() error = %v", err)
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("code actions diff (-want +got):\n%s", diff)
			}
		})
	}
}

func TestHandler_handleTextDocumentCodeAction_ExtendsExistingIgnore(t *testing.T) {
	const uri lsp.DocumentURI = "file:///a.sql"
	const src = "SELECT x\nFROM UNNEST([1]) AS x\n  -- bqls:ignore other-rule -- legacy\n  WHERE x = CAST(NULL AS INT64)"
	h := newTestHandlerWithFile(t, uri, src)
	h.initializeParams.Capabilities.TextDocument.CodeAction.CodeActionLiteralSupport.CodeActionKind.ValueSet = []lsp.CodeActionKind{lsp.CAKQuickFix}

	pos := lsp.Position{Line: 3, Character: 10}
	actions := h.lintCodeActions(t.Context(), uri, lsp.Range{Start: pos, End: pos})

	var got []lsp.TextEdit
	for _, a := range actions {
		if ca, ok := a.(lsp.CodeAction); ok && ca.Title == "Ignore null-comparison on this line" {
			got = ca.Edit.Changes[string(uri)]
		}
	}
	insertAt := lsp.Position{Line: 2, Character: 27}
	want := []lsp.TextEdit{{Range: lsp.Range{Start: insertAt, End: insertAt}, NewText: ", null-comparison"}}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("ignore edit diff (-want +got):\n%s", diff)
	}
}

func TestHandler_lintCodeActions_LiteralNullComparisonError(t *testing.T) {
	const uri lsp.DocumentURI = "file:///a.sql"
	r := func(sl, sc, el, ec int) lsp.Range {
		return lsp.Range{Start: lsp.Position{Line: sl, Character: sc}, End: lsp.Position{Line: el, Character: ec}}
	}
	tests := map[string]struct {
		src     string
		cursor  lsp.Position
		want    []any
		wantNil bool
	}{
		"<> NULL is rewritten to IS NOT NULL": {
			src:    "SELECT x FROM UNNEST([1]) AS x\nWHERE x <> NULL",
			cursor: lsp.Position{Line: 1, Character: 6},
			want: []any{lsp.CodeAction{
				Title: "Replace with IS NOT NULL",
				Kind:  lsp.CAKQuickFix,
				Diagnostics: []lsp.Diagnostic{{
					Range:    r(1, 11, 1, 11),
					Severity: lsp.Error,
					Message:  "Operands of <> cannot be literal NULL",
				}},
				IsPreferred: true,
				Edit: &lsp.WorkspaceEdit{Changes: map[string][]lsp.TextEdit{
					string(uri): {{Range: r(1, 6, 1, 15), NewText: "x IS NOT NULL"}},
				}},
			}},
		},
		"NULL on the left with =": {
			src:    "SELECT x FROM UNNEST([1]) AS x WHERE NULL = (x + 1)",
			cursor: lsp.Position{Line: 0, Character: 40},
			want: []any{lsp.CodeAction{
				Title: "Replace with IS NULL",
				Kind:  lsp.CAKQuickFix,
				Diagnostics: []lsp.Diagnostic{{
					Range:    r(0, 37, 0, 37),
					Severity: lsp.Error,
					Message:  "Operands of = cannot be literal NULL",
				}},
				IsPreferred: true,
				Edit: &lsp.WorkspaceEdit{Changes: map[string][]lsp.TextEdit{
					string(uri): {{Range: r(0, 37, 0, 51), NewText: "(x + 1) IS NULL"}},
				}},
			}},
		},
		// Positions are in UTF-16 units, so text after non-ASCII characters
		// is still rewritten in the right place.
		"fix when the rewritten text contains non-ASCII": {
			src:    "SELECT x FROM UNNEST(['a']) AS x WHERE NULL = 'あ'",
			cursor: lsp.Position{Line: 0, Character: 40},
			want: []any{lsp.CodeAction{
				Title: "Replace with IS NULL",
				Kind:  lsp.CAKQuickFix,
				Diagnostics: []lsp.Diagnostic{{
					Range:    r(0, 39, 0, 39),
					Severity: lsp.Error,
					Message:  "Operands of = cannot be literal NULL",
				}},
				IsPreferred: true,
				Edit: &lsp.WorkspaceEdit{Changes: map[string][]lsp.TextEdit{
					string(uri): {{Range: r(0, 39, 0, 49), NewText: "'あ' IS NULL"}},
				}},
			}},
		},
		"fix after non-ASCII text on the line": {
			src:    "SELECT 'あ' AS s, x FROM UNNEST([1]) AS x WHERE x = NULL",
			cursor: lsp.Position{Line: 0, Character: 52},
			want: []any{lsp.CodeAction{
				Title: "Replace with IS NULL",
				Kind:  lsp.CAKQuickFix,
				Diagnostics: []lsp.Diagnostic{{
					Range:    r(0, 51, 0, 51),
					Severity: lsp.Error,
					Message:  "Operands of = cannot be literal NULL",
				}},
				IsPreferred: true,
				Edit: &lsp.WorkspaceEdit{Changes: map[string][]lsp.TextEdit{
					string(uri): {{Range: r(0, 47, 0, 55), NewText: "x IS NULL"}},
				}},
			}},
		},
		// The cursor is at UTF-16 column 51, inside the comparison, but at
		// byte column 51 it would be before it.
		"cursor compared in UTF-16 units after surrogate pair": {
			src:    "SELECT '😀😀' AS s, x FROM UNNEST([1]) AS x WHERE x = NULL",
			cursor: lsp.Position{Line: 0, Character: 51},
			want: []any{lsp.CodeAction{
				Title: "Replace with IS NULL",
				Kind:  lsp.CAKQuickFix,
				Diagnostics: []lsp.Diagnostic{{
					Range:    r(0, 54, 0, 54),
					Severity: lsp.Error,
					Message:  "Operands of = cannot be literal NULL",
				}},
				IsPreferred: true,
				Edit: &lsp.WorkspaceEdit{Changes: map[string][]lsp.TextEdit{
					string(uri): {{Range: r(0, 50, 0, 58), NewText: "x IS NULL"}},
				}},
			}},
		},
		"fix kept when non-ASCII text follows the comparison": {
			src:    "SELECT x FROM UNNEST([1]) AS x WHERE x = NULL AND 'あ' = 'あ'",
			cursor: lsp.Position{Line: 0, Character: 37},
			want: []any{lsp.CodeAction{
				Title: "Replace with IS NULL",
				Kind:  lsp.CAKQuickFix,
				Diagnostics: []lsp.Diagnostic{{
					Range:    r(0, 41, 0, 41),
					Severity: lsp.Error,
					Message:  "Operands of = cannot be literal NULL",
				}},
				IsPreferred: true,
				Edit: &lsp.WorkspaceEdit{Changes: map[string][]lsp.TextEdit{
					string(uri): {{Range: r(0, 37, 0, 45), NewText: "x IS NULL"}},
				}},
			}},
		},
		"tab before the comparison": {
			src:    "SELECT x FROM UNNEST([1]) AS x\nWHERE\tx = NULL",
			cursor: lsp.Position{Line: 1, Character: 6},
			want: []any{lsp.CodeAction{
				Title: "Replace with IS NULL",
				Kind:  lsp.CAKQuickFix,
				Diagnostics: []lsp.Diagnostic{{
					Range:    r(1, 10, 1, 10),
					Severity: lsp.Error,
					Message:  "Operands of = cannot be literal NULL",
				}},
				IsPreferred: true,
				Edit: &lsp.WorkspaceEdit{Changes: map[string][]lsp.TextEdit{
					string(uri): {{Range: r(1, 6, 1, 14), NewText: "x IS NULL"}},
				}},
			}},
		},
		"comparison after a variable longer than its dummy value": {
			src:    "DECLARE yyyyyy INT64;\nSELECT x FROM UNNEST([yyyyyy]) AS x WHERE x = NULL",
			cursor: lsp.Position{Line: 1, Character: 42},
			want: []any{lsp.CodeAction{
				Title: "Replace with IS NULL",
				Kind:  lsp.CAKQuickFix,
				Diagnostics: []lsp.Diagnostic{{
					Range:    r(1, 46, 1, 46),
					Severity: lsp.Error,
					Message:  "Operands of = cannot be literal NULL",
				}},
				IsPreferred: true,
				Edit: &lsp.WorkspaceEdit{Changes: map[string][]lsp.TextEdit{
					string(uri): {{Range: r(1, 42, 1, 50), NewText: "x IS NULL"}},
				}},
			}},
		},
		"cursor away from the comparison": {
			src:     "SELECT x FROM UNNEST([1]) AS x\nWHERE x <> NULL",
			cursor:  lsp.Position{Line: 0, Character: 0},
			wantNil: true,
		},
	}
	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			h := newTestHandlerWithFile(t, uri, tt.src)
			h.initializeParams.Capabilities.TextDocument.CodeAction.CodeActionLiteralSupport.CodeActionKind.ValueSet = []lsp.CodeActionKind{lsp.CAKQuickFix}

			got := h.lintCodeActions(t.Context(), uri, lsp.Range{Start: tt.cursor, End: tt.cursor})
			if tt.wantNil {
				if len(got) != 0 {
					t.Errorf("lintCodeActions() = %+v, want none", got)
				}
				return
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("lintCodeActions() diff (-want +got):\n%s", diff)
			}
		})
	}
}

func TestHandler_lintCodeActions_AfterNonASCIIText(t *testing.T) {
	const uri lsp.DocumentURI = "file:///a.sql"
	// The comparison starts at byte 55 but at UTF-16 character 49, which is
	// what the client sends.
	const src = "SELECT 'あいう' AS s, x FROM UNNEST([1]) AS x WHERE x = CAST(NULL AS INT64)"
	h := newTestHandlerWithFile(t, uri, src)
	h.initializeParams.Capabilities.TextDocument.CodeAction.CodeActionLiteralSupport.CodeActionKind.ValueSet = []lsp.CodeActionKind{lsp.CAKQuickFix}

	cursor := lsp.Position{Line: 0, Character: 50}
	got := h.lintCodeActions(t.Context(), uri, lsp.Range{Start: cursor, End: cursor})

	diagRange := lsp.Range{Start: lsp.Position{Line: 0, Character: 49}, End: lsp.Position{Line: 0, Character: 72}}
	lintDiag := lsp.Diagnostic{
		Range:    diagRange,
		Severity: lsp.Warning,
		Code:     "null-comparison",
		Source:   "bqls-lint",
		Message:  "Comparison with NULL using = always yields NULL; use IS NULL",
	}
	want := []any{
		lsp.CodeAction{
			Title:       "Replace with IS NULL",
			Kind:        lsp.CAKQuickFix,
			Diagnostics: []lsp.Diagnostic{lintDiag},
			IsPreferred: true,
			Edit: &lsp.WorkspaceEdit{Changes: map[string][]lsp.TextEdit{
				string(uri): {{Range: diagRange, NewText: "x IS NULL"}},
			}},
		},
		lsp.CodeAction{
			Title:       "Ignore null-comparison on this line",
			Kind:        lsp.CAKQuickFix,
			Diagnostics: []lsp.Diagnostic{lintDiag},
			Edit: &lsp.WorkspaceEdit{Changes: map[string][]lsp.TextEdit{
				string(uri): {{
					Range:   lsp.Range{Start: lsp.Position{Line: 0}, End: lsp.Position{Line: 0}},
					NewText: "-- bqls:ignore null-comparison\n",
				}},
			}},
		},
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("lintCodeActions() diff (-want +got):\n%s", diff)
	}

	fixed := applyUTF16Edits(t, src, want[0].(lsp.CodeAction).Edit.Changes[string(uri)])
	if wantSrc := "SELECT 'あいう' AS s, x FROM UNNEST([1]) AS x WHERE x IS NULL"; fixed != wantSrc {
		t.Errorf("applied fix = %q, want %q", fixed, wantSrc)
	}
}

// Non-ASCII text after the rule list, and on other lines, must not disturb
// where the rule is appended.
func TestHandler_handleTextDocumentCodeAction_ExtendsExistingIgnoreWithTrailingNonASCIIText(t *testing.T) {
	const uri lsp.DocumentURI = "file:///a.sql"
	const src = "SELECT '😀'\nFROM UNNEST([1]) AS x\n\t-- bqls:ignore other-rule -- あ😀\n  WHERE x = CAST(NULL AS INT64)"
	h := newTestHandlerWithFile(t, uri, src)
	h.initializeParams.Capabilities.TextDocument.CodeAction.CodeActionLiteralSupport.CodeActionKind.ValueSet = []lsp.CodeActionKind{lsp.CAKQuickFix}

	pos := lsp.Position{Line: 3, Character: 10}
	actions := h.lintCodeActions(t.Context(), uri, lsp.Range{Start: pos, End: pos})

	var got []lsp.TextEdit
	for _, a := range actions {
		if ca, ok := a.(lsp.CodeAction); ok && ca.Title == "Ignore null-comparison on this line" {
			got = ca.Edit.Changes[string(uri)]
		}
	}
	fixed := applyUTF16Edits(t, src, got)
	want := "SELECT '😀'\nFROM UNNEST([1]) AS x\n\t-- bqls:ignore other-rule, null-comparison -- あ😀\n  WHERE x = CAST(NULL AS INT64)"
	if fixed != want {
		t.Errorf("applied ignore edit = %q, want %q", fixed, want)
	}
}

// applyUTF16Edits applies edits whose Characters are UTF-16 units, the way an
// LSP client does, so tests check what the user would actually end up with.
// Like LSP, every edit refers to the original src.
func applyUTF16Edits(t *testing.T, src string, edits []lsp.TextEdit) string {
	t.Helper()
	if len(edits) == 0 {
		t.Fatal("no edits to apply")
	}
	lines := strings.SplitAfter(src, "\n")
	offset := func(pos lsp.Position) int {
		if pos.Line >= len(lines) {
			return len(src)
		}
		off := 0
		for i := 0; i < pos.Line; i++ {
			off += len(lines[i])
		}
		units := 0
		for i, r := range lines[pos.Line] {
			if units >= pos.Character {
				return off + i
			}
			units += utf16.RuneLen(r)
		}
		return off + len(lines[pos.Line])
	}
	sorted := slices.Clone(edits)
	slices.SortStableFunc(sorted, func(a, b lsp.TextEdit) int { return offset(a.Range.Start) - offset(b.Range.Start) })
	var b strings.Builder
	last := 0
	for _, e := range sorted {
		start, end := offset(e.Range.Start), offset(e.Range.End)
		if start < last {
			t.Fatalf("overlapping edits: %v", edits)
		}
		b.WriteString(src[last:start])
		b.WriteString(e.NewText)
		last = end
	}
	b.WriteString(src[last:])
	return b.String()
}

func TestHandler_lintCodeActions_IgnoreAroundMultiLineLiterals(t *testing.T) {
	const uri lsp.DocumentURI = "file:///a.sql"
	const rule = "null-comparison"
	insertAt := func(line, char int, text string) []lsp.TextEdit {
		pos := lsp.Position{Line: line, Character: char}
		return []lsp.TextEdit{{Range: lsp.Range{Start: pos, End: pos}, NewText: text}}
	}
	tests := map[string]struct {
		src  string
		line int
		// want is nil when no ignore action may be offered.
		want []lsp.TextEdit
	}{
		"line starting in code gets a directive above": {
			src:  "SELECT x\nFROM UNNEST([1]) AS x WHERE x = CAST(NULL AS INT64)",
			line: 1,
			want: insertAt(1, 0, "-- bqls:ignore null-comparison\n"),
		},
		"directive above a CRLF line ends with CRLF": {
			src:  "SELECT x\r\nFROM UNNEST([1]) AS x WHERE x = CAST(NULL AS INT64)\r\nLIMIT 1",
			line: 1,
			want: insertAt(1, 0, "-- bqls:ignore null-comparison\r\n"),
		},
		"directive above the last line of a CRLF file ends with CRLF": {
			src:  "SELECT x\r\nFROM UNNEST([1]) AS x WHERE x = CAST(NULL AS INT64)",
			line: 1,
			want: insertAt(1, 0, "-- bqls:ignore null-comparison\r\n"),
		},
		"line starting inside a multi-line string gets a trailing directive": {
			src:  "SELECT x, '''\nfoo''' AS s FROM UNNEST([1]) AS x WHERE x = CAST(NULL AS INT64)",
			line: 1,
			want: insertAt(1, 63, " -- bqls:ignore null-comparison"),
		},
		"line starting inside a block comment gets a trailing directive": {
			src:  "SELECT x /* note\nstill note */ FROM UNNEST([1]) AS x WHERE x = CAST(NULL AS INT64)",
			line: 1,
			want: insertAt(1, 65, " -- bqls:ignore null-comparison"),
		},
		"directive text inside a string on the line above is not extended": {
			src:  "SELECT x, '''\n-- bqls:ignore other\n''' AS s FROM UNNEST([1]) AS x WHERE x = CAST(NULL AS INT64)",
			line: 2,
			want: insertAt(2, 60, " -- bqls:ignore null-comparison"),
		},
		"directive-like line above that starts inside a string is not extended": {
			src:  "SELECT x, '''\n-- bqls:ignore other'''\nAS s FROM UNNEST([1]) AS x WHERE x = CAST(NULL AS INT64)",
			line: 2,
			want: insertAt(2, 0, "-- bqls:ignore null-comparison\n"),
		},
		"trailing directive position counts UTF-16 units": {
			src:  "SELECT x, '''\nあ''' AS s FROM UNNEST([1]) AS x WHERE x = CAST(NULL AS INT64)",
			line: 1,
			want: insertAt(1, 61, " -- bqls:ignore null-comparison"),
		},
		"trailing directive position counts surrogate pairs as two units": {
			src:  "SELECT x, '''あ\n😀 ''' AS s FROM UNNEST([1]) AS x WHERE x = CAST(NULL AS INT64)",
			line: 1,
			want: insertAt(1, 63, " -- bqls:ignore null-comparison"),
		},
		"no action when the line ends inside another multi-line string": {
			src:  "SELECT '''\nfoo''' AS s, x = CAST(NULL AS INT64) AS b, '''\nbar''' AS t FROM UNNEST([1]) AS x",
			line: 1,
		},
		"no action when the line already ends with a comment": {
			src:  "SELECT x, '''\nfoo''' AS s FROM UNNEST([1]) AS x WHERE x = CAST(NULL AS INT64) -- note",
			line: 1,
		},
	}

	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			h := newTestHandlerWithFile(t, uri, tt.src)
			h.initializeParams.Capabilities.TextDocument.CodeAction.CodeActionLiteralSupport.CodeActionKind.ValueSet = []lsp.CodeActionKind{lsp.CAKQuickFix}

			rng := lsp.Range{Start: lsp.Position{Line: tt.line}, End: lsp.Position{Line: tt.line, Character: 1000}}
			var got []lsp.TextEdit
			for _, a := range h.lintCodeActions(t.Context(), uri, rng) {
				if ca, ok := a.(lsp.CodeAction); ok && ca.Title == "Ignore null-comparison on this line" {
					got = ca.Edit.Changes[string(uri)]
				}
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Fatalf("ignore edit diff (-want +got):\n%s", diff)
			}
			if got == nil {
				return
			}

			newSrc := applyTextEdit(t, tt.src, got[0])
			targetLine := tt.line + strings.Count(got[0].NewText, "\n")
			if !lint.ParseSuppressions(newSrc).Suppressed(rule, targetLine) {
				t.Errorf("%s is not suppressed on line %d of the edited source:\n%s", rule, targetLine, newSrc)
			}
		})
	}
}

// applyTextEdit applies a single-line insertion whose character offset is in
// UTF-16 units, as LSP clients do.
func applyTextEdit(t *testing.T, src string, e lsp.TextEdit) string {
	t.Helper()
	lines := strings.Split(src, "\n")
	line := lines[e.Range.Start.Line]
	units := 0
	offset := -1
	for i, r := range line {
		if units == e.Range.Start.Character {
			offset = i
			break
		}
		units += len(utf16.Encode([]rune{r}))
	}
	if offset < 0 {
		if units != e.Range.Start.Character {
			t.Fatalf("edit character %d is not a UTF-16 boundary of %q", e.Range.Start.Character, line)
		}
		offset = len(line)
	}
	lines[e.Range.Start.Line] = line[:offset] + e.NewText + line[offset:]
	return strings.Join(lines, "\n")
}
