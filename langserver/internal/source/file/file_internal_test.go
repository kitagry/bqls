package file

import (
	"errors"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/kitagry/bqls/langserver/internal/lsp"
)

func TestParseZetaSQLError_ConvertsColumnToByteOffset(t *testing.T) {
	// GoogleSQL reports 1-based columns counted in code points, with tabs
	// expanded to the next multiple of 8. The expected Character is the byte
	// offset of "X" within its line.
	tests := map[string]struct {
		src    string
		errMsg string
		want   lsp.Position
	}{
		"ascii only": {
			src:    "SELECT X",
			errMsg: "msg [at 1:8]",
			want:   lsp.Position{Line: 0, Character: 7},
		},
		"multibyte character before the error": {
			src:    "SELECT 'あ', X",
			errMsg: "msg [at 1:13]",
			want:   lsp.Position{Line: 0, Character: 14},
		},
		"surrogate pair character before the error": {
			src:    "SELECT '😀', X",
			errMsg: "msg [at 1:13]",
			want:   lsp.Position{Line: 0, Character: 15},
		},
		"tab at the beginning of the line": {
			src:    "\tX",
			errMsg: "msg [at 1:9]",
			want:   lsp.Position{Line: 0, Character: 1},
		},
		"tab in the middle of the line": {
			src:    "SELECT\t1, X",
			errMsg: "msg [at 1:12]",
			want:   lsp.Position{Line: 0, Character: 10},
		},
		"consecutive tabs": {
			src:    "SELECT 1,\t\tX",
			errMsg: "msg [at 1:25]",
			want:   lsp.Position{Line: 0, Character: 11},
		},
		"tab and multibyte on the second line": {
			src:    "SELECT 1;\n\t'あ' || X",
			errMsg: "msg [at 2:16]",
			want:   lsp.Position{Line: 1, Character: 10},
		},
		"crlf line endings": {
			src:    "SELECT 1,\r\n\tX",
			errMsg: "msg [at 2:9]",
			want:   lsp.Position{Line: 1, Character: 1},
		},
		"end of script": {
			src:    "SELECT 'あ' +",
			errMsg: "msg [at 1:13]",
			want:   lsp.Position{Line: 0, Character: 14},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			got := parseZetaSQLError(tt.src, errors.New(tt.errMsg))
			if diff := cmp.Diff(Error{Msg: "msg", Position: tt.want}, got); diff != "" {
				t.Errorf("parseZetaSQLError(%q, %q) diff (-want +got):\n%s", tt.src, tt.errMsg, diff)
			}
		})
	}
}

func TestFixFunctions_KeepSourceWhenErrorIsOutOfRange(t *testing.T) {
	const src = "SELECT x"
	tests := map[string]struct {
		parsedErr Error
	}{
		"position beyond the end of the line": {
			parsedErr: Error{Position: lsp.Position{Line: 0, Character: 100}, TermLength: 1, IncompleteColumnName: "x"},
		},
		"position beyond the last line": {
			parsedErr: Error{Position: lsp.Position{Line: 5, Character: 0}, TermLength: 1, IncompleteColumnName: "x"},
		},
	}

	fixes := map[string]func(src string, parsedErr Error) (string, []FixOffset){
		"fixDeclarationError": func(src string, parsedErr Error) (string, []FixOffset) {
			return fixDeclarationError(src, parsedErr, "0")
		},
		"fixErrorToLiteral": fixErrorToLiteral,
		"fixErrorForWhereStatement": func(src string, parsedErr Error) (string, []FixOffset) {
			return fixErrorForWhereStatement(src, nil, parsedErr)
		},
		"fixSelectListMustNotBeEmptyStatement": func(src string, parsedErr Error) (string, []FixOffset) {
			fixed, _, fo := fixSelectListMustNotBeEmptyStatement(src, parsedErr)
			return fixed, fo
		},
		"fixOnlyWithClauseSyntaxError": func(src string, parsedErr Error) (string, []FixOffset) {
			fixed, _, fo := fixOnlyWithClauseSyntaxError(src, parsedErr)
			return fixed, fo
		},
		"fixUnexpectedEndOfScriptWithDeletion": func(src string, parsedErr Error) (string, []FixOffset) {
			fixed, _, fo := fixUnexpectedEndOfScriptWithDeletion(src+" GROUP BY", parsedErr, []string{"GROUP BY"})
			return strings.TrimSuffix(fixed, " GROUP BY"), fo
		},
		"fixUnexpectedEndOfScriptWithCondition": func(src string, parsedErr Error) (string, []FixOffset) {
			fixed, _, fo := fixUnexpectedEndOfScriptWithCondition(src+" WHERE", parsedErr, []string{"WHERE"})
			return strings.TrimSuffix(fixed, " WHERE"), fo
		},
	}

	for fixName, fix := range fixes {
		for name, tt := range tests {
			t.Run(fixName+"/"+name, func(t *testing.T) {
				gotSrc, gotOffsets := fix(src, tt.parsedErr)
				if gotSrc != src || len(gotOffsets) != 0 {
					t.Errorf("expected source to be kept as %q without fix offsets, got %q, %v", src, gotSrc, gotOffsets)
				}
			})
		}
	}
}

func TestFixFunctions_KeepSourceWhenTermOverflowsSource(t *testing.T) {
	const src = "SELECT x"
	parsedErr := Error{Position: lsp.Position{Line: 0, Character: 7}, TermLength: 11, IncompleteColumnName: "my_variable"}

	fixes := map[string]func(src string, parsedErr Error) (string, []FixOffset){
		"fixDeclarationError": func(src string, parsedErr Error) (string, []FixOffset) {
			return fixDeclarationError(src, parsedErr, "0")
		},
		"fixErrorToLiteral": fixErrorToLiteral,
		"fixErrorForWhereStatement": func(src string, parsedErr Error) (string, []FixOffset) {
			return fixErrorForWhereStatement(src, nil, parsedErr)
		},
	}

	for name, fix := range fixes {
		t.Run(name, func(t *testing.T) {
			gotSrc, gotOffsets := fix(src, parsedErr)
			if gotSrc != src || len(gotOffsets) != 0 {
				t.Errorf("expected source to be kept as %q without fix offsets, got %q, %v", src, gotSrc, gotOffsets)
			}
		})
	}
}

func TestAddInformationFunctions_KeepErrorWhenOutOfRange(t *testing.T) {
	const src = "SELECT x"
	tests := map[string]struct {
		add       func(src string, parsedErr Error) Error
		parsedErr Error
	}{
		"addInformationToNotExistInStructError": {
			add:       addInformationToNotExistInStructError,
			parsedErr: Error{Msg: "Field name too_long_name does not exist in STRUCT", Position: lsp.Position{Line: 0, Character: 7}},
		},
		"addInformationToNotFoundInsideTableError": {
			add:       addInformationToNotFoundInsideTableError,
			parsedErr: Error{Msg: "Name too_long_name not found inside t", Position: lsp.Position{Line: 0, Character: 7}},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			got := tt.add(src, tt.parsedErr)
			if diff := cmp.Diff(tt.parsedErr, got); diff != "" {
				t.Errorf("expected error to be unchanged (-want +got):\n%s", diff)
			}
		})
	}
}
