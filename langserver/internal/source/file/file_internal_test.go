package file

import (
	"errors"
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
