package completion_test

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/kitagry/bqls/langserver/internal/source/completion"
)

func TestCompletionItem_ToLspCompletionItem_TextEditRange(t *testing.T) {
	tests := map[string]struct {
		typedPrefix string
		position    lsp.Position
		wantStart   lsp.Position
	}{
		"ascii prefix": {
			typedPrefix: "my_v",
			position:    lsp.Position{Line: 1, Character: 10},
			wantStart:   lsp.Position{Line: 1, Character: 6},
		},
		// position is in UTF-16 units, so the prefix length must be too.
		"backtick-quoted CJK prefix": {
			typedPrefix: "`あい",
			position:    lsp.Position{Line: 1, Character: 10},
			wantStart:   lsp.Position{Line: 1, Character: 7},
		},
		"prefix with surrogate pair": {
			typedPrefix: "`😀",
			position:    lsp.Position{Line: 0, Character: 10},
			wantStart:   lsp.Position{Line: 0, Character: 7},
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			item := completion.CompletionItem{NewText: "x", TypedPrefix: tt.typedPrefix}
			got := item.ToLspCompletionItem(tt.position, true)
			want := lsp.Range{Start: tt.wantStart, End: tt.position}
			if diff := cmp.Diff(want, got.TextEdit.Range); diff != "" {
				t.Errorf("TextEdit.Range diff (-want +got):\n%s", diff)
			}
		})
	}
}
