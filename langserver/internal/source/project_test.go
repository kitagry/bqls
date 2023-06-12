package source

import (
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/kitagry/bqls/langserver/internal/lsp"
)

func TestParseZetaSQLError(t *testing.T) {
	tests := map[string]struct {
		err    error
		expect Error
	}{
		"parse ZetaSQL error": {
			err: fmt.Errorf("INVALID_ARGUMENT: Syntax error: Unclosed identifier literal [at 1:28]"),
			expect: Error{
				Position: lsp.Position{
					Line:      0,
					Character: 27,
				},
				Msg: "INVALID_ARGUMENT: Syntax error: Unclosed identifier literal",
			},
		},
	}

	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			got := parseZetaSQLError(tt.err)
			if diff := cmp.Diff(tt.expect, got); diff != "" {
				t.Errorf("parseZetaSQLError result diff (-expect, +got)\n%s", diff)
			}
		})
	}
}
