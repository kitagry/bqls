package langserver

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/google/go-cmp/cmp"
	"github.com/kitagry/bqls/langserver/internal/bigquery/mock_bigquery"
	"github.com/kitagry/bqls/langserver/internal/lint"
	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/kitagry/bqls/langserver/internal/source"
	"github.com/kitagry/bqls/langserver/internal/source/file"
	"github.com/sirupsen/logrus"
)

func newTestHandlerWithFile(t *testing.T, uri lsp.DocumentURI, src string) *Handler {
	t.Helper()
	ctrl := gomock.NewController(t)
	p := source.NewProjectWithBQClient("/", mock_bigquery.NewMockClient(ctrl), logrus.New())
	if err := p.UpdateFile(uri, src, 1); err != nil {
		t.Fatal(err)
	}
	return &Handler{project: p, logger: logrus.New()}
}

func TestHandler_diagnose(t *testing.T) {
	const uri lsp.DocumentURI = "file:///a.sql"
	tests := map[string]struct {
		src  string
		cfg  lint.Config
		want []lsp.Diagnostic
	}{
		"lint diagnostic carries rule code and source": {
			src: "SELECT x FROM UNNEST([1]) AS x WHERE x = CAST(NULL AS INT64)",
			want: []lsp.Diagnostic{{
				Range:    lsp.Range{Start: lsp.Position{Line: 0, Character: 37}, End: lsp.Position{Line: 0, Character: 60}},
				Severity: lsp.Warning,
				Code:     "null-comparison",
				Source:   "bqls-lint",
				Message:  "Comparison with NULL using = always yields NULL; use IS NULL",
			}},
		},
		"lint rule disabled by settings": {
			src:  "SELECT x FROM UNNEST([1]) AS x WHERE x = CAST(NULL AS INT64)",
			cfg:  lint.Config{Rules: map[string]bool{"null-comparison": false}},
			want: []lsp.Diagnostic{},
		},
		"analysis errors only": {
			src: "SELECT y",
			want: []lsp.Diagnostic{{
				Range:    lsp.Range{Start: lsp.Position{Line: 0, Character: 7}, End: lsp.Position{Line: 0, Character: 8}},
				Severity: lsp.Error,
				Message:  "Unrecognized name: y",
			}},
		},
	}

	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			h := newTestHandlerWithFile(t, uri, tt.src)
			h.initializeParams.InitializationOptions.Lint = tt.cfg

			got, err := h.diagnose(t.Context(), uri)
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(map[lsp.DocumentURI][]lsp.Diagnostic{uri: tt.want}, got); diff != "" {
				t.Errorf("diagnose() diff (-want +got):\n%s", diff)
			}
		})
	}
}

func TestConvertErrorsToDiagnostics_ReturnsRangeInUTF16CodeUnits(t *testing.T) {
	// file.Error positions are byte offsets within the line, while LSP
	// characters are UTF-16 code units by default.
	tests := map[string]struct {
		src       string
		err       file.Error
		wantRange lsp.Range
	}{
		"ascii only": {
			src:       "SELECT x",
			err:       file.Error{Msg: "msg", Position: lsp.Position{Line: 0, Character: 7}, TermLength: 1},
			wantRange: lsp.Range{Start: lsp.Position{Line: 0, Character: 7}, End: lsp.Position{Line: 0, Character: 8}},
		},
		"multibyte character before the error": {
			src:       "SELECT 'あ', x",
			err:       file.Error{Msg: "msg", Position: lsp.Position{Line: 0, Character: 14}, TermLength: 1},
			wantRange: lsp.Range{Start: lsp.Position{Line: 0, Character: 12}, End: lsp.Position{Line: 0, Character: 13}},
		},
		"surrogate pair character before the error": {
			src:       "SELECT '😀', x",
			err:       file.Error{Msg: "msg", Position: lsp.Position{Line: 0, Character: 15}, TermLength: 1},
			wantRange: lsp.Range{Start: lsp.Position{Line: 0, Character: 13}, End: lsp.Position{Line: 0, Character: 14}},
		},
		"tab before the error": {
			src:       "SELECT 1;\n\tx",
			err:       file.Error{Msg: "msg", Position: lsp.Position{Line: 1, Character: 1}, TermLength: 1},
			wantRange: lsp.Range{Start: lsp.Position{Line: 1, Character: 1}, End: lsp.Position{Line: 1, Character: 2}},
		},
		"multibyte term": {
			src:       "SELECT あいう",
			err:       file.Error{Msg: "msg", Position: lsp.Position{Line: 0, Character: 7}, TermLength: 9},
			wantRange: lsp.Range{Start: lsp.Position{Line: 0, Character: 7}, End: lsp.Position{Line: 0, Character: 10}},
		},
		"term overflowing the end of the line": {
			src:       "SELECT 'あ'",
			err:       file.Error{Msg: "msg", Position: lsp.Position{Line: 0, Character: 7}, TermLength: 100},
			wantRange: lsp.Range{Start: lsp.Position{Line: 0, Character: 7}, End: lsp.Position{Line: 0, Character: 10}},
		},
		"line beyond the end of the source": {
			src:       "SELECT 1",
			err:       file.Error{Msg: "msg", Position: lsp.Position{Line: 3, Character: 2}, TermLength: 1},
			wantRange: lsp.Range{Start: lsp.Position{Line: 3, Character: 2}, End: lsp.Position{Line: 3, Character: 3}},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			got := convertErrorsToDiagnostics(tt.src, []file.Error{tt.err})
			want := []lsp.Diagnostic{{Range: tt.wantRange, Message: "msg", Severity: lsp.Error}}
			if diff := cmp.Diff(want, got); diff != "" {
				t.Errorf("convertErrorsToDiagnostics(%q) diff (-want +got):\n%s", tt.src, diff)
			}
		})
	}
}
