package lint_test

import (
	"encoding/json"
	"fmt"
	"testing"

	googlesql "github.com/goccy/go-googlesql"
	"github.com/google/go-cmp/cmp"
	"github.com/kitagry/bqls/langserver/internal/lint"
	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/kitagry/bqls/langserver/internal/source/file"
)

// lineReporter reports a diagnostic on each given line, independent of the AST.
func lineReporter(name string, defaultEnabled bool, lines ...int) *lint.Analyzer {
	return &lint.Analyzer{
		Name:           name,
		Doc:            "test analyzer",
		DefaultEnabled: defaultEnabled,
		Run: func(pass *lint.Pass) {
			for _, l := range lines {
				pass.Report(lint.Diagnostic{
					Range:    lsp.Range{Start: lsp.Position{Line: l}, End: lsp.Position{Line: l, Character: 1}},
					Message:  name,
					Severity: lint.SeverityWarning,
				})
			}
		},
	}
}

func TestRun(t *testing.T) {
	tests := map[string]struct {
		src       string
		errs      []file.Error
		cfg       lint.Config
		analyzers []*lint.Analyzer
		want      []string
	}{
		"default enabled analyzer runs": {
			src:       "SELECT 1",
			analyzers: []*lint.Analyzer{lineReporter("a", true, 0)},
			want:      []string{"a@0"},
		},
		"default disabled analyzer does not run": {
			src:       "SELECT 1",
			analyzers: []*lint.Analyzer{lineReporter("a", false, 0)},
			want:      nil,
		},
		"config disables default enabled analyzer": {
			src:       "SELECT 1",
			cfg:       lint.Config{Rules: map[string]bool{"a": false}},
			analyzers: []*lint.Analyzer{lineReporter("a", true, 0), lineReporter("b", true, 0)},
			want:      []string{"b@0"},
		},
		"config enables default disabled analyzer": {
			src:       "SELECT 1",
			cfg:       lint.Config{Rules: map[string]bool{"a": true}},
			analyzers: []*lint.Analyzer{lineReporter("a", false, 0)},
			want:      []string{"a@0"},
		},
		"suppressed diagnostics are dropped": {
			src:       "SELECT 1\n-- bqls:ignore a\nSELECT 2\nSELECT 3",
			analyzers: []*lint.Analyzer{lineReporter("a", true, 2, 3), lineReporter("b", true, 2)},
			want:      []string{"a@3", "b@2"},
		},
		"file with analysis errors is not linted": {
			src:       "SELECT 1",
			errs:      []file.Error{{Msg: "error"}},
			analyzers: []*lint.Analyzer{lineReporter("a", true, 0)},
			want:      nil,
		},
	}

	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			f := file.ParsedFile{Src: tt.src, Node: &googlesql.ASTScript{}, Errors: tt.errs}
			diags := lint.Run(t.Context(), f, tt.cfg, lint.Env{}, tt.analyzers...)

			var got []string
			for _, d := range diags {
				got = append(got, fmt.Sprintf("%s@%d", d.Rule, d.Range.Start.Line))
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("Run() diff (-want +got):\n%s", diff)
			}
		})
	}
}

func TestRun_NoAST(t *testing.T) {
	f := file.ParsedFile{Src: "SELECT 1"}
	if got := lint.Run(t.Context(), f, lint.Config{}, lint.Env{}, lineReporter("a", true, 0)); len(got) != 0 {
		t.Errorf("Run() = %v, want no diagnostics without an AST", got)
	}
}

func TestConfig_JSON(t *testing.T) {
	var got lint.Config
	if err := json.Unmarshal([]byte(`{"rules": {"null-comparison": false}}`), &got); err != nil {
		t.Fatal(err)
	}
	want := lint.Config{Rules: map[string]bool{"null-comparison": false}}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("Config diff (-want +got):\n%s", diff)
	}
}
