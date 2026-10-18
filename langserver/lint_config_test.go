package langserver

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kitagry/bqls/langserver/internal/lint"
	"github.com/kitagry/bqls/langserver/internal/lsp"
)

const nullComparisonQuery = "SELECT x FROM UNNEST([1]) AS x WHERE x = CAST(NULL AS INT64)"

func writeConfigFile(t *testing.T, path, content string, mtime time.Time) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	// The handler reloads on a changed mtime, which a fast test could
	// otherwise leave unchanged.
	if err := os.Chtimes(path, mtime, mtime); err != nil {
		t.Fatal(err)
	}
}

func lintRulesReported(t *testing.T, h *Handler, uri lsp.DocumentURI) []string {
	t.Helper()
	got, err := h.diagnose(t.Context(), uri)
	if err != nil {
		t.Fatal(err)
	}
	var rules []string
	for _, d := range got[uri] {
		if d.Source == lintDiagnosticSource {
			rules = append(rules, d.Code)
		}
	}
	return rules
}

func TestHandler_diagnose_ConfigFile(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, ".git"), 0o755); err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(root, lint.ConfigFileName)
	uri := lsp.DocumentURI("file://" + filepath.ToSlash(filepath.Join(root, "a.sql")))
	base := time.Now().Add(-time.Hour)

	writeConfigFile(t, configPath, "lint:\n  rules:\n    null-comparison: false\n", base)
	h := newTestHandlerWithFile(t, uri, nullComparisonQuery)
	if got := lintRulesReported(t, h, uri); len(got) != 0 {
		t.Errorf("rules reported = %v, want none when the config file disables the rule", got)
	}

	h.initializeParams.InitializationOptions.Lint = lint.Config{Rules: map[string]bool{"null-comparison": true}}
	if got := lintRulesReported(t, h, uri); len(got) != 1 {
		t.Errorf("rules reported = %v, want null-comparison since LSP settings take precedence", got)
	}
	h.initializeParams.InitializationOptions.Lint = lint.Config{}

	writeConfigFile(t, configPath, "lint:\n  rules: {}\n", base.Add(time.Minute))
	if got := lintRulesReported(t, h, uri); len(got) != 1 {
		t.Errorf("rules reported = %v, want null-comparison after the config file changed", got)
	}

	writeConfigFile(t, configPath, "lint:\n  rules:\n    null-comparison: false\n", base.Add(2*time.Minute))
	if got := lintRulesReported(t, h, uri); len(got) != 0 {
		t.Errorf("rules reported = %v, want none after the rule was disabled again", got)
	}

	if err := os.Remove(configPath); err != nil {
		t.Fatal(err)
	}
	if got := lintRulesReported(t, h, uri); len(got) != 1 {
		t.Errorf("rules reported = %v, want null-comparison after the config file was removed", got)
	}
}

func TestHandler_diagnose_InvalidConfigFileIsIgnored(t *testing.T) {
	tests := map[string]string{
		"invalid yaml": "lint: [\n",
		"unknown rule": "lint:\n  rules:\n    no-such-rule: false\n    null-comparison: false\n",
	}
	for n, content := range tests {
		t.Run(n, func(t *testing.T) {
			root := t.TempDir()
			if err := os.Mkdir(filepath.Join(root, ".git"), 0o755); err != nil {
				t.Fatal(err)
			}
			writeConfigFile(t, filepath.Join(root, lint.ConfigFileName), content, time.Now())
			uri := lsp.DocumentURI("file://" + filepath.ToSlash(filepath.Join(root, "a.sql")))

			h := newTestHandlerWithFile(t, uri, nullComparisonQuery)
			if got := lintRulesReported(t, h, uri); len(got) != 1 {
				t.Errorf("rules reported = %v, want the defaults when the config file is invalid", got)
			}
		})
	}
}
