package lintcli

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
)

const disableUnusedCTE = "lint:\n  rules:\n    unused-cte: false\n"

func TestRun_ConfigFile(t *testing.T) {
	tests := map[string]struct {
		files      map[string]string
		args       []string
		stdin      string
		wantCode   int
		wantStdout string
	}{
		"config at the repository root applies": {
			files: map[string]string{".git/HEAD": "", ".bqls.yaml": disableUnusedCTE, "q/a.sql": unusedCTESQL},
			args:  []string{"q/a.sql"},
		},
		"flags take precedence over the config file": {
			files:      map[string]string{".git/HEAD": "", ".bqls.yaml": disableUnusedCTE, "a.sql": unusedCTESQL},
			args:       []string{"--enable", "unused-cte", "a.sql"},
			wantCode:   1,
			wantStdout: "a.sql:1:6: info: CTE `a` is defined but never used (unused-cte)\n",
		},
		"each file uses its nearest config": {
			files: map[string]string{
				".git/HEAD":    "",
				"q/.bqls.yaml": disableUnusedCTE,
				"q/a.sql":      unusedCTESQL,
				"b.sql":        unusedCTESQL,
			},
			args:       []string{"q/a.sql", "b.sql"},
			wantCode:   1,
			wantStdout: "b.sql:1:6: info: CTE `a` is defined but never used (unused-cte)\n",
		},
		"config outside the repository is ignored": {
			files:      map[string]string{".bqls.yaml": disableUnusedCTE, "repo/.git/HEAD": "", "repo/a.sql": unusedCTESQL},
			args:       []string{"repo/a.sql"},
			wantCode:   1,
			wantStdout: "repo/a.sql:1:6: info: CTE `a` is defined but never used (unused-cte)\n",
		},
		"--config overrides the lookup": {
			files: map[string]string{
				".git/HEAD":      "",
				".bqls.yaml":     "lint:\n  rules:\n    unused-cte: true\n",
				"conf/bqls.yaml": disableUnusedCTE,
				"a.sql":          unusedCTESQL,
			},
			args: []string{"--config", "conf/bqls.yaml", "a.sql"},
		},
		"stdin uses the config found from the working directory": {
			files: map[string]string{".git/HEAD": "", ".bqls.yaml": disableUnusedCTE},
			args:  []string{"-"},
			stdin: unusedCTESQL,
		},
	}
	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			t.Chdir(t.TempDir())
			writeFiles(t, tt.files)

			got := runWith(t, mockFactory(t), tt.stdin, tt.args...)
			if got.code != tt.wantCode {
				t.Errorf("exit code = %d, want %d (stdout: %s, stderr: %s)", got.code, tt.wantCode, got.stdout, got.stderr)
			}
			if diff := cmp.Diff(tt.wantStdout, got.stdout); diff != "" {
				t.Errorf("stdout diff (-want +got):\n%s", diff)
			}
		})
	}
}

func TestRun_ConfigFileErrors(t *testing.T) {
	tests := map[string]struct {
		files      map[string]string
		args       []string
		wantStderr []string
	}{
		"invalid yaml": {
			files:      map[string]string{".git/HEAD": "", ".bqls.yaml": "lint: [\n", "a.sql": cleanSQL},
			args:       []string{"a.sql"},
			wantStderr: []string{".bqls.yaml"},
		},
		"unknown rule": {
			files:      map[string]string{".git/HEAD": "", ".bqls.yaml": "lint:\n  rules:\n    no-such-rule: false\n", "a.sql": cleanSQL},
			args:       []string{"a.sql"},
			wantStderr: []string{".bqls.yaml", `unknown rule "no-such-rule"`},
		},
		"options for a rule without options": {
			files:      map[string]string{".git/HEAD": "", ".bqls.yaml": "lint:\n  rules:\n    unused-cte:\n      max: 1\n", "a.sql": cleanSQL},
			args:       []string{"a.sql"},
			wantStderr: []string{".bqls.yaml", "takes no options"},
		},
		"missing --config file": {
			files:      map[string]string{"a.sql": cleanSQL},
			args:       []string{"--config", "missing.yaml", "a.sql"},
			wantStderr: []string{"missing.yaml"},
		},
	}
	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			t.Chdir(t.TempDir())
			writeFiles(t, tt.files)

			got := runWith(t, mockFactory(t), "", tt.args...)
			if got.code != 2 {
				t.Errorf("exit code = %d, want 2 (stdout: %s, stderr: %s)", got.code, got.stdout, got.stderr)
			}
			for _, want := range tt.wantStderr {
				if !strings.Contains(got.stderr, want) {
					t.Errorf("stderr = %q, want it to contain %q", got.stderr, filepath.FromSlash(want))
				}
			}
		})
	}
}
