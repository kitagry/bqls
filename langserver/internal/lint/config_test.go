package lint_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	googlesql "github.com/goccy/go-googlesql"
	"github.com/google/go-cmp/cmp"
	"github.com/kitagry/bqls/langserver/internal/lint"
	"github.com/kitagry/bqls/langserver/internal/source/file"
)

type thresholdOptions struct {
	Max int `json:"max"`
}

// thresholdReporter reports once with the configured max as the message.
func thresholdReporter(name string) *lint.Analyzer {
	return &lint.Analyzer{
		Name:           name,
		DefaultEnabled: true,
		NewOptions:     func() any { return &thresholdOptions{Max: 10} },
		Run: func(pass *lint.Pass) {
			opts := pass.Options.(*thresholdOptions)
			pass.Report(lint.Diagnostic{Message: strings.Repeat("x", opts.Max)})
		},
	}
}

func TestConfig_JSON_RuleOptions(t *testing.T) {
	var got lint.Config
	src := `{"rules": {"a": false, "b": {"enabled": true, "max": 3}, "c": {"max": 1}}}`
	if err := json.Unmarshal([]byte(src), &got); err != nil {
		t.Fatal(err)
	}
	want := lint.Config{
		Rules: map[string]bool{"a": false, "b": true},
		Options: map[string]json.RawMessage{
			"b": json.RawMessage(`{"max":3}`),
			"c": json.RawMessage(`{"max":1}`),
		},
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("Config diff (-want +got):\n%s", diff)
	}
}

func TestConfig_JSON_Invalid(t *testing.T) {
	tests := map[string]string{
		"rule is a number":      `{"rules": {"a": 1}}`,
		"enabled is not a bool": `{"rules": {"a": {"enabled": "yes"}}}`,
	}
	for n, src := range tests {
		t.Run(n, func(t *testing.T) {
			var got lint.Config
			if err := json.Unmarshal([]byte(src), &got); err == nil {
				t.Errorf("Unmarshal(%s) = %+v, want error", src, got)
			}
		})
	}
}

func TestRun_Options(t *testing.T) {
	tests := map[string]struct {
		cfg  lint.Config
		want string
	}{
		"defaults without options": {want: strings.Repeat("x", 10)},
		"configured options override defaults": {
			cfg:  lint.Config{Options: map[string]json.RawMessage{"a": json.RawMessage(`{"max":3}`)}},
			want: "xxx",
		},
	}
	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			f := file.ParsedFile{Src: "SELECT 1", Node: &googlesql.ASTScript{}}
			got := lint.Run(t.Context(), f, tt.cfg, lint.Env{}, thresholdReporter("a"))
			if len(got) != 1 || got[0].Message != tt.want {
				t.Errorf("Run() = %+v, want one diagnostic with message %q", got, tt.want)
			}
		})
	}
}

func TestConfig_Validate(t *testing.T) {
	analyzers := []*lint.Analyzer{lineReporter("plain", true), thresholdReporter("threshold")}
	tests := map[string]struct {
		cfg     lint.Config
		wantErr string
	}{
		"known rules and options": {
			cfg: lint.Config{
				Rules:   map[string]bool{"plain": false},
				Options: map[string]json.RawMessage{"threshold": json.RawMessage(`{"max":3}`)},
			},
		},
		"unknown rule": {
			cfg:     lint.Config{Rules: map[string]bool{"nope": false}},
			wantErr: `unknown rule "nope"`,
		},
		"unknown rule with options": {
			cfg:     lint.Config{Options: map[string]json.RawMessage{"nope": json.RawMessage(`{}`)}},
			wantErr: `unknown rule "nope"`,
		},
		"options for a rule without options": {
			cfg:     lint.Config{Options: map[string]json.RawMessage{"plain": json.RawMessage(`{"max":3}`)}},
			wantErr: `rule "plain" takes no options`,
		},
		"empty options for a rule without options": {
			cfg: lint.Config{Options: map[string]json.RawMessage{"plain": json.RawMessage(`{}`)}},
		},
		"unknown option": {
			cfg:     lint.Config{Options: map[string]json.RawMessage{"threshold": json.RawMessage(`{"min":3}`)}},
			wantErr: `rule "threshold"`,
		},
		"option of wrong type": {
			cfg:     lint.Config{Options: map[string]json.RawMessage{"threshold": json.RawMessage(`{"max":"3"}`)}},
			wantErr: `rule "threshold"`,
		},
	}
	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			err := tt.cfg.Validate(analyzers)
			if tt.wantErr == "" {
				if err != nil {
					t.Errorf("Validate() = %v, want nil", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("Validate() = %v, want error containing %q", err, tt.wantErr)
			}
		})
	}
}

func TestConfig_Override(t *testing.T) {
	base := lint.Config{
		Rules: map[string]bool{"a": false, "b": true},
		Options: map[string]json.RawMessage{
			"b": json.RawMessage(`{"max":1,"min":2}`),
			"c": json.RawMessage(`{"max":1}`),
		},
	}
	over := lint.Config{
		Rules: map[string]bool{"a": true},
		Options: map[string]json.RawMessage{
			"b": json.RawMessage(`{"max":5}`),
			"d": json.RawMessage(`{"max":7}`),
		},
	}
	want := lint.Config{
		Rules: map[string]bool{"a": true, "b": true},
		Options: map[string]json.RawMessage{
			"b": json.RawMessage(`{"max":5,"min":2}`),
			"c": json.RawMessage(`{"max":1}`),
			"d": json.RawMessage(`{"max":7}`),
		},
	}
	got, err := base.Override(over)
	if err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("Override() diff (-want +got):\n%s", diff)
	}
	// Override must not write through to either input's maps.
	if base.Rules["a"] || len(base.Options) != 2 || string(base.Options["b"]) != `{"max":1,"min":2}` {
		t.Errorf("Override() modified base: %+v", base)
	}
}

func TestConfig_Equal(t *testing.T) {
	a := lint.Config{Rules: map[string]bool{"a": false}, Options: map[string]json.RawMessage{"b": json.RawMessage(`{"max":1}`)}}
	tests := map[string]struct {
		b    lint.Config
		want bool
	}{
		"same":              {b: lint.Config{Rules: map[string]bool{"a": false}, Options: map[string]json.RawMessage{"b": json.RawMessage(`{"max":1}`)}}, want: true},
		"different rules":   {b: lint.Config{Rules: map[string]bool{"a": true}, Options: map[string]json.RawMessage{"b": json.RawMessage(`{"max":1}`)}}},
		"different options": {b: lint.Config{Rules: map[string]bool{"a": false}, Options: map[string]json.RawMessage{"b": json.RawMessage(`{"max":2}`)}}},
		"missing options":   {b: lint.Config{Rules: map[string]bool{"a": false}}},
	}
	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			if got := a.Equal(tt.b); got != tt.want {
				t.Errorf("Equal() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParseConfigFile(t *testing.T) {
	tests := map[string]struct {
		src     string
		want    lint.Config
		wantErr string
	}{
		"rules with options": {
			src: `lint:
  rules:
    unused-cte: false
    bytes-processed-threshold:
      enabled: true
      max_bytes: 10GB
`,
			want: lint.Config{
				Rules:   map[string]bool{"unused-cte": false, "bytes-processed-threshold": true},
				Options: map[string]json.RawMessage{"bytes-processed-threshold": json.RawMessage(`{"max_bytes":"10GB"}`)},
			},
		},
		"empty file":                 {src: ""},
		"other top-level keys":       {src: "project_id: p\n"},
		"unknown key under lint":     {src: "lint:\n  rule:\n    a: false\n", wantErr: `unknown key "rule"`},
		"invalid yaml":               {src: "lint: [\n", wantErr: "yaml"},
		"rule value of invalid type": {src: "lint:\n  rules:\n    a: 1\n", wantErr: "a"},
	}
	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			got, err := lint.ParseConfigFile([]byte(tt.src))
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Errorf("ParseConfigFile() error = %v, want it to contain %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("ParseConfigFile() diff (-want +got):\n%s", diff)
			}
		})
	}
}

func TestLoadConfigFile_ErrorMentionsPath(t *testing.T) {
	path := filepath.Join(t.TempDir(), lint.ConfigFileName)
	if err := os.WriteFile(path, []byte("lint: [\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := lint.LoadConfigFile(path); err == nil || !strings.Contains(err.Error(), path) {
		t.Errorf("LoadConfigFile() error = %v, want it to mention %s", err, path)
	}
}

func TestFindConfigFile(t *testing.T) {
	touch := func(t *testing.T, path string) {
		t.Helper()
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, nil, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	tests := map[string]struct {
		files []string
		dirs  []string
		start string
		want  string
	}{
		"config in the start directory": {
			files: []string{"repo/q/.bqls.yaml"},
			dirs:  []string{"repo/.git"},
			start: "repo/q",
			want:  "repo/q/.bqls.yaml",
		},
		"config at the repository root": {
			files: []string{"repo/.bqls.yaml"},
			dirs:  []string{"repo/.git", "repo/q/sub"},
			start: "repo/q/sub",
			want:  "repo/.bqls.yaml",
		},
		"nearest config wins": {
			files: []string{"repo/.bqls.yaml", "repo/q/.bqls.yaml"},
			dirs:  []string{"repo/.git", "repo/q/sub"},
			start: "repo/q/sub",
			want:  "repo/q/.bqls.yaml",
		},
		"lookup stops at the repository root": {
			files: []string{".bqls.yaml"},
			dirs:  []string{"repo/.git", "repo/q"},
			start: "repo/q",
		},
		"a .git file marks a worktree root": {
			files: []string{".bqls.yaml", "repo/.git"},
			dirs:  []string{"repo/q"},
			start: "repo/q",
		},
	}
	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			root := t.TempDir()
			for _, f := range tt.files {
				touch(t, filepath.Join(root, f))
			}
			for _, d := range tt.dirs {
				if err := os.MkdirAll(filepath.Join(root, d), 0o755); err != nil {
					t.Fatal(err)
				}
			}

			got, ok := lint.FindConfigFile(filepath.Join(root, tt.start))
			want := ""
			if tt.want != "" {
				want = filepath.Join(root, tt.want)
			}
			if got != want || ok != (tt.want != "") {
				t.Errorf("FindConfigFile() = (%q, %v), want (%q, %v)", got, ok, want, tt.want != "")
			}
		})
	}
}
