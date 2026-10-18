package lint

import (
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"

	"go.yaml.in/yaml/v3"
)

// ConfigFileName is the settings file shared by the CLI and the language server.
const ConfigFileName = ".bqls.yaml"

// FindConfigFile looks for ConfigFileName in dir and its parents, stopping at
// the repository root (a directory containing .git, which is a file in
// worktrees) so that a checkout never picks up settings from outside it.
func FindConfigFile(dir string) (string, bool) {
	for {
		path := filepath.Join(dir, ConfigFileName)
		if info, err := os.Stat(path); err == nil && !info.IsDir() {
			return path, true
		}
		if _, err := os.Stat(filepath.Join(dir, ".git")); err == nil {
			return "", false
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", false
		}
		dir = parent
	}
}

func LoadConfigFile(path string) (Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return Config{}, err
	}
	cfg, err := ParseConfigFile(b)
	if err != nil {
		return Config{}, fmt.Errorf("%s: %w", path, err)
	}
	return cfg, nil
}

// ParseConfigFile reads the "lint" section of a config file. Other top-level
// keys are left for other features.
func ParseConfigFile(b []byte) (Config, error) {
	var doc struct {
		Lint map[string]any `yaml:"lint"`
	}
	if err := yaml.Unmarshal(b, &doc); err != nil {
		return Config{}, err
	}
	// Config ignores unknown keys so that editors can send settings from
	// newer clients, but in a file an unknown key is most likely a typo.
	for _, k := range slices.Sorted(maps.Keys(doc.Lint)) {
		if k != "rules" {
			return Config{}, fmt.Errorf("unknown key %q under lint (valid: rules)", k)
		}
	}
	// Going through JSON gives the file the same schema as the LSP settings.
	j, err := json.Marshal(doc.Lint)
	if err != nil {
		return Config{}, err
	}
	var cfg Config
	if err := json.Unmarshal(j, &cfg); err != nil {
		return Config{}, err
	}
	return cfg, nil
}
