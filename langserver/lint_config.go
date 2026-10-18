package langserver

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/kitagry/bqls/langserver/internal/lint"
	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/sirupsen/logrus"
)

// lintConfigFor returns the lint settings for uri: the LSP settings applied on
// top of the config file nearest to it. An unreadable or invalid config file
// is logged and ignored, so that editing it never breaks diagnostics.
func (h *Handler) lintConfigFor(uri lsp.DocumentURI) lint.Config {
	settings := h.lintConfig()
	if !uri.IsFile() {
		return settings
	}
	path, err := uri.FilePath()
	if err != nil {
		return settings
	}
	configPath, ok := lint.FindConfigFile(filepath.Dir(filepath.FromSlash(path)))
	if !ok {
		return settings
	}
	fileCfg, err := h.lintConfigFiles.load(configPath, h.logger)
	if err != nil {
		return settings
	}
	cfg, err := fileCfg.Override(settings)
	if err != nil {
		h.logger.Warnf("failed to apply lint settings on top of %s: %v", configPath, err)
		return settings
	}
	return cfg
}

// lintConfigFiles caches config files by path and reloads one when its
// modification time or size changes, because diagnostics run on every edit
// and the client does not tell the server about changes to the file.
type lintConfigFiles struct {
	mu    sync.Mutex
	files map[string]lintConfigFile
}

type lintConfigFile struct {
	modTime time.Time
	size    int64
	cfg     lint.Config
	err     error
}

func (c *lintConfigFiles) load(path string, logger *logrus.Logger) (lint.Config, error) {
	info, err := os.Stat(path)
	if err != nil {
		return lint.Config{}, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if f, ok := c.files[path]; ok && f.modTime.Equal(info.ModTime()) && f.size == info.Size() {
		return f.cfg, f.err
	}

	cfg, err := lint.LoadConfigFile(path)
	if err == nil {
		if verr := cfg.Validate(lint.Analyzers); verr != nil {
			cfg, err = lint.Config{}, fmt.Errorf("%s: %w", path, verr)
		}
	}
	// Logged only on (re)load so that a broken file is reported once per
	// change rather than on every keystroke.
	if err != nil {
		logger.Warnf("ignoring lint config file: %v", err)
	}
	if c.files == nil {
		c.files = make(map[string]lintConfigFile)
	}
	c.files[path] = lintConfigFile{modTime: info.ModTime(), size: info.Size(), cfg: cfg, err: err}
	return cfg, err
}
