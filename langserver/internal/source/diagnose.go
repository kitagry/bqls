package source

import (
	"context"
	"fmt"
	"maps"
	"strings"
	"sync"

	bq "cloud.google.com/go/bigquery"
	"github.com/kitagry/bqls/langserver/internal/lint"
	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/kitagry/bqls/langserver/internal/source/file"
)

// Diagnostics may be shared with other callers through the cache, so callers
// must treat its slices and maps as read-only.
type Diagnostics struct {
	Errors []file.Error
	// ErrorFixes holds quick fixes for Errors, keyed by index into Errors.
	ErrorFixes map[int][]lint.SuggestedFix
	Lint       []lint.Diagnostic
}

// diagnoseEntry memoizes one Diagnose result. Its mutex is held while
// computing so concurrent callers for the same input wait instead of
// re-analyzing.
type diagnoseEntry struct {
	src   string
	rules map[string]bool

	mu     sync.Mutex
	done   bool
	result Diagnostics
}

// Diagnose analyzes uri once and derives both analysis errors and lint
// diagnostics from that single parse. It returns false when uri is not open.
// Results are reused until the text or cfg changes, because code actions
// request them on every cursor move.
func (p *Project) Diagnose(ctx context.Context, uri lsp.DocumentURI, cfg lint.Config) (Diagnostics, bool) {
	sql := p.cache.Get(uri)
	if sql == nil {
		return Diagnostics{}, false
	}

	e := p.diagnoseEntry(uri, sql.RawText, cfg)
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.done {
		return e.result, true
	}
	result := p.diagnose(ctx, uri, sql.RawText, cfg)
	// A cancelled run may have skipped table lookups, so its result is incomplete.
	if ctx.Err() == nil {
		e.result, e.done = result, true
	}
	return result, true
}

func (p *Project) diagnoseEntry(uri lsp.DocumentURI, src string, cfg lint.Config) *diagnoseEntry {
	p.diagnoseMu.Lock()
	defer p.diagnoseMu.Unlock()
	// DeleteFile removes the text before taking diagnoseMu, so this check
	// keeps a run that raced with closing the file from re-inserting an entry.
	if p.cache.Get(uri) == nil {
		return &diagnoseEntry{}
	}
	if e, ok := p.diagnoseCache[uri]; ok && e.src == src && maps.Equal(e.rules, cfg.Rules) {
		return e
	}
	// Clone so a caller mutating its config map cannot change the cache key.
	e := &diagnoseEntry{src: src, rules: maps.Clone(cfg.Rules)}
	p.diagnoseCache[uri] = e
	return e
}

// InvalidateDiagnostics drops the cached Diagnose result for uri. Table
// metadata fetch failures are cached with the result, so callers use this
// when the user asks for a fresh check without editing, e.g. on save.
func (p *Project) InvalidateDiagnostics(uri lsp.DocumentURI) {
	p.diagnoseMu.Lock()
	defer p.diagnoseMu.Unlock()
	delete(p.diagnoseCache, uri)
}

func (p *Project) diagnose(ctx context.Context, uri lsp.DocumentURI, src string, cfg lint.Config) Diagnostics {
	parsedFile := p.analyzer.ParseFile(uri, src)
	defer parsedFile.Close()

	env := lint.Env{LookupTable: func(ctx context.Context, path string) (*bq.TableMetadata, error) {
		if meta, ok := parsedFile.TableMetadata(path); ok {
			return meta, nil
		}
		return p.lookupTable(ctx, path)
	}}
	return Diagnostics{
		Errors:     parsedFile.Errors,
		ErrorFixes: errorFixes(parsedFile),
		Lint:       lint.Run(ctx, parsedFile, cfg, env, lint.Analyzers...),
	}
}

func (p *Project) lookupTable(ctx context.Context, path string) (*bq.TableMetadata, error) {
	parts := strings.Split(path, ".")
	if len(parts) != 3 {
		return nil, fmt.Errorf("table path must be project.dataset.table: %s", path)
	}
	return p.bqClient.GetTableMetadata(ctx, parts[0], parts[1], parts[2])
}

func errorFixes(f file.ParsedFile) map[int][]lint.SuggestedFix {
	var result map[int][]lint.SuggestedFix
	for i, e := range f.Errors {
		fix, ok := lint.LiteralNullComparisonFix(f, e)
		if !ok {
			continue
		}
		if result == nil {
			result = make(map[int][]lint.SuggestedFix)
		}
		result[i] = append(result[i], fix)
	}
	return result
}
