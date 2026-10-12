package source

import (
	"context"
	"fmt"
	"strings"

	bq "cloud.google.com/go/bigquery"
	"github.com/kitagry/bqls/langserver/internal/lint"
	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/kitagry/bqls/langserver/internal/source/file"
)

type Diagnostics struct {
	Errors []file.Error
	// ErrorFixes holds quick fixes for Errors, keyed by index into Errors.
	ErrorFixes map[int][]lint.SuggestedFix
	Lint       []lint.Diagnostic
}

// Diagnose analyzes uri once and derives both analysis errors and lint
// diagnostics from that single parse. It returns false when uri is not open.
func (p *Project) Diagnose(ctx context.Context, uri lsp.DocumentURI, cfg lint.Config) (Diagnostics, bool) {
	sql := p.cache.Get(uri)
	if sql == nil {
		return Diagnostics{}, false
	}

	parsedFile := p.analyzer.ParseFile(uri, sql.RawText)
	defer parsedFile.Close()

	env := lint.Env{LookupTable: p.lookupTable}
	return Diagnostics{
		Errors:     parsedFile.Errors,
		ErrorFixes: errorFixes(parsedFile),
		Lint:       lint.Run(ctx, parsedFile, cfg, env, lint.Analyzers...),
	}, true
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
