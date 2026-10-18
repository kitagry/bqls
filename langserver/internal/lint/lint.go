// Package lint provides a small analysis framework, modeled on
// golang.org/x/tools/go/analysis, for reporting style and correctness
// warnings on SQL that already analyzes without errors.
package lint

import (
	"context"

	bq "cloud.google.com/go/bigquery"
	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/kitagry/bqls/langserver/internal/source/file"
)

// Severity is restricted to non-error levels so that lint findings never look
// like analysis failures.
type Severity = lsp.DiagnosticSeverity

const (
	SeverityWarning     Severity = lsp.Warning
	SeverityInformation Severity = lsp.Information
)

// Analyzer is a single lint rule.
type Analyzer struct {
	// Name is the rule identifier used in settings, suppression comments,
	// and as the LSP diagnostic code.
	Name           string
	Doc            string
	DefaultEnabled bool
	// NewOptions returns a pointer to the rule's default options, which the
	// configured options are decoded onto as JSON (unknown fields are
	// rejected) and handed to Run as Pass.Options. Nil means the rule takes
	// no options.
	NewOptions func() any
	Run        func(*Pass)
}

// TableMetadataLookup fetches metadata for a `project.dataset.table` path.
type TableMetadataLookup func(ctx context.Context, path string) (*bq.TableMetadata, error)

// Env carries optional external inputs to analyzers. Every field may be nil.
type Env struct {
	LookupTable TableMetadataLookup
	DryRun      *bq.JobStatus
}

// Pass is the input to a single Analyzer run over one file.
type Pass struct {
	Context  context.Context
	Analyzer *Analyzer
	File     file.ParsedFile
	// LookupTable is nil-safe to call; it returns (nil, nil) when no lookup is configured.
	LookupTable TableMetadataLookup
	DryRun      *bq.JobStatus
	// Options is the value from Analyzer.NewOptions with the configured
	// options applied, or nil when the analyzer takes none.
	Options any

	report func(Diagnostic)
}

// Report records a diagnostic. Rule defaults to the analyzer's name, and any
// severity other than Information is reported as Warning.
func (p *Pass) Report(d Diagnostic) {
	if d.Rule == "" {
		d.Rule = p.Analyzer.Name
	}
	if d.Severity != SeverityInformation {
		d.Severity = SeverityWarning
	}
	p.report(d)
}

type Diagnostic struct {
	Rule           string
	Range          lsp.Range
	Message        string
	Severity       Severity
	SuggestedFixes []SuggestedFix
}

type SuggestedFix struct {
	Title string
	Edits []lsp.TextEdit
}

// Run executes the enabled analyzers over f and returns unsuppressed diagnostics.
// Files that failed analysis are skipped because rules rely on a complete
// resolved AST and would otherwise produce noise on top of real errors.
func Run(ctx context.Context, f file.ParsedFile, cfg Config, env Env, analyzers ...*Analyzer) []Diagnostic {
	if f.Node == nil || len(f.Errors) > 0 {
		return nil
	}

	lookup := env.LookupTable
	if lookup == nil {
		lookup = func(context.Context, string) (*bq.TableMetadata, error) { return nil, nil }
	}

	suppressions := ParseSuppressions(f.Src)
	var result []Diagnostic
	for _, a := range analyzers {
		if !cfg.Enabled(a) {
			continue
		}
		// Validate is where invalid options are reported; running the rule
		// with settings the user did not ask for would be misleading.
		opts, err := cfg.options(a)
		if err != nil {
			continue
		}
		pass := &Pass{
			Context:     ctx,
			Analyzer:    a,
			File:        f,
			LookupTable: lookup,
			DryRun:      env.DryRun,
			Options:     opts,
			report: func(d Diagnostic) {
				if suppressions.Suppressed(d.Rule, d.Range.Start.Line) {
					return
				}
				result = append(result, d)
			},
		}
		a.Run(pass)
	}
	return result
}
