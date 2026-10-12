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
	Run            func(*Pass)
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

	report func(Diagnostic)
}

// Report records a diagnostic. Rule defaults to the analyzer's name.
func (p *Pass) Report(d Diagnostic) {
	if d.Rule == "" {
		d.Rule = p.Analyzer.Name
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

// Config is the user-facing lint settings, e.g. {"rules": {"null-comparison": false}}.
type Config struct {
	Rules map[string]bool `json:"rules,omitempty"`
}

// Enabled reports whether a is enabled, letting user settings override the default.
func (c Config) Enabled(a *Analyzer) bool {
	if v, ok := c.Rules[a.Name]; ok {
		return v
	}
	return a.DefaultEnabled
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
		pass := &Pass{
			Context:     ctx,
			Analyzer:    a,
			File:        f,
			LookupTable: lookup,
			DryRun:      env.DryRun,
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
