// Package lint provides a small analysis framework, modeled on
// golang.org/x/tools/go/analysis, for reporting style and correctness
// warnings on SQL that already analyzes without errors.
package lint

import (
	"context"
	"slices"
	"strings"

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

// Config is the user-facing lint settings, e.g. {"rules": {"null-comparison": false}}.
// The Diagnose cache in package source keys on these fields, so update it
// when adding one.
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
				d.SuggestedFixes = slices.DeleteFunc(d.SuggestedFixes, func(fix SuggestedFix) bool {
					return !EditsASCIISafe(f.Src, fix.Edits)
				})
				if len(d.SuggestedFixes) == 0 {
					d.SuggestedFixes = nil
				}
				result = append(result, d)
			},
		}
		a.Run(pass)
	}
	return result
}

// EditsASCIISafe reports whether every edit only spans ASCII text from the
// start of its first line. Positions are byte offsets while LSP clients count
// UTF-16 units, so an edit preceded by non-ASCII text would land on the wrong
// characters.
func EditsASCIISafe(src string, edits []lsp.TextEdit) bool {
	lines := strings.Split(src, "\n")
	for _, e := range edits {
		for l := e.Range.Start.Line; l <= e.Range.End.Line; l++ {
			if l >= len(lines) {
				return false
			}
			text := lines[l]
			if l == e.Range.End.Line && e.Range.End.Character < len(text) {
				text = text[:e.Range.End.Character]
			}
			if !isASCII(text) {
				return false
			}
		}
	}
	return true
}

func isASCII(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] >= 0x80 {
			return false
		}
	}
	return true
}
