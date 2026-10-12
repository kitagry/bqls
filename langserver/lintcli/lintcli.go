// Package lintcli implements the `bqls lint` command, which reports the same
// diagnostics as the language server for SQL files on disk.
package lintcli

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	bq "cloud.google.com/go/bigquery"
	googlesql "github.com/goccy/go-googlesql"
	"github.com/kitagry/bqls/langserver/internal/bigquery"
	"github.com/kitagry/bqls/langserver/internal/lint"
	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/kitagry/bqls/langserver/internal/source"
	"github.com/sirupsen/logrus"
)

const (
	exitOK       = 0
	exitFindings = 1
	exitFailure  = 2

	stdinName = "<stdin>"
	stdinURI  = "file:///dev/stdin"
)

type clientFactory func(ctx context.Context, projectID, location string) (bigquery.Client, error)

// Run executes `bqls lint` with args (excluding the "lint" subcommand itself)
// and returns the process exit code.
func Run(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	return run(ctx, args, stdin, stdout, stderr, newBigQueryClient)
}

func newBigQueryClient(ctx context.Context, projectID, location string) (bigquery.Client, error) {
	// The LSP's persistent cache is skipped: it only speeds up listing APIs that
	// lint never calls, and it would write a SQLite file on every CI run.
	// Duplicate metadata lookups are handled by memoClient instead.
	return bigquery.New(ctx, projectID, location, false, newLogger(io.Discard))
}

type ruleToggle struct {
	name    string
	enabled bool
}

func run(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer, newClient clientFactory) int {
	fset := flag.NewFlagSet("bqls lint", flag.ContinueOnError)
	fset.SetOutput(stderr)
	project := fset.String("project", "", "BigQuery project ID (default: the gcloud default project)")
	location := fset.String("location", "", "BigQuery location (default: US)")
	format := fset.String("format", "text", "output format: text, json, or github")
	var toggles []ruleToggle
	fset.Func("enable", "enable a rule (repeatable)", func(s string) error {
		toggles = append(toggles, ruleToggle{s, true})
		return nil
	})
	fset.Func("disable", "disable a rule (repeatable)", func(s string) error {
		toggles = append(toggles, ruleToggle{s, false})
		return nil
	})
	fset.Usage = func() { printUsage(stdout, fset) }

	if err := fset.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitFailure
	}

	cfg, err := buildConfig(toggles)
	if err != nil {
		fmt.Fprintf(stderr, "bqls lint: %v\n", err)
		return exitFailure
	}
	formatter, ok := formatters[*format]
	if !ok {
		fmt.Fprintf(stderr, "bqls lint: unknown format %q (valid: text, json, github)\n", *format)
		return exitFailure
	}
	if fset.NArg() == 0 {
		fmt.Fprintln(stderr, "bqls lint: no input files; usage: bqls lint [flags] <file|dir|->...")
		return exitFailure
	}

	inputs, err := readInputs(fset.Args(), stdin)
	if err != nil {
		fmt.Fprintf(stderr, "bqls lint: %v\n", err)
		return exitFailure
	}

	if err := googlesql.Init(); err != nil {
		fmt.Fprintf(stderr, "bqls lint: failed to initialize googlesql: %v\n", err)
		return exitFailure
	}
	client, err := newClient(ctx, *project, *location)
	if err != nil {
		fmt.Fprintf(stderr, "bqls lint: failed to create BigQuery client: %v\n", err)
		return exitFailure
	}
	p := source.NewProjectWithBQClient("/", newMemoClient(client), newLogger(stderr))
	defer p.Close()

	var findings []finding
	for _, in := range inputs {
		if err := p.UpdateFile(in.uri, in.src, 1); err != nil {
			fmt.Fprintf(stderr, "bqls lint: %s: %v\n", in.name, err)
			return exitFailure
		}
		diags, _ := p.Diagnose(ctx, in.uri, cfg)
		findings = append(findings, toFindings(in.name, diags)...)
		p.DeleteFile(in.uri)
	}
	slices.SortStableFunc(findings, func(a, b finding) int {
		return cmp.Or(cmp.Compare(a.File, b.File), cmp.Compare(a.Line, b.Line), cmp.Compare(a.Column, b.Column))
	})

	if err := formatter(stdout, findings); err != nil {
		fmt.Fprintf(stderr, "bqls lint: %v\n", err)
		return exitFailure
	}
	if len(findings) > 0 {
		return exitFindings
	}
	return exitOK
}

func newLogger(w io.Writer) *logrus.Logger {
	logger := logrus.New()
	logger.Out = w
	// The analyzer logs routine lookup details at info level, which would bury
	// the findings on stderr.
	logger.SetLevel(logrus.WarnLevel)
	return logger
}

func printUsage(w io.Writer, fset *flag.FlagSet) {
	fmt.Fprint(w, `Usage: bqls lint [flags] <file|dir|->...

Lint BigQuery SQL files. Directories are searched recursively for *.sql files,
and "-" reads from stdin.

Exit codes: 0 no findings, 1 findings reported, 2 usage or runtime error.

Flags:
`)
	fset.SetOutput(w)
	fset.PrintDefaults()
	fmt.Fprint(w, "\nRules:\n")
	for _, a := range lint.Analyzers {
		state := "off"
		if a.DefaultEnabled {
			state = "on"
		}
		fmt.Fprintf(w, "  %-18s (default %s) %s\n", a.Name, state, a.Doc)
	}
}

func buildConfig(toggles []ruleToggle) (lint.Config, error) {
	var cfg lint.Config
	for _, t := range toggles {
		if !slices.ContainsFunc(lint.Analyzers, func(a *lint.Analyzer) bool { return a.Name == t.name }) {
			return lint.Config{}, fmt.Errorf("unknown rule %q (valid: %s)", t.name, strings.Join(ruleNames(), ", "))
		}
		if cfg.Rules == nil {
			cfg.Rules = make(map[string]bool)
		}
		cfg.Rules[t.name] = t.enabled
	}
	return cfg, nil
}

func ruleNames() []string {
	names := make([]string, len(lint.Analyzers))
	for i, a := range lint.Analyzers {
		names[i] = a.Name
	}
	return names
}

type input struct {
	name string
	uri  lsp.DocumentURI
	src  string
}

func readInputs(args []string, stdin io.Reader) ([]input, error) {
	var inputs []input
	for _, arg := range args {
		if arg == "-" {
			b, err := io.ReadAll(stdin)
			if err != nil {
				return nil, fmt.Errorf("read stdin: %w", err)
			}
			inputs = append(inputs, input{name: stdinName, uri: stdinURI, src: string(b)})
			continue
		}

		paths, err := expandPath(arg)
		if err != nil {
			return nil, err
		}
		for _, path := range paths {
			b, err := os.ReadFile(path)
			if err != nil {
				return nil, err
			}
			abs, err := filepath.Abs(path)
			if err != nil {
				return nil, err
			}
			inputs = append(inputs, input{name: path, uri: lsp.DocumentURI("file://" + filepath.ToSlash(abs)), src: string(b)})
		}
	}
	return inputs, nil
}

func expandPath(arg string) ([]string, error) {
	info, err := os.Stat(arg)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return []string{arg}, nil
	}
	var paths []string
	err = filepath.WalkDir(arg, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && filepath.Ext(path) == ".sql" {
			paths = append(paths, path)
		}
		return nil
	})
	return paths, err
}

// memoClient remembers table metadata, including failures, for the whole run
// so that a table read by several files, or by both the analyzer and a lint
// rule, is fetched from the API once.
type memoClient struct {
	bigquery.Client
	mu     sync.Mutex
	tables map[string]tableResult
}

type tableResult struct {
	md  *bq.TableMetadata
	err error
}

func newMemoClient(c bigquery.Client) *memoClient {
	return &memoClient{Client: c, tables: make(map[string]tableResult)}
}

func (c *memoClient) GetTableMetadata(ctx context.Context, projectID, datasetID, tableID string) (*bq.TableMetadata, error) {
	key := projectID + ":" + datasetID + ":" + tableID
	c.mu.Lock()
	defer c.mu.Unlock()
	if r, ok := c.tables[key]; ok {
		return r.md, r.err
	}
	md, err := c.Client.GetTableMetadata(ctx, projectID, datasetID, tableID)
	c.tables[key] = tableResult{md, err}
	return md, err
}

// finding uses 1-based line/column numbers, as editors and CI annotations do.
// Columns count bytes, and the end position is exclusive.
type finding struct {
	File      string `json:"file"`
	Line      int    `json:"line"`
	Column    int    `json:"column"`
	EndLine   int    `json:"endLine"`
	EndColumn int    `json:"endColumn"`
	Severity  string `json:"severity"`
	Rule      string `json:"rule"`
	Message   string `json:"message"`
}

func toFindings(name string, diags source.Diagnostics) []finding {
	var result []finding
	for _, e := range diags.Errors {
		end := e.Position
		end.Character += e.TermLength
		result = append(result, newFinding(name, lsp.Range{Start: e.Position, End: end}, cmp.Or(e.Severity, lsp.Error), "", e.Msg))
	}
	for _, d := range diags.Lint {
		result = append(result, newFinding(name, d.Range, d.Severity, d.Rule, d.Message))
	}
	return result
}

func newFinding(name string, r lsp.Range, severity lsp.DiagnosticSeverity, rule, msg string) finding {
	return finding{
		File:      name,
		Line:      r.Start.Line + 1,
		Column:    r.Start.Character + 1,
		EndLine:   r.End.Line + 1,
		EndColumn: r.End.Character + 1,
		Severity:  severityName(severity),
		Rule:      rule,
		Message:   msg,
	}
}

func severityName(s lsp.DiagnosticSeverity) string {
	switch s {
	case lsp.Error:
		return "error"
	case lsp.Warning:
		return "warning"
	case lsp.Information:
		return "info"
	default:
		return "hint"
	}
}

var formatters = map[string]func(io.Writer, []finding) error{
	"text":   writeText,
	"json":   writeJSON,
	"github": writeGitHub,
}

func writeText(w io.Writer, findings []finding) error {
	for _, f := range findings {
		rule := ""
		if f.Rule != "" {
			rule = " (" + f.Rule + ")"
		}
		if _, err := fmt.Fprintf(w, "%s:%d:%d: %s: %s%s\n", f.File, f.Line, f.Column, f.Severity, f.Message, rule); err != nil {
			return err
		}
	}
	return nil
}

func writeJSON(w io.Writer, findings []finding) error {
	if findings == nil {
		findings = []finding{}
	}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	enc.SetEscapeHTML(false)
	return enc.Encode(findings)
}

// writeGitHub emits workflow commands that GitHub Actions renders as
// annotations on the pull request diff.
func writeGitHub(w io.Writer, findings []finding) error {
	for _, f := range findings {
		title := "bqls"
		if f.Rule != "" {
			title = "bqls(" + f.Rule + ")"
		}
		_, err := fmt.Fprintf(w, "::%s file=%s,line=%d,col=%d,endLine=%d,endColumn=%d,title=%s::%s\n",
			gitHubCommand(f.Severity), escapeGitHubProperty(f.File), f.Line, f.Column, f.EndLine, f.EndColumn,
			escapeGitHubProperty(title), escapeGitHubData(f.Message))
		if err != nil {
			return err
		}
	}
	return nil
}

func gitHubCommand(severity string) string {
	switch severity {
	case "error":
		return "error"
	case "warning":
		return "warning"
	default:
		return "notice"
	}
}

var (
	gitHubDataEscaper     = strings.NewReplacer("%", "%25", "\r", "%0D", "\n", "%0A")
	gitHubPropertyEscaper = strings.NewReplacer("%", "%25", "\r", "%0D", "\n", "%0A", ":", "%3A", ",", "%2C")
)

func escapeGitHubData(s string) string     { return gitHubDataEscaper.Replace(s) }
func escapeGitHubProperty(s string) string { return gitHubPropertyEscaper.Replace(s) }
