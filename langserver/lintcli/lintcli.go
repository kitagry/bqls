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
	configPath := fset.String("config", "", "config file to use instead of looking up "+lint.ConfigFileName+" from each file's directory to the repository root")
	fix := fset.Bool("fix", false, "apply suggested fixes, write the files back (stdin: print the fixed SQL), and report what is left")
	var toggles []ruleToggle
	fset.Func("enable", "enable a rule (repeatable)", func(s string) error {
		toggles = append(toggles, ruleToggle{s, true})
		return nil
	})
	fset.Func("disable", "disable a rule (repeatable)", func(s string) error {
		toggles = append(toggles, ruleToggle{s, false})
		return nil
	})
	// Usage is printed below so that --help goes to stdout and parse errors to stderr.
	fset.Usage = func() {}

	if err := fset.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			printUsage(stdout, fset)
			return exitOK
		}
		printUsage(stderr, fset)
		return exitFailure
	}

	flagCfg, err := buildConfig(toggles)
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

	// The flag package stops at the first file, so a later flag would
	// otherwise be silently treated as a missing file.
	for _, arg := range fset.Args()[1:] {
		if strings.HasPrefix(arg, "-") && arg != "-" {
			fmt.Fprintf(stderr, "bqls lint: flags must come before files: %q\n", arg)
			return exitFailure
		}
	}

	configs := newConfigResolver(*configPath, flagCfg)
	inputs, readErrs := readInputs(fset.Args(), stdin)
	for _, err := range readErrs {
		fmt.Fprintf(stderr, "bqls lint: %v\n", err)
	}
	cfgs := make([]lint.Config, len(inputs))
	for i, in := range inputs {
		cfg, err := configs.resolve(in)
		if err != nil {
			fmt.Fprintf(stderr, "bqls lint: %v\n", err)
			return exitFailure
		}
		cfgs[i] = cfg
	}

	// The fixed SQL from stdin owns stdout, so the findings move to stderr.
	findingsOut := stdout
	if *fix && slices.ContainsFunc(inputs, func(in input) bool { return in.abs == "" }) {
		findingsOut = stderr
	}
	if len(inputs) == 0 {
		if len(readErrs) > 0 {
			return exitFailure
		}
		return formatAndExit(findingsOut, stderr, formatter, nil)
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
	memo := newMemoClient(client)
	p := source.NewProjectWithBQClient("/", memo, newLogger(stderr))
	defer p.Close()

	var findings []finding
	var fixedStdin *string
	writeFailed := false
	for i, in := range inputs {
		if err := p.UpdateFile(in.uri, in.src, 1); err != nil {
			fmt.Fprintf(stderr, "bqls lint: %s: %v\n", in.name, err)
			return exitFailure
		}
		diags, _ := p.Diagnose(ctx, in.uri, cfgs[i])
		if *fix {
			fixed, skipped := applyFixes(diags.Src, suggestedFixes(diags))
			if skipped > 0 {
				fmt.Fprintf(stderr, "bqls lint: %s: skipped %d fix(es) overlapping another fix; run again to apply them\n", in.name, skipped)
			}
			if fixed != in.src {
				if in.abs != "" {
					if err := os.WriteFile(in.name, []byte(fixed), 0o644); err != nil {
						fmt.Fprintf(stderr, "bqls lint: %v\n", err)
						writeFailed = true
					}
				}
				if err := p.UpdateFile(in.uri, fixed, 2); err != nil {
					fmt.Fprintf(stderr, "bqls lint: %s: %v\n", in.name, err)
					return exitFailure
				}
				diags, _ = p.Diagnose(ctx, in.uri, cfgs[i])
			}
			if in.abs == "" {
				fixedStdin = &fixed
			}
		}
		findings = append(findings, toFindings(in, diags)...)
		p.DeleteFile(in.uri)
	}
	if fixedStdin != nil {
		if _, err := io.WriteString(stdout, *fixedStdin); err != nil {
			fmt.Fprintf(stderr, "bqls lint: %v\n", err)
			writeFailed = true
		}
	}
	slices.SortStableFunc(findings, func(a, b finding) int {
		return cmp.Or(cmp.Compare(a.File, b.File), cmp.Compare(a.Line, b.Line), cmp.Compare(a.Column, b.Column))
	})

	code := formatAndExit(findingsOut, stderr, formatter, findings)
	// Without the table schema these files could not be checked, and the
	// analyzer reported "Table not found" in its place, so the run must not
	// look like an ordinary set of findings.
	for _, msg := range memo.failures() {
		fmt.Fprintf(stderr, "bqls lint: failed to fetch table metadata: %s\n", msg)
		code = exitFailure
	}
	if len(readErrs) > 0 || writeFailed {
		code = exitFailure
	}
	return code
}

func formatAndExit(stdout, stderr io.Writer, formatter func(io.Writer, []finding) error, findings []finding) int {
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

Settings are read from the nearest `+lint.ConfigFileName+` between each file's
directory (the working directory for stdin) and the repository root, unless
--config is given. --enable and --disable take precedence over the file.

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
		if cfg.Rules == nil {
			cfg.Rules = make(map[string]bool)
		}
		cfg.Rules[t.name] = t.enabled
	}
	if err := cfg.Validate(lint.Analyzers); err != nil {
		return lint.Config{}, err
	}
	return cfg, nil
}

// configResolver finds the config file for each input and caches it by path,
// since the files of one run usually share a config.
type configResolver struct {
	explicit string
	flags    lint.Config
	loaded   map[string]configResult
}

type configResult struct {
	cfg lint.Config
	err error
}

func newConfigResolver(explicit string, flags lint.Config) *configResolver {
	return &configResolver{explicit: explicit, flags: flags, loaded: make(map[string]configResult)}
}

// resolve returns the settings for in: the flags applied on top of its config file.
func (r *configResolver) resolve(in input) (lint.Config, error) {
	path := r.explicit
	if path == "" {
		dir := filepath.Dir(in.abs)
		if in.abs == "" {
			var err error
			if dir, err = os.Getwd(); err != nil {
				return lint.Config{}, err
			}
		}
		var ok bool
		if path, ok = lint.FindConfigFile(dir); !ok {
			return r.flags, nil
		}
	}
	res, ok := r.loaded[path]
	if !ok {
		res.cfg, res.err = loadConfigFile(path)
		r.loaded[path] = res
	}
	if res.err != nil {
		return lint.Config{}, res.err
	}
	return res.cfg.Override(r.flags)
}

func loadConfigFile(path string) (lint.Config, error) {
	cfg, err := lint.LoadConfigFile(path)
	if err != nil {
		return lint.Config{}, err
	}
	if err := cfg.Validate(lint.Analyzers); err != nil {
		return lint.Config{}, fmt.Errorf("%s: %w", path, err)
	}
	return cfg, nil
}

// suggestedFixes returns the preferred fix of every diagnostic in position
// order, so that applyFixes keeps the earlier of two overlapping fixes.
func suggestedFixes(diags source.Diagnostics) []lint.SuggestedFix {
	type located struct {
		pos lsp.Position
		fix lint.SuggestedFix
	}
	var fixes []located
	for i, e := range diags.Errors {
		if fs := diags.ErrorFixes[i]; len(fs) > 0 {
			fixes = append(fixes, located{e.Position, fs[0]})
		}
	}
	for _, d := range diags.Lint {
		if len(d.SuggestedFixes) > 0 {
			fixes = append(fixes, located{d.Range.Start, d.SuggestedFixes[0]})
		}
	}
	slices.SortStableFunc(fixes, func(a, b located) int {
		return cmp.Or(cmp.Compare(a.pos.Line, b.pos.Line), cmp.Compare(a.pos.Character, b.pos.Character))
	})
	result := make([]lint.SuggestedFix, len(fixes))
	for i, f := range fixes {
		result[i] = f.fix
	}
	return result
}

// applyFixes applies fixes in order, skipping each fix with an edit that
// overlaps an edit already taken, because the two were computed against the
// same text and cannot both be applied safely. Positions are byte based.
// It returns the new text and the number of skipped fixes.
func applyFixes(src string, fixes []lint.SuggestedFix) (string, int) {
	lineStarts := []int{0}
	for i := range len(src) {
		if src[i] == '\n' {
			lineStarts = append(lineStarts, i+1)
		}
	}
	offset := func(p lsp.Position) int {
		if p.Line >= len(lineStarts) {
			return len(src)
		}
		return min(lineStarts[p.Line]+p.Character, len(src))
	}

	type span struct {
		start, end int
		text       string
	}
	// Two insertions at one point, or an insertion at the start of a
	// replacement, have no defined order, so a shared start also conflicts.
	overlaps := func(a, b span) bool {
		return a.start == b.start || (a.start < b.end && b.start < a.end)
	}
	var accepted []span
	skipped := 0
	for _, fix := range fixes {
		spans := make([]span, len(fix.Edits))
		conflict := false
		for i, e := range fix.Edits {
			spans[i] = span{offset(e.Range.Start), offset(e.Range.End), e.NewText}
			conflict = conflict || slices.ContainsFunc(accepted, func(a span) bool { return overlaps(a, spans[i]) })
		}
		if conflict {
			skipped++
			continue
		}
		accepted = append(accepted, spans...)
	}

	// Applying from the end keeps the offsets of earlier spans valid.
	slices.SortFunc(accepted, func(a, b span) int { return cmp.Compare(b.start, a.start) })
	for _, s := range accepted {
		src = src[:s.start] + s.text + src[s.end:]
	}
	return src, skipped
}

type input struct {
	name string
	// abs is empty for stdin.
	abs string
	uri lsp.DocumentURI
	src string
}

// readInputs returns every input it could read, deduplicated, along with the
// errors for the rest so that one bad path does not hide findings elsewhere.
func readInputs(args []string, stdin io.Reader) ([]input, []error) {
	var inputs []input
	var errs []error
	seen := make(map[lsp.DocumentURI]bool)
	for _, arg := range args {
		if arg == "-" {
			if seen[stdinURI] {
				continue
			}
			seen[stdinURI] = true
			b, err := io.ReadAll(stdin)
			if err != nil {
				errs = append(errs, fmt.Errorf("read stdin: %w", err))
				continue
			}
			inputs = append(inputs, input{name: stdinName, uri: stdinURI, src: string(b)})
			continue
		}

		paths, err := expandPath(arg)
		if err != nil {
			errs = append(errs, err)
		}
		for _, path := range paths {
			abs, err := filepath.Abs(path)
			if err != nil {
				errs = append(errs, err)
				continue
			}
			uri := lsp.DocumentURI("file://" + filepath.ToSlash(abs))
			if seen[uri] {
				continue
			}
			seen[uri] = true
			b, err := os.ReadFile(path)
			if err != nil {
				errs = append(errs, err)
				continue
			}
			inputs = append(inputs, input{name: filepath.Clean(path), abs: abs, uri: uri, src: string(b)})
		}
	}
	return inputs, errs
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
		// Hidden directories such as .git hold tool state, not queries to lint.
		if d.IsDir() && path != arg && strings.HasPrefix(d.Name(), ".") {
			return filepath.SkipDir
		}
		if !d.IsDir() && filepath.Ext(path) == ".sql" {
			paths = append(paths, path)
		}
		return nil
	})
	return paths, err
}

// memoClient remembers table and routine metadata and dry-run schemas, including
// failures, for the whole run so that a table read by several files, or by
// both the analyzer and a lint rule, is fetched from the API once.
//
// It also records lookups that failed for reasons other than the table not
// existing, because the analyzer turns every lookup error into "Table not
// found", which would hide auth or network problems behind fake findings.
type memoClient struct {
	bigquery.Client
	mu       sync.Mutex
	tables   map[string]tableResult
	routines map[string]routineResult
	schemas  map[string]schemaResult
	failMsgs []string
}

type schemaResult struct {
	schema bq.Schema
	err    error
}

type routineResult struct {
	md  *bq.RoutineMetadata
	err error
}

type tableResult struct {
	md  *bq.TableMetadata
	err error
}

func newMemoClient(c bigquery.Client) *memoClient {
	return &memoClient{Client: c, tables: make(map[string]tableResult), routines: make(map[string]routineResult), schemas: make(map[string]schemaResult)}
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
	c.recordFailure(err)
	return md, err
}

func (c *memoClient) GetRoutineMetadata(ctx context.Context, projectID, datasetID, routineID string) (*bq.RoutineMetadata, error) {
	key := projectID + ":" + datasetID + ":" + routineID
	c.mu.Lock()
	defer c.mu.Unlock()
	if r, ok := c.routines[key]; ok {
		return r.md, r.err
	}
	md, err := c.Client.GetRoutineMetadata(ctx, projectID, datasetID, routineID)
	c.routines[key] = routineResult{md, err}
	c.recordFailure(err)
	return md, err
}

func (c *memoClient) DryRunSchema(ctx context.Context, query string) (bq.Schema, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if r, ok := c.schemas[query]; ok {
		return r.schema, r.err
	}
	schema, err := c.Client.DryRunSchema(ctx, query)
	c.schemas[query] = schemaResult{schema, err}
	c.recordFailure(err)
	return schema, err
}

// recordFailure must be called with c.mu held.
func (c *memoClient) recordFailure(err error) {
	if err != nil && bigquery.IsRuntimeFailure(err) && !slices.Contains(c.failMsgs, err.Error()) {
		c.failMsgs = append(c.failMsgs, err.Error())
	}
}

func (c *memoClient) failures() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.failMsgs)
}

// finding uses 1-based line/column numbers, as editors and CI annotations do.
// The end position is exclusive. Columns are byte offsets, so lines with
// non-ASCII text currently get shifted columns (#375).
type finding struct {
	File      string `json:"file"`
	Line      int    `json:"line"`
	Column    int    `json:"column"`
	EndLine   int    `json:"endLine"`
	EndColumn int    `json:"endColumn"`
	Severity  string `json:"severity"`
	Rule      string `json:"rule"`
	Message   string `json:"message"`

	abs string
}

func toFindings(in input, diags source.Diagnostics) []finding {
	var result []finding
	for _, e := range diags.Errors {
		end := e.Position
		end.Character += e.TermLength
		result = append(result, newFinding(in, lsp.Range{Start: e.Position, End: end}, cmp.Or(e.Severity, lsp.Error), "", e.Msg))
	}
	for _, d := range diags.Lint {
		result = append(result, newFinding(in, d.Range, d.Severity, d.Rule, d.Message))
	}
	return result
}

func newFinding(in input, r lsp.Range, severity lsp.DiagnosticSeverity, rule, msg string) finding {
	return finding{
		File:      in.name,
		Line:      r.Start.Line + 1,
		Column:    r.Start.Character + 1,
		EndLine:   r.End.Line + 1,
		EndColumn: r.End.Character + 1,
		Severity:  severityName(severity),
		Rule:      rule,
		Message:   msg,
		abs:       in.abs,
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

// textNewlineEscaper keeps one finding per line so that grep and editors'
// quickfix parsers see the whole message.
var textNewlineEscaper = strings.NewReplacer("\r", `\r`, "\n", `\n`)

func writeText(w io.Writer, findings []finding) error {
	for _, f := range findings {
		rule := ""
		if f.Rule != "" {
			rule = " (" + f.Rule + ")"
		}
		if _, err := fmt.Fprintf(w, "%s:%d:%d: %s: %s%s\n", f.File, f.Line, f.Column, f.Severity, textNewlineEscaper.Replace(f.Message), rule); err != nil {
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
	base := os.Getenv("GITHUB_WORKSPACE")
	if base == "" {
		base, _ = os.Getwd()
	}
	for _, f := range findings {
		title := "bqls"
		if f.Rule != "" {
			title = "bqls(" + f.Rule + ")"
		}
		// Annotations only attach to files given relative to the repository
		// root, and stdin has no file to attach to.
		file := ""
		if f.abs != "" {
			file = "file=" + escapeGitHubProperty(gitHubPath(base, f)) + ","
		}
		_, err := fmt.Fprintf(w, "::%s %sline=%d,col=%d,endLine=%d,endColumn=%d,title=%s::%s\n",
			gitHubCommand(f.Severity), file, f.Line, f.Column, f.EndLine, f.EndColumn,
			escapeGitHubProperty(title), escapeGitHubData(f.Message))
		if err != nil {
			return err
		}
	}
	return nil
}

func gitHubPath(base string, f finding) string {
	if base == "" {
		return f.File
	}
	rel, err := filepath.Rel(base, f.abs)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return f.File
	}
	return filepath.ToSlash(rel)
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
