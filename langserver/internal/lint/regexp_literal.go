package lint

import (
	"errors"
	"fmt"
	"regexp"
	"regexp/syntax"
	"strings"

	googlesql "github.com/goccy/go-googlesql"
	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/kitagry/bqls/langserver/internal/source/file"
)

var RegexpLiteral = &Analyzer{
	Name:           "regexp-literal",
	Doc:            "reports constant regular expressions that BigQuery rejects when evaluated",
	DefaultEnabled: true,
	Run:            runRegexpLiteral,
}

// regexpFunctions lists the functions whose second argument is an RE2
// pattern. REGEXP_SUBSTR resolves to regexp_extract.
var regexpFunctions = map[string]bool{
	"regexp_contains":    true,
	"regexp_extract":     true,
	"regexp_extract_all": true,
	"regexp_instr":       true,
	"regexp_replace":     true,
}

// singleGroupFunctions fail on a pattern with more than one capturing group.
var singleGroupFunctions = map[string]bool{
	"regexp_extract":     true,
	"regexp_extract_all": true,
	"regexp_instr":       true,
}

func runRegexpLiteral(pass *Pass) {
	for _, output := range pass.File.RNode {
		for _, call := range file.ListResolvedAstNode[*googlesql.ResolvedFunctionCall](output) {
			name, ok := functionName(call)
			if !ok || !regexpFunctions[name] {
				continue
			}
			args, err := call.ArgumentList()
			if err != nil || len(args) < 2 {
				continue
			}
			lit, re, ok := checkRegexpPattern(pass, args[1])
			if !ok {
				continue
			}
			if singleGroupFunctions[name] && re.MaxCap() > 1 {
				pass.Report(Diagnostic{
					Range:   lit.loc,
					Message: fmt.Sprintf("The regular expression %s has %d capturing groups, but REGEXP_EXTRACT, REGEXP_EXTRACT_ALL, REGEXP_SUBSTR and REGEXP_INSTR allow at most one, so the query fails when this is evaluated; make the other groups non-capturing with (?:...)", lit.src, re.MaxCap()),
				})
			}
		}
	}
}

// checkRegexpPattern reports expr when it is an invalid pattern literal, and
// returns it parsed when it is a valid one.
func checkRegexpPattern(pass *Pass, expr googlesql.ResolvedExprNode) (stringLiteral, *syntax.Regexp, bool) {
	lit, ok := writtenStringLiteral(pass, expr)
	if !ok {
		return stringLiteral{}, nil, false
	}
	re, err := syntax.Parse(lit.value, syntax.Perl)
	if err == nil {
		return lit, re, true
	}
	var serr *syntax.Error
	if !errors.As(err, &serr) || !reportedByRE2(serr) {
		return stringLiteral{}, nil, false
	}
	pass.Report(Diagnostic{
		Range:   lit.loc,
		Message: fmt.Sprintf("Invalid regular expression %s: %s: `%s`, so the query fails when this is evaluated", lit.src, serr.Code, serr.Expr),
	})
	return stringLiteral{}, nil, false
}

// reportedByRE2 reports whether BigQuery's RE2 is known to reject the
// pattern too. Go's regexp/syntax is also RE2, but it lacks \C, ships its own
// Unicode tables and enforces its own size limits.
func reportedByRE2(err *syntax.Error) bool {
	switch err.Code {
	case syntax.ErrInvalidEscape:
		return err.Expr != `\C`
	case syntax.ErrInvalidCharRange:
		return !strings.HasPrefix(err.Expr, `\p`) && !strings.HasPrefix(err.Expr, `\P`)
	case syntax.ErrLarge, syntax.ErrNestingDepth, syntax.ErrInvalidUTF8:
		return false
	}
	return true
}

type stringLiteral struct {
	// value is the literal's value. BYTES are read as Latin-1, one rune per
	// byte, as RE2 reads BYTES patterns.
	value string
	src   string
	loc   lsp.Range
}

var quotedLiteral = regexp.MustCompile(`^(?i:[rb]{0,2})['"]`)

// writtenStringLiteral returns expr when it is a STRING or BYTES literal in
// the source. Declared variables are analyzed as substituted literals, so the
// source text decides.
func writtenStringLiteral(pass *Pass, expr googlesql.ResolvedExprNode) (stringLiteral, bool) {
	lit, ok := expr.(*googlesql.ResolvedLiteral)
	if !ok {
		return stringLiteral{}, false
	}
	loc, err := lit.GetParseLocationRangeOrNULL()
	if err != nil || loc == nil {
		return stringLiteral{}, false
	}
	src, ok := pass.File.ExtractSQL(loc)
	if !ok || !quotedLiteral.MatchString(src) {
		return stringLiteral{}, false
	}
	r, ok := pass.File.ToLspRange(loc)
	if !ok {
		return stringLiteral{}, false
	}
	v, err := lit.Value()
	if err != nil || v == nil {
		return stringLiteral{}, false
	}
	typ, err := lit.Type()
	if err != nil || typ == nil {
		return stringLiteral{}, false
	}
	var value string
	if isBytes, err := typ.IsBytes(); err == nil && isBytes {
		b, err := v.BytesValue()
		if err != nil {
			return stringLiteral{}, false
		}
		runes := make([]rune, len(b))
		for i := range len(b) {
			runes[i] = rune(b[i])
		}
		value = string(runes)
	} else {
		if value, err = v.StringValue(); err != nil {
			return stringLiteral{}, false
		}
	}
	return stringLiteral{value: value, src: src, loc: r}, true
}
