package lint_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/kitagry/bqls/langserver/internal/lint"
)

// regexpDiag reports msg on the first occurrence of lit in the single-line src.
func regexpDiag(src, lit, msg string) lint.Diagnostic {
	start := strings.Index(src, lit)
	return lint.Diagnostic{
		Rule:     "regexp-literal",
		Range:    rng(0, start, 0, start+len(lit)),
		Message:  msg,
		Severity: lint.SeverityWarning,
	}
}

func invalidRegexpMsg(lit, reason string) string {
	return "Invalid regular expression " + lit + ": " + reason + ", so the query fails when this is evaluated"
}

func runRegexpLiteral(t *testing.T, src string) []lint.Diagnostic {
	t.Helper()
	cfg := lint.Config{Rules: map[string]bool{"regexp-literal": true}}
	return lint.Run(t.Context(), parse(t, src), cfg, lint.Env{}, lint.RegexpLiteral)
}

func TestRegexpLiteral_InvalidPattern(t *testing.T) {
	tests := map[string]struct {
		src  string
		lit  string
		msg  string
		want bool
	}{
		"unclosed group": {
			src: "SELECT REGEXP_CONTAINS('a', '(')", lit: "'('",
			msg: invalidRegexpMsg("'('", "missing closing ): `(`"), want: true,
		},
		"raw string": {
			src: `SELECT REGEXP_CONTAINS('a', r'\Z')`, lit: `r'\Z'`,
			msg: invalidRegexpMsg(`r'\Z'`, "invalid escape sequence: `\\Z`"), want: true,
		},
		"repeat count over 1000": {
			src: "SELECT REGEXP_CONTAINS('a', 'a{1001}')", lit: "'a{1001}'",
			msg: invalidRegexpMsg("'a{1001}'", "invalid repeat count: `{1001}`"), want: true,
		},
		"BYTES pattern": {
			src: "SELECT REGEXP_CONTAINS(b'a', b'(')", lit: "b'('",
			msg: invalidRegexpMsg("b'('", "missing closing ): `(`"), want: true,
		},
		"BYTES pattern with a byte that is not UTF-8": {
			src: `SELECT REGEXP_CONTAINS(b'a', b'\xff(')`, lit: `b'\xff('`,
			msg: invalidRegexpMsg(`b'\xff('`, "missing closing ): `ÿ(`"), want: true,
		},
		"REGEXP_EXTRACT": {
			src: "SELECT REGEXP_EXTRACT('a', '[a')", lit: "'[a'",
			msg: invalidRegexpMsg("'[a'", "missing closing ]: `[a`"), want: true,
		},
		"REGEXP_EXTRACT_ALL": {
			src: "SELECT REGEXP_EXTRACT_ALL('a', '*')", lit: "'*'",
			msg: invalidRegexpMsg("'*'", "missing argument to repetition operator: `*`"), want: true,
		},
		"REGEXP_SUBSTR": {
			src: "SELECT REGEXP_SUBSTR('a', '(?=a)')", lit: "'(?=a)'",
			msg: invalidRegexpMsg("'(?=a)'", "invalid or unsupported Perl syntax: `(?=`"), want: true,
		},
		"REGEXP_INSTR": {
			src: "SELECT REGEXP_INSTR('a', ')')", lit: "')'",
			msg: invalidRegexpMsg("')'", "unexpected ): `)`"), want: true,
		},
		"REGEXP_REPLACE": {
			src: "SELECT REGEXP_REPLACE('a', 'a**', 'b')", lit: "'a**'",
			msg: invalidRegexpMsg("'a**'", "invalid nested repetition operator: `**`"), want: true,
		},
		"valid pattern":                 {src: `SELECT REGEXP_CONTAINS('a', r'^\d+(?:\.\d*)?$')`},
		"named groups":                  {src: `SELECT REGEXP_CONTAINS('a', r'(?P<x>a)(?<y>b)')`},
		"Unicode class":                 {src: `SELECT REGEXP_CONTAINS('a', r'\p{Greek}\pL')`},
		"BYTES pattern read as Latin-1": {src: `SELECT REGEXP_CONTAINS(b'a', b'\xff')`},
		// BigQuery's RE2 accepts \C (any byte); Go does not.
		"any byte": {src: `SELECT REGEXP_CONTAINS('a', r'a\Cb')`},
		// Unicode classes depend on the Unicode version each side ships.
		"unknown Unicode class":  {src: `SELECT REGEXP_CONTAINS('a', r'\p{Foo}')`},
		"not a literal":          {src: "SELECT REGEXP_CONTAINS('a', '(' || 'a')"},
		"declared variable":      {src: "DECLARE v STRING DEFAULT '(';\nSELECT REGEXP_CONTAINS('a', v)"},
		"NULL pattern":           {src: "SELECT REGEXP_CONTAINS('a', NULL)"},
		"value is not a pattern": {src: "SELECT REGEXP_CONTAINS('(', 'a')"},
		"other function":         {src: "SELECT STRPOS('a', '(')"},
		"suppressed": {
			src: "SELECT\n-- bqls:ignore regexp-literal\nREGEXP_CONTAINS('a', '(')",
		},
	}
	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			var want []lint.Diagnostic
			if tt.want {
				want = []lint.Diagnostic{regexpDiag(tt.src, tt.lit, tt.msg)}
			}
			got := runRegexpLiteral(t, tt.src)
			if diff := cmp.Diff(want, got, cmpopts.IgnoreFields(lint.Diagnostic{}, "SuggestedFixes")); diff != "" {
				t.Errorf("diff (-want +got):\n%s", diff)
			}
		})
	}
}

func tooManyGroupsMsg(lit string, n int) string {
	return fmt.Sprintf("The regular expression %s has %d capturing groups, but REGEXP_EXTRACT, REGEXP_EXTRACT_ALL, REGEXP_SUBSTR and REGEXP_INSTR allow at most one, so the query fails when this is evaluated; make the other groups non-capturing with (?:...)", lit, n)
}

func TestRegexpLiteral_TooManyCapturingGroups(t *testing.T) {
	tests := map[string]struct {
		src  string
		lit  string
		n    int
		want bool
	}{
		"REGEXP_EXTRACT":     {src: "SELECT REGEXP_EXTRACT('ab', '(a)(b)')", lit: "'(a)(b)'", n: 2, want: true},
		"REGEXP_EXTRACT_ALL": {src: "SELECT REGEXP_EXTRACT_ALL('ab', '(a)(b)(c)')", lit: "'(a)(b)(c)'", n: 3, want: true},
		"REGEXP_SUBSTR":      {src: "SELECT REGEXP_SUBSTR('ab', '(a)(b)')", lit: "'(a)(b)'", n: 2, want: true},
		"REGEXP_INSTR":       {src: "SELECT REGEXP_INSTR('ab', '(a)(b)')", lit: "'(a)(b)'", n: 2, want: true},
		"named groups":       {src: "SELECT REGEXP_EXTRACT('ab', '(?P<x>a)(?<y>b)')", lit: "'(?P<x>a)(?<y>b)'", n: 2, want: true},
		"nested groups":      {src: "SELECT REGEXP_EXTRACT('ab', '((a)b)')", lit: "'((a)b)'", n: 2, want: true},
		"BYTES":              {src: "SELECT REGEXP_EXTRACT(b'ab', b'(a)(b)')", lit: "b'(a)(b)'", n: 2, want: true},
		"one group":          {src: "SELECT REGEXP_EXTRACT('ab', '(a)(?:b)')"},
		"escaped parens":     {src: `SELECT REGEXP_EXTRACT('ab', r'\(a\)(b)[(]\Q(\E')`},
		"REGEXP_CONTAINS":    {src: "SELECT REGEXP_CONTAINS('ab', '(a)(b)')"},
		"REGEXP_REPLACE":     {src: "SELECT REGEXP_REPLACE('ab', '(a)(b)', 'x')"},
	}
	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			var want []lint.Diagnostic
			if tt.want {
				want = []lint.Diagnostic{regexpDiag(tt.src, tt.lit, tooManyGroupsMsg(tt.lit, tt.n))}
			}
			got := runRegexpLiteral(t, tt.src)
			if diff := cmp.Diff(want, got, cmpopts.IgnoreFields(lint.Diagnostic{}, "SuggestedFixes")); diff != "" {
				t.Errorf("diff (-want +got):\n%s", diff)
			}
		})
	}
}
