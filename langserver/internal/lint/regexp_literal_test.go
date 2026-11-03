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

func missingGroupMsg(repl string, group int, pattern, groups string) string {
	return fmt.Sprintf(`The replacement %s refers to group \%d, but the regular expression %s has %s, so the query fails when this is evaluated`, repl, group, pattern, groups)
}

func badRewriteMsg(repl string) string {
	return `In the replacement ` + repl + `, a backslash must be followed by a digit or another backslash (write \\ for a literal backslash), so the query fails when this is evaluated`
}

func TestRegexpLiteral_Replacement(t *testing.T) {
	tests := map[string]struct {
		src  string
		lit  string
		msg  string
		want bool
	}{
		"group beyond the last one": {
			src: `SELECT REGEXP_REPLACE('ab', '(a)', r'\2')`, lit: `r'\2'`,
			msg: missingGroupMsg(`r'\2'`, 2, "'(a)'", "1 capturing group"), want: true,
		},
		"reports the highest group": {
			src: `SELECT REGEXP_REPLACE('ab', 'a', r'\1\3\2')`, lit: `r'\1\3\2'`,
			msg: missingGroupMsg(`r'\1\3\2'`, 3, "'a'", "no capturing groups"), want: true,
		},
		"escaped backslash in a quoted string": {
			src: `SELECT REGEXP_REPLACE('ab', '(a)(b)', '\\3')`, lit: `'\\3'`,
			msg: missingGroupMsg(`'\\3'`, 3, "'(a)(b)'", "2 capturing groups"), want: true,
		},
		"BYTES": {
			src: `SELECT REGEXP_REPLACE(b'ab', b'(a)', b'\\2')`, lit: `b'\\2'`,
			msg: missingGroupMsg(`b'\\2'`, 2, "b'(a)'", "1 capturing group"), want: true,
		},
		"backslash before a letter": {
			src: `SELECT REGEXP_REPLACE('ab', 'a', r'\n')`, lit: `r'\n'`,
			msg: badRewriteMsg(`r'\n'`), want: true,
		},
		"backslash at the end": {
			src: `SELECT REGEXP_REPLACE('ab', 'a', 'x\\')`, lit: `'x\\'`,
			msg: badRewriteMsg(`'x\\'`), want: true,
		},
		// The rewrite string is checked on its own, whatever the pattern is.
		"bad escape with a pattern that is not a literal": {
			src: `SELECT REGEXP_REPLACE('ab', CONCAT('a', 'b'), r'\a')`, lit: `r'\a'`,
			msg: badRewriteMsg(`r'\a'`), want: true,
		},
		"existing groups":                {src: `SELECT REGEXP_REPLACE('ab', '(a)(b)', r'\2\1')`},
		"whole match":                    {src: `SELECT REGEXP_REPLACE('ab', 'a', r'\0')`},
		"escaped backslash":              {src: `SELECT REGEXP_REPLACE('ab', 'a', r'\\1')`},
		"single digit after a backslash": {src: `SELECT REGEXP_REPLACE('ab', '(a)', r'\10')`},
		"pattern that is not a literal":  {src: `SELECT REGEXP_REPLACE('ab', CONCAT('a', 'b'), r'\2')`},
		"replacement that is not a literal": {
			src: "DECLARE v STRING DEFAULT r'\\2';\nSELECT REGEXP_REPLACE('ab', '(a)', v)",
		},
		// Only the invalid pattern is reported; BigQuery fails on it first.
		"invalid pattern": {
			src: `SELECT REGEXP_REPLACE('ab', '(', r'\2')`, lit: "'('",
			msg: invalidRegexpMsg("'('", "missing closing ): `(`"), want: true,
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

func TestRegexpLiteral_TooManyCapturingGroupsFix(t *testing.T) {
	const call = "SELECT REGEXP_EXTRACT('ab', "
	type fix struct{ title, fixed string }
	keep := func(group int, pattern string) fix {
		return fix{fmt.Sprintf("Make every capturing group except group %d non-capturing", group), call + pattern + ")"}
	}
	tests := map[string]struct {
		pattern string
		want    []fix
	}{
		"two groups": {
			pattern: "'(a)(b)'",
			want:    []fix{keep(1, "'(a)(?:b)'"), keep(2, "'(?:a)(b)'")},
		},
		"nested groups": {
			pattern: "'((a)b)(c)'",
			want: []fix{
				keep(1, "'((?:a)b)(?:c)'"),
				keep(2, "'(?:(a)b)(?:c)'"),
				keep(3, "'(?:(?:a)b)(c)'"),
			},
		},
		"named groups": {
			pattern: "'(?P<x>a)(?<y>b)'",
			want:    []fix{keep(1, "'(?P<x>a)(?:b)'"), keep(2, "'(?:a)(?<y>b)'")},
		},
		"escapes in a quoted string": {
			pattern: `'(\\d+)-(\\d+)'`,
			want:    []fix{keep(1, `'(\\d+)-(?:\\d+)'`), keep(2, `'(?:\\d+)-(\\d+)'`)},
		},
		"parens that are not groups": {
			pattern: `r'[(][[:alpha:]](a)\(\Q(\E(b)'`,
			want:    []fix{keep(1, `r'[(][[:alpha:]](a)\(\Q(\E(?:b)'`), keep(2, `r'[(][[:alpha:]](?:a)\(\Q(\E(b)'`)},
		},
		"triple-quoted raw string": {
			pattern: `R"""(a)'(b)"""`,
			want:    []fix{keep(1, `R"""(a)'(?:b)"""`), keep(2, `R"""(?:a)'(b)"""`)},
		},
		"multibyte text before a group": {
			pattern: "'é(a)(b)'",
			want:    []fix{keep(1, "'é(a)(?:b)'"), keep(2, "'é(?:a)(b)'")},
		},
		"BYTES with an escape": {
			pattern: `b'\xff(a)(b)'`,
			want:    []fix{keep(1, `b'\xff(a)(?:b)'`), keep(2, `b'\xff(?:a)(b)'`)},
		},
		// The first group's paren is written as an escape, which cannot take
		// "?:" after it.
		"group opened by an escape": {
			pattern: `'\x28a)(b)'`,
			want:    []fix{keep(1, `'\x28a)(?:b)'`)},
		},
	}
	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			src := call + tt.pattern + ")"
			if strings.HasPrefix(tt.pattern, "b") {
				src = "SELECT REGEXP_EXTRACT(b'ab', " + tt.pattern + ")"
				for i := range tt.want {
					tt.want[i].fixed = strings.Replace(tt.want[i].fixed, "'ab'", "b'ab'", 1)
				}
			}
			diags := runRegexpLiteral(t, src)
			if len(diags) != 1 {
				t.Fatalf("got %d diagnostics, want 1: %+v", len(diags), diags)
			}
			var got []fix
			for _, f := range diags[0].SuggestedFixes {
				fixed := applyFixes(t, src, []lint.Diagnostic{{SuggestedFixes: []lint.SuggestedFix{f}}})
				got = append(got, fix{f.Title, fixed})
				if after := runRegexpLiteral(t, fixed); len(after) != 0 {
					t.Errorf("diagnostics after %q: %+v", f.Title, after)
				}
			}
			if diff := cmp.Diff(tt.want, got, cmp.AllowUnexported(fix{})); diff != "" {
				t.Errorf("fixes diff (-want +got):\n%s", diff)
			}
		})
	}
}
