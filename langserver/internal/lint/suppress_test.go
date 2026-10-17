package lint_test

import (
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/kitagry/bqls/langserver/internal/lint"
)

func TestSuppressions_Suppressed(t *testing.T) {
	type check struct {
		rule string
		line int
		want bool
	}
	tests := map[string]struct {
		src    string
		checks []check
	}{
		"standalone line comment suppresses next line": {
			src: "SELECT 1\n-- bqls:ignore null-comparison\nWHERE x = NULL",
			checks: []check{
				{rule: "null-comparison", line: 2, want: true},
				{rule: "null-comparison", line: 1, want: false},
				{rule: "other-rule", line: 2, want: false},
			},
		},
		"trailing comment suppresses its own line": {
			src: "SELECT 1\nWHERE x = NULL -- bqls:ignore null-comparison\nAND y = 1",
			checks: []check{
				{rule: "null-comparison", line: 1, want: true},
				{rule: "null-comparison", line: 2, want: false},
			},
		},
		"hash comment": {
			src: "# bqls:ignore null-comparison\nSELECT x = NULL",
			checks: []check{
				{rule: "null-comparison", line: 1, want: true},
			},
		},
		"block comment trailing": {
			src: "SELECT x = NULL /* bqls:ignore null-comparison */",
			checks: []check{
				{rule: "null-comparison", line: 0, want: true},
			},
		},
		"block comment followed by code applies to the same line": {
			src: "SELECT 1\n/* bqls:ignore null-comparison */ SELECT x = NULL\nSELECT 2",
			checks: []check{
				{rule: "null-comparison", line: 1, want: true},
				{rule: "null-comparison", line: 2, want: false},
			},
		},
		"standalone multi-line block comment suppresses the line after it ends": {
			src: "/*\n bqls:ignore null-comparison\n*/\nSELECT x = NULL",
			checks: []check{
				{rule: "null-comparison", line: 3, want: true},
				{rule: "null-comparison", line: 1, want: false},
			},
		},
		"indented standalone comment": {
			src: "SELECT 1\n    -- bqls:ignore null-comparison\n    WHERE x = NULL",
			checks: []check{
				{rule: "null-comparison", line: 2, want: true},
			},
		},
		"no rule names suppresses all rules": {
			src: "-- bqls:ignore\nSELECT x = NULL",
			checks: []check{
				{rule: "null-comparison", line: 1, want: true},
				{rule: "any-rule", line: 1, want: true},
			},
		},
		"comma and space separated rules": {
			src: "-- bqls:ignore null-comparison, unused-cte partition-filter\nSELECT 1",
			checks: []check{
				{rule: "null-comparison", line: 1, want: true},
				{rule: "unused-cte", line: 1, want: true},
				{rule: "partition-filter", line: 1, want: true},
				{rule: "other", line: 1, want: false},
			},
		},
		"directive inside single quoted string is ignored": {
			src: "SELECT '-- bqls:ignore'\nSELECT x = NULL",
			checks: []check{
				{rule: "null-comparison", line: 0, want: false},
				{rule: "null-comparison", line: 1, want: false},
			},
		},
		"directive inside double quoted string with escaped quote is ignored": {
			src: "SELECT \"a\\\" -- bqls:ignore\"\nSELECT x = NULL",
			checks: []check{
				{rule: "null-comparison", line: 1, want: false},
			},
		},
		"directive inside triple quoted string is ignored": {
			src: "SELECT '''\n-- bqls:ignore\n'''\nSELECT x = NULL",
			checks: []check{
				{rule: "null-comparison", line: 2, want: false},
				{rule: "null-comparison", line: 3, want: false},
			},
		},
		"directive inside raw string is ignored": {
			src: "SELECT r'\\' # bqls:ignore'\nSELECT x = NULL",
			checks: []check{
				{rule: "null-comparison", line: 1, want: false},
			},
		},
		"directive inside backtick identifier is ignored": {
			src: "SELECT `-- bqls:ignore`\nFROM t",
			checks: []check{
				{rule: "null-comparison", line: 1, want: false},
			},
		},
		"comment after string on same line is trailing": {
			src: "SELECT 'a' -- bqls:ignore null-comparison\nSELECT 1",
			checks: []check{
				{rule: "null-comparison", line: 0, want: true},
				{rule: "null-comparison", line: 1, want: false},
			},
		},
		"other comments are not directives": {
			src: "-- just a comment bqls:ignore\nSELECT x = NULL",
			checks: []check{
				{rule: "null-comparison", line: 1, want: false},
			},
		},
		"similar prefix is not a directive": {
			src: "-- bqls:ignored\nSELECT x = NULL",
			checks: []check{
				{rule: "null-comparison", line: 1, want: false},
			},
		},
	}

	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			s := lint.ParseSuppressions(tt.src)
			for _, c := range tt.checks {
				if got := s.Suppressed(c.rule, c.line); got != c.want {
					t.Errorf("Suppressed(%q, %d) = %v, want %v", c.rule, c.line, got, c.want)
				}
			}
		})
	}
}

func TestParseIgnoreLine(t *testing.T) {
	tests := map[string]struct {
		line   string
		wantOK bool
		want   lint.IgnoreLine
	}{
		"line comment with rules": {
			line:   "  -- bqls:ignore a, b",
			wantOK: true,
			want:   lint.IgnoreLine{Rules: []string{"a", "b"}, RulesEnd: 21},
		},
		"explanation after rules": {
			line:   "-- bqls:ignore a -- because",
			wantOK: true,
			want:   lint.IgnoreLine{Rules: []string{"a"}, RulesEnd: 16},
		},
		"block comment": {
			line:   "/* bqls:ignore a */",
			wantOK: true,
			want:   lint.IgnoreLine{Rules: []string{"a"}, RulesEnd: 16},
		},
		"hash comment ignoring all rules": {
			line:   "# bqls:ignore",
			wantOK: true,
			want:   lint.IgnoreLine{RulesEnd: 13},
		},
		"trailing directive after code is not standalone": {
			line: "SELECT 1 -- bqls:ignore a",
		},
		"block comment followed by code is not standalone": {
			line: "/* bqls:ignore a */ SELECT 1",
		},
		"plain comment": {
			line: "-- hello",
		},
	}
	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			got, ok := lint.ParseIgnoreLine(tt.line)
			if ok != tt.wantOK {
				t.Fatalf("ParseIgnoreLine() ok = %v, want %v", ok, tt.wantOK)
			}
			if diff := cmp.Diff(tt.want, got); ok && diff != "" {
				t.Errorf("ParseIgnoreLine() diff (-want +got):\n%s", diff)
			}
		})
	}
}

func TestLineStates(t *testing.T) {
	code := lint.LineState{StartsInCode: true, EndsInCode: true}
	tests := map[string]struct {
		src  string
		want []lint.LineState
	}{
		"plain code lines": {
			src:  "SELECT 1\nFROM t",
			want: []lint.LineState{code, code},
		},
		"line comment ends its line outside code": {
			src:  "SELECT 1 -- note\nFROM t",
			want: []lint.LineState{{StartsInCode: true}, code},
		},
		"hash comment ends its line outside code": {
			src:  "SELECT 1 # note\nFROM t",
			want: []lint.LineState{{StartsInCode: true}, code},
		},
		"CRLF line endings": {
			src:  "SELECT '''\r\nfoo\r\nbar''' AS s\r\n",
			want: []lint.LineState{{StartsInCode: true}, {}, {EndsInCode: true}, code},
		},
		"unterminated block comment ends the last line inside it": {
			src:  "SELECT 1 /* a\nb",
			want: []lint.LineState{{StartsInCode: true}, {}},
		},
		"triple-quoted string spanning lines": {
			src:  "SELECT '''\nfoo\nbar''' AS s",
			want: []lint.LineState{{StartsInCode: true}, {}, {EndsInCode: true}},
		},
		"block comment spanning lines": {
			src:  "SELECT 1 /* a\nb */ FROM t",
			want: []lint.LineState{{StartsInCode: true}, {EndsInCode: true}},
		},
		"comment markers inside a multi-line string are not comments": {
			src:  "SELECT '''\n-- not a comment\n''' AS s",
			want: []lint.LineState{{StartsInCode: true}, {}, {EndsInCode: true}},
		},
		"unterminated string ends the last line inside it": {
			src:  "SELECT '''\nfoo",
			want: []lint.LineState{{StartsInCode: true}, {}},
		},
		"escaped quote does not close a multi-line string": {
			src:  "SELECT '''a\\'''\nb''' AS s",
			want: []lint.LineState{{StartsInCode: true}, {EndsInCode: true}},
		},
	}
	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			got := lint.LineStates(tt.src)
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("LineStates() diff (-want +got):\n%s", diff)
			}
		})
	}
}
