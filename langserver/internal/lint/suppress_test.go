package lint_test

import (
	"testing"

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
