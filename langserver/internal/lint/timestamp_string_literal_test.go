package lint_test

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/kitagry/bqls/langserver/internal/lint"
)

func TestTimestampStringLiteral(t *testing.T) {
	const from = "SELECT ts FROM UNNEST([CURRENT_TIMESTAMP()]) AS ts WHERE "
	msg := func(lit string) string {
		return "The string literal " + lit + " has no time zone, so it is converted to TIMESTAMP in the default time zone (UTC), not local time. Add an offset or a time zone name (e.g. '2024-01-01 00:00:00+09', TIMESTAMP('2024-01-01', 'Asia/Tokyo')), or write TIMESTAMP '...' if UTC is intended"
	}
	diag := func(line, start int, lit string) lint.Diagnostic {
		return lint.Diagnostic{
			Rule:     "timestamp-string-literal",
			Range:    rng(line, start, line, start+len(lit)),
			Message:  msg(lit),
			Severity: lint.SeverityWarning,
		}
	}
	tests := map[string]struct {
		src  string
		want []lint.Diagnostic
	}{
		"date-only literal": {
			src:  from + "ts >= '2024-01-01'",
			want: []lint.Diagnostic{diag(0, len(from)+6, "'2024-01-01'")},
		},
		"literal on the left": {
			src:  from + "'2024-01-01 12:00:00' < ts",
			want: []lint.Diagnostic{diag(0, len(from), "'2024-01-01 12:00:00'")},
		},
		"double-quoted literal with T separator and fraction": {
			src:  from + `ts = "2024-01-01T12:00:00.123456"`,
			want: []lint.Diagnostic{diag(0, len(from)+5, `"2024-01-01T12:00:00.123456"`)},
		},
		"not equal": {
			src:  from + "ts != '2024-01-01'",
			want: []lint.Diagnostic{diag(0, len(from)+6, "'2024-01-01'")},
		},
		"IS DISTINCT FROM": {
			src:  from + "ts IS DISTINCT FROM '2024-01-01'",
			want: []lint.Diagnostic{diag(0, len(from)+20, "'2024-01-01'")},
		},
		"BETWEEN reports each bound": {
			src: from + "ts BETWEEN '2024-01-01' AND '2024-02-01'",
			want: []lint.Diagnostic{
				diag(0, len(from)+11, "'2024-01-01'"),
				diag(0, len(from)+28, "'2024-02-01'"),
			},
		},
		"IN list reports only the literals without a time zone": {
			src:  from + "ts IN ('2024-01-01 00:00:00+09', '2024-01-02')",
			want: []lint.Diagnostic{diag(0, len(from)+33, "'2024-01-02'")},
		},
		"NOT BETWEEN": {
			src:  from + "ts NOT BETWEEN '2024-01-01' AND TIMESTAMP '2024-02-01'",
			want: []lint.Diagnostic{diag(0, len(from)+15, "'2024-01-01'")},
		},
		"NOT IN": {
			src:  from + "ts NOT IN ('2024-01-01')",
			want: []lint.Diagnostic{diag(0, len(from)+11, "'2024-01-01'")},
		},
		"parenthesized literal": {
			src:  from + "ts > ('2024-01-01')",
			want: []lint.Diagnostic{diag(0, len(from)+6, "'2024-01-01'")},
		},
		"inside a CTE on a later line": {
			src:  "WITH c AS (\n  SELECT ts FROM UNNEST([CURRENT_TIMESTAMP()]) AS ts\n  WHERE ts >= '2024-01-01'\n)\nSELECT ts FROM c",
			want: []lint.Diagnostic{diag(2, 14, "'2024-01-01'")},
		},
		"numeric offset": {
			src:  from + "ts >= '2024-01-01 00:00:00+09:00'",
			want: nil,
		},
		"short numeric offset": {
			src:  from + "ts >= '2024-01-01 00:00:00-5'",
			want: nil,
		},
		"Z suffix": {
			src:  from + "ts >= '2024-01-01T00:00:00Z'",
			want: nil,
		},
		"time zone name": {
			src:  from + "ts >= '2024-01-01 00:00:00 Asia/Tokyo'",
			want: nil,
		},
		"UTC written explicitly": {
			src:  from + "ts >= '2024-01-01 00:00:00 UTC'",
			want: nil,
		},
		"TIMESTAMP literal": {
			src:  from + "ts >= TIMESTAMP '2024-01-01'",
			want: nil,
		},
		"CAST": {
			src:  from + "ts >= CAST('2024-01-01' AS TIMESTAMP)",
			want: nil,
		},
		"TIMESTAMP function with a time zone": {
			src:  from + "ts >= TIMESTAMP('2024-01-01', 'Asia/Tokyo')",
			want: nil,
		},
		"DATETIME comparison has no time zone": {
			src:  "SELECT d FROM UNNEST([CURRENT_DATETIME()]) AS d WHERE d >= '2024-01-01'",
			want: nil,
		},
		"declared TIMESTAMP variable": {
			src:  "DECLARE v TIMESTAMP;\n" + from + "ts >= v",
			want: nil,
		},
		"not a comparison": {
			src:  "SELECT TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), '2024-01-01', DAY)",
			want: nil,
		},
		// @@time_zone changes how these literals are interpreted, so the author
		// may already have chosen the zone.
		"script sets @@time_zone": {
			src:  "SET @@time_zone = 'Asia/Tokyo';\n" + from + "ts >= '2024-01-01'",
			want: nil,
		},
		"suppressed by comment": {
			src:  from + "\n-- bqls:ignore timestamp-string-literal\nts >= '2024-01-01'",
			want: nil,
		},
	}

	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			parsed := parse(t, tt.src)
			cfg := lint.Config{Rules: map[string]bool{"timestamp-string-literal": true}}
			got := lint.Run(t.Context(), parsed, cfg, lint.Env{}, lint.TimestampStringLiteral)
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("diff (-want +got):\n%s", diff)
			}
		})
	}
}
