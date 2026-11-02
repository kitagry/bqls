package lint_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/kitagry/bqls/langserver/internal/lint"
	"github.com/kitagry/bqls/langserver/internal/lsp"
)

func timestampFix(r lsp.Range, lit, zone string) lint.SuggestedFix {
	text := "TIMESTAMP(" + lit + ", '" + zone + "')"
	return lint.SuggestedFix{
		Title: "Replace with " + text,
		Edits: []lsp.TextEdit{{Range: r, NewText: text}},
	}
}

func TestTimestampStringLiteral(t *testing.T) {
	const from = "SELECT ts FROM UNNEST([CURRENT_TIMESTAMP()]) AS ts WHERE "
	msg := func(lit string) string {
		return "The string literal " + lit + " has no time zone, so it is converted to TIMESTAMP in the default time zone (UTC), not local time. Add an offset or a time zone name (e.g. '2024-01-01 00:00:00+09', TIMESTAMP('2024-01-01', 'Asia/Tokyo')), or write TIMESTAMP '...' if UTC is intended"
	}
	diag := func(line, start int, lit string) lint.Diagnostic {
		r := rng(line, start, line, start+len(lit))
		return lint.Diagnostic{
			Rule:           "timestamp-string-literal",
			Range:          r,
			Message:        msg(lit),
			Severity:       lint.SeverityWarning,
			SuggestedFixes: []lint.SuggestedFix{timestampFix(r, lit, "UTC")},
		}
	}
	tests := map[string]struct {
		src  string
		want []lint.Diagnostic
		// wantFixed is src after applying every fix; it must analyze cleanly
		// and no longer be reported.
		wantFixed string
	}{
		"date-only literal": {
			src:       from + "ts >= '2024-01-01'",
			want:      []lint.Diagnostic{diag(0, len(from)+6, "'2024-01-01'")},
			wantFixed: from + "ts >= TIMESTAMP('2024-01-01', 'UTC')",
		},
		"raw triple-quoted literal": {
			src:       from + "ts >= r\"\"\"2024-01-01\"\"\"",
			want:      []lint.Diagnostic{diag(0, len(from)+6, `r"""2024-01-01"""`)},
			wantFixed: from + `ts >= TIMESTAMP(r"""2024-01-01""", 'UTC')`,
		},
		"literal on the left": {
			src:  from + "'2024-01-01 12:00:00' < ts",
			want: []lint.Diagnostic{diag(0, len(from), "'2024-01-01 12:00:00'")},
		},
		"double-quoted literal with T separator and fraction": {
			src:       from + `ts = "2024-01-01T12:00:00.123456"`,
			want:      []lint.Diagnostic{diag(0, len(from)+5, `"2024-01-01T12:00:00.123456"`)},
			wantFixed: from + `ts = TIMESTAMP("2024-01-01T12:00:00.123456", 'UTC')`,
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
			wantFixed: from + "ts BETWEEN TIMESTAMP('2024-01-01', 'UTC') AND TIMESTAMP('2024-02-01', 'UTC')",
		},
		"IN list reports only the literals without a time zone": {
			src:       from + "ts IN ('2024-01-01 00:00:00+09', '2024-01-02')",
			want:      []lint.Diagnostic{diag(0, len(from)+33, "'2024-01-02'")},
			wantFixed: from + "ts IN ('2024-01-01 00:00:00+09', TIMESTAMP('2024-01-02', 'UTC'))",
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
			src:       from + "ts > ('2024-01-01')",
			want:      []lint.Diagnostic{diag(0, len(from)+6, "'2024-01-01'")},
			wantFixed: from + "ts > (TIMESTAMP('2024-01-01', 'UTC'))",
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
				t.Fatalf("diff (-want +got):\n%s", diff)
			}
			if tt.wantFixed == "" {
				return
			}
			fixed := applyFixes(t, tt.src, got)
			if diff := cmp.Diff(tt.wantFixed, fixed); diff != "" {
				t.Fatalf("fixed source diff (-want +got):\n%s", diff)
			}
			if after := lint.Run(t.Context(), parse(t, fixed), cfg, lint.Env{}, lint.TimestampStringLiteral); len(after) != 0 {
				t.Errorf("diagnostics after fix: %+v", after)
			}
		})
	}
}

func timestampStringLiteralConfig(options string) lint.Config {
	return lint.Config{
		Rules:   map[string]bool{"timestamp-string-literal": true},
		Options: map[string]json.RawMessage{"timestamp-string-literal": json.RawMessage(options)},
	}
}

func TestTimestampStringLiteral_TimeZone(t *testing.T) {
	const src = "SELECT ts FROM UNNEST([CURRENT_TIMESTAMP()]) AS ts WHERE ts >= '2024-01-01'"
	r := rng(0, 63, 0, 75)
	tests := map[string]struct {
		options string
		want    []lint.SuggestedFix
	}{
		// The UTC fix keeps the result, so it stays first and is the one
		// `--fix` applies.
		"configured zone is offered after UTC": {
			options: `{"time_zone": "Asia/Tokyo"}`,
			want:    []lint.SuggestedFix{timestampFix(r, "'2024-01-01'", "UTC"), timestampFix(r, "'2024-01-01'", "Asia/Tokyo")},
		},
		"offset": {
			options: `{"time_zone": "+09:00"}`,
			want:    []lint.SuggestedFix{timestampFix(r, "'2024-01-01'", "UTC"), timestampFix(r, "'2024-01-01'", "+09:00")},
		},
		"UTC is not offered twice": {
			options: `{"time_zone": "UTC"}`,
			want:    []lint.SuggestedFix{timestampFix(r, "'2024-01-01'", "UTC")},
		},
	}
	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			cfg := timestampStringLiteralConfig(tt.options)
			if err := cfg.Validate(lint.Analyzers); err != nil {
				t.Fatalf("Validate() = %v", err)
			}
			got := lint.Run(t.Context(), parse(t, src), cfg, lint.Env{}, lint.TimestampStringLiteral)
			if len(got) != 1 {
				t.Fatalf("got %d diagnostics, want 1: %+v", len(got), got)
			}
			if diff := cmp.Diff(tt.want, got[0].SuggestedFixes); diff != "" {
				t.Errorf("fixes diff (-want +got):\n%s", diff)
			}
			fixed := applyFixes(t, src, []lint.Diagnostic{{SuggestedFixes: got[0].SuggestedFixes[len(got[0].SuggestedFixes)-1:]}})
			parse(t, fixed)
		})
	}
}

func TestTimestampStringLiteral_InvalidTimeZone(t *testing.T) {
	for _, zone := range []string{"Asia/Nowhere", "Local", "', 'UTC", "+9:00:00"} {
		t.Run(zone, func(t *testing.T) {
			opts, err := json.Marshal(map[string]string{"time_zone": zone})
			if err != nil {
				t.Fatal(err)
			}
			err = timestampStringLiteralConfig(string(opts)).Validate(lint.Analyzers)
			if err == nil || !strings.Contains(err.Error(), `rule "timestamp-string-literal"`) || !strings.Contains(err.Error(), "time_zone") {
				t.Errorf("Validate() = %v, want an error about time_zone of timestamp-string-literal", err)
			}
		})
	}
}
