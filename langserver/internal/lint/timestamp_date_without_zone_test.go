package lint_test

import (
	"encoding/json"
	"testing"

	"github.com/kitagry/bqls/langserver/internal/lint"
)

func timestampDateWithoutZoneMessage(fn string) string {
	return fn + " of a TIMESTAMP without a time zone works in UTC, so dates, hours and day boundaries follow UTC, not local time; pass a time zone (e.g. 'Asia/Tokyo'), or 'UTC' if UTC is intended"
}

func TestTimestampDateWithoutZone(t *testing.T) {
	type report struct {
		// text is the exact reported source text.
		text string
		fn   string
		// fixed is src after applying the first (UTC) fix.
		fixed string
	}
	tests := map[string]struct {
		src  string
		want []report
	}{
		"DATE": {
			src:  "SELECT DATE(ts) FROM p.d.t",
			want: []report{{"DATE(ts)", "DATE", "SELECT DATE(ts, 'UTC') FROM p.d.t"}},
		},
		"DATETIME and TIME": {
			src: "SELECT DATETIME(ts), TIME(ts) FROM p.d.t",
			want: []report{
				{"DATETIME(ts)", "DATETIME", "SELECT DATETIME(ts, 'UTC'), TIME(ts) FROM p.d.t"},
				{"TIME(ts)", "TIME", "SELECT DATETIME(ts), TIME(ts, 'UTC') FROM p.d.t"},
			},
		},
		"computed timestamp": {
			src:  "SELECT DATE(TIMESTAMP_ADD(ts, INTERVAL 1 HOUR)) FROM p.d.t",
			want: []report{{"DATE(TIMESTAMP_ADD(ts, INTERVAL 1 HOUR))", "DATE", "SELECT DATE(TIMESTAMP_ADD(ts, INTERVAL 1 HOUR), 'UTC') FROM p.d.t"}},
		},
		"CURRENT_TIMESTAMP": {
			src:  "SELECT DATE(CURRENT_TIMESTAMP())",
			want: []report{{"DATE(CURRENT_TIMESTAMP())", "DATE", "SELECT DATE(CURRENT_TIMESTAMP(), 'UTC')"}},
		},
		"EXTRACT HOUR": {
			src:  "SELECT EXTRACT(HOUR FROM ts) FROM p.d.t",
			want: []report{{"EXTRACT(HOUR FROM ts)", "EXTRACT", "SELECT EXTRACT(HOUR FROM ts AT TIME ZONE 'UTC') FROM p.d.t"}},
		},
		"EXTRACT DATE": {
			src:  "SELECT EXTRACT(DATE FROM ts) FROM p.d.t",
			want: []report{{"EXTRACT(DATE FROM ts)", "EXTRACT", "SELECT EXTRACT(DATE FROM ts AT TIME ZONE 'UTC') FROM p.d.t"}},
		},
		"EXTRACT DAYOFWEEK in WHERE": {
			src:  "SELECT id FROM p.d.t WHERE EXTRACT(DAYOFWEEK FROM ts) = 1",
			want: []report{{"EXTRACT(DAYOFWEEK FROM ts)", "EXTRACT", "SELECT id FROM p.d.t WHERE EXTRACT(DAYOFWEEK FROM ts AT TIME ZONE 'UTC') = 1"}},
		},
		"TIMESTAMP_TRUNC to DAY": {
			src:  "SELECT TIMESTAMP_TRUNC(ts, DAY) FROM p.d.t",
			want: []report{{"TIMESTAMP_TRUNC(ts, DAY)", "TIMESTAMP_TRUNC", "SELECT TIMESTAMP_TRUNC(ts, DAY, 'UTC') FROM p.d.t"}},
		},
		"TIMESTAMP_TRUNC to WEEK(MONDAY)": {
			src:  "SELECT TIMESTAMP_TRUNC(ts, WEEK(MONDAY)) FROM p.d.t",
			want: []report{{"TIMESTAMP_TRUNC(ts, WEEK(MONDAY))", "TIMESTAMP_TRUNC", "SELECT TIMESTAMP_TRUNC(ts, WEEK(MONDAY), 'UTC') FROM p.d.t"}},
		},
		"FORMAT_TIMESTAMP": {
			src:  "SELECT FORMAT_TIMESTAMP('%F %H', ts) FROM p.d.t",
			want: []report{{"FORMAT_TIMESTAMP('%F %H', ts)", "FORMAT_TIMESTAMP", "SELECT FORMAT_TIMESTAMP('%F %H', ts, 'UTC') FROM p.d.t"}},
		},
		"with a time zone": {
			src: "SELECT DATE(ts, 'Asia/Tokyo'), DATETIME(ts, 'UTC'), EXTRACT(HOUR FROM ts AT TIME ZONE 'UTC'), TIMESTAMP_TRUNC(ts, DAY, 'UTC'), FORMAT_TIMESTAMP('%F', ts, 'UTC') FROM p.d.t",
		},
		// Below an hour, every time zone in common use agrees with UTC.
		"sub-hour parts": {
			src: "SELECT EXTRACT(MINUTE FROM ts), EXTRACT(SECOND FROM ts), TIMESTAMP_TRUNC(ts, HOUR), TIMESTAMP_TRUNC(ts, MINUTE) FROM p.d.t",
		},
		"not a TIMESTAMP": {
			src: "SELECT DATE(dt), EXTRACT(HOUR FROM dt), DATE(2024, 1, 1) FROM p.d.t, UNNEST([CURRENT_DATETIME()]) AS dt",
		},
		// The script states the zone these functions use.
		"script sets @@time_zone": {
			src: "SET @@time_zone = 'Asia/Tokyo';\nSELECT DATE(ts) FROM p.d.t",
		},
		"suppressed by comment": {
			src: "SELECT\n-- bqls:ignore timestamp-date-without-zone\nDATE(ts) FROM p.d.t",
		},
	}

	cfg := lint.Config{Rules: map[string]bool{"timestamp-date-without-zone": true}}
	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			got := lint.Run(t.Context(), parseEvents(t, tt.src), cfg, lint.Env{}, lint.TimestampDateWithoutZone)
			if len(got) != len(tt.want) {
				t.Fatalf("got %d diagnostics, want %d: %+v", len(got), len(tt.want), got)
			}
			for i, w := range tt.want {
				d := got[i]
				if text := textInRange(tt.src, d); text != w.text {
					t.Errorf("reported text = %q, want %q", text, w.text)
				}
				if want := timestampDateWithoutZoneMessage(w.fn); d.Rule != "timestamp-date-without-zone" || d.Severity != lint.SeverityWarning || d.Message != want {
					t.Errorf("diagnostic = %+v, want warning %q", d, want)
				}
				// Without time_zone, the only fix states UTC, which keeps
				// the result.
				if len(d.SuggestedFixes) != 1 || d.SuggestedFixes[0].ChangesResult {
					t.Fatalf("fixes = %+v, want one that keeps the result", d.SuggestedFixes)
				}
				fixed := applyFixes(t, tt.src, []lint.Diagnostic{d})
				if fixed != w.fixed {
					t.Errorf("fixed = %q, want %q", fixed, w.fixed)
				}
				if after := lint.Run(t.Context(), parseEvents(t, fixed), cfg, lint.Env{}, lint.TimestampDateWithoutZone); len(after) != len(got)-1 {
					t.Errorf("got %d diagnostics after fix %q, want %d", len(after), fixed, len(got)-1)
				}
			}
		})
	}
}

func TestTimestampDateWithoutZone_TimeZoneOption(t *testing.T) {
	src := "SELECT DATE(ts), EXTRACT(HOUR FROM ts) FROM p.d.t"
	cfg := lint.Config{
		Rules:   map[string]bool{"timestamp-date-without-zone": true},
		Options: map[string]json.RawMessage{"timestamp-date-without-zone": json.RawMessage(`{"time_zone": "Asia/Tokyo"}`)},
	}
	got := lint.Run(t.Context(), parseEvents(t, src), cfg, lint.Env{}, lint.TimestampDateWithoutZone)
	if len(got) != 2 {
		t.Fatalf("got %d diagnostics, want 2: %+v", len(got), got)
	}
	want := []string{
		"SELECT DATE(ts, 'Asia/Tokyo'), EXTRACT(HOUR FROM ts) FROM p.d.t",
		"SELECT DATE(ts), EXTRACT(HOUR FROM ts AT TIME ZONE 'Asia/Tokyo') FROM p.d.t",
	}
	for i, d := range got {
		if len(d.SuggestedFixes) != 2 {
			t.Fatalf("got %d fixes, want UTC and Asia/Tokyo", len(d.SuggestedFixes))
		}
		// The configured zone changes the result, so it is never preferred.
		zoned := d.SuggestedFixes[1]
		if !zoned.ChangesResult {
			t.Error("the time_zone fix has ChangesResult = false, want true")
		}
		if fixed := applyFixes(t, src, []lint.Diagnostic{{SuggestedFixes: []lint.SuggestedFix{zoned}}}); fixed != want[i] {
			t.Errorf("fixed = %q, want %q", fixed, want[i])
		}
	}
}

func TestTimestampDateWithoutZone_InvalidTimeZone(t *testing.T) {
	cfg := lint.Config{Options: map[string]json.RawMessage{"timestamp-date-without-zone": json.RawMessage(`{"time_zone": "Mars/Base"}`)}}
	if err := cfg.Validate([]*lint.Analyzer{lint.TimestampDateWithoutZone}); err == nil {
		t.Error("Validate() = nil, want an error about time_zone")
	}
}

func TestTimestampDateWithoutZone_OffByDefault(t *testing.T) {
	if got := lint.Run(t.Context(), parseEvents(t, "SELECT DATE(ts) FROM p.d.t"), lint.Config{}, lint.Env{}, lint.TimestampDateWithoutZone); len(got) != 0 {
		t.Errorf("got %+v, want nothing while the rule is off by default", got)
	}
}
