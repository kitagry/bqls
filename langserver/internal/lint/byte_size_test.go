package lint_test

import (
	"encoding/json"
	"math"
	"strconv"
	"testing"

	"github.com/kitagry/bqls/langserver/internal/lint"
)

func TestByteSize_UnmarshalJSON(t *testing.T) {
	tests := map[string]struct {
		in   string
		want lint.ByteSize
	}{
		"integer":             {in: `1024`, want: 1024},
		"integer as string":   {in: `"1024"`, want: 1024},
		"bytes unit":          {in: `"100B"`, want: 100},
		"binary unit":         {in: `"100GiB"`, want: 100 << 30},
		"decimal unit":        {in: `"100GB"`, want: 100_000_000_000},
		"space before unit":   {in: `"1 TiB"`, want: 1 << 40},
		"case insensitive":    {in: `"1tib"`, want: 1 << 40},
		"fraction":            {in: `"1.5KiB"`, want: 1536},
		"fraction is floored": {in: `"0.5B"`, want: 0},
		"pebibyte":            {in: `"1PiB"`, want: 1 << 50},
		"exbibyte":            {in: `"7EiB"`, want: 7 << 60},
		"exabyte":             {in: `"9EB"`, want: 9_000_000_000_000_000_000},
		"max int64":           {in: strconv.FormatInt(math.MaxInt64, 10), want: math.MaxInt64},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			var got lint.ByteSize
			if err := json.Unmarshal([]byte(tt.in), &got); err != nil {
				t.Fatalf("Unmarshal(%s) failed: %v", tt.in, err)
			}
			if got != tt.want {
				t.Errorf("Unmarshal(%s) = %d, want %d", tt.in, got, tt.want)
			}
		})
	}
}

func TestByteSize_UnmarshalJSON_Invalid(t *testing.T) {
	tests := map[string]string{
		"empty":        `""`,
		"no number":    `"GiB"`,
		"unknown unit": `"100XB"`,
		// Single-letter units are rejected because users mean binary units
		// in a BigQuery context, while "G" conventionally means 10^9.
		"single-letter unit":        `"100G"`,
		"negative string":           `"-1GiB"`,
		"overflow":                  `"8EiB"`,
		"overflow in PiB":           `"8192PiB"`,
		"boolean":                   `true`,
		"fractional number literal": `1.5`,
	}
	for name, in := range tests {
		t.Run(name, func(t *testing.T) {
			var got lint.ByteSize
			if err := json.Unmarshal([]byte(in), &got); err == nil {
				t.Errorf("Unmarshal(%s) = %d, want an error", in, got)
			}
		})
	}
}
