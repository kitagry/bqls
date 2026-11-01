package helper_test

import (
	"encoding/json"
	"math"
	"strconv"
	"testing"

	"github.com/kitagry/bqls/langserver/internal/source/helper"
)

func TestByteSize_UnmarshalJSON(t *testing.T) {
	tests := map[string]struct {
		in   string
		want helper.ByteSize
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
			var got helper.ByteSize
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
			var got helper.ByteSize
			if err := json.Unmarshal([]byte(in), &got); err == nil {
				t.Errorf("Unmarshal(%s) = %d, want an error", in, got)
			}
		})
	}
}

func TestByteSize_String(t *testing.T) {
	tests := map[string]struct {
		bytes int64
		want  string
	}{
		"zero":              {bytes: 0, want: "0 bytes"},
		"below one KiB":     {bytes: 1023, want: "1023 bytes"},
		"exactly one KiB":   {bytes: 1024, want: "1 KiB"},
		"fraction of a MiB": {bytes: 1536 * 1024, want: "1.50 MiB"},
		"exactly one TiB":   {bytes: 1 << 40, want: "1 TiB"},
		"just below one MiB rounds up to the next unit": {bytes: 1<<20 - 1, want: "1 MiB"},
		"just below one GiB rounds up to the next unit": {bytes: 1<<30 - 1, want: "1 GiB"},
		"stays in the unit when not rounded up":         {bytes: 1048570, want: "1023.99 KiB"},
		"exactly one PiB":                               {bytes: 1 << 50, want: "1 PiB"},
		"exactly one EiB":                               {bytes: 1 << 60, want: "1 EiB"},
		"max int64":                                     {bytes: math.MaxInt64, want: "8 EiB"},
		"negative just below one MiB":                   {bytes: -(1<<20 - 1), want: "-1 MiB"},
		"negative is signed":                            {bytes: -1536, want: "-1.50 KiB"},
		"negative below one KiB":                        {bytes: -1, want: "-1 bytes"},
	}
	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			if got := helper.ByteSize(tt.bytes).String(); got != tt.want {
				t.Errorf("ByteSize(%d).String() = %q, want %q", tt.bytes, got, tt.want)
			}
		})
	}
}
