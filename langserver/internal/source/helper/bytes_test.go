package helper_test

import (
	"math"
	"testing"

	"github.com/kitagry/bqls/langserver/internal/source/helper"
)

func TestBytesConvert(t *testing.T) {
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
			if got := helper.BytesConvert(tt.bytes); got != tt.want {
				t.Errorf("BytesConvert(%d) = %q, want %q", tt.bytes, got, tt.want)
			}
		})
	}
}
