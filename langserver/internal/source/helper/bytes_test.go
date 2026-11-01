package helper_test

import (
	"testing"

	"github.com/kitagry/bqls/langserver/internal/source/helper"
)

func TestBytesConvert(t *testing.T) {
	tests := map[string]struct {
		bytes int64
		want  string
	}{
		"zero":                   {bytes: 0, want: "0 bytes"},
		"below one KiB":          {bytes: 1023, want: "1023 bytes"},
		"exactly one KiB":        {bytes: 1024, want: "1 KiB"},
		"fraction of a MiB":      {bytes: 1536 * 1024, want: "1.50 MiB"},
		"exactly one TiB":        {bytes: 1 << 40, want: "1 TiB"},
		"negative is signed":     {bytes: -1536, want: "-1.50 KiB"},
		"negative below one KiB": {bytes: -1, want: "-1 bytes"},
	}
	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			if got := helper.BytesConvert(tt.bytes); got != tt.want {
				t.Errorf("BytesConvert(%d) = %q, want %q", tt.bytes, got, tt.want)
			}
		})
	}
}
