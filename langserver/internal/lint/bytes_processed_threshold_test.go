package lint_test

import (
	"encoding/json"
	"strings"
	"testing"

	bq "cloud.google.com/go/bigquery"
	"github.com/google/go-cmp/cmp"
	"github.com/kitagry/bqls/langserver/internal/lint"
	"github.com/kitagry/bqls/langserver/internal/lsp"
)

const tib = int64(1) << 40

func dryRunProcessing(bytes int64) *bq.JobStatus {
	return &bq.JobStatus{Statistics: &bq.JobStatistics{TotalBytesProcessed: bytes}}
}

func bytesProcessedThreshold(r lsp.Range, message string) lint.Diagnostic {
	return lint.Diagnostic{
		Rule:     "bytes-processed-threshold",
		Range:    r,
		Message:  message,
		Severity: lint.SeverityWarning,
	}
}

var enableBytesProcessedThreshold = lint.Config{Rules: map[string]bool{"bytes-processed-threshold": true}}

func TestBytesProcessedThreshold(t *testing.T) {
	tests := map[string]struct {
		src    string
		cfg    lint.Config
		dryRun *bq.JobStatus
		want   []lint.Diagnostic
	}{
		"no dry-run result": {
			src: "SELECT 1",
			cfg: enableBytesProcessedThreshold,
		},
		"dry-run result without statistics": {
			src:    "SELECT 1",
			cfg:    enableBytesProcessedThreshold,
			dryRun: &bq.JobStatus{},
		},
		"below the default threshold of 1 TiB": {
			src:    "SELECT 1",
			cfg:    enableBytesProcessedThreshold,
			dryRun: dryRunProcessing(tib - 1),
		},
		"exactly the default threshold": {
			src:    "SELECT 1",
			cfg:    enableBytesProcessedThreshold,
			dryRun: dryRunProcessing(tib),
		},
		"above the default threshold": {
			src:    "SELECT 1",
			cfg:    enableBytesProcessedThreshold,
			dryRun: dryRunProcessing(2 * tib),
			want: []lint.Diagnostic{bytesProcessedThreshold(rng(0, 0, 0, 8),
				"This query will process 2 TiB, which exceeds the threshold of 1 TiB")},
		},
		"reported on the first line of the first statement": {
			src:    "-- header\nSELECT 1\nUNION ALL SELECT 2;\nSELECT 3",
			cfg:    enableBytesProcessedThreshold,
			dryRun: dryRunProcessing(2 * tib),
			want: []lint.Diagnostic{bytesProcessedThreshold(rng(1, 0, 1, 8),
				"This query will process 2 TiB, which exceeds the threshold of 1 TiB")},
		},
		"above a configured max_bytes": {
			src: "SELECT 1",
			cfg: lint.Config{
				Rules:   map[string]bool{"bytes-processed-threshold": true},
				Options: map[string]json.RawMessage{"bytes-processed-threshold": json.RawMessage(`{"max_bytes":1024}`)},
			},
			dryRun: dryRunProcessing(1536),
			want: []lint.Diagnostic{bytesProcessedThreshold(rng(0, 0, 0, 8),
				"This query will process 1.50 KiB, which exceeds the threshold of 1 KiB")},
		},
		"below a configured max_bytes": {
			src: "SELECT 1",
			cfg: lint.Config{
				Rules:   map[string]bool{"bytes-processed-threshold": true},
				Options: map[string]json.RawMessage{"bytes-processed-threshold": json.RawMessage(`{"max_bytes":2048}`)},
			},
			dryRun: dryRunProcessing(1536),
		},
		"above a max_bytes with a unit": {
			src: "SELECT 1",
			cfg: lint.Config{
				Rules:   map[string]bool{"bytes-processed-threshold": true},
				Options: map[string]json.RawMessage{"bytes-processed-threshold": json.RawMessage(`{"max_bytes":"100GiB"}`)},
			},
			dryRun: dryRunProcessing(101 << 30),
			want: []lint.Diagnostic{bytesProcessedThreshold(rng(0, 0, 0, 8),
				"This query will process 101 GiB, which exceeds the threshold of 100 GiB")},
		},
		"disabled by default": {
			src:    "SELECT 1",
			dryRun: dryRunProcessing(2 * tib),
		},
		"suppressed by an ignore comment": {
			src:    "-- bqls:ignore bytes-processed-threshold\nSELECT 1",
			cfg:    enableBytesProcessedThreshold,
			dryRun: dryRunProcessing(2 * tib),
		},
	}
	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			got := lint.Run(t.Context(), parse(t, tt.src), tt.cfg, lint.Env{DryRun: tt.dryRun}, lint.BytesProcessedThreshold)
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("Run() diff (-want +got):\n%s", diff)
			}
		})
	}
}

func TestBytesProcessedThreshold_NegativeMaxBytes(t *testing.T) {
	cfg := lint.Config{
		Rules:   map[string]bool{"bytes-processed-threshold": true},
		Options: map[string]json.RawMessage{"bytes-processed-threshold": json.RawMessage(`{"max_bytes":-1}`)},
	}

	t.Run("Validate reports it", func(t *testing.T) {
		err := cfg.Validate(lint.Analyzers)
		if err == nil || !strings.Contains(err.Error(), `rule "bytes-processed-threshold"`) || !strings.Contains(err.Error(), "max_bytes") {
			t.Errorf("Validate() = %v, want an error about max_bytes of bytes-processed-threshold", err)
		}
	})

	t.Run("Run skips the rule", func(t *testing.T) {
		got := lint.Run(t.Context(), parse(t, "SELECT 1"), cfg, lint.Env{DryRun: dryRunProcessing(2 * tib)}, lint.BytesProcessedThreshold)
		if len(got) != 0 {
			t.Errorf("Run() = %+v, want no diagnostics for invalid options", got)
		}
	})
}

func TestBytesProcessedThreshold_MaxBytesInConfigFile(t *testing.T) {
	tests := map[string]struct {
		maxBytes string
		wantErr  string
	}{
		"binary unit":  {maxBytes: "100GiB"},
		"integer":      {maxBytes: "107374182400"},
		"unknown unit": {maxBytes: "100G", wantErr: `unknown unit "G"`},
	}
	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			cfg, err := lint.ParseConfigFile([]byte("lint:\n  rules:\n    bytes-processed-threshold:\n      enabled: true\n      max_bytes: " + tt.maxBytes + "\n"))
			if err != nil {
				t.Fatal(err)
			}
			err = cfg.Validate(lint.Analyzers)
			if tt.wantErr == "" {
				if err != nil {
					t.Errorf("Validate() = %v, want nil", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("Validate() = %v, want it to contain %q", err, tt.wantErr)
			}
		})
	}
}
