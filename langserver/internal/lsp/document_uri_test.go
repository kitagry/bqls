package lsp_test

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/kitagry/bqls/langserver/internal/lsp"
)

func TestParseVirtualTextDocument(t *testing.T) {
	tests := map[string]struct {
		uri         string
		expected    lsp.VirtualTextDocumentInfo
		expectedErr error
	}{
		"Parse project/dataset/table": {
			uri: "bqls://project/p/dataset/d/table/t",
			expected: lsp.VirtualTextDocumentInfo{
				ProjectID: "p",
				DatasetID: "d",
				TableID:   "t",
			},
		},
		"Parse project job": {
			uri: "bqls://project/p/job/j/location/l",
			expected: lsp.VirtualTextDocumentInfo{
				ProjectID: "p",
				JobID:     "j",
				Location:  "l",
			},
		},
	}

	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			got, err := lsp.DocumentURI(tt.uri).VirtualTextDocumentInfo()
			if err != tt.expectedErr {
				t.Fatalf("ParseVirtualTextDocument error expected %v, got %v", tt.expectedErr, err)
			}

			if diff := cmp.Diff(tt.expected, got); diff != "" {
				t.Errorf("ParseVirtualTextDocument result diff (-expect, +got)\n%s", diff)
			}
		})
	}
}
