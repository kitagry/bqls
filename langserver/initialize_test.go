package langserver

import (
	"encoding/json"
	"testing"
)

func TestInitializeOption_SupportsAsyncVirtualTextDocument(t *testing.T) {
	tests := map[string]struct {
		json     string
		expected bool
	}{
		"flag set to true": {
			json:     `{"project_id": "p", "location": "l", "supports_async_virtual_text_document": true}`,
			expected: true,
		},
		"flag omitted defaults to false": {
			json:     `{"project_id": "p", "location": "l"}`,
			expected: false,
		},
	}

	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			var got InitializeOption
			if err := json.Unmarshal([]byte(tt.json), &got); err != nil {
				t.Fatalf("failed to unmarshal: %v", err)
			}
			if got.SupportsAsyncVirtualTextDocument != tt.expected {
				t.Errorf("SupportsAsyncVirtualTextDocument = %v, want %v", got.SupportsAsyncVirtualTextDocument, tt.expected)
			}
		})
	}
}
