package langserver

import (
	"encoding/json"
	"testing"

	"github.com/kitagry/bqls/langserver/internal/lsp"
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

func TestHandler_buildInitializeResult_ServerInfo(t *testing.T) {
	h := &Handler{version: "v1.2.3"}

	result := h.buildInitializeResult()

	if result.ServerInfo == nil {
		t.Fatal("ServerInfo is nil, want non-nil")
	}
	if result.ServerInfo.Name != "bqls" {
		t.Errorf("ServerInfo.Name = %q, want %q", result.ServerInfo.Name, "bqls")
	}
	if result.ServerInfo.Version != "v1.2.3" {
		t.Errorf("ServerInfo.Version = %q, want %q", result.ServerInfo.Version, "v1.2.3")
	}
}

func TestServerInfo_JSON(t *testing.T) {
	tests := map[string]struct {
		info lsp.ServerInfo
		want string
	}{
		"with version": {
			info: lsp.ServerInfo{Name: "bqls", Version: "v1.2.3"},
			want: `{"name":"bqls","version":"v1.2.3"}`,
		},
		"version omitted when empty": {
			info: lsp.ServerInfo{Name: "bqls"},
			want: `{"name":"bqls"}`,
		},
	}

	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			got, err := json.Marshal(tt.info)
			if err != nil {
				t.Fatalf("failed to marshal: %v", err)
			}
			if string(got) != tt.want {
				t.Errorf("json = %s, want %s", got, tt.want)
			}
		})
	}
}
