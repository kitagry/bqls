package langserver

import (
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/sourcegraph/jsonrpc2"
)

func TestHandler_handle_dollarPrefixedMethods(t *testing.T) {
	tests := map[string]struct {
		method    string
		notif     bool
		wantError bool
	}{
		"$/setTrace notification is ignored": {
			method:    "$/setTrace",
			notif:     true,
			wantError: false,
		},
		"unknown $/ notification is ignored": {
			method:    "$/cancelRequest",
			notif:     true,
			wantError: false,
		},
		"unknown $/ request still errors": {
			method:    "$/unknownRequest",
			notif:     false,
			wantError: true,
		},
		"unknown non-$/ method still errors": {
			method:    "unknown/method",
			notif:     false,
			wantError: true,
		},
	}

	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			h := &Handler{logger: logrus.New()}
			req := &jsonrpc2.Request{Method: tt.method, Notif: tt.notif}

			got, err := h.handle(t.Context(), nil, req)

			if tt.wantError {
				if err == nil {
					t.Fatalf("handle() error = nil, want error for method %q", tt.method)
				}
				return
			}

			if err != nil {
				t.Fatalf("handle() error = %v, want nil for method %q", err, tt.method)
			}
			if got != nil {
				t.Errorf("handle() result = %v, want nil for method %q", got, tt.method)
			}
		})
	}
}
