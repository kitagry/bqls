package langserver

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/kitagry/bqls/langserver/internal/lsp"
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

func TestHandler_handle_shutdown(t *testing.T) {
	h := newHandler(logrus.New(), "")

	got, err := h.handle(t.Context(), nil, &jsonrpc2.Request{Method: "shutdown"})

	if err != nil {
		t.Fatalf("handle(shutdown) error = %v, want nil", err)
	}
	if got != nil {
		t.Errorf("handle(shutdown) result = %v, want nil", got)
	}
}

func TestHandler_handle_afterShutdown(t *testing.T) {
	tests := map[string]struct {
		method   string
		notif    bool
		wantCode int64
	}{
		"request is rejected with InvalidRequest": {
			method:   "textDocument/hover",
			notif:    false,
			wantCode: jsonrpc2.CodeInvalidRequest,
		},
		"second shutdown is rejected with InvalidRequest": {
			method:   "shutdown",
			notif:    false,
			wantCode: jsonrpc2.CodeInvalidRequest,
		},
		"notification is ignored": {
			method: "textDocument/didOpen",
			notif:  true,
		},
	}

	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			h := newHandler(logrus.New(), "")
			if _, err := h.handle(t.Context(), nil, &jsonrpc2.Request{Method: "shutdown"}); err != nil {
				t.Fatalf("handle(shutdown) error = %v", err)
			}

			got, err := h.handle(t.Context(), nil, &jsonrpc2.Request{Method: tt.method, Notif: tt.notif})

			if got != nil {
				t.Errorf("handle(%q) result = %v, want nil", tt.method, got)
			}
			if tt.wantCode == 0 {
				if err != nil {
					t.Errorf("handle(%q) error = %v, want nil", tt.method, err)
				}
				return
			}
			var rpcErr *jsonrpc2.Error
			if !errors.As(err, &rpcErr) {
				t.Fatalf("handle(%q) error = %v, want *jsonrpc2.Error", tt.method, err)
			}
			if rpcErr.Code != tt.wantCode {
				t.Errorf("handle(%q) error code = %d, want %d", tt.method, rpcErr.Code, tt.wantCode)
			}
		})
	}
}

func TestHandler_handle_exit(t *testing.T) {
	tests := map[string]struct {
		shutdownFirst bool
		want          int
	}{
		"exit after shutdown reports 0": {
			shutdownFirst: true,
			want:          0,
		},
		"exit without shutdown reports 1": {
			shutdownFirst: false,
			want:          1,
		},
	}

	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			h := newHandler(logrus.New(), "")
			if tt.shutdownFirst {
				if _, err := h.handle(t.Context(), nil, &jsonrpc2.Request{Method: "shutdown"}); err != nil {
					t.Fatalf("handle(shutdown) error = %v", err)
				}
			}

			if _, err := h.handle(t.Context(), nil, &jsonrpc2.Request{Method: "exit", Notif: true}); err != nil {
				t.Fatalf("handle(exit) error = %v", err)
			}

			select {
			case got := <-h.ExitNotify():
				if got != tt.want {
					t.Errorf("exit code = %d, want %d", got, tt.want)
				}
			default:
				t.Fatal("ExitNotify() did not receive an exit code")
			}
		})
	}
}

func TestHandler_Handle_recoversPanicAsInternalError(t *testing.T) {
	// A nil project makes the definition handler panic without depending on
	// any particular parser bug.
	h := &Handler{logger: logrus.New()}

	serverStream, clientStream := net.Pipe()
	serverConn := jsonrpc2.NewConn(t.Context(), jsonrpc2.NewBufferedStream(serverStream, jsonrpc2.VSCodeObjectCodec{}), h)
	defer serverConn.Close()
	clientConn := jsonrpc2.NewConn(t.Context(), jsonrpc2.NewBufferedStream(clientStream, jsonrpc2.VSCodeObjectCodec{}), jsonrpc2.HandlerWithError(func(context.Context, *jsonrpc2.Conn, *jsonrpc2.Request) (any, error) {
		return nil, nil
	}))
	defer clientConn.Close()

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	params := lsp.TextDocumentPositionParams{TextDocument: lsp.TextDocumentIdentifier{URI: "file:///a.sql"}}
	err := clientConn.Call(ctx, "textDocument/definition", params, nil)

	var rpcErr *jsonrpc2.Error
	if !errors.As(err, &rpcErr) {
		t.Fatalf("Call() error = %v, want *jsonrpc2.Error", err)
	}
	if rpcErr.Code != jsonrpc2.CodeInternalError {
		t.Errorf("error code = %d, want %d", rpcErr.Code, jsonrpc2.CodeInternalError)
	}
}
