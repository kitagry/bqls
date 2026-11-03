package langserver

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/sirupsen/logrus"
	"github.com/sourcegraph/jsonrpc2"
)

var errNoCredentials = errors.New("credentials: could not find default credentials. See https://cloud.google.com/docs/authentication/external/set-up-adc for more information")

func TestHandler_notifyAuthError_ShowsLoginGuidance(t *testing.T) {
	h := newHandler(logrus.New(), "test")
	conn := newFakeConn()
	h.conn = conn

	h.notifyAuthError(errNoCredentials)

	got := conn.waitForNotify(t)
	if got.method != "window/showMessage" {
		t.Fatalf("method = %q, want %q", got.method, "window/showMessage")
	}
	params, ok := got.params.(lsp.ShowMessageParams)
	if !ok {
		t.Fatalf("params = %T, want lsp.ShowMessageParams", got.params)
	}
	if params.Type != lsp.MTWarning {
		t.Errorf("Type = %v, want %v", params.Type, lsp.MTWarning)
	}
	for _, want := range []string{"gcloud auth login", "gcloud auth application-default login", errNoCredentials.Error()} {
		if !strings.Contains(params.Message, want) {
			t.Errorf("Message = %q, want it to contain %q", params.Message, want)
		}
	}
}

func TestHandler_notifyAuthError_SendsAuthenticationRequiredToSupportingClient(t *testing.T) {
	h := newHandler(logrus.New(), "test")
	conn := newFakeConn()
	h.conn = conn
	h.initializeParams.InitializationOptions.SupportsAuthenticationRequiredNotification = true

	h.notifyAuthError(errNoCredentials)

	got := conn.waitForNotify(t)
	if got.method != "bqls/authenticationRequired" {
		t.Fatalf("method = %q, want %q", got.method, "bqls/authenticationRequired")
	}
	params, ok := got.params.(lsp.AuthenticationRequiredParams)
	if !ok {
		t.Fatalf("params = %T, want lsp.AuthenticationRequiredParams", got.params)
	}
	if !strings.Contains(params.Message, "gcloud auth application-default login") {
		t.Errorf("Message = %q, want it to contain the login command", params.Message)
	}
	if params.Error != errNoCredentials.Error() {
		t.Errorf("Error = %q, want %q", params.Error, errNoCredentials.Error())
	}
}

func TestHandler_handleWorkspaceExecuteCommand_ReloadCredentials(t *testing.T) {
	const uri lsp.DocumentURI = "file:///a.sql"
	h, _ := newRebuildableHandler(t)
	if err := h.project.UpdateFile(uri, "SELECT 1", 1); err != nil {
		t.Fatal(err)
	}
	project := h.project

	params := json.RawMessage(`{"command": "bqls.reloadCredentials"}`)
	_, err := h.handleWorkspaceExecuteCommand(t.Context(), nil, &jsonrpc2.Request{Params: &params})
	if err != nil {
		t.Fatalf("handleWorkspaceExecuteCommand() error = %v", err)
	}

	if h.project == project {
		t.Error("project was kept, want it rebuilt with a new BigQuery client")
	}
	select {
	case got := <-h.diagnosticRequest:
		if got != uri {
			t.Errorf("diagnostics requested for %s, want %s", got, uri)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("diagnostics were not re-requested for the open document")
	}
}
