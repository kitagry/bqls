package langserver

import (
	"context"
	"fmt"

	"github.com/kitagry/bqls/langserver/internal/lsp"
)

// notifyAuthError tells the user how to log in, since the raw error from
// BigQuery doesn't.
func (h *Handler) notifyAuthError(err error) {
	ctx := context.Background()
	h.settingsMu.RLock()
	supportsNotification := h.initializeParams.InitializationOptions.SupportsAuthenticationRequiredNotification
	h.settingsMu.RUnlock()

	const guidance = "bqls could not authenticate to BigQuery. Run `gcloud auth login` and `gcloud auth application-default login`, then bqls picks up the new credentials."
	var sendErr error
	if supportsNotification {
		sendErr = h.conn.Notify(ctx, "bqls/authenticationRequired", lsp.AuthenticationRequiredParams{
			Message: guidance,
			Error:   err.Error(),
		})
	} else {
		sendErr = h.showMessage(ctx, lsp.MTWarning, fmt.Sprintf("%s (%v)", guidance, err))
	}
	if sendErr != nil {
		h.logger.Errorf("failed to send the login guidance: %v", sendErr)
	}
}

// commandReloadCredentials recreates the BigQuery client right away, so that
// a new login takes effect without waiting for the next failing call.
func (h *Handler) commandReloadCredentials() (any, error) {
	if err := h.setupByInitializeParams(); err != nil {
		return nil, err
	}
	h.rediagnoseOpenFiles()
	return nil, nil
}
