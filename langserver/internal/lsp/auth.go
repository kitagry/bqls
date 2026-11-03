package lsp

// AuthenticationRequiredParams is pushed by the server as
// bqls/authenticationRequired when BigQuery rejects the credentials, to
// clients that declare supports_authentication_required_notification, so they
// can offer to log in instead of only showing Message.
type AuthenticationRequiredParams struct {
	Message string `json:"message"`
	// Error is the authentication error BigQuery returned.
	Error string `json:"error"`
}
