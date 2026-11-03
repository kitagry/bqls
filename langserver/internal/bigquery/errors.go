package bigquery

import (
	"errors"
	"net/http"
	"strings"

	"cloud.google.com/go/auth"
	"google.golang.org/api/googleapi"
)

// IsRuntimeFailure reports whether err says nothing about the query itself,
// e.g. missing credentials, denied access, throttling, or an unreachable API.
func IsRuntimeFailure(err error) bool {
	var apiErr *googleapi.Error
	if errors.As(err, &apiErr) {
		return apiErr.Code == http.StatusUnauthorized || apiErr.Code == http.StatusForbidden ||
			apiErr.Code == http.StatusTooManyRequests || apiErr.Code >= 500
	}
	return true
}

// IsAuthError reports whether err means the user has to log in again, e.g.
// Application Default Credentials are missing, expired, or need
// reauthentication. Denied access is not one, since logging in won't fix it.
func IsAuthError(err error) bool {
	if err == nil {
		return false
	}
	var apiErr *googleapi.Error
	if errors.As(err, &apiErr) {
		return apiErr.Code == http.StatusUnauthorized
	}
	// The token endpoint rejects a revoked or expired refresh token, or one
	// that needs reauthentication, with a 4xx; a 5xx is only an outage.
	var authErr *auth.Error
	if errors.As(err, &authErr) {
		return authErr.Response == nil || authErr.Response.StatusCode < http.StatusInternalServerError
	}
	return strings.Contains(err.Error(), "could not find default credentials")
}
