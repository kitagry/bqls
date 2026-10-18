package bigquery

import (
	"errors"
	"net/http"

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
