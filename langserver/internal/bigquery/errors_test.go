package bigquery

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"testing"

	"cloud.google.com/go/auth"
	"google.golang.org/api/googleapi"
)

func TestIsAuthError(t *testing.T) {
	tests := map[string]struct {
		err      error
		expected bool
	}{
		"missing application default credentials": {
			err:      fmt.Errorf("bigquery.NewClient: %w", errors.New("dialing: credentials: could not find default credentials. See https://cloud.google.com/docs/authentication/external/set-up-adc for more information")),
			expected: true,
		},
		"revoked refresh token is rejected by the token endpoint": {
			err: &url.Error{Op: "Get", URL: "https://bigquery.googleapis.com/", Err: &auth.Error{
				Response: &http.Response{StatusCode: http.StatusBadRequest},
				Body:     []byte(`{"error": "invalid_grant", "error_description": "Bad Request"}`),
			}},
			expected: true,
		},
		"reauthentication is required": {
			err: &url.Error{Op: "Get", URL: "https://bigquery.googleapis.com/", Err: &auth.Error{
				Response: &http.Response{StatusCode: http.StatusBadRequest},
				Body:     []byte(`{"error": "invalid_grant", "error_description": "reauth related error (invalid_rapt)"}`),
			}},
			expected: true,
		},
		"token endpoint is temporarily unavailable": {
			err: &auth.Error{
				Response: &http.Response{StatusCode: http.StatusServiceUnavailable},
			},
			expected: false,
		},
		"unauthorized": {
			err:      &googleapi.Error{Code: http.StatusUnauthorized, Message: "Request had invalid authentication credentials."},
			expected: true,
		},
		"access denied is a permission problem, not an authentication one": {
			err:      &googleapi.Error{Code: http.StatusForbidden, Message: "Access Denied: Table my-project:my_dataset.my_table"},
			expected: false,
		},
		"not found": {
			err:      &googleapi.Error{Code: http.StatusNotFound},
			expected: false,
		},
		"network failure": {
			err:      &url.Error{Op: "Get", URL: "https://bigquery.googleapis.com/", Err: errors.New("dial tcp: lookup bigquery.googleapis.com: no such host")},
			expected: false,
		},
		"canceled": {
			err:      context.Canceled,
			expected: false,
		},
		"nil": {
			err:      nil,
			expected: false,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			if got := IsAuthError(tt.err); got != tt.expected {
				t.Errorf("IsAuthError(%v) = %v, want %v", tt.err, got, tt.expected)
			}
		})
	}
}
