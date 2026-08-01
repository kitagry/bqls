package bigquery

import (
	"errors"
	"testing"

	"google.golang.org/api/googleapi"
)

func TestFormatCacheError(t *testing.T) {
	tests := map[string]struct {
		err      error
		expected string
	}{
		"googleapi.Error is condensed to code and message, dropping nested Details": {
			err: &googleapi.Error{
				Code:    403,
				Message: "Cloud Resource Manager API has not been used in project kitagry before or it is disabled.",
				Details: []interface{}{
					map[string]interface{}{"@type": "type.googleapis.com/google.rpc.ErrorInfo", "reason": "SERVICE_DISABLED"},
				},
			},
			expected: "googleapi: 403: Cloud Resource Manager API has not been used in project kitagry before or it is disabled.",
		},
		"wrapped googleapi.Error is also condensed": {
			err:      errors.Join(&googleapi.Error{Code: 500, Message: "internal error"}),
			expected: "googleapi: 500: internal error",
		},
		"non googleapi.Error falls back to err.Error()": {
			err:      errors.New("some other error"),
			expected: "some other error",
		},
	}

	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			got := formatCacheError(tt.err)
			if got != tt.expected {
				t.Errorf("formatCacheError() = %q, want %q", got, tt.expected)
			}
		})
	}
}
