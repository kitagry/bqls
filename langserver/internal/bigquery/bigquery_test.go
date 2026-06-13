package bigquery

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestParseLinkedResource(t *testing.T) {
	tests := map[string]struct {
		linkedResource string
		expected       TableSearchResult
		expectError    bool
	}{
		"valid bigquery table resource": {
			linkedResource: "//bigquery.googleapis.com/projects/my-project/datasets/my_dataset/tables/my_table",
			expected: TableSearchResult{
				ProjectID: "my-project",
				DatasetID: "my_dataset",
				TableID:   "my_table",
			},
		},
		"project id with hyphen and number": {
			linkedResource: "//bigquery.googleapis.com/projects/my-project-123/datasets/dataset1/tables/table1",
			expected: TableSearchResult{
				ProjectID: "my-project-123",
				DatasetID: "dataset1",
				TableID:   "table1",
			},
		},
		"invalid: missing tables segment": {
			linkedResource: "//bigquery.googleapis.com/projects/my-project/datasets/my_dataset",
			expectError:    true,
		},
		"invalid: wrong service": {
			linkedResource: "//storage.googleapis.com/projects/my-project/datasets/my_dataset/tables/my_table",
			expectError:    true,
		},
		"empty string": {
			linkedResource: "",
			expectError:    true,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			got, err := parseLinkedResource(tt.linkedResource)
			if tt.expectError {
				if err == nil {
					t.Errorf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(tt.expected, got); diff != "" {
				t.Errorf("parseLinkedResource result diff (-expected, +got)\n%s", diff)
			}
		})
	}
}
