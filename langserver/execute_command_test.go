package langserver

import (
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/google/go-cmp/cmp"
)

func TestParseSpreadsheetURL(t *testing.T) {
	tests := map[string]struct {
		sheetURL              string
		expectedSpreadsheetID string
		expectedSheetID       int
	}{
		"parse spreadsheetID": {
			sheetURL:              "https://docs.google.com/spreadsheets/d/asdf_asdfasdf/edit",
			expectedSpreadsheetID: "asdf_asdfasdf",
			expectedSheetID:       0,
		},
		"parse sheetID": {
			sheetURL:              "https://docs.google.com/spreadsheets/d/asdf_asdfasdf/edit?gid=123#gid=123",
			expectedSpreadsheetID: "asdf_asdfasdf",
			expectedSheetID:       123,
		},
		"has extra query params": {
			sheetURL:              "https://docs.google.com/spreadsheets/d/asdf_asdfasdf/edit?gid=123&foo=bar#gid=123",
			expectedSpreadsheetID: "asdf_asdfasdf",
			expectedSheetID:       123,
		},
	}

	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			gotSpreadsheetID, gotSheetID, err := parseSpreadsheetURL(tt.sheetURL)
			if err != nil {
				t.Fatal(err)
			}
			if gotSpreadsheetID != tt.expectedSpreadsheetID {
				t.Errorf("got %q, want %q", gotSpreadsheetID, tt.expectedSpreadsheetID)
			}
			if gotSheetID != tt.expectedSheetID {
				t.Errorf("got %d, want %d", gotSheetID, tt.expectedSheetID)
			}
		})
	}
}

func TestFormatCSV(t *testing.T) {
	tests := map[string]struct {
		record []bigquery.Value
		schema bigquery.Schema
		expect []string
	}{
		"nil returns empty string": {
			record: []bigquery.Value{nil},
			schema: bigquery.Schema{{Name: "int", Type: bigquery.IntegerFieldType}},
			expect: []string{""},
		},
		"int64 returns int string": {
			record: []bigquery.Value{int64(1)},
			schema: bigquery.Schema{{Name: "int", Type: bigquery.IntegerFieldType}},
			expect: []string{"1"},
		},
		"time.Time returns RFC3339 format string": {
			record: []bigquery.Value{timeMustParse("2021-01-01T00:00:00Z")},
			schema: bigquery.Schema{{Name: "timestamp", Type: bigquery.TimestampFieldType}},
			expect: []string{"2021-01-01T00:00:00Z"},
		},
		"nested int returns int array string": {
			record: []bigquery.Value{[]bigquery.Value{int64(1)}},
			schema: bigquery.Schema{{Name: "int", Type: bigquery.IntegerFieldType, Repeated: true}},
			expect: []string{"[1]"},
		},
		"struct returns struct string": {
			record: []bigquery.Value{[]bigquery.Value{"a", "b"}},
			schema: bigquery.Schema{
				{
					Name: "struct",
					Type: bigquery.RecordFieldType,
					Schema: bigquery.Schema{
						{Name: "string1", Type: bigquery.StringFieldType},
						{Name: "string2", Type: bigquery.StringFieldType},
					},
				},
			},
			expect: []string{`{"string1":"a","string2":"b"}`},
		},
		"nested struct returns nested struct string": {
			record: []bigquery.Value{[]bigquery.Value{[]bigquery.Value{"a", "b"}}},
			schema: bigquery.Schema{
				{
					Name: "struct",
					Type: bigquery.RecordFieldType,
					Schema: bigquery.Schema{
						{Name: "string1", Type: bigquery.StringFieldType},
						{Name: "string2", Type: bigquery.StringFieldType},
					},
					Repeated: true,
				},
			},
			expect: []string{`[{"string1":"a","string2":"b"}]`},
		},
		"nested nested struct returns nested nested struct string": {
			record: []bigquery.Value{[]bigquery.Value{[]bigquery.Value{[]bigquery.Value{"a", "b"}}}},
			schema: bigquery.Schema{
				{
					Name: "struct",
					Type: bigquery.RecordFieldType,
					Schema: bigquery.Schema{
						{
							Name: "struct2",
							Type: bigquery.RecordFieldType,
							Schema: bigquery.Schema{
								{Name: "string1", Type: bigquery.StringFieldType},
								{Name: "string2", Type: bigquery.StringFieldType},
							},
							Repeated: true,
						},
					},
				},
			},
			expect: []string{`{"struct2":[{"string1":"a","string2":"b"}]}`},
		},
	}

	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			got, err := formatCSV(tt.record, tt.schema)
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(tt.expect, got); diff != "" {
				t.Errorf("formatCSV result diff (-expect, +got)\n%s", diff)
			}
		})
	}
}

func timeMustParse(s string) bigquery.Value {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err)
	}
	return t
}
