package docparse_test

import (
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/kitagry/bqls/langserver/internal/function/internal/docparse"
)

func TestParseFunctionList(t *testing.T) {
	src := "# comment\n" +
		"ABS\thttps://bq.example.com/ref/mathematical_functions#abs\n" +
		"\n" +
		"NET.HOST\thttps://bq.example.com/ref/net_functions#nethost\n"

	got, err := docparse.ParseFunctionList(strings.NewReader(src))
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"ABS":      "https://bq.example.com/ref/mathematical_functions#abs",
		"NET.HOST": "https://bq.example.com/ref/net_functions#nethost",
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("ParseFunctionList() mismatch (-want +got):\n%s", diff)
	}
}

func TestParseFunctionList_Invalid(t *testing.T) {
	if _, err := docparse.ParseFunctionList(strings.NewReader("ABS\n")); err == nil {
		t.Error("expected an error for a line without URL")
	}
	if _, err := docparse.ParseFunctionList(strings.NewReader("# only comment\n")); err == nil {
		t.Error("expected an error for an empty list")
	}
}

func TestSelect(t *testing.T) {
	list := map[string]string{
		"ABS":     "https://bq.example.com/ref/mathematical_functions#abs",
		"STRING":  "https://bq.example.com/ref/string_functions#string",
		"MAX_BY":  "https://bq.example.com/ref/aggregate_functions#max_by",
		"EXTRACT": "https://bq.example.com/ref/date_functions#extract",
	}
	pages := []docparse.Page{
		{Name: "json_functions", Functions: []docparse.Function{
			{Name: "STRING", Description: "json", URL: "x"},
			{Name: "ARRAY_FILTER", Description: "not in bq", URL: "x"},
		}},
		{Name: "mathematical_functions", Functions: []docparse.Function{
			{Name: "ABS", Description: "abs", URL: "x"},
		}},
		{Name: "string_functions", Functions: []docparse.Function{
			{Name: "STRING", Description: "string", URL: "x"},
		}},
		{Name: "datetime_functions", Functions: []docparse.Function{
			{Name: "EXTRACT", Description: "datetime", URL: "x"},
		}},
		{Name: "timestamp_functions", Functions: []docparse.Function{
			{Name: "EXTRACT", Description: "timestamp", URL: "x"},
		}},
	}

	got, report := docparse.Select(pages, list)

	want := []docparse.Function{
		// The candidate from the page that BigQuery documents the function on wins.
		{Name: "STRING", Description: "string", URL: list["STRING"]},
		{Name: "ABS", Description: "abs", URL: list["ABS"]},
		// No page matches: the first candidate wins.
		{Name: "EXTRACT", Description: "datetime", URL: list["EXTRACT"]},
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("Select() functions mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff([]string{"ARRAY_FILTER"}, report.NotInBigQuery); diff != "" {
		t.Errorf("NotInBigQuery mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff([]string{"MAX_BY"}, report.NotInDocs); diff != "" {
		t.Errorf("NotInDocs mismatch (-want +got):\n%s", diff)
	}
}
