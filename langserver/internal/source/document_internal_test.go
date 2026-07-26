package source

import "testing"

func TestColumnMarkdownTable(t *testing.T) {
	got := columnMarkdownTable([]columnNameType{
		{name: "id", typeName: "INT64"},
		{name: "name", typeName: "STRING"},
	})
	want := "| Name | Type |\n" +
		"| --- | --- |\n" +
		"| id | INT64 |\n" +
		"| name | STRING |\n"
	if got != want {
		t.Errorf("columnMarkdownTable() =\n%q\nwant\n%q", got, want)
	}
}
