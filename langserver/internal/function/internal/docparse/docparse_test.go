package docparse_test

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/kitagry/bqls/langserver/internal/function/internal/docparse"
)

const pageURL = "https://docs.example.com/ref/sample_functions"

const sample = "# Sample functions\n" +
	"\n" +
	"## Function list\n" +
	"\n" +
	"<table>\n" +
	"<tr>\n" +
	"  <td><a href=\"https://github.com/google/googlesql/blob/master/docs/sample_functions.md#any_value\"><code>ANY_VALUE</code></a>\n" +
	"</td>\n" +
	"</tr>\n" +
	"<tr>\n" +
	"  <td><a href=\"https://github.com/google/googlesql/blob/master/docs/sample_functions.md#nethost\"><code>NET.HOST</code></a>\n" +
	"</td>\n" +
	"</tr>\n" +
	"</table>\n" +
	"\n" +
	"## `ANY_VALUE`\n" +
	"\n" +
	"```googlesql\n" +
	"ANY_VALUE(expression)\n" +
	"```\n" +
	"\n" +
	"**Description**\n" +
	"\n" +
	"Returns `expression` for some row.\n" +
	"See [Aggregate function calls][agg-calls] and [below][local].\n" +
	"\n" +
	"<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->\n" +
	"\n" +
	"[agg-calls]: https://github.com/google/googlesql/blob/master/docs/aggregate-function-calls.md\n" +
	"\n" +
	"<!-- mdlint on -->\n" +
	"\n" +
	"Second paragraph with `[][]` kept as is.\n" +
	"\n" +
	"**Return type**\n" +
	"\n" +
	"Matches the input.\n" +
	"\n" +
	"**Examples**\n" +
	"\n" +
	"```googlesql\n" +
	"SELECT ANY_VALUE(x) FROM t;\n" +
	"\n" +
	"/*-----+\n" +
	" | x   |\n" +
	" +-----*/\n" +
	"```\n" +
	"\n" +
	"```googlesql\n" +
	"SELECT 2;\n" +
	"```\n" +
	"\n" +
	"[local]: #any_value\n" +
	"\n" +
	"## `NET.HOST`\n" +
	"\n" +
	"```googlesql\n" +
	"NET.HOST(url)\n" +
	"```\n" +
	"\n" +
	"**Description**\n" +
	"\n" +
	"Extracts the host.\n" +
	"\n" +
	"## Not a function\n" +
	"\n" +
	"text\n"

func TestParse(t *testing.T) {
	got, err := docparse.Parse([]byte(sample), pageURL)
	if err != nil {
		t.Fatal(err)
	}

	want := []docparse.Function{
		{
			Name:   "ANY_VALUE",
			Method: "ANY_VALUE(expression)",
			Description: "Returns `expression` for some row.\n" +
				"See [Aggregate function calls](https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/aggregate-function-calls) and [below](" + pageURL + "#any_value).\n" +
				"\n" +
				"Second paragraph with `[][]` kept as is.",
			ExampleSQLs: []string{
				"SELECT ANY_VALUE(x) FROM t;\n\n/*-----+\n | x   |\n +-----*/",
				"SELECT 2;",
			},
			URL: pageURL + "#any_value",
		},
		{
			Name:        "NET.HOST",
			Method:      "NET.HOST(url)",
			Description: "Extracts the host.",
			ExampleSQLs: nil,
			URL:         pageURL + "#nethost",
		},
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("Parse() mismatch (-want +got):\n%s", diff)
	}
}

func TestParse_NoFunctions(t *testing.T) {
	got, err := docparse.Parse([]byte("# Title\n\n## Section\n\ntext\n"), pageURL)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Errorf("Parse() = %v, want empty", got)
	}
}

func TestParse_IndentedFenceAndCodeInLinkText(t *testing.T) {
	src := "## `A`\n" +
		"\n" +
		"```googlesql\n" +
		"A(x)\n" +
		"```\n" +
		"\n" +
		"**Description**\n" +
		"\n" +
		"See [`CAST AS NUMERIC`][cast] and `[a][cast]`.\n" +
		"\n" +
		"[cast]: #cast\n" +
		"\n" +
		"**Examples**\n" +
		"\n" +
		"  ```googlesql\n" +
		"  SELECT 1;\n" +
		"  ```\n" +
		"\n" +
		"## `B`\n" +
		"\n" +
		"**Description**\n" +
		"\n" +
		"b\n"

	got, err := docparse.Parse([]byte(src), pageURL)
	if err != nil {
		t.Fatal(err)
	}
	want := []docparse.Function{
		{
			Name:        "A",
			Method:      "A(x)",
			Description: "See [`CAST AS NUMERIC`](" + pageURL + "#cast) and `[a][cast]`.",
			ExampleSQLs: []string{"SELECT 1;"},
			URL:         pageURL + "#a",
		},
		{Name: "B", Description: "b", URL: pageURL + "#b"},
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("Parse() mismatch (-want +got):\n%s", diff)
	}
}

func TestParse_DescriptionWithoutHeading(t *testing.T) {
	src := "## `SUBSTRING`\n" +
		"\n" +
		"```googlesql\n" +
		"SUBSTRING(value, position)\n" +
		"```\n" +
		"\n" +
		"Alias for [`SUBSTR`][substr].\n" +
		"\n" +
		"[substr]: #substr\n" +
		"\n" +
		"**Return type**\n" +
		"\n" +
		"STRING\n"

	got, err := docparse.Parse([]byte(src), pageURL)
	if err != nil {
		t.Fatal(err)
	}
	want := []docparse.Function{{
		Name:        "SUBSTRING",
		Method:      "SUBSTRING(value, position)",
		Description: "Alias for [`SUBSTR`](" + pageURL + "#substr).",
		URL:         pageURL + "#substring",
	}}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("Parse() mismatch (-want +got):\n%s", diff)
	}
}

func TestParse_MultipleSyntaxBlocks(t *testing.T) {
	src := "## `JSON_VALUE`\n" +
		"\n" +
		"```googlesql\n" +
		"JSON_VALUE(json_string_expr)\n" +
		"```\n" +
		"\n" +
		"```googlesql\n" +
		"JSON_VALUE(json_expr)\n" +
		"```\n" +
		"\n" +
		"**Description**\n" +
		"\n" +
		"Extracts a value.\n"

	got, err := docparse.Parse([]byte(src), pageURL)
	if err != nil {
		t.Fatal(err)
	}
	want := []docparse.Function{{
		Name:        "JSON_VALUE",
		Method:      "JSON_VALUE(json_string_expr)\n\nJSON_VALUE(json_expr)",
		Description: "Extracts a value.",
		URL:         pageURL + "#json_value",
	}}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("Parse() mismatch (-want +got):\n%s", diff)
	}
}

func TestParse_HTMLSyntaxBlock(t *testing.T) {
	src := "## `SAFE_CAST` \n" +
		"<a id=\"safe_casting\"></a>\n" +
		"\n" +
		"<pre class=\"lang-sql prettyprint\">\n" +
		"<code>SAFE_CAST(expression AS typename)</code>\n" +
		"</pre>\n" +
		"\n" +
		"**Description**\n" +
		"\n" +
		"Like CAST.\n"

	got, err := docparse.Parse([]byte(src), pageURL)
	if err != nil {
		t.Fatal(err)
	}
	want := []docparse.Function{{
		Name:        "SAFE_CAST",
		Method:      "SAFE_CAST(expression AS typename)",
		Description: "Like CAST.",
		URL:         pageURL + "#safe_cast",
	}}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("Parse() mismatch (-want +got):\n%s", diff)
	}
}
