package lint_test

import (
	"testing"

	bq "cloud.google.com/go/bigquery"
	"github.com/golang/mock/gomock"
	"github.com/kitagry/bqls/langserver/internal/bigquery/mock_bigquery"
	"github.com/kitagry/bqls/langserver/internal/lint"
	"github.com/kitagry/bqls/langserver/internal/source/file"
	"github.com/sirupsen/logrus"
)

// parseTyped analyzes src against a table p.d.t whose columns have different types.
func parseTyped(t *testing.T, src string) file.ParsedFile {
	t.Helper()
	ctrl := gomock.NewController(t)
	bqClient := mock_bigquery.NewMockClient(ctrl)
	bqClient.EXPECT().GetDefaultProject().Return("p").AnyTimes()
	bqClient.EXPECT().GetTableMetadata(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&bq.TableMetadata{Schema: bq.Schema{
			{Name: "i", Type: bq.IntegerFieldType},
			{Name: "f", Type: bq.FloatFieldType},
			{Name: "n", Type: bq.NumericFieldType},
			{Name: "d", Type: bq.DateFieldType},
			{Name: "dt", Type: bq.DateTimeFieldType},
			{Name: "s", Type: bq.StringFieldType},
		}}, nil).AnyTimes()
	parsed := file.NewAnalyzer(logrus.New(), bqClient).ParseFile("file:///a.sql", src)
	if len(parsed.Errors) > 0 {
		t.Fatalf("unexpected analysis errors: %v", parsed.Errors)
	}
	return parsed
}

func joinKeyImplicitCastMessage(from, to string) string {
	msg := "join keys of different types are compared: " + from + " is implicitly cast to " + to
	if to == "FLOAT64" {
		return msg + ", which cannot represent every value exactly, so different keys may compare equal; give both columns the same type, or write an explicit CAST"
	}
	return msg + "; mismatched key types usually point to a schema design mistake, so give both columns the same type, or write an explicit CAST"
}

func TestJoinKeyImplicitCast(t *testing.T) {
	type report struct {
		// text is the exact text of the reported range.
		text, from, to string
	}
	tests := map[string]struct {
		src  string
		want []report
	}{
		"int64 and float64": {
			src:  "SELECT 1 FROM p.d.t AS a JOIN p.d.t AS b ON a.i = b.f",
			want: []report{{"a.i = b.f", "INT64", "FLOAT64"}},
		},
		"cast on the right": {
			src:  "SELECT 1 FROM p.d.t AS a JOIN p.d.t AS b ON a.f = b.i",
			want: []report{{"a.f = b.i", "INT64", "FLOAT64"}},
		},
		"int64 and numeric": {
			src:  "SELECT 1 FROM p.d.t AS a JOIN p.d.t AS b ON a.i = b.n",
			want: []report{{"a.i = b.n", "INT64", "NUMERIC"}},
		},
		"numeric and float64": {
			src:  "SELECT 1 FROM p.d.t AS a JOIN p.d.t AS b ON a.n = b.f",
			want: []report{{"a.n = b.f", "NUMERIC", "FLOAT64"}},
		},
		"date and datetime": {
			src:  "SELECT 1 FROM p.d.t AS a JOIN p.d.t AS b ON a.d = b.dt",
			want: []report{{"a.d = b.dt", "DATE", "DATETIME"}},
		},
		"is not distinct from": {
			src:  "SELECT 1 FROM p.d.t AS a JOIN p.d.t AS b ON a.i IS NOT DISTINCT FROM b.f",
			want: []report{{"a.i IS NOT DISTINCT FROM b.f", "INT64", "FLOAT64"}},
		},
		"left join": {
			src:  "SELECT 1 FROM p.d.t AS a LEFT JOIN p.d.t AS b ON a.i = b.f",
			want: []report{{"a.i = b.f", "INT64", "FLOAT64"}},
		},
		"one conjunct of and": {
			src:  "SELECT 1 FROM p.d.t AS a JOIN p.d.t AS b ON a.s = b.s AND a.i = b.f",
			want: []report{{"a.i = b.f", "INT64", "FLOAT64"}},
		},
		"two keys": {
			src:  "SELECT 1 FROM p.d.t AS a JOIN p.d.t AS b ON a.i = b.f AND a.d = b.dt",
			want: []report{{"a.i = b.f", "INT64", "FLOAT64"}, {"a.d = b.dt", "DATE", "DATETIME"}},
		},
		"using": {
			src:  "SELECT 1 FROM p.d.t AS a JOIN (SELECT f AS i FROM p.d.t) AS b USING (i)",
			want: []report{{"JOIN", "INT64", "FLOAT64"}},
		},
		"subquery on one side": {
			src:  "SELECT 1 FROM p.d.t AS a JOIN (SELECT f AS g FROM p.d.t) AS b ON a.i = b.g",
			want: []report{{"a.i = b.g", "INT64", "FLOAT64"}},
		},
		"inside a cte": {
			src:  "WITH c AS (SELECT a.i FROM p.d.t AS a JOIN p.d.t AS b ON a.i = b.f) SELECT i FROM c",
			want: []report{{"a.i = b.f", "INT64", "FLOAT64"}},
		},
		"multiline": {
			src:  "SELECT 1\nFROM p.d.t AS a\nJOIN p.d.t AS b\n  ON a.i = b.f",
			want: []report{{"a.i = b.f", "INT64", "FLOAT64"}},
		},

		// No implicit cast between two join keys.
		"same type":                     {src: "SELECT 1 FROM p.d.t AS a JOIN p.d.t AS b ON a.i = b.i"},
		"using with the same type":      {src: "SELECT 1 FROM p.d.t AS a JOIN p.d.t AS b USING (i)"},
		"literal":                       {src: "SELECT 1 FROM p.d.t AS a JOIN p.d.t AS b ON a.i = b.i AND a.i = 1.5"},
		"explicit cast":                 {src: "SELECT 1 FROM p.d.t AS a JOIN p.d.t AS b ON CAST(a.i AS FLOAT64) = b.f"},
		"explicit cast to another type": {src: "SELECT 1 FROM p.d.t AS a JOIN p.d.t AS b ON CAST(a.i AS NUMERIC) = b.f"},
		"expression":                    {src: "SELECT 1 FROM p.d.t AS a JOIN p.d.t AS b ON a.i = b.f + 0"},
		"or":                            {src: "SELECT 1 FROM p.d.t AS a JOIN p.d.t AS b ON a.i = b.f OR a.s = b.s"},
		"not equality":                  {src: "SELECT 1 FROM p.d.t AS a JOIN p.d.t AS b ON a.i < b.f"},
		"same side":                     {src: "SELECT 1 FROM p.d.t AS a JOIN p.d.t AS b ON a.s = b.s AND a.i = a.f"},
		"comma join with where":         {src: "SELECT 1 FROM p.d.t AS a, p.d.t AS b WHERE a.i = b.f"},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			parsed := parseTyped(t, tt.src)
			got := lint.Run(t.Context(), parsed, lint.Config{}, lint.Env{}, lint.JoinKeyImplicitCast)
			if len(got) != len(tt.want) {
				t.Fatalf("got %d diagnostics %+v, want %d", len(got), got, len(tt.want))
			}
			for i, d := range got {
				w := tt.want[i]
				if text := textInRange(tt.src, d); text != w.text {
					t.Errorf("diagnostic %d range = %q, want %q", i, text, w.text)
				}
				if d.Rule != "join-key-implicit-cast" || d.Message != joinKeyImplicitCastMessage(w.from, w.to) || d.Severity != lint.SeverityWarning || len(d.SuggestedFixes) != 0 {
					t.Errorf("unexpected diagnostic metadata: %+v", d)
				}
			}
		})
	}
}

func TestJoinKeyImplicitCastSuppressed(t *testing.T) {
	src := "SELECT 1\nFROM p.d.t AS a\nJOIN p.d.t AS b\n  -- bqls:ignore join-key-implicit-cast\n  ON a.i = b.f"
	parsed := parseTyped(t, src)
	if got := lint.Run(t.Context(), parsed, lint.Config{}, lint.Env{}, lint.JoinKeyImplicitCast); len(got) != 0 {
		t.Errorf("got %+v, want suppressed", got)
	}
}

func TestJoinKeyImplicitCastConfig(t *testing.T) {
	src := "SELECT 1 FROM p.d.t AS a JOIN p.d.t AS b ON a.i = b.f"
	parsed := parseTyped(t, src)
	off := lint.Config{Rules: map[string]bool{"join-key-implicit-cast": false}}
	if got := lint.Run(t.Context(), parsed, off, lint.Env{}, lint.JoinKeyImplicitCast); len(got) != 0 {
		t.Errorf("disabled rule reported %+v", got)
	}
	if got := lint.Run(t.Context(), parsed, lint.Config{}, lint.Env{}, lint.JoinKeyImplicitCast); len(got) != 1 {
		t.Errorf("rule should be enabled by default, got %+v", got)
	}
}
