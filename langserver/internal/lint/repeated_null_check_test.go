package lint_test

import (
	"context"
	"fmt"
	"testing"

	bq "cloud.google.com/go/bigquery"
	"github.com/golang/mock/gomock"
	"github.com/kitagry/bqls/langserver/internal/bigquery/mock_bigquery"
	"github.com/kitagry/bqls/langserver/internal/lint"
	"github.com/kitagry/bqls/langserver/internal/source/file"
	"github.com/sirupsen/logrus"
)

var repeatedTables = map[string]*bq.TableMetadata{
	"p.d.t": {Type: bq.RegularTable, Schema: bq.Schema{
		{Name: "id", Type: bq.IntegerFieldType},
		{Name: "tags", Type: bq.StringFieldType, Repeated: true},
		{Name: "s", Type: bq.RecordFieldType, Schema: bq.Schema{
			{Name: "items", Type: bq.IntegerFieldType, Repeated: true},
		}},
	}},
	"p.d.u": {Type: bq.RegularTable, Schema: bq.Schema{
		{Name: "id", Type: bq.IntegerFieldType},
		{Name: "labels", Type: bq.StringFieldType, Repeated: true},
	}},
	"p.d.v": {Type: bq.ViewTable, Schema: bq.Schema{
		{Name: "id", Type: bq.IntegerFieldType},
		{Name: "tags", Type: bq.StringFieldType, Repeated: true},
	}},
}

func lookupRepeatedTable(_ context.Context, path string) (*bq.TableMetadata, error) {
	m, ok := repeatedTables[path]
	if !ok {
		return nil, fmt.Errorf("table %s not found", path)
	}
	return m, nil
}

func parseRepeated(t *testing.T, src string) file.ParsedFile {
	t.Helper()
	ctrl := gomock.NewController(t)
	bqClient := mock_bigquery.NewMockClient(ctrl)
	bqClient.EXPECT().GetDefaultProject().Return("p").AnyTimes()
	bqClient.EXPECT().GetTableMetadata(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, project, dataset, table string) (*bq.TableMetadata, error) {
			return lookupRepeatedTable(ctx, project+"."+dataset+"."+table)
		}).AnyTimes()
	parsed := file.NewAnalyzer(logrus.New(), bqClient).ParseFile("file:///a.sql", src)
	if len(parsed.Errors) > 0 {
		t.Fatalf("unexpected analysis errors: %v", parsed.Errors)
	}
	return parsed
}

func repeatedNullCheckMessage(column, table string, negated bool) string {
	if negated {
		return fmt.Sprintf("`%s` is a REPEATED column of `%s`, which BigQuery stores as an empty array, never NULL, so IS NOT NULL is always TRUE; to check for a non-empty array, use ARRAY_LENGTH(%s) > 0", column, table, column)
	}
	return fmt.Sprintf("`%s` is a REPEATED column of `%s`, which BigQuery stores as an empty array, never NULL, so IS NULL is always FALSE; to check for an empty array, use ARRAY_LENGTH(%s) = 0", column, table, column)
}

func TestRepeatedNullCheck(t *testing.T) {
	type report struct {
		// text is the exact reported source text.
		text    string
		column  string
		table   string
		negated bool
		fixed   string
	}
	tests := map[string]struct {
		src  string
		want []report
	}{
		"IS NULL": {
			src:  "SELECT id FROM p.d.t WHERE tags IS NULL",
			want: []report{{"tags IS NULL", "tags", "p.d.t", false, "ARRAY_LENGTH(tags) = 0"}},
		},
		"IS NOT NULL": {
			src:  "SELECT id FROM p.d.t WHERE tags IS NOT NULL",
			want: []report{{"tags IS NOT NULL", "tags", "p.d.t", true, "ARRAY_LENGTH(tags) > 0"}},
		},
		"qualified": {
			src:  "SELECT a.id FROM p.d.t AS a WHERE a.tags IS NULL",
			want: []report{{"a.tags IS NULL", "tags", "p.d.t", false, "ARRAY_LENGTH(a.tags) = 0"}},
		},
		"in the SELECT list": {
			src:  "SELECT tags IS NULL AS no_tags FROM p.d.t",
			want: []report{{"tags IS NULL", "tags", "p.d.t", false, "ARRAY_LENGTH(tags) = 0"}},
		},
		"through a CTE and an alias": {
			src:  "WITH c AS (SELECT id, tags AS tt FROM p.d.t)\nSELECT id FROM c WHERE tt IS NULL",
			want: []report{{"tt IS NULL", "tags", "p.d.t", false, "ARRAY_LENGTH(tt) = 0"}},
		},
		"inner join": {
			src:  "SELECT t.id FROM p.d.t AS t JOIN p.d.u AS u USING (id) WHERE u.labels IS NULL",
			want: []report{{"u.labels IS NULL", "labels", "p.d.u", false, "ARRAY_LENGTH(u.labels) = 0"}},
		},
		"preserved side of a LEFT JOIN": {
			src:  "SELECT t.id FROM p.d.t AS t LEFT JOIN p.d.u AS u USING (id) WHERE t.tags IS NOT NULL",
			want: []report{{"t.tags IS NOT NULL", "tags", "p.d.t", true, "ARRAY_LENGTH(t.tags) > 0"}},
		},
		// An unmatched row has NULL there.
		"optional side of a LEFT JOIN": {
			src: "SELECT t.id FROM p.d.t AS t LEFT JOIN p.d.u AS u USING (id) WHERE u.labels IS NULL",
		},
		"FULL JOIN": {
			src: "SELECT 1 FROM p.d.t AS t FULL JOIN p.d.u AS u USING (id) WHERE t.tags IS NULL",
		},
		// s itself may be NULL, and then so is s.items.
		"field of a NULLABLE RECORD": {
			src: "SELECT id FROM p.d.t WHERE s.items IS NULL",
		},
		"view": {
			src: "SELECT id FROM p.d.v WHERE tags IS NULL",
		},
		"computed array": {
			src: "SELECT id FROM p.d.t WHERE ARRAY_CONCAT(tags, ['x']) IS NULL OR IF(id > 0, tags, NULL) IS NULL",
		},
		"array subquery": {
			src: "SELECT id FROM p.d.t WHERE ARRAY(SELECT x FROM UNNEST(tags) AS x) IS NULL",
		},
		"non-array column": {
			src: "SELECT id FROM p.d.t WHERE id IS NULL",
		},
		"suppressed by comment": {
			src: "SELECT id FROM p.d.t\n-- bqls:ignore repeated-null-check\nWHERE tags IS NULL",
		},
	}

	env := lint.Env{LookupTable: lookupRepeatedTable}
	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			got := lint.Run(t.Context(), parseRepeated(t, tt.src), lint.Config{}, env, lint.RepeatedNullCheck)
			if len(got) != len(tt.want) {
				t.Fatalf("got %d diagnostics, want %d: %+v", len(got), len(tt.want), got)
			}
			for i, w := range tt.want {
				d := got[i]
				if text := textInRange(tt.src, d); text != w.text {
					t.Errorf("reported text = %q, want %q", text, w.text)
				}
				if want := repeatedNullCheckMessage(w.column, w.table, w.negated); d.Rule != "repeated-null-check" || d.Severity != lint.SeverityWarning || d.Message != want {
					t.Errorf("diagnostic = %+v, want warning %q", d, want)
				}
				if len(d.SuggestedFixes) != 1 {
					t.Fatalf("got %d fixes, want 1", len(d.SuggestedFixes))
				}
				fix := d.SuggestedFixes[0]
				// An empty-array check is what was probably meant, but it
				// changes the result, so applying it is the user's call.
				if !fix.ChangesResult {
					t.Error("fix.ChangesResult = false, want true")
				}
				if want := "Replace with " + w.fixed; fix.Title != want {
					t.Errorf("fix title = %q, want %q", fix.Title, want)
				}
				fixed := applyFixes(t, tt.src, []lint.Diagnostic{d})
				if after := lint.Run(t.Context(), parseRepeated(t, fixed), lint.Config{}, env, lint.RepeatedNullCheck); len(after) != 0 {
					t.Errorf("diagnostics after fix %q: %+v", fixed, after)
				}
			}
		})
	}
}

func TestRepeatedNullCheck_WithoutMetadata(t *testing.T) {
	src := "SELECT id FROM p.d.t WHERE tags IS NULL"
	if got := lint.Run(t.Context(), parseRepeated(t, src), lint.Config{}, lint.Env{}, lint.RepeatedNullCheck); len(got) != 0 {
		t.Errorf("got %+v, want nothing without table metadata", got)
	}
}
