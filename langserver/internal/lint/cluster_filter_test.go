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

var clusteredTables = map[string]*bq.TableMetadata{
	"p.d.orders": {
		Type:       bq.RegularTable,
		Clustering: &bq.Clustering{Fields: []string{"customer_id", "region", "status"}},
		Schema: bq.Schema{
			{Name: "customer_id", Type: bq.IntegerFieldType},
			{Name: "region", Type: bq.StringFieldType},
			{Name: "status", Type: bq.StringFieldType},
			{Name: "amount", Type: bq.IntegerFieldType},
			{Name: "other_id", Type: bq.IntegerFieldType},
		},
	},
	"p.d.plain": {
		Type:   bq.RegularTable,
		Schema: bq.Schema{{Name: "customer_id", Type: bq.IntegerFieldType}, {Name: "amount", Type: bq.IntegerFieldType}},
	},
	"p.d.cview": {
		Type:       bq.ViewTable,
		Clustering: &bq.Clustering{Fields: []string{"customer_id"}},
		Schema:     bq.Schema{{Name: "customer_id", Type: bq.IntegerFieldType}, {Name: "amount", Type: bq.IntegerFieldType}},
	},
}

func lookupClusteredTable(_ context.Context, path string) (*bq.TableMetadata, error) {
	m, ok := clusteredTables[path]
	if !ok {
		return nil, fmt.Errorf("table %s not found", path)
	}
	return m, nil
}

func parseClustered(t *testing.T, src string) file.ParsedFile {
	t.Helper()
	ctrl := gomock.NewController(t)
	bqClient := mock_bigquery.NewMockClient(ctrl)
	bqClient.EXPECT().GetDefaultProject().Return("p").AnyTimes()
	bqClient.EXPECT().GetTableMetadata(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, project, dataset, table string) (*bq.TableMetadata, error) {
			return lookupClusteredTable(ctx, project+"."+dataset+"."+table)
		}).AnyTimes()
	parsed := file.NewAnalyzer(logrus.New(), bqClient).ParseFile("file:///a.sql", src)
	if len(parsed.Errors) > 0 {
		t.Fatalf("unexpected analysis errors: %v", parsed.Errors)
	}
	return parsed
}

func TestClusterFilter(t *testing.T) {
	const noFirst = "Query on clustered table `p.d.orders` has no filter on its first clustering column `customer_id`, so the clustering cannot reduce the data read; filter on the clustering columns in order, starting with the first"
	skipped := func(col, missing string) string {
		return "Query on clustered table `p.d.orders` filters clustering column `" + col + "` but not `" + missing + "` before it, so that filter cannot reduce the data read; filter on the clustering columns in order, starting with the first"
	}
	type report struct {
		// text is the exact reported source text.
		text, message string
	}
	tests := map[string]struct {
		src  string
		want []report
	}{
		"no filter": {
			src:  "SELECT amount FROM p.d.orders",
			want: []report{{"p.d.orders", noFirst}},
		},
		"filter on a later column only": {
			src:  "SELECT amount FROM p.d.orders WHERE region = 'jp'",
			want: []report{{"p.d.orders", noFirst}},
		},
		"skipped column": {
			src:  "SELECT amount FROM p.d.orders WHERE customer_id = 1 AND status = 'done'",
			want: []report{{"p.d.orders", skipped("status", "region")}},
		},
		"through a CTE": {
			src:  "WITH o AS (SELECT * FROM p.d.orders) SELECT amount FROM o WHERE region = 'jp'",
			want: []report{{"p.d.orders", noFirst}},
		},
		"first column": {
			src: "SELECT amount FROM p.d.orders WHERE customer_id = 1",
		},
		"columns in order": {
			src: "SELECT amount FROM p.d.orders WHERE customer_id = 1 AND region = 'jp' AND status = 'done'",
		},
		"first column through a CTE": {
			src: "WITH o AS (SELECT * FROM p.d.orders) SELECT amount FROM o WHERE customer_id IN (1, 2)",
		},
		"not clustered": {
			src: "SELECT amount FROM p.d.plain",
		},
		"view": {
			src: "SELECT amount FROM p.d.cview",
		},
		// Nothing is read.
		"COUNT(*)": {
			src: "SELECT COUNT(*) FROM p.d.orders",
		},
		"suppressed by comment": {
			src: "-- bqls:ignore cluster-filter\nSELECT amount FROM p.d.orders",
		},
	}

	cfg := lint.Config{Rules: map[string]bool{"cluster-filter": true}}
	env := lint.Env{LookupTable: lookupClusteredTable}
	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			got := lint.Run(t.Context(), parseClustered(t, tt.src), cfg, env, lint.ClusterFilter)
			if len(got) != len(tt.want) {
				t.Fatalf("got %d diagnostics, want %d: %+v", len(got), len(tt.want), got)
			}
			for i, w := range tt.want {
				d := got[i]
				if text := textInRange(tt.src, d); text != w.text {
					t.Errorf("reported text = %q, want %q", text, w.text)
				}
				if d.Rule != "cluster-filter" || d.Severity != lint.SeverityWarning || d.Message != w.message {
					t.Errorf("diagnostic = %+v, want warning %q", d, w.message)
				}
			}
		})
	}
}

func TestClusterFilterNotPrunable(t *testing.T) {
	msg := func(col, reason string) string {
		return "Filter on clustering column `" + col + "` of `p.d.orders` cannot reduce the data read because " + reason
	}
	const wrapped = "it is wrapped in a function or CAST"
	type report struct {
		// text is the exact reported source text.
		text, message string
	}
	tests := map[string]struct {
		src  string
		want []report
	}{
		"CAST": {
			src:  "SELECT amount FROM p.d.orders WHERE CAST(customer_id AS STRING) = '10000'",
			want: []report{{"CAST(customer_id AS STRING) = '10000'", msg("customer_id", wrapped)}},
		},
		"function": {
			src:  "SELECT amount FROM p.d.orders WHERE LOWER(region) = 'jp'",
			want: []report{{"LOWER(region) = 'jp'", msg("region", wrapped)}},
		},
		"arithmetic": {
			src:  "SELECT amount FROM p.d.orders WHERE customer_id + 1 = 10",
			want: []report{{"customer_id + 1 = 10", msg("customer_id", wrapped)}},
		},
		"another column": {
			src:  "SELECT amount FROM p.d.orders WHERE customer_id = other_id",
			want: []report{{"customer_id = other_id", msg("customer_id", "it is compared with another column of the table")}},
		},
		"plain comparison": {
			src: "SELECT amount FROM p.d.orders WHERE customer_id = 1 AND region IN ('jp', 'us') AND status LIKE 'done%'",
		},
		// One prunable AND term is enough.
		"another term prunes": {
			src: "SELECT amount FROM p.d.orders WHERE CAST(customer_id AS STRING) = '1' AND customer_id < 100",
		},
		"not clustered": {
			src: "SELECT amount FROM p.d.plain WHERE CAST(customer_id AS STRING) = '1'",
		},
		"suppressed by comment": {
			src: "SELECT amount FROM p.d.orders\n-- bqls:ignore cluster-filter-not-prunable\nWHERE LOWER(region) = 'jp'",
		},
	}

	cfg := lint.Config{Rules: map[string]bool{"cluster-filter-not-prunable": true}}
	env := lint.Env{LookupTable: lookupClusteredTable}
	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			got := lint.Run(t.Context(), parseClustered(t, tt.src), cfg, env, lint.ClusterFilterNotPrunable)
			if len(got) != len(tt.want) {
				t.Fatalf("got %d diagnostics, want %d: %+v", len(got), len(tt.want), got)
			}
			for i, w := range tt.want {
				d := got[i]
				if text := textInRange(tt.src, d); text != w.text {
					t.Errorf("reported text = %q, want %q", text, w.text)
				}
				if d.Rule != "cluster-filter-not-prunable" || d.Severity != lint.SeverityWarning || d.Message != w.message {
					t.Errorf("diagnostic = %+v, want warning %q", d, w.message)
				}
			}
		})
	}
}

func TestClusterFilter_OffByDefault(t *testing.T) {
	env := lint.Env{LookupTable: lookupClusteredTable}
	src := "SELECT amount FROM p.d.orders WHERE LOWER(region) = 'jp'"
	if got := lint.Run(t.Context(), parseClustered(t, src), lint.Config{}, env, lint.ClusterFilter, lint.ClusterFilterNotPrunable); len(got) != 0 {
		t.Errorf("got %+v, want nothing while both rules are off by default", got)
	}
}
