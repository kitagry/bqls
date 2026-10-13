package source_test

import (
	"context"
	"fmt"
	"slices"
	"testing"

	bq "cloud.google.com/go/bigquery"

	"github.com/golang/mock/gomock"
	"github.com/google/go-cmp/cmp"
	"github.com/kitagry/bqls/langserver/internal/bigquery/mock_bigquery"
	"github.com/kitagry/bqls/langserver/internal/lint"
	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/kitagry/bqls/langserver/internal/source"
	"github.com/sirupsen/logrus"
)

func TestProject_Diagnose(t *testing.T) {
	const uri lsp.DocumentURI = "file:///a.sql"
	tests := map[string]struct {
		src       string
		cfg       lint.Config
		wantErrs  []string
		wantRules []string
	}{
		"lint diagnostics for a valid file": {
			src:       "SELECT x FROM UNNEST([1]) AS x WHERE x = CAST(NULL AS INT64)",
			wantRules: []string{"null-comparison"},
		},
		"disabled rule is not reported": {
			src: "SELECT x FROM UNNEST([1]) AS x WHERE x = CAST(NULL AS INT64)",
			cfg: lint.Config{Rules: map[string]bool{"null-comparison": false}},
		},
		"no lint when analysis fails": {
			src:      "SELECT x FROM UNNEST([1]) AS x WHERE x = CAST(NULL AS INT64) AND y = 1",
			wantErrs: []string{"Unrecognized name: y"},
		},
		"valid pipe query": {
			src: "FROM UNNEST([1, 2]) AS x |> WHERE x > 1 |> AGGREGATE COUNT(*) AS c GROUP BY x",
		},
		"lint diagnostics for a pipe query": {
			src:       "FROM UNNEST([1]) AS x |> WHERE x = CAST(NULL AS INT64)",
			wantRules: []string{"null-comparison"},
		},
		"unknown name in a pipe query": {
			src:      "FROM UNNEST([1]) AS x |> WHERE y = 1",
			wantErrs: []string{"Unrecognized name: y"},
		},
	}

	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			p := source.NewProjectWithBQClient("/", mock_bigquery.NewMockClient(ctrl), logrus.New())
			if err := p.UpdateFile(uri, tt.src, 1); err != nil {
				t.Fatal(err)
			}

			got, ok := p.Diagnose(t.Context(), uri, tt.cfg)
			if !ok {
				t.Fatal("Diagnose() ok = false, want true for an open file")
			}
			var gotErrs, gotRules []string
			for _, e := range got.Errors {
				gotErrs = append(gotErrs, e.Msg)
			}
			for _, d := range got.Lint {
				gotRules = append(gotRules, d.Rule)
			}
			if diff := cmp.Diff(tt.wantErrs, gotErrs); diff != "" {
				t.Errorf("errors diff (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(tt.wantRules, gotRules); diff != "" {
				t.Errorf("lint rules diff (-want +got):\n%s", diff)
			}
		})
	}
}

func TestProject_Diagnose_UnknownFile(t *testing.T) {
	ctrl := gomock.NewController(t)
	p := source.NewProjectWithBQClient("/", mock_bigquery.NewMockClient(ctrl), logrus.New())
	if _, ok := p.Diagnose(t.Context(), "file:///missing.sql", lint.Config{}); ok {
		t.Error("Diagnose() ok = true, want false for a file that is not open")
	}
}

func TestProject_Diagnose_FetchesEachTableOnce(t *testing.T) {
	const uri lsp.DocumentURI = "file:///a.sql"
	tests := map[string]struct {
		src string
	}{
		"DELETE with a subquery on a partitioned table": {
			src: "DELETE FROM p.d.t WHERE x IN (SELECT x FROM p.d.part)",
		},
		"MERGE using a partitioned table": {
			src: "MERGE p.d.t T USING p.d.part S ON T.x = S.x WHEN MATCHED THEN DELETE",
		},
	}

	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			bqClient := mock_bigquery.NewMockClient(ctrl)
			bqClient.EXPECT().GetDefaultProject().Return("p").AnyTimes()
			tables := map[string]*bq.TableMetadata{
				"p.d.t": {
					Type:   bq.RegularTable,
					Schema: bq.Schema{{Name: "x", Type: bq.IntegerFieldType}},
				},
				"p.d.part": {
					Type: bq.RegularTable,
					Schema: bq.Schema{
						{Name: "x", Type: bq.IntegerFieldType},
						{Name: "dt", Type: bq.DateFieldType},
					},
					TimePartitioning: &bq.TimePartitioning{Field: "dt"},
				},
			}
			var calls []string
			bqClient.EXPECT().GetTableMetadata(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
				DoAndReturn(func(_ context.Context, project, dataset, table string) (*bq.TableMetadata, error) {
					path := project + "." + dataset + "." + table
					calls = append(calls, path)
					meta, ok := tables[path]
					if !ok {
						return nil, fmt.Errorf("table %s not found", path)
					}
					return meta, nil
				}).AnyTimes()

			p := source.NewProjectWithBQClient("/", bqClient, logrus.New())
			if err := p.UpdateFile(uri, tt.src, 1); err != nil {
				t.Fatal(err)
			}

			got, ok := p.Diagnose(t.Context(), uri, lint.Config{})
			if !ok {
				t.Fatal("Diagnose() ok = false, want true for an open file")
			}

			var gotRules []string
			for _, d := range got.Lint {
				gotRules = append(gotRules, d.Rule)
			}
			// The partition-filter diagnostic proves the lint rule still sees the metadata.
			if diff := cmp.Diff([]string{"partition-filter"}, gotRules); diff != "" {
				t.Errorf("lint rules diff (-want +got):\n%s", diff)
			}
			slices.Sort(calls)
			if diff := cmp.Diff([]string{"p.d.part", "p.d.t"}, calls); diff != "" {
				t.Errorf("GetTableMetadata calls diff (-want +got):\n%s", diff)
			}
		})
	}
}
