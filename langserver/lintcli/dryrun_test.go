package lintcli

import (
	"context"
	"regexp"
	"strings"
	"testing"

	bq "cloud.google.com/go/bigquery"
	"github.com/golang/mock/gomock"
	"github.com/google/go-cmp/cmp"
	"github.com/kitagry/bqls/langserver/internal/bigquery"
	"github.com/kitagry/bqls/langserver/internal/bigquery/mock_bigquery"
	"google.golang.org/api/googleapi"
)

// dryRunClient returns a factory whose client answers dry-runs of each query
// in estimates with that many processed bytes, and fails the test on any
// other query job.
func dryRunClient(t *testing.T, estimates map[string]int64) clientFactory {
	t.Helper()
	ctrl := gomock.NewController(t)
	c := mock_bigquery.NewMockClient(ctrl)
	c.EXPECT().GetDefaultProject().Return("p").AnyTimes()
	c.EXPECT().Close().Return(nil).AnyTimes()
	for q, bytes := range estimates {
		job := mock_bigquery.NewMockBigqueryJob(ctrl)
		job.EXPECT().LastStatus().Return(&bq.JobStatus{State: bq.Done, Statistics: &bq.JobStatistics{TotalBytesProcessed: bytes}}).AnyTimes()
		c.EXPECT().Run(gomock.Any(), q, true).Return(job, nil).Times(1)
	}
	return func(context.Context, string, string) (bigquery.Client, error) { return c, nil }
}

func TestRun_DryRun(t *testing.T) {
	const twoTiB = 2 << 40
	tests := map[string]struct {
		files     map[string]string
		estimates map[string]int64
		args      []string
		want      result
	}{
		"estimate above the threshold is reported": {
			files:     map[string]string{"a.sql": cleanSQL},
			estimates: map[string]int64{cleanSQL: twoTiB},
			args:      []string{"--dry-run", "--enable", "bytes-processed-threshold", "a.sql"},
			want: result{
				code:   1,
				stdout: "a.sql:1:1: warning: This query will process 2 TiB, which exceeds the threshold of 1 TiB (bytes-processed-threshold)\n",
			},
		},
		"estimate below the threshold is not reported": {
			files:     map[string]string{"a.sql": cleanSQL},
			estimates: map[string]int64{cleanSQL: 1024},
			args:      []string{"--dry-run", "--enable", "bytes-processed-threshold", "a.sql"},
		},
		"without --dry-run no query job is run": {
			files: map[string]string{"a.sql": cleanSQL},
			args:  []string{"--enable", "bytes-processed-threshold", "a.sql"},
		},
		"file with analysis errors is not dry-run": {
			files: map[string]string{"a.sql": analysisErrorSQL},
			args:  []string{"--dry-run", "--enable", "bytes-processed-threshold", "a.sql"},
			want: result{
				code:   1,
				stdout: "a.sql:1:8: error: Unrecognized name: y\n",
			},
		},
		"fixed text is dry-run": {
			files:     map[string]string{"a.sql": unusedCTESQL},
			estimates: map[string]int64{"SELECT 2\n": twoTiB},
			args:      []string{"--dry-run", "--fix", "--enable", "bytes-processed-threshold", "a.sql"},
			want: result{
				code:   1,
				stdout: "a.sql:1:1: warning: This query will process 2 TiB, which exceeds the threshold of 1 TiB (bytes-processed-threshold)\n",
			},
		},
	}
	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			t.Chdir(t.TempDir())
			writeFiles(t, tt.files)

			got := runWith(t, dryRunClient(t, tt.estimates), "", tt.args...)
			if diff := cmp.Diff(tt.want, got, cmp.AllowUnexported(result{})); diff != "" {
				t.Errorf("run result diff (-want +got):\n%s", diff)
			}
		})
	}
}

func TestRun_DryRunFailureIsRuntimeFailure(t *testing.T) {
	t.Chdir(t.TempDir())
	writeFiles(t, map[string]string{"a.sql": nullComparisonSQL})

	ctrl := gomock.NewController(t)
	c := mock_bigquery.NewMockClient(ctrl)
	c.EXPECT().GetDefaultProject().Return("p").AnyTimes()
	c.EXPECT().Close().Return(nil).AnyTimes()
	c.EXPECT().Run(gomock.Any(), nullComparisonSQL, true).
		Return(nil, &googleapi.Error{Code: 403, Message: "Access Denied: Project p"})
	factory := func(context.Context, string, string) (bigquery.Client, error) { return c, nil }

	got := runWith(t, factory, "", "--dry-run", "a.sql")

	want := result{
		code:   2,
		stdout: "a.sql:1:38: warning: Comparison with NULL using = always yields NULL; use IS NULL (null-comparison)\n",
	}
	if diff := cmp.Diff(want, got, cmp.AllowUnexported(result{}), cmp.FilterPath(func(p cmp.Path) bool {
		return p.Last().String() == ".stderr"
	}, cmp.Ignore())); diff != "" {
		t.Errorf("run result diff (-want +got):\n%s", diff)
	}
	if !strings.Contains(got.stderr, "a.sql") || !strings.Contains(got.stderr, "Access Denied: Project p") {
		t.Errorf("stderr = %q, want the file name and the dry-run failure", got.stderr)
	}
}

func TestRun_HelpAlignsRuleDescriptions(t *testing.T) {
	got := runWith(t, mockFactory(t), "", "--help")

	_, rules, _ := strings.Cut(got.stdout, "\nRules:\n")
	columns := map[int]bool{}
	for _, line := range strings.Split(strings.TrimRight(rules, "\n"), "\n") {
		loc := regexp.MustCompile(`\(default (on|off)\)`).FindStringIndex(line)
		if loc == nil {
			t.Fatalf("rule line %q has no default marker", line)
		}
		columns[loc[0]] = true
	}
	if len(columns) != 1 {
		t.Errorf("rule descriptions start at columns %v, want one column; rules:\n%s", columns, rules)
	}
}
