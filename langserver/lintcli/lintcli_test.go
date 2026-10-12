package lintcli

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	bq "cloud.google.com/go/bigquery"
	"github.com/golang/mock/gomock"
	"github.com/google/go-cmp/cmp"
	"github.com/kitagry/bqls/langserver/internal/bigquery"
	"github.com/kitagry/bqls/langserver/internal/bigquery/mock_bigquery"
)

const (
	nullComparisonSQL = "SELECT x FROM UNNEST([1]) AS x WHERE x = CAST(NULL AS INT64)\n"
	unusedCTESQL      = "WITH a AS (SELECT 1) SELECT 2\n"
	cleanSQL          = "SELECT 1\n"
	analysisErrorSQL  = "SELECT y\n"
)

func writeFiles(t *testing.T, files map[string]string) {
	t.Helper()
	for name, content := range files {
		if err := os.MkdirAll(filepath.Dir(name), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(name, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

func mockFactory(t *testing.T) clientFactory {
	t.Helper()
	ctrl := gomock.NewController(t)
	c := mock_bigquery.NewMockClient(ctrl)
	c.EXPECT().GetDefaultProject().Return("p").AnyTimes()
	c.EXPECT().Close().Return(nil).AnyTimes()
	return func(context.Context, string, string) (bigquery.Client, error) { return c, nil }
}

type result struct {
	code   int
	stdout string
	stderr string
}

func runWith(t *testing.T, newClient clientFactory, stdin string, args ...string) result {
	t.Helper()
	var stdout, stderr bytes.Buffer
	code := run(t.Context(), args, strings.NewReader(stdin), &stdout, &stderr, newClient)
	return result{code: code, stdout: stdout.String(), stderr: stderr.String()}
}

func TestRun_TextOutput(t *testing.T) {
	tests := map[string]struct {
		files      map[string]string
		args       []string
		wantCode   int
		wantStdout string
	}{
		"clean file exits 0 without output": {
			files:    map[string]string{"a.sql": cleanSQL},
			args:     []string{"a.sql"},
			wantCode: 0,
		},
		"lint findings are sorted by file and position": {
			files:    map[string]string{"b.sql": nullComparisonSQL, "a.sql": unusedCTESQL},
			args:     []string{"b.sql", "a.sql"},
			wantCode: 1,
			wantStdout: "a.sql:1:6: info: CTE `a` is defined but never used (unused-cte)\n" +
				"b.sql:1:38: warning: Comparison with NULL using = always yields NULL; use IS NULL (null-comparison)\n",
		},
		"analysis error is reported as error without rule": {
			files:      map[string]string{"a.sql": analysisErrorSQL},
			args:       []string{"a.sql"},
			wantCode:   1,
			wantStdout: "a.sql:1:8: error: Unrecognized name: y\n",
		},
		"directory is searched recursively for sql files": {
			files: map[string]string{
				"q/a.sql":       unusedCTESQL,
				"q/sub/b.sql":   unusedCTESQL,
				"q/ignored.txt": analysisErrorSQL,
			},
			args:     []string{"q"},
			wantCode: 1,
			wantStdout: "q/a.sql:1:6: info: CTE `a` is defined but never used (unused-cte)\n" +
				"q/sub/b.sql:1:6: info: CTE `a` is defined but never used (unused-cte)\n",
		},
		"disabled rule is not reported": {
			files:    map[string]string{"a.sql": unusedCTESQL},
			args:     []string{"--disable", "unused-cte", "a.sql"},
			wantCode: 0,
		},
		"later flag wins when a rule is both disabled and enabled": {
			files:      map[string]string{"a.sql": unusedCTESQL},
			args:       []string{"--disable", "unused-cte", "--enable", "unused-cte", "a.sql"},
			wantCode:   1,
			wantStdout: "a.sql:1:6: info: CTE `a` is defined but never used (unused-cte)\n",
		},
	}

	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			t.Chdir(t.TempDir())
			writeFiles(t, tt.files)

			got := runWith(t, mockFactory(t), "", tt.args...)
			if got.code != tt.wantCode {
				t.Errorf("exit code = %d, want %d (stderr: %s)", got.code, tt.wantCode, got.stderr)
			}
			if diff := cmp.Diff(tt.wantStdout, got.stdout); diff != "" {
				t.Errorf("stdout diff (-want +got):\n%s", diff)
			}
		})
	}
}

func TestRun_Stdin(t *testing.T) {
	got := runWith(t, mockFactory(t), unusedCTESQL, "-")
	want := "<stdin>:1:6: info: CTE `a` is defined but never used (unused-cte)\n"
	if diff := cmp.Diff(want, got.stdout); diff != "" {
		t.Errorf("stdout diff (-want +got):\n%s", diff)
	}
}

func TestRun_JSONOutput(t *testing.T) {
	t.Chdir(t.TempDir())
	writeFiles(t, map[string]string{"a.sql": analysisErrorSQL, "b.sql": nullComparisonSQL})

	got := runWith(t, mockFactory(t), "", "--format", "json", "a.sql", "b.sql")
	want := `[
  {
    "file": "a.sql",
    "line": 1,
    "column": 8,
    "endLine": 1,
    "endColumn": 9,
    "severity": "error",
    "rule": "",
    "message": "Unrecognized name: y"
  },
  {
    "file": "b.sql",
    "line": 1,
    "column": 38,
    "endLine": 1,
    "endColumn": 61,
    "severity": "warning",
    "rule": "null-comparison",
    "message": "Comparison with NULL using = always yields NULL; use IS NULL"
  }
]
`
	if got.code != 1 {
		t.Errorf("exit code = %d, want 1", got.code)
	}
	if diff := cmp.Diff(want, got.stdout); diff != "" {
		t.Errorf("stdout diff (-want +got):\n%s", diff)
	}
}

func TestRun_JSONOutputWithoutFindingsIsEmptyArray(t *testing.T) {
	t.Chdir(t.TempDir())
	writeFiles(t, map[string]string{"a.sql": cleanSQL})

	got := runWith(t, mockFactory(t), "", "--format", "json", "a.sql")
	if diff := cmp.Diff("[]\n", got.stdout); diff != "" {
		t.Errorf("stdout diff (-want +got):\n%s", diff)
	}
}

func TestRun_GitHubOutput(t *testing.T) {
	t.Chdir(t.TempDir())
	writeFiles(t, map[string]string{"a.sql": analysisErrorSQL, "b.sql": nullComparisonSQL, "c.sql": unusedCTESQL})

	got := runWith(t, mockFactory(t), "", "--format", "github", "a.sql", "b.sql", "c.sql")
	want := "::error file=a.sql,line=1,col=8,endLine=1,endColumn=9,title=bqls::Unrecognized name: y\n" +
		"::warning file=b.sql,line=1,col=38,endLine=1,endColumn=61,title=bqls(null-comparison)::Comparison with NULL using = always yields NULL; use IS NULL\n" +
		"::notice file=c.sql,line=1,col=6,endLine=1,endColumn=7,title=bqls(unused-cte)::CTE `a` is defined but never used\n"
	if diff := cmp.Diff(want, got.stdout); diff != "" {
		t.Errorf("stdout diff (-want +got):\n%s", diff)
	}
}

func TestGitHubEscape(t *testing.T) {
	if got, want := escapeGitHubData("100%\r\na:b,c"), "100%25%0D%0Aa:b,c"; got != want {
		t.Errorf("escapeGitHubData() = %q, want %q", got, want)
	}
	if got, want := escapeGitHubProperty("100%\r\na:b,c"), "100%25%0D%0Aa%3Ab%2Cc"; got != want {
		t.Errorf("escapeGitHubProperty() = %q, want %q", got, want)
	}
}

func TestRun_UsageErrors(t *testing.T) {
	tests := map[string]struct {
		args       []string
		wantStderr string
	}{
		"no arguments":   {args: nil, wantStderr: "no input"},
		"unknown rule":   {args: []string{"--disable", "no-such-rule", "a.sql"}, wantStderr: "null-comparison"},
		"unknown format": {args: []string{"--format", "xml", "a.sql"}, wantStderr: "xml"},
		"missing file":   {args: []string{"missing.sql"}, wantStderr: "missing.sql"},
		"undefined flag": {args: []string{"--no-such-flag", "a.sql"}, wantStderr: "no-such-flag"},
	}
	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			t.Chdir(t.TempDir())
			writeFiles(t, map[string]string{"a.sql": cleanSQL})

			got := runWith(t, mockFactory(t), "", tt.args...)
			if got.code != 2 {
				t.Errorf("exit code = %d, want 2", got.code)
			}
			if !strings.Contains(got.stderr, tt.wantStderr) {
				t.Errorf("stderr = %q, want it to contain %q", got.stderr, tt.wantStderr)
			}
		})
	}
}

func TestRun_Help(t *testing.T) {
	got := runWith(t, mockFactory(t), "", "--help")
	if got.code != 0 {
		t.Errorf("exit code = %d, want 0", got.code)
	}
	for _, want := range []string{"bqls lint", "null-comparison", "unused-cte", "partition-filter", "-format"} {
		if !strings.Contains(got.stdout, want) {
			t.Errorf("stdout = %q, want it to contain %q", got.stdout, want)
		}
	}
}

func TestRun_ClientFactory(t *testing.T) {
	t.Chdir(t.TempDir())
	writeFiles(t, map[string]string{"a.sql": cleanSQL})

	t.Run("receives project and location flags", func(t *testing.T) {
		var gotProject, gotLocation string
		inner := mockFactory(t)
		factory := func(ctx context.Context, project, location string) (bigquery.Client, error) {
			gotProject, gotLocation = project, location
			return inner(ctx, project, location)
		}
		got := runWith(t, factory, "", "--project", "proj", "--location", "asia-northeast1", "a.sql")
		if got.code != 0 {
			t.Errorf("exit code = %d, want 0 (stderr: %s)", got.code, got.stderr)
		}
		if gotProject != "proj" || gotLocation != "asia-northeast1" {
			t.Errorf("factory got (%q, %q), want (%q, %q)", gotProject, gotLocation, "proj", "asia-northeast1")
		}
	})

	t.Run("failure exits 2", func(t *testing.T) {
		factory := func(context.Context, string, string) (bigquery.Client, error) {
			return nil, errors.New("auth failed")
		}
		got := runWith(t, factory, "", "a.sql")
		if got.code != 2 {
			t.Errorf("exit code = %d, want 2", got.code)
		}
		if !strings.Contains(got.stderr, "auth failed") {
			t.Errorf("stderr = %q, want it to contain %q", got.stderr, "auth failed")
		}
	})
}

// The analyzer's catalog preload and the partition-filter lookup both fetch
// table metadata, and several files commonly read the same table; each should
// hit the API only once per run.
func TestRun_TableMetadataFetchedOncePerTable(t *testing.T) {
	t.Chdir(t.TempDir())
	const src = "SELECT x FROM `p.d.t` WHERE x = 1\n"
	writeFiles(t, map[string]string{"a.sql": src, "b.sql": src})

	ctrl := gomock.NewController(t)
	c := mock_bigquery.NewMockClient(ctrl)
	c.EXPECT().GetDefaultProject().Return("p").AnyTimes()
	c.EXPECT().Close().Return(nil).AnyTimes()
	c.EXPECT().GetTableMetadata(gomock.Any(), "p", "d", "t").
		Return(&bq.TableMetadata{
			Schema:           bq.Schema{{Name: "x", Type: bq.IntegerFieldType}, {Name: "dt", Type: bq.DateFieldType}},
			Type:             bq.RegularTable,
			TimePartitioning: &bq.TimePartitioning{Type: bq.DayPartitioningType, Field: "dt"},
		}, nil).Times(1)
	factory := func(context.Context, string, string) (bigquery.Client, error) { return c, nil }

	got := runWith(t, factory, "", "a.sql", "b.sql")
	if got.code != 1 {
		t.Errorf("exit code = %d, want 1 (stdout: %s, stderr: %s)", got.code, got.stdout, got.stderr)
	}
	if !strings.Contains(got.stdout, "(partition-filter)") {
		t.Errorf("stdout = %q, want partition-filter findings", got.stdout)
	}
}
