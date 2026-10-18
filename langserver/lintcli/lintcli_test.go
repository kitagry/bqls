package lintcli

import (
	"bytes"
	"context"
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	bq "cloud.google.com/go/bigquery"
	"github.com/golang/mock/gomock"
	"github.com/google/go-cmp/cmp"
	"github.com/kitagry/bqls/langserver/internal/bigquery"
	"github.com/kitagry/bqls/langserver/internal/bigquery/mock_bigquery"
	"google.golang.org/api/googleapi"
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
		"directories starting with a dot are skipped below the root": {
			files: map[string]string{
				"a.sql":        unusedCTESQL,
				".git/b.sql":   unusedCTESQL,
				"q/.tmp/c.sql": unusedCTESQL,
			},
			args:       []string{"."},
			wantCode:   1,
			wantStdout: "a.sql:1:6: info: CTE `a` is defined but never used (unused-cte)\n",
		},
		"dot directory given as root is searched": {
			files:      map[string]string{".sql/a.sql": unusedCTESQL},
			args:       []string{".sql"},
			wantCode:   1,
			wantStdout: ".sql/a.sql:1:6: info: CTE `a` is defined but never used (unused-cte)\n",
		},
		"same file given twice is linted once": {
			files:      map[string]string{"q/a.sql": unusedCTESQL},
			args:       []string{"q/a.sql", "./q/a.sql", "q"},
			wantCode:   1,
			wantStdout: "q/a.sql:1:6: info: CTE `a` is defined but never used (unused-cte)\n",
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

func TestRun_StdinGivenTwiceIsLintedOnce(t *testing.T) {
	got := runWith(t, mockFactory(t), unusedCTESQL, "-", "-")
	want := "<stdin>:1:6: info: CTE `a` is defined but never used (unused-cte)\n"
	if diff := cmp.Diff(want, got.stdout); diff != "" {
		t.Errorf("stdout diff (-want +got):\n%s", diff)
	}
}

func TestRun_UnreadableInputDoesNotStopOthers(t *testing.T) {
	t.Chdir(t.TempDir())
	writeFiles(t, map[string]string{"a.sql": unusedCTESQL})

	got := runWith(t, mockFactory(t), "", "missing.sql", "a.sql")
	if got.code != 2 {
		t.Errorf("exit code = %d, want 2", got.code)
	}
	if want := "a.sql:1:6: info: CTE `a` is defined but never used (unused-cte)\n"; got.stdout != want {
		t.Errorf("stdout = %q, want %q", got.stdout, want)
	}
	if !strings.Contains(got.stderr, "missing.sql") {
		t.Errorf("stderr = %q, want it to mention missing.sql", got.stderr)
	}
}

func TestWriteText_EscapesNewlines(t *testing.T) {
	var buf bytes.Buffer
	if err := writeText(&buf, []finding{{File: "a.sql", Line: 1, Column: 2, Severity: "error", Message: "a\r\nb"}}); err != nil {
		t.Fatal(err)
	}
	if got, want := buf.String(), "a.sql:1:2: error: a\\r\\nb\n"; got != want {
		t.Errorf("writeText() = %q, want %q", got, want)
	}
}

func TestRun_TableMetadataErrors(t *testing.T) {
	tests := map[string]struct {
		err        error
		wantCode   int
		wantStdout string
		wantStderr string
	}{
		"not found is a finding": {
			err:        &googleapi.Error{Code: 404, Message: "Not found: Table p:d.t"},
			wantCode:   1,
			wantStdout: "a.sql:1:15: error: Table not found: `p.d.t`\n",
		},
		"permission denied is a runtime failure": {
			err:        &googleapi.Error{Code: 403, Message: "Access Denied: Table p:d.t"},
			wantCode:   2,
			wantStderr: "Access Denied: Table p:d.t",
		},
		"transport failure is a runtime failure": {
			err:        &url.Error{Op: "Get", URL: "https://bigquery.googleapis.com", Err: errors.New("connection refused")},
			wantCode:   2,
			wantStderr: "connection refused",
		},
	}
	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			t.Chdir(t.TempDir())
			const src = "SELECT x FROM `p.d.t`\n"
			writeFiles(t, map[string]string{"a.sql": src, "b.sql": src})

			ctrl := gomock.NewController(t)
			c := mock_bigquery.NewMockClient(ctrl)
			c.EXPECT().GetDefaultProject().Return("p").AnyTimes()
			c.EXPECT().Close().Return(nil).AnyTimes()
			c.EXPECT().GetTableMetadata(gomock.Any(), "p", "d", "t").Return(nil, tt.err).AnyTimes()
			factory := func(context.Context, string, string) (bigquery.Client, error) { return c, nil }

			got := runWith(t, factory, "", "a.sql")
			if got.code != tt.wantCode {
				t.Errorf("exit code = %d, want %d (stdout: %s, stderr: %s)", got.code, tt.wantCode, got.stdout, got.stderr)
			}
			if tt.wantStdout != "" && got.stdout != tt.wantStdout {
				t.Errorf("stdout = %q, want %q", got.stdout, tt.wantStdout)
			}
			if tt.wantStderr == "" && got.stderr != "" {
				t.Errorf("stderr = %q, want empty", got.stderr)
			}
			if tt.wantStderr != "" && strings.Count(got.stderr, tt.wantStderr) != 1 {
				t.Errorf("stderr = %q, want it to contain %q exactly once", got.stderr, tt.wantStderr)
			}
		})
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

func TestRun_GitHubOutputPaths(t *testing.T) {
	t.Run("relative to GITHUB_WORKSPACE", func(t *testing.T) {
		root := t.TempDir()
		t.Setenv("GITHUB_WORKSPACE", root)
		t.Chdir(root)
		writeFiles(t, map[string]string{"sub/a.sql": unusedCTESQL})
		t.Chdir(filepath.Join(root, "sub"))

		got := runWith(t, mockFactory(t), "", "--format", "github", "a.sql")
		want := "::notice file=sub/a.sql,line=1,col=6,endLine=1,endColumn=7,title=bqls(unused-cte)::CTE `a` is defined but never used\n"
		if diff := cmp.Diff(want, got.stdout); diff != "" {
			t.Errorf("stdout diff (-want +got):\n%s", diff)
		}
	})

	t.Run("relative to cwd without GITHUB_WORKSPACE", func(t *testing.T) {
		t.Setenv("GITHUB_WORKSPACE", "")
		root := t.TempDir()
		t.Chdir(root)
		writeFiles(t, map[string]string{"a.sql": unusedCTESQL})

		got := runWith(t, mockFactory(t), "", "--format", "github", filepath.Join(root, "a.sql"))
		want := "::notice file=a.sql,line=1,col=6,endLine=1,endColumn=7,title=bqls(unused-cte)::CTE `a` is defined but never used\n"
		if diff := cmp.Diff(want, got.stdout); diff != "" {
			t.Errorf("stdout diff (-want +got):\n%s", diff)
		}
	})

	t.Run("stdin has no file property", func(t *testing.T) {
		got := runWith(t, mockFactory(t), unusedCTESQL, "--format", "github", "-")
		want := "::notice line=1,col=6,endLine=1,endColumn=7,title=bqls(unused-cte)::CTE `a` is defined but never used\n"
		if diff := cmp.Diff(want, got.stdout); diff != "" {
			t.Errorf("stdout diff (-want +got):\n%s", diff)
		}
	})
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
		"no arguments":    {args: nil, wantStderr: "no input"},
		"unknown rule":    {args: []string{"--disable", "no-such-rule", "a.sql"}, wantStderr: "null-comparison"},
		"unknown format":  {args: []string{"--format", "xml", "a.sql"}, wantStderr: "xml"},
		"missing file":    {args: []string{"missing.sql"}, wantStderr: "missing.sql"},
		"undefined flag":  {args: []string{"--no-such-flag", "a.sql"}, wantStderr: "no-such-flag"},
		"flag after file": {args: []string{"a.sql", "--format", "json"}, wantStderr: "flags must come before files"},
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
			if got.stdout != "" {
				t.Errorf("stdout = %q, want empty on usage errors", got.stdout)
			}
		})
	}
}

func TestRun_UndefinedFlagPrintsUsageToStderr(t *testing.T) {
	got := runWith(t, mockFactory(t), "", "--no-such-flag")
	if !strings.Contains(got.stderr, "Usage: bqls lint") {
		t.Errorf("stderr = %q, want usage", got.stderr)
	}
}

func TestRun_Help(t *testing.T) {
	got := runWith(t, mockFactory(t), "", "--help")
	if got.code != 0 {
		t.Errorf("exit code = %d, want 0", got.code)
	}
	for _, want := range []string{"bqls lint", "null-comparison", "in-null", "unused-cte", "partition-filter", "-format"} {
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

func TestRun_PartitionFilterOnDatasetQualifiedTable(t *testing.T) {
	t.Chdir(t.TempDir())
	writeFiles(t, map[string]string{"a.sql": "SELECT x FROM d.t\n"})

	ctrl := gomock.NewController(t)
	c := mock_bigquery.NewMockClient(ctrl)
	c.EXPECT().GetDefaultProject().Return("p").AnyTimes()
	c.EXPECT().Close().Return(nil).AnyTimes()
	c.EXPECT().GetTableMetadata(gomock.Any(), "p", "d", "t").
		Return(&bq.TableMetadata{
			Schema:           bq.Schema{{Name: "x", Type: bq.IntegerFieldType}, {Name: "dt", Type: bq.DateFieldType}},
			Type:             bq.RegularTable,
			TimePartitioning: &bq.TimePartitioning{Type: bq.DayPartitioningType, Field: "dt"},
		}, nil).AnyTimes()
	factory := func(context.Context, string, string) (bigquery.Client, error) { return c, nil }

	got := runWith(t, factory, "", "a.sql")
	want := result{
		code:   1,
		stdout: "a.sql:1:15: warning: Query on partitioned table `p.d.t` has no filter on partition column `dt`; it may scan all partitions (partition-filter)\n",
	}
	if diff := cmp.Diff(want, got, cmp.AllowUnexported(result{})); diff != "" {
		t.Errorf("run result diff (-want +got):\n%s", diff)
	}
}

// INFORMATION_SCHEMA views have no table metadata, so their schema comes from a
// dry run, which should also run once per view per run.
func TestRun_InformationSchemaDryRunOncePerView(t *testing.T) {
	t.Chdir(t.TempDir())
	const src = "SELECT job_id FROM `region-us`.INFORMATION_SCHEMA.JOBS\n"
	writeFiles(t, map[string]string{"a.sql": src, "b.sql": src})

	ctrl := gomock.NewController(t)
	c := mock_bigquery.NewMockClient(ctrl)
	c.EXPECT().GetDefaultProject().Return("p").AnyTimes()
	c.EXPECT().Close().Return(nil).AnyTimes()
	c.EXPECT().DryRunSchema(gomock.Any(), "SELECT * FROM `p`.`region-us`.`INFORMATION_SCHEMA`.`JOBS`").
		Return(bq.Schema{{Name: "job_id", Type: bq.StringFieldType}}, nil).Times(1)
	factory := func(context.Context, string, string) (bigquery.Client, error) { return c, nil }

	got := runWith(t, factory, "", "a.sql", "b.sql")
	if diff := cmp.Diff(result{}, got, cmp.AllowUnexported(result{})); diff != "" {
		t.Errorf("run result diff (-want +got):\n%s", diff)
	}
}

func TestRun_InformationSchemaDryRunRuntimeFailure(t *testing.T) {
	t.Chdir(t.TempDir())
	writeFiles(t, map[string]string{"a.sql": "SELECT job_id FROM `region-us`.INFORMATION_SCHEMA.JOBS\n"})

	ctrl := gomock.NewController(t)
	c := mock_bigquery.NewMockClient(ctrl)
	c.EXPECT().GetDefaultProject().Return("p").AnyTimes()
	c.EXPECT().Close().Return(nil).AnyTimes()
	c.EXPECT().DryRunSchema(gomock.Any(), gomock.Any()).
		Return(nil, &googleapi.Error{Code: 403, Message: "Access Denied: Project p"}).AnyTimes()
	factory := func(context.Context, string, string) (bigquery.Client, error) { return c, nil }

	got := runWith(t, factory, "", "a.sql")
	if got.code != 2 {
		t.Errorf("exit code = %d, want 2 (stdout: %s, stderr: %s)", got.code, got.stdout, got.stderr)
	}
	if strings.Count(got.stderr, "Access Denied: Project p") != 1 {
		t.Errorf("stderr = %q, want it to contain the failure exactly once", got.stderr)
	}
}
