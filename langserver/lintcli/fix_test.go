package lintcli

import (
	"context"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	bq "cloud.google.com/go/bigquery"
	"github.com/golang/mock/gomock"
	"github.com/google/go-cmp/cmp"
	"github.com/kitagry/bqls/langserver/internal/bigquery"
	"github.com/kitagry/bqls/langserver/internal/bigquery/mock_bigquery"
	"github.com/kitagry/bqls/langserver/internal/lint"
	"github.com/kitagry/bqls/langserver/internal/lsp"
)

func TestRun_Fix(t *testing.T) {
	tests := map[string]struct {
		files      map[string]string
		args       []string
		wantCode   int
		wantStdout string
		wantStderr string
		wantFiles  map[string]string
	}{
		"lint and error fixes are written back": {
			files: map[string]string{
				"a.sql": nullComparisonSQL,
				"b.sql": "SELECT x FROM UNNEST([1]) AS x WHERE x = NULL\n",
			},
			args:     []string{"--fix", "a.sql", "b.sql"},
			wantCode: 0,
			wantStderr: "bqls lint: a.sql: fixed 1 finding (null-comparison)\n" +
				"bqls lint: b.sql: fixed 1 finding\n",
			wantFiles: map[string]string{
				"a.sql": "SELECT x FROM UNNEST([1]) AS x WHERE x IS NULL\n",
				"b.sql": "SELECT x FROM UNNEST([1]) AS x WHERE x IS NULL\n",
			},
		},
		"findings left after fixing are reported against the fixed text": {
			files: map[string]string{
				"a.sql": "SELECT x FROM UNNEST([1]) AS x WHERE x = CAST(NULL AS INT64) OR x IN (NULL /* c */)\n",
			},
			args:       []string{"--fix", "a.sql"},
			wantCode:   1,
			wantStderr: "bqls lint: a.sql: fixed 1 finding (null-comparison)\n",
			wantStdout: "a.sql:1:51: warning: IN with only NULL values always yields NULL (in-null)\n",
			wantFiles: map[string]string{
				"a.sql": "SELECT x FROM UNNEST([1]) AS x WHERE x IS NULL OR x IN (NULL /* c */)\n",
			},
		},
		"a fix with several edits is applied whole": {
			files: map[string]string{
				"a.sql": "SELECT x FROM UNNEST([1]) AS x WHERE x NOT IN (NULL, 1, NULL)\n",
			},
			args:       []string{"--fix", "a.sql"},
			wantCode:   0,
			wantStderr: "bqls lint: a.sql: fixed 1 finding (in-null)\n",
			wantFiles: map[string]string{
				"a.sql": "SELECT x FROM UNNEST([1]) AS x WHERE x NOT IN (1)\n",
			},
		},
		// The UTC fix comes first and keeps the result; one in the configured
		// zone would change it.
		"timestamp literal gets its time zone made explicit": {
			files: map[string]string{
				"a.sql":      "SELECT ts FROM UNNEST([CURRENT_TIMESTAMP()]) AS ts WHERE ts >= '2024-01-01'\n",
				".bqls.yaml": "lint:\n  rules:\n    timestamp-string-literal:\n      enabled: true\n      time_zone: Asia/Tokyo\n",
			},
			args:       []string{"--fix", "a.sql"},
			wantCode:   0,
			wantStderr: "bqls lint: a.sql: fixed 1 finding (timestamp-string-literal)\n",
			wantFiles: map[string]string{
				"a.sql": "SELECT ts FROM UNNEST([CURRENT_TIMESTAMP()]) AS ts WHERE ts >= TIMESTAMP('2024-01-01', 'UTC')\n",
			},
		},
		// DIV truncates where the CAST rounds, so the fix is offered in the
		// editor but not applied.
		"fixes that change the result are not applied": {
			files:      map[string]string{"a.sql": "SELECT CAST(x / 2 AS INT64) FROM UNNEST([7]) AS x\n"},
			args:       []string{"--fix", "a.sql"},
			wantCode:   1,
			wantStdout: "a.sql:1:8: warning: CAST to INT64 rounds the FLOAT64 result of an integer division to the nearest integer, halfway cases away from zero (CAST(7 / 2 AS INT64) is 4); use DIV for integer division that truncates toward zero, or ROUND / FLOOR / CEIL to state the rounding (cast-division-rounds)\n",
			wantFiles:  map[string]string{"a.sql": "SELECT CAST(x / 2 AS INT64) FROM UNNEST([7]) AS x\n"},
		},
		"extraction pattern keeps its first capturing group": {
			files:      map[string]string{"a.sql": "SELECT REGEXP_EXTRACT('ab', '(a)(b)')\n"},
			args:       []string{"--fix", "a.sql"},
			wantCode:   0,
			wantStderr: "bqls lint: a.sql: fixed 1 finding (regexp-literal)\n",
			wantFiles: map[string]string{
				"a.sql": "SELECT REGEXP_EXTRACT('ab', '(a)(?:b)')\n",
			},
		},
		"join rewrites are written back": {
			files: map[string]string{
				"a.sql": "SELECT a.x, b.y FROM (SELECT x FROM UNNEST([1, 2]) AS x) AS a, (SELECT y FROM UNNEST([3, 4]) AS y) AS b;\n" +
					"SELECT a.x FROM (SELECT x FROM UNNEST([1, 2]) AS x) AS a LEFT JOIN (SELECT x FROM UNNEST([1]) AS x) AS b ON a.x = b.x WHERE b.x = 1;\n",
			},
			args:       []string{"--fix", "a.sql"},
			wantCode:   0,
			wantStderr: "bqls lint: a.sql: fixed 2 findings (cross-join, left-join-as-inner)\n",
			wantFiles: map[string]string{
				"a.sql": "SELECT a.x, b.y FROM (SELECT x FROM UNNEST([1, 2]) AS x) AS a CROSS JOIN (SELECT y FROM UNNEST([3, 4]) AS y) AS b;\n" +
					"SELECT a.x FROM (SELECT x FROM UNNEST([1, 2]) AS x) AS a INNER JOIN (SELECT x FROM UNNEST([1]) AS x) AS b ON a.x = b.x WHERE b.x = 1;\n",
			},
		},
		"column order fixes are written back": {
			files: map[string]string{
				"a.sql": "SELECT 1 AS a, 2 AS b UNION ALL SELECT 2 AS b, 1 AS a\n",
				"b.sql": "CREATE TEMP TABLE t (a INT64, b INT64);\nINSERT INTO t SELECT 2 AS b, 1 AS a;\n",
			},
			args:     []string{"--fix", "a.sql", "b.sql"},
			wantCode: 0,
			wantStderr: "bqls lint: a.sql: fixed 1 finding (union-column-order)\n" +
				"bqls lint: b.sql: fixed 1 finding (insert-column-order)\n",
			wantFiles: map[string]string{
				"a.sql": "SELECT 1 AS a, 2 AS b UNION ALL SELECT 1 AS a, 2 AS b\n",
				"b.sql": "CREATE TEMP TABLE t (a INT64, b INT64);\nINSERT INTO t (b, a) SELECT 2 AS b, 1 AS a;\n",
			},
		},
		"fixes that overlap an applied fix are skipped": {
			files: map[string]string{
				"a.sql": "SELECT x FROM UNNEST([1]) AS x WHERE (x = CAST(NULL AS INT64)) IN (NULL)\n",
			},
			args:       []string{"--fix", "a.sql"},
			wantCode:   1,
			wantStdout: "a.sql:1:39: warning: Comparison with NULL using = always yields NULL; use IS NULL (null-comparison)\n",
			wantStderr: "bqls lint: a.sql: fixed 1 finding (in-null)\n" +
				"bqls lint: a.sql: skipped 1 fix(es) overlapping another fix; run again to apply them\n",
			wantFiles: map[string]string{
				"a.sql": "SELECT x FROM UNNEST([1]) AS x WHERE (x = CAST(NULL AS INT64)) IS NULL\n",
			},
		},
		// stdout stays machine readable; the summary is for people.
		"the summary stays on stderr with the json format": {
			files:      map[string]string{"a.sql": nullComparisonSQL},
			args:       []string{"--fix", "--format", "json", "a.sql"},
			wantCode:   0,
			wantStdout: "[]\n",
			wantStderr: "bqls lint: a.sql: fixed 1 finding (null-comparison)\n",
		},
		"fixes of disabled rules are not applied": {
			files:     map[string]string{"a.sql": unusedCTESQL},
			args:      []string{"--fix", "--disable", "unused-cte", "a.sql"},
			wantCode:  0,
			wantFiles: map[string]string{"a.sql": unusedCTESQL},
		},
	}
	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			t.Chdir(t.TempDir())
			writeFiles(t, tt.files)

			got := runWith(t, mockFactory(t), "", tt.args...)
			if got.code != tt.wantCode {
				t.Errorf("exit code = %d, want %d (stdout: %s, stderr: %s)", got.code, tt.wantCode, got.stdout, got.stderr)
			}
			if diff := cmp.Diff(tt.wantStdout, got.stdout); diff != "" {
				t.Errorf("stdout diff (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(tt.wantStderr, got.stderr); diff != "" {
				t.Errorf("stderr diff (-want +got):\n%s", diff)
			}
			for name, want := range tt.wantFiles {
				b, err := os.ReadFile(name)
				if err != nil {
					t.Fatal(err)
				}
				if diff := cmp.Diff(want, string(b)); diff != "" {
					t.Errorf("%s diff (-want +got):\n%s", name, diff)
				}
			}
		})
	}
}

func TestRun_FixSelectStar(t *testing.T) {
	t.Chdir(t.TempDir())
	writeFiles(t, map[string]string{
		".git/HEAD":  "",
		".bqls.yaml": "lint:\n  rules:\n    select-star:\n      enabled: true\n      min_columns: 2\n",
		"a.sql":      "SELECT *\nFROM p.d.t\n",
	})
	ctrl := gomock.NewController(t)
	c := mock_bigquery.NewMockClient(ctrl)
	c.EXPECT().GetDefaultProject().Return("p").AnyTimes()
	c.EXPECT().Close().Return(nil).AnyTimes()
	c.EXPECT().GetTableMetadata(gomock.Any(), "p", "d", "t").Return(&bq.TableMetadata{
		Schema: bq.Schema{{Name: "a", Type: bq.IntegerFieldType}, {Name: "b", Type: bq.StringFieldType}},
	}, nil).AnyTimes()

	got := runWith(t, func(context.Context, string, string) (bigquery.Client, error) { return c, nil }, "", "--fix", "a.sql")
	if got.code != 0 {
		t.Errorf("exit code = %d, want 0 (stdout: %s, stderr: %s)", got.code, got.stdout, got.stderr)
	}
	b, err := os.ReadFile("a.sql")
	if err != nil {
		t.Fatal(err)
	}
	if want := "SELECT\n  a,\n  b\nFROM p.d.t\n"; string(b) != want {
		t.Errorf("a.sql = %q, want %q", b, want)
	}
}

func TestRun_FixDoesNotRewriteUnchangedFiles(t *testing.T) {
	t.Chdir(t.TempDir())
	writeFiles(t, map[string]string{"a.sql": cleanSQL})
	old := time.Now().Add(-time.Hour).Truncate(time.Second)
	if err := os.Chtimes("a.sql", old, old); err != nil {
		t.Fatal(err)
	}

	got := runWith(t, mockFactory(t), "", "--fix", "a.sql")
	if got.code != 0 {
		t.Errorf("exit code = %d, want 0 (stderr: %s)", got.code, got.stderr)
	}
	if got.stderr != "" {
		t.Errorf("stderr = %q, want nothing when no fix was applied", got.stderr)
	}
	info, err := os.Stat("a.sql")
	if err != nil {
		t.Fatal(err)
	}
	if !info.ModTime().Equal(old) {
		t.Errorf("a.sql was rewritten (mtime %v, want %v)", info.ModTime(), old)
	}
}

func TestRun_FixWriteFailureReportsTheFileOnDisk(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("root can write read-only files")
	}
	t.Chdir(t.TempDir())
	writeFiles(t, map[string]string{"a.sql": nullComparisonSQL})
	if err := os.Chmod("a.sql", 0o444); err != nil {
		t.Fatal(err)
	}

	// dryRunClient fails the test if the unsaved fixed text is dry-run.
	factory := dryRunClient(t, map[string]int64{nullComparisonSQL: 2 << 40})
	got := runWith(t, factory, "", "--fix", "--dry-run", "--enable", "bytes-processed-threshold", "a.sql")
	if got.code != 2 {
		t.Errorf("exit code = %d, want 2 (stderr: %s)", got.code, got.stderr)
	}
	if want := "a.sql:1:1: warning: This query will process 2 TiB, which exceeds the threshold of 1 TiB (bytes-processed-threshold)\n" +
		"a.sql:1:38: warning: Comparison with NULL using = always yields NULL; use IS NULL (null-comparison)\n"; got.stdout != want {
		t.Errorf("stdout = %q, want %q", got.stdout, want)
	}
	if !strings.Contains(got.stderr, "permission denied") {
		t.Errorf("stderr = %q, want the write error", got.stderr)
	}
	if strings.Contains(got.stderr, "fixed") {
		t.Errorf("stderr = %q, want no summary for a file that was not written", got.stderr)
	}
}

func TestRun_FixWriteFailureDoesNotNoteSkippedFixes(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("root can write read-only files")
	}
	t.Chdir(t.TempDir())
	writeFiles(t, map[string]string{"a.sql": "SELECT x FROM UNNEST([1]) AS x WHERE (x = CAST(NULL AS INT64)) IN (NULL)\n"})
	if err := os.Chmod("a.sql", 0o444); err != nil {
		t.Fatal(err)
	}

	got := runWith(t, mockFactory(t), "", "--fix", "a.sql")
	if got.code != 2 {
		t.Errorf("exit code = %d, want 2 (stderr: %s)", got.code, got.stderr)
	}
	if strings.Contains(got.stderr, "skipped") {
		t.Errorf("stderr = %q, want no note to run again for a file that was not written", got.stderr)
	}
}

func TestRun_FixStdin(t *testing.T) {
	src := "SELECT x FROM UNNEST([1]) AS x WHERE x = CAST(NULL AS INT64) OR x IN (NULL /* c */)\n"
	got := runWith(t, mockFactory(t), src, "--fix", "-")
	if got.code != 1 {
		t.Errorf("exit code = %d, want 1 (stderr: %s)", got.code, got.stderr)
	}
	// stdout carries only the SQL so that editors can pipe through it.
	if want := "SELECT x FROM UNNEST([1]) AS x WHERE x IS NULL OR x IN (NULL /* c */)\n"; got.stdout != want {
		t.Errorf("stdout = %q, want %q", got.stdout, want)
	}
	// The summary comes while fixing, before the findings that are left.
	if want := "bqls lint: <stdin>: fixed 1 finding (null-comparison)\n" +
		"<stdin>:1:51: warning: IN with only NULL values always yields NULL (in-null)\n"; got.stderr != want {
		t.Errorf("stderr = %q, want %q", got.stderr, want)
	}
}

func TestRun_FixStdinWithoutFixesEchoesInput(t *testing.T) {
	got := runWith(t, mockFactory(t), analysisErrorSQL, "--fix", "-")
	if got.stdout != analysisErrorSQL {
		t.Errorf("stdout = %q, want the input unchanged", got.stdout)
	}
	if !strings.Contains(got.stderr, "Unrecognized name: y") {
		t.Errorf("stderr = %q, want the analysis error", got.stderr)
	}
}

func TestApplyFixes(t *testing.T) {
	edit := func(sl, sc, el, ec int, text string) lsp.TextEdit {
		return lsp.TextEdit{Range: lsp.Range{Start: lsp.Position{Line: sl, Character: sc}, End: lsp.Position{Line: el, Character: ec}}, NewText: text}
	}
	fix := func(edits ...lsp.TextEdit) lint.SuggestedFix { return lint.SuggestedFix{Edits: edits} }

	tests := map[string]struct {
		src         string
		fixes       []lint.SuggestedFix
		want        string
		wantApplied []int
	}{
		"edits on several lines": {
			src:         "abc\ndef\n",
			fixes:       []lint.SuggestedFix{fix(edit(1, 0, 1, 1, "D")), fix(edit(0, 1, 0, 2, "B"))},
			want:        "aBc\nDef\n",
			wantApplied: []int{0, 1},
		},
		"adjacent edits both apply": {
			src:         "abcdef",
			fixes:       []lint.SuggestedFix{fix(edit(0, 0, 0, 3, "X")), fix(edit(0, 3, 0, 6, "Y"))},
			want:        "XY",
			wantApplied: []int{0, 1},
		},
		"overlapping fix is skipped": {
			src:         "abcdef",
			fixes:       []lint.SuggestedFix{fix(edit(0, 0, 0, 4, "X")), fix(edit(0, 2, 0, 6, "Y"))},
			want:        "Xef",
			wantApplied: []int{0},
		},
		"insertions at the same position conflict": {
			src:         "ab",
			fixes:       []lint.SuggestedFix{fix(edit(0, 1, 0, 1, "X")), fix(edit(0, 1, 0, 1, "Y"))},
			want:        "aXb",
			wantApplied: []int{0},
		},
		"a fix is skipped as a whole": {
			src: "abcdef",
			fixes: []lint.SuggestedFix{
				fix(edit(0, 0, 0, 2, "X")),
				fix(edit(0, 4, 0, 5, "Y"), edit(0, 1, 0, 3, "Z")),
			},
			want:        "Xcdef",
			wantApplied: []int{0},
		},
		"multi-line edit": {
			src:         "ab\ncd\nef",
			fixes:       []lint.SuggestedFix{fix(edit(0, 1, 2, 1, "-"))},
			want:        "a-f",
			wantApplied: []int{0},
		},
	}
	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			got, applied := applyFixes(tt.src, tt.fixes)
			if got != tt.want || !slices.Equal(applied, tt.wantApplied) {
				t.Errorf("applyFixes() = (%q, %v), want (%q, %v)", got, applied, tt.want, tt.wantApplied)
			}
		})
	}
}
