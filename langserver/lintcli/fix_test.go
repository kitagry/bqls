package lintcli

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/kitagry/bqls/langserver/internal/lint"
	"github.com/kitagry/bqls/langserver/internal/lsp"
)

func TestRun_Fix(t *testing.T) {
	tests := map[string]struct {
		files      map[string]string
		args       []string
		wantCode   int
		wantStdout string
		wantFiles  map[string]string
	}{
		"lint and error fixes are written back": {
			files: map[string]string{
				"a.sql": nullComparisonSQL,
				"b.sql": "SELECT x FROM UNNEST([1]) AS x WHERE x = NULL\n",
			},
			args:     []string{"--fix", "a.sql", "b.sql"},
			wantCode: 0,
			wantFiles: map[string]string{
				"a.sql": "SELECT x FROM UNNEST([1]) AS x WHERE x IS NULL\n",
				"b.sql": "SELECT x FROM UNNEST([1]) AS x WHERE x IS NULL\n",
			},
		},
		"findings left after fixing are reported against the fixed text": {
			files: map[string]string{
				"a.sql": "SELECT x FROM UNNEST([1]) AS x WHERE x = CAST(NULL AS INT64) OR x IN (NULL)\n",
			},
			args:       []string{"--fix", "a.sql"},
			wantCode:   1,
			wantStdout: "a.sql:1:51: warning: IN with only NULL values always yields NULL (in-null)\n",
			wantFiles: map[string]string{
				"a.sql": "SELECT x FROM UNNEST([1]) AS x WHERE x IS NULL OR x IN (NULL)\n",
			},
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
	info, err := os.Stat("a.sql")
	if err != nil {
		t.Fatal(err)
	}
	if !info.ModTime().Equal(old) {
		t.Errorf("a.sql was rewritten (mtime %v, want %v)", info.ModTime(), old)
	}
}

func TestRun_FixStdin(t *testing.T) {
	src := "SELECT x FROM UNNEST([1]) AS x WHERE x = CAST(NULL AS INT64) OR x IN (NULL)\n"
	got := runWith(t, mockFactory(t), src, "--fix", "-")
	if got.code != 1 {
		t.Errorf("exit code = %d, want 1 (stderr: %s)", got.code, got.stderr)
	}
	// stdout carries only the SQL so that editors can pipe through it.
	if want := "SELECT x FROM UNNEST([1]) AS x WHERE x IS NULL OR x IN (NULL)\n"; got.stdout != want {
		t.Errorf("stdout = %q, want %q", got.stdout, want)
	}
	if want := "<stdin>:1:51: warning: IN with only NULL values always yields NULL (in-null)\n"; got.stderr != want {
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
		wantSkipped int
	}{
		"edits on several lines": {
			src:   "abc\ndef\n",
			fixes: []lint.SuggestedFix{fix(edit(1, 0, 1, 1, "D")), fix(edit(0, 1, 0, 2, "B"))},
			want:  "aBc\nDef\n",
		},
		"adjacent edits both apply": {
			src:   "abcdef",
			fixes: []lint.SuggestedFix{fix(edit(0, 0, 0, 3, "X")), fix(edit(0, 3, 0, 6, "Y"))},
			want:  "XY",
		},
		"overlapping fix is skipped": {
			src:         "abcdef",
			fixes:       []lint.SuggestedFix{fix(edit(0, 0, 0, 4, "X")), fix(edit(0, 2, 0, 6, "Y"))},
			want:        "Xef",
			wantSkipped: 1,
		},
		"insertions at the same position conflict": {
			src:         "ab",
			fixes:       []lint.SuggestedFix{fix(edit(0, 1, 0, 1, "X")), fix(edit(0, 1, 0, 1, "Y"))},
			want:        "aXb",
			wantSkipped: 1,
		},
		"a fix is skipped as a whole": {
			src: "abcdef",
			fixes: []lint.SuggestedFix{
				fix(edit(0, 0, 0, 2, "X")),
				fix(edit(0, 4, 0, 5, "Y"), edit(0, 1, 0, 3, "Z")),
			},
			want:        "Xcdef",
			wantSkipped: 1,
		},
		"multi-line edit": {
			src:   "ab\ncd\nef",
			fixes: []lint.SuggestedFix{fix(edit(0, 1, 2, 1, "-"))},
			want:  "a-f",
		},
	}
	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			got, skipped := applyFixes(tt.src, tt.fixes)
			if got != tt.want || skipped != tt.wantSkipped {
				t.Errorf("applyFixes() = (%q, %d), want (%q, %d)", got, skipped, tt.want, tt.wantSkipped)
			}
		})
	}
}
