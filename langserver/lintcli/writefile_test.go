//go:build unix

package lintcli

import (
	"io/fs"
	"os"
	"slices"
	"strings"
	"syscall"
	"testing"
)

const fixedNullComparisonSQL = "SELECT x FROM UNNEST([1]) AS x WHERE x IS NULL\n"

// failingFile stands in for the disk failing partway through a write, which
// cannot be caused on demand.
type failingFile struct {
	tempFile
	failOn string
}

func (f *failingFile) Write(p []byte) (int, error) {
	if f.failOn == "write" {
		n, _ := f.tempFile.Write(p[:len(p)/2])
		return n, syscall.ENOSPC
	}
	return f.tempFile.Write(p)
}

func (f *failingFile) Sync() error {
	if f.failOn == "sync" {
		return syscall.EIO
	}
	return f.tempFile.Sync()
}

func (f *failingFile) Close() error {
	err := f.tempFile.Close()
	if f.failOn == "close" {
		return syscall.EIO
	}
	return err
}

func replaceFS(t *testing.T, newCreateTemp func(dir, pattern string) (tempFile, error), newRename func(oldpath, newpath string) error) {
	t.Helper()
	oldCreateTemp, oldRename := createTemp, rename
	t.Cleanup(func() { createTemp, rename = oldCreateTemp, oldRename })
	if newCreateTemp != nil {
		createTemp = newCreateTemp
	}
	if newRename != nil {
		rename = newRename
	}
}

func dirEntries(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names
}

func assertUnchanged(t *testing.T, name, want string) {
	t.Helper()
	b, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != want {
		t.Errorf("%s = %q, want the original %q", name, b, want)
	}
}

func TestRun_FixWriteFailureLeavesTheOriginal(t *testing.T) {
	tests := map[string]struct {
		failOn  string
		rename  func(oldpath, newpath string) error
		wantErr error
	}{
		"disk full while writing": {failOn: "write", wantErr: syscall.ENOSPC},
		"sync fails":              {failOn: "sync", wantErr: syscall.EIO},
		"close fails":             {failOn: "close", wantErr: syscall.EIO},
		"rename fails": {
			rename: func(oldpath, newpath string) error {
				return &os.LinkError{Op: "rename", Old: oldpath, New: newpath, Err: syscall.EXDEV}
			},
			wantErr: syscall.EXDEV,
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Chdir(t.TempDir())
			writeFiles(t, map[string]string{"a.sql": nullComparisonSQL})
			replaceFS(t, func(dir, pattern string) (tempFile, error) {
				f, err := os.CreateTemp(dir, pattern)
				if err != nil {
					return nil, err
				}
				return &failingFile{tempFile: f, failOn: tt.failOn}, nil
			}, tt.rename)

			got := runWith(t, mockFactory(t), "", "--fix", "a.sql")
			if got.code != 2 {
				t.Errorf("exit code = %d, want 2 (stderr: %s)", got.code, got.stderr)
			}
			// The errno text differs between platforms, so take it from the errno.
			if !strings.Contains(got.stderr, tt.wantErr.Error()) {
				t.Errorf("stderr = %q, want the write error %q", got.stderr, tt.wantErr.Error())
			}
			assertUnchanged(t, "a.sql", nullComparisonSQL)
			if names := dirEntries(t, "."); !slices.Equal(names, []string{"a.sql"}) {
				t.Errorf("directory holds %v, want only a.sql (the temporary file must be removed)", names)
			}
		})
	}
}

func TestRun_FixReadOnlyFileIsNotReplaced(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("root can write read-only files")
	}
	t.Chdir(t.TempDir())
	writeFiles(t, map[string]string{"a.sql": nullComparisonSQL})
	if err := os.Chmod("a.sql", 0o444); err != nil {
		t.Fatal(err)
	}

	got := runWith(t, mockFactory(t), "", "--fix", "a.sql")
	if got.code != 2 {
		t.Errorf("exit code = %d, want 2 (stderr: %s)", got.code, got.stderr)
	}
	if !strings.Contains(got.stderr, "permission denied") {
		t.Errorf("stderr = %q, want the write error", got.stderr)
	}
	assertUnchanged(t, "a.sql", nullComparisonSQL)
	if names := dirEntries(t, "."); !slices.Equal(names, []string{"a.sql"}) {
		t.Errorf("directory holds %v, want only a.sql", names)
	}
}

func TestRun_FixInReadOnlyDirectoryFails(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("root can write to read-only directories")
	}
	t.Chdir(t.TempDir())
	writeFiles(t, map[string]string{"q/a.sql": nullComparisonSQL})
	if err := os.Chmod("q", 0o555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod("q", 0o755) })

	got := runWith(t, mockFactory(t), "", "--fix", "q/a.sql")
	if got.code != 2 {
		t.Errorf("exit code = %d, want 2 (stderr: %s)", got.code, got.stderr)
	}
	if !strings.Contains(got.stderr, "q/a.sql") || !strings.Contains(got.stderr, "permission denied") {
		t.Errorf("stderr = %q, want the write error naming q/a.sql", got.stderr)
	}
	if strings.Contains(got.stderr, ".bqls-") {
		t.Errorf("stderr = %q, want no mention of the temporary file", got.stderr)
	}
	assertUnchanged(t, "q/a.sql", nullComparisonSQL)
	if names := dirEntries(t, "q"); !slices.Equal(names, []string{"a.sql"}) {
		t.Errorf("directory holds %v, want only a.sql", names)
	}
}

func TestRun_FixWritesThroughSymlink(t *testing.T) {
	t.Chdir(t.TempDir())
	writeFiles(t, map[string]string{"real/a.sql": nullComparisonSQL})
	if err := os.Mkdir("links", 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("../real/a.sql", "links/a.sql"); err != nil {
		t.Fatal(err)
	}

	got := runWith(t, mockFactory(t), "", "--fix", "links/a.sql")
	if got.code != 0 {
		t.Errorf("exit code = %d, want 0 (stderr: %s)", got.code, got.stderr)
	}
	info, err := os.Lstat("links/a.sql")
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode()&fs.ModeSymlink == 0 {
		t.Errorf("links/a.sql mode = %v, want it to stay a symlink", info.Mode())
	}
	assertUnchanged(t, "real/a.sql", fixedNullComparisonSQL)
	if names := dirEntries(t, "links"); !slices.Equal(names, []string{"a.sql"}) {
		t.Errorf("links/ holds %v, want only the link", names)
	}
}

func TestRun_FixKeepsFileMode(t *testing.T) {
	t.Chdir(t.TempDir())
	writeFiles(t, map[string]string{"a.sql": nullComparisonSQL})
	// Not the umask default nor the 0o600 of a fresh temporary file.
	if err := os.Chmod("a.sql", 0o640); err != nil {
		t.Fatal(err)
	}

	got := runWith(t, mockFactory(t), "", "--fix", "a.sql")
	if got.code != 0 {
		t.Errorf("exit code = %d, want 0 (stderr: %s)", got.code, got.stderr)
	}
	assertUnchanged(t, "a.sql", fixedNullComparisonSQL)
	info, err := os.Stat("a.sql")
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != 0o640 {
		t.Errorf("mode = %v, want %v", got, fs.FileMode(0o640))
	}
}

func TestRun_FixKeepsFileGroup(t *testing.T) {
	groups, err := os.Getgroups()
	if err != nil {
		t.Fatal(err)
	}
	// A user can only give a file a group they belong to, so the test needs
	// one other than the group a new file gets.
	i := slices.IndexFunc(groups, func(g int) bool { return g != os.Getgid() })
	if i < 0 {
		t.Skip("the user belongs to no supplementary group")
	}
	gid := groups[i]
	t.Chdir(t.TempDir())
	writeFiles(t, map[string]string{"a.sql": nullComparisonSQL})
	if err := os.Chown("a.sql", -1, gid); err != nil {
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
	if st, ok := info.Sys().(*syscall.Stat_t); !ok || int(st.Gid) != gid {
		t.Errorf("gid = %v, want %d", info.Sys(), gid)
	}
}

func TestWriteFileAtomic_RefusesNonRegularFile(t *testing.T) {
	t.Chdir(t.TempDir())
	if err := syscall.Mkfifo("a.sql", 0o644); err != nil {
		t.Skipf("mkfifo: %v", err)
	}

	// Opening the FIFO for writing would block with no reader, so a hang here
	// is the failure.
	err := writeFileAtomic("a.sql", []byte(fixedNullComparisonSQL))
	if err == nil {
		t.Fatal("writeFileAtomic succeeded, want an error for a FIFO")
	}
	info, err := os.Lstat("a.sql")
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode()&fs.ModeNamedPipe == 0 {
		t.Errorf("a.sql mode = %v, want it to stay a FIFO", info.Mode())
	}
	if names := dirEntries(t, "."); !slices.Equal(names, []string{"a.sql"}) {
		t.Errorf("directory holds %v, want only a.sql", names)
	}
}
