package lintcli

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
)

// tempFile is the part of *os.File that writeFileAtomic uses, so that tests
// can stand in for a disk that fails partway through a write.
type tempFile interface {
	io.Writer
	Name() string
	Chmod(mode fs.FileMode) error
	Chown(uid, gid int) error
	Sync() error
	Close() error
}

var (
	createTemp = func(dir, pattern string) (tempFile, error) { return os.CreateTemp(dir, pattern) }
	rename     = os.Rename
)

// writeFileAtomic replaces the file at name with data so that a failure at
// any point leaves the original as it was: data goes to a temporary file next
// to it, which is renamed over it only once fully written.
func writeFileAtomic(name string, data []byte) (err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("write %s: %w", name, err)
		}
	}()
	// Renaming over a symlink would replace the link with a plain file.
	target, err := filepath.EvalSymlinks(name)
	if err != nil {
		return err
	}
	info, err := os.Stat(target)
	if err != nil {
		return err
	}
	// Renaming over a FIFO or a device would replace it with a plain file, and
	// opening a FIFO whose reader is gone below would block forever.
	if !info.Mode().IsRegular() {
		return errors.New("not a regular file")
	}
	// Rename only needs the directory to be writable, but a read-only file is
	// one the user means to keep as it is.
	f, err := os.OpenFile(target, os.O_WRONLY, 0)
	if err != nil {
		return err
	}
	f.Close()

	tmp, err := createTemp(filepath.Dir(target), "."+filepath.Base(target)+".bqls-*")
	if err != nil {
		return withoutTempPath(err)
	}
	defer func() {
		if err != nil {
			tmp.Close()
			os.Remove(tmp.Name())
			err = withoutTempPath(err)
		}
	}()
	if _, err := tmp.Write(data); err != nil {
		return err
	}
	// Before the mode, as a chown by anyone but root clears the setuid and
	// setgid bits.
	keepOwner(tmp, info)
	if err := tmp.Chmod(info.Mode() & (fs.ModePerm | fs.ModeSetuid | fs.ModeSetgid | fs.ModeSticky)); err != nil {
		return err
	}
	// Without it a crash soon after the rename can leave the file empty on
	// file systems that write data back lazily. The directory is not synced:
	// losing the rename in a crash only leaves the original file.
	if err := tmp.Sync(); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return rename(tmp.Name(), target)
}

// withoutTempPath drops the temporary file's name from err: the file is gone
// by the time the error is shown, and the caller names the file being fixed.
func withoutTempPath(err error) error {
	var pathErr *fs.PathError
	if errors.As(err, &pathErr) {
		return pathErr.Err
	}
	var linkErr *os.LinkError
	if errors.As(err, &linkErr) {
		return linkErr.Err
	}
	return err
}
