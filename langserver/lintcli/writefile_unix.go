//go:build unix

package lintcli

import (
	"io/fs"
	"syscall"
)

// keepOwner gives f the owner and group of the file it replaces, as far as
// the user may: only root can give a file away, and others can only pick a
// group they belong to. Failing that the file ends up owned by the user, as
// with any editor that saves through a new file, rather than not being fixed.
func keepOwner(f tempFile, orig fs.FileInfo) {
	st, ok := orig.Sys().(*syscall.Stat_t)
	if !ok {
		return
	}
	// Giving the file away fails as a whole for anyone but root, which would
	// also lose a group the user could still set on its own.
	if f.Chown(int(st.Uid), int(st.Gid)) != nil {
		_ = f.Chown(-1, int(st.Gid))
	}
}
