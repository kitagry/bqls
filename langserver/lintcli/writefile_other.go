//go:build !unix

package lintcli

import "io/fs"

func keepOwner(tempFile, fs.FileInfo) {}
