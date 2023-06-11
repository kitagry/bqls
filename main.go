package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"runtime"
	"runtime/debug"

	"github.com/kitagry/bqls/langserver"
	"github.com/sourcegraph/jsonrpc2"
)

const (
	name = "bqls"
)

var (
	version  = "v0.0.0"
	revision = ""
)

type exitCode int

const (
	exitCodeOK exitCode = iota
	exitCodeErr
)

func main() {
	os.Exit(int(run(os.Args[1:])))
}

func run(args []string) exitCode {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	fs.Usage = func() {
		fs.SetOutput(os.Stdout)
		fmt.Printf(`%[1]s - BigQuery language server

Version: %s (rev: %s/%s)

You can use your favorite lsp client.
`, name, version, getRevision(), runtime.Version())
		fs.PrintDefaults()
	}

	var showVersion bool
	fs.BoolVar(&showVersion, "version", false, "print version")
	if err := fs.Parse(args); err != nil {
		if err == flag.ErrHelp {
			return exitCodeOK
		}
		return exitCodeErr
	}

	if showVersion {
		fmt.Printf("%s %s (rev: %s/%s)\n", name, version, getRevision(), runtime.Version())
		return exitCodeOK
	}

	handler := langserver.NewHandler()
	defer handler.Close()
	<-jsonrpc2.NewConn(context.Background(), jsonrpc2.NewBufferedStream(stdrwc{}, jsonrpc2.VSCodeObjectCodec{}), handler).DisconnectNotify()
	return exitCodeOK
}

func getRevision() string {
	if revision != "" {
		return revision
	}
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return ""
	}
	var revision string
	var modified string
	for _, s := range info.Settings {
		switch s.Key {
		case "vcs.revision":
			revision = s.Value
		case "vcs.modified":
			modified = s.Value
		}
	}
	if modified == "true" {
		revision += "(modified)"
	}
	return revision
}

type stdrwc struct{}

func (stdrwc) Read(p []byte) (int, error) {
	return os.Stdin.Read(p)
}

func (c stdrwc) Write(p []byte) (int, error) {
	return os.Stdout.Write(p)
}

func (c stdrwc) Close() error {
	if err := os.Stdin.Close(); err != nil {
		return err
	}
	return os.Stdout.Close()
}
