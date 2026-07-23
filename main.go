package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"runtime"
	"runtime/debug"
	"time"

	googlesql "github.com/goccy/go-googlesql"
	jsonrpc "github.com/gumeniukcom/golang-jsonrpc2/v2"
	"github.com/gumeniukcom/golang-jsonrpc2/v2/jsonrpcstdio"
	"github.com/kitagry/bqls/langserver"
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

	showVersion := fs.Bool("version", false, "print version")
	isDebug := fs.Bool("debug", false, "log debug")
	if err := fs.Parse(args); err != nil {
		if err == flag.ErrHelp {
			return exitCodeOK
		}
		return exitCodeErr
	}

	if *showVersion {
		fmt.Printf("%s %s (rev: %s/%s)\n", name, version, getRevision(), runtime.Version())
		return exitCodeOK
	}

	if err := googlesql.Init(); err != nil {
		fmt.Fprintf(os.Stderr, "failed to initialize googlesql: %v\n", err)
		return exitCodeErr
	}

	handler := langserver.NewHandler(*isDebug)
	defer handler.Close()

	rpc := jsonrpc.New()
	// LSP sessions and long-running BigQuery execute-command handlers need a
	// generous per-request timeout; the 30s default would break them.
	rpc.SetDefaultTimeOut(15 * time.Minute)
	handler.Register(rpc)

	// WithMaxMessageSize(256 MiB) is mandatory: LSP full-document sync sends
	// entire files in textDocument/didOpen, and the 8 MiB transport default
	// would fatally kill the stream on a large document.
	if err := jsonrpcstdio.Serve(
		context.Background(),
		rpc,
		jsonrpcstdio.FramingContentLength,
		os.Stdin,
		os.Stdout,
		jsonrpcstdio.WithMaxMessageSize(256<<20),
	); err != nil {
		fmt.Fprintf(os.Stderr, "jsonrpc serve error: %v\n", err)
		return exitCodeErr
	}
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
