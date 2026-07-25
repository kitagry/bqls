package main

import (
	"os"
	"testing"
	"time"
)

// vscode-languageclient auto-appends the -stdio flag when using TransportKind.stdio,
// so bqls must accept and ignore it.
func TestRun_StdioFlag(t *testing.T) {
	origStdin := os.Stdin
	origStdout := os.Stdout
	t.Cleanup(func() {
		os.Stdin = origStdin
		os.Stdout = origStdout
	})

	inR, inW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	inW.Close() // trigger an immediate EOF so the jsonrpc2 connection disconnects right away

	outR, outW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { outR.Close() })

	os.Stdin = inR
	os.Stdout = outW

	done := make(chan exitCode, 1)
	go func() {
		done <- run([]string{"-stdio"})
	}()

	select {
	case got := <-done:
		outW.Close()
		if got != exitCodeOK {
			t.Errorf("run([]string{\"-stdio\"}) = %v, want %v", got, exitCodeOK)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("run did not return within timeout")
	}
}
