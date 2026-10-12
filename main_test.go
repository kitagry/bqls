package main

import (
	"fmt"
	"io"
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

func TestRun_LintSubcommand(t *testing.T) {
	origStderr := os.Stderr
	t.Cleanup(func() { os.Stderr = origStderr })
	devNull, err := os.Open(os.DevNull)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { devNull.Close() })
	os.Stderr = devNull

	// Without inputs, lint mode reports a usage error instead of starting the language server.
	if got := run([]string{"lint"}); got != 2 {
		t.Errorf("run([]string{\"lint\"}) = %v, want 2", got)
	}
}

func TestRun_Exit(t *testing.T) {
	shutdown := `{"jsonrpc":"2.0","id":1,"method":"shutdown"}`
	exit := `{"jsonrpc":"2.0","method":"exit"}`

	tests := map[string]struct {
		messages []string
		want     exitCode
	}{
		"exit after shutdown returns OK": {
			messages: []string{shutdown, exit},
			want:     exitCodeOK,
		},
		"exit without shutdown returns Err": {
			messages: []string{exit},
			want:     exitCodeErr,
		},
	}

	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
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
			// stdin stays open so that run can only return via the exit notification.
			t.Cleanup(func() { inW.Close() })

			outR, outW, err := os.Pipe()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { outR.Close() })
			go func() { _, _ = io.Copy(io.Discard, outR) }()

			os.Stdin = inR
			os.Stdout = outW

			done := make(chan exitCode, 1)
			go func() {
				done <- run(nil)
			}()

			for _, m := range tt.messages {
				if _, err := fmt.Fprintf(inW, "Content-Length: %d\r\n\r\n%s", len(m), m); err != nil {
					t.Fatal(err)
				}
			}

			select {
			case got := <-done:
				if got != tt.want {
					t.Errorf("run() = %v, want %v", got, tt.want)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("run did not return within timeout")
			}
		})
	}
}
