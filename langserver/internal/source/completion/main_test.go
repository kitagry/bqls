package completion

import (
	"fmt"
	"os"
	"testing"

	googlesql "github.com/goccy/go-googlesql"
)

func TestMain(m *testing.M) {
	if err := googlesql.Init(); err != nil {
		fmt.Fprintf(os.Stderr, "failed to initialize googlesql: %v\n", err)
		os.Exit(1)
	}
	defer googlesql.Close()
	os.Exit(m.Run())
}
