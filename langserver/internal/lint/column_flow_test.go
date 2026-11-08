package lint_test

import (
	"context"
	"testing"

	"github.com/kitagry/bqls/langserver/internal/lint"
)

func TestRun_SharesColumnFlowAcrossAnalyzers(t *testing.T) {
	src := "SELECT * FROM `p.d.plain` WHERE dt = '2024-01-01';\nSELECT * FROM `p.d.plain` WHERE dt = '2024-01-02'"
	parsed := parsePartitioned(t, src)

	var got [][]any
	record := func(name string) *lint.Analyzer {
		return &lint.Analyzer{
			Name:           name,
			Doc:            "test analyzer",
			DefaultEnabled: true,
			Run: func(pass *lint.Pass) {
				got = append(got, lint.ColumnFlows(pass))
			},
		}
	}
	lint.Run(context.Background(), parsed, lint.Config{}, lint.Env{}, record("first"), record("second"))

	if len(got) != 2 {
		t.Fatalf("analyzers ran %d times, want 2", len(got))
	}
	if len(got[0]) != 2 {
		t.Fatalf("got %d column flows, want one per statement (2)", len(got[0]))
	}
	for i := range got[0] {
		if got[0][i] == nil {
			t.Errorf("statement %d: column flow is nil", i)
		}
		if got[0][i] != got[1][i] {
			t.Errorf("statement %d: analyzers got different column flows, want the one computed once per Run", i)
		}
	}
}
