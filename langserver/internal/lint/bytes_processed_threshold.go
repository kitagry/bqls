package lint

import (
	"fmt"
	"strings"

	googlesql "github.com/goccy/go-googlesql"
	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/kitagry/bqls/langserver/internal/source/file"
	"github.com/kitagry/bqls/langserver/internal/source/helper"
)

// BytesProcessedThreshold is off by default because it only has input when a
// dry-run has been run, which costs a BigQuery API call.
var BytesProcessedThreshold = &Analyzer{
	Name:           "bytes-processed-threshold",
	Doc:            "reports queries whose dry-run estimate exceeds max_bytes",
	DefaultEnabled: false,
	NewOptions:     func() any { return &BytesProcessedThresholdOptions{MaxBytes: 1 << 40} },
	Run:            runBytesProcessedThreshold,
}

type BytesProcessedThresholdOptions struct {
	MaxBytes int64 `json:"max_bytes"`
}

func (o *BytesProcessedThresholdOptions) Validate() error {
	if o.MaxBytes < 0 {
		return fmt.Errorf("max_bytes must not be negative, got %d", o.MaxBytes)
	}
	return nil
}

func runBytesProcessedThreshold(pass *Pass) {
	if pass.DryRun == nil || pass.DryRun.Statistics == nil {
		return
	}
	opts := pass.Options.(*BytesProcessedThresholdOptions)
	processed := pass.DryRun.Statistics.TotalBytesProcessed
	if processed <= opts.MaxBytes {
		return
	}
	r, ok := firstStatementLine(pass.File)
	if !ok {
		return
	}
	pass.Report(Diagnostic{
		Range: r,
		Message: fmt.Sprintf("This query will process %s, which exceeds the threshold of %s",
			helper.BytesConvert(processed), helper.BytesConvert(opts.MaxBytes)),
	})
}

// firstStatementLine spans the first line of the first statement: the estimate
// covers the whole script, so it is anchored where a `bqls:ignore` comment
// above the script can suppress it without underlining the entire query.
func firstStatementLine(f file.ParsedFile) (lsp.Range, bool) {
	stmts := file.ListAstNode[googlesql.ASTStatementNode](f.Node)
	if len(stmts) == 0 {
		return lsp.Range{}, false
	}
	off := astOffsets(stmts[0])
	r, ok := f.OffsetsToLspRange(off.start, off.start)
	if !ok {
		return lsp.Range{}, false
	}
	lines := strings.Split(f.Src, "\n")
	r.End.Character = len(strings.TrimRight(lines[r.Start.Line], "\r"))
	return r, true
}
