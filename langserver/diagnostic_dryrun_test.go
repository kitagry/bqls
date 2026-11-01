package langserver

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	bq "cloud.google.com/go/bigquery"
	"github.com/golang/mock/gomock"
	"github.com/google/go-cmp/cmp"
	"github.com/kitagry/bqls/langserver/internal/bigquery"
	"github.com/kitagry/bqls/langserver/internal/bigquery/mock_bigquery"
	"github.com/kitagry/bqls/langserver/internal/lint"
	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/kitagry/bqls/langserver/internal/source"
	"github.com/sirupsen/logrus"
)

// startDryRunScheduler runs scheduleDryRun for p with the
// bytes-processed-threshold rule enabled on top of the defaults.
func startDryRunScheduler(t *testing.T, p *source.Project) (*Handler, *fakeConn) {
	t.Helper()
	conn := newFakeConn()
	h := newHandler(logrus.New(), "")
	h.conn = conn
	h.project = p
	h.initializeParams.InitializationOptions.Lint = lint.Config{Rules: map[string]bool{"bytes-processed-threshold": true}}
	go h.scheduleDryRun()
	t.Cleanup(func() { close(h.dryrunRequest) })
	return h, conn
}

func dryRunJobStatus(ctrl *gomock.Controller, status *bq.JobStatus) bigquery.BigqueryJob {
	job := mock_bigquery.NewMockBigqueryJob(ctrl)
	job.EXPECT().LastStatus().Return(status).AnyTimes()
	return job
}

func TestHandler_scheduleDryRun_PublishesDiagnosticsFromTheDryRun(t *testing.T) {
	const uri lsp.DocumentURI = "file:///a.sql"
	ctrl := gomock.NewController(t)
	bqClient := mock_bigquery.NewMockClient(ctrl)
	bqClient.EXPECT().Run(gomock.Any(), "SELECT 1", true).
		Return(dryRunJobStatus(ctrl, &bq.JobStatus{State: bq.Done, Statistics: &bq.JobStatistics{TotalBytesProcessed: 2 << 40}}), nil)
	p := source.NewProjectWithBQClient("/", bqClient, logrus.New())
	if err := p.UpdateFile(uri, "SELECT 1", 1); err != nil {
		t.Fatal(err)
	}
	h, conn := startDryRunScheduler(t, p)

	h.dryrunRequest <- uri

	want := []fakeNotify{
		{
			method: "textDocument/publishDiagnostics",
			params: lsp.PublishDiagnosticsParams{URI: uri, Diagnostics: []lsp.Diagnostic{{
				Range:    lsp.Range{Start: lsp.Position{Line: 0, Character: 0}, End: lsp.Position{Line: 0, Character: 8}},
				Severity: lsp.Warning,
				Code:     "bytes-processed-threshold",
				Source:   "bqls-lint",
				Message:  "This query will process 2 TiB, which exceeds the threshold of 1 TiB",
			}}},
		},
		{
			method: "window/showMessage",
			params: lsp.ShowMessageParams{Type: lsp.Info, Message: "This query will process 2 TiB when run."},
		},
	}
	got := []fakeNotify{conn.waitForNotify(t), conn.waitForNotify(t)}
	if diff := cmp.Diff(want, got, cmp.AllowUnexported(fakeNotify{})); diff != "" {
		t.Errorf("notifications mismatch (-want +got):\n%s", diff)
	}
}

func TestHandler_scheduleDryRun_OmitsDryRunDiagnosticsWhenTextChangedDuringDryRun(t *testing.T) {
	const uri lsp.DocumentURI = "file:///a.sql"
	ctrl := gomock.NewController(t)
	bqClient := mock_bigquery.NewMockClient(ctrl)
	p := source.NewProjectWithBQClient("/", bqClient, logrus.New())
	bqClient.EXPECT().Run(gomock.Any(), "SELECT 1", true).
		DoAndReturn(func(context.Context, string, bool) (bigquery.BigqueryJob, error) {
			if err := p.UpdateFile(uri, "SELECT 2", 2); err != nil {
				t.Error(err)
			}
			return dryRunJobStatus(ctrl, &bq.JobStatus{State: bq.Done, Statistics: &bq.JobStatistics{TotalBytesProcessed: 2 << 40}}), nil
		})
	if err := p.UpdateFile(uri, "SELECT 1", 1); err != nil {
		t.Fatal(err)
	}
	h, conn := startDryRunScheduler(t, p)

	h.dryrunRequest <- uri

	want := fakeNotify{
		method: "textDocument/publishDiagnostics",
		params: lsp.PublishDiagnosticsParams{URI: uri, Diagnostics: []lsp.Diagnostic{}},
	}
	if diff := cmp.Diff(want, conn.waitForNotify(t), cmp.AllowUnexported(fakeNotify{})); diff != "" {
		t.Errorf("publishDiagnostics mismatch (-want +got):\n%s", diff)
	}
}

func TestHandler_scheduleDryRun_KeepsPublishedDiagnosticsOnDryRunError(t *testing.T) {
	const uri lsp.DocumentURI = "file:///a.sql"
	ctrl := gomock.NewController(t)
	bqClient := mock_bigquery.NewMockClient(ctrl)
	bqClient.EXPECT().Run(gomock.Any(), "SELECT 1", true).
		Return(nil, fmt.Errorf("fail to run query: %w", errors.New("access denied")))
	p := source.NewProjectWithBQClient("/", bqClient, logrus.New())
	if err := p.UpdateFile(uri, "SELECT 1", 1); err != nil {
		t.Fatal(err)
	}
	h, conn := startDryRunScheduler(t, p)

	h.dryrunRequest <- uri

	want := fakeNotify{
		method: "window/showMessage",
		params: lsp.ShowMessageParams{Type: lsp.MTError, Message: "access denied"},
	}
	if diff := cmp.Diff(want, conn.waitForNotify(t), cmp.AllowUnexported(fakeNotify{})); diff != "" {
		t.Errorf("notification mismatch (-want +got):\n%s", diff)
	}
	conn.assertNoMoreNotify(t, 100*time.Millisecond)
}

func TestHandler_scheduleDryRun_PublishesDiagnosticsWhenDryRunHasNoStatistics(t *testing.T) {
	const uri lsp.DocumentURI = "file:///a.sql"
	const src = "SELECT x FROM UNNEST([1]) AS x WHERE x = CAST(NULL AS INT64)"
	ctrl := gomock.NewController(t)
	bqClient := mock_bigquery.NewMockClient(ctrl)
	bqClient.EXPECT().Run(gomock.Any(), src, true).
		Return(dryRunJobStatus(ctrl, &bq.JobStatus{State: bq.Done}), nil)
	p := source.NewProjectWithBQClient("/", bqClient, logrus.New())
	if err := p.UpdateFile(uri, src, 1); err != nil {
		t.Fatal(err)
	}
	h, conn := startDryRunScheduler(t, p)

	h.dryrunRequest <- uri

	want := fakeNotify{
		method: "textDocument/publishDiagnostics",
		params: lsp.PublishDiagnosticsParams{URI: uri, Diagnostics: []lsp.Diagnostic{{
			Range:    lsp.Range{Start: lsp.Position{Line: 0, Character: 37}, End: lsp.Position{Line: 0, Character: 60}},
			Severity: lsp.Warning,
			Code:     "null-comparison",
			Source:   "bqls-lint",
			Message:  "Comparison with NULL using = always yields NULL; use IS NULL",
		}}},
	}
	if diff := cmp.Diff(want, conn.waitForNotify(t), cmp.AllowUnexported(fakeNotify{})); diff != "" {
		t.Errorf("publishDiagnostics mismatch (-want +got):\n%s", diff)
	}
	conn.assertNoMoreNotify(t, 100*time.Millisecond)
}
