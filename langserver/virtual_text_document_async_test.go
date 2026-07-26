package langserver

import (
	"context"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/golang/mock/gomock"
	"github.com/kitagry/bqls/langserver/internal/bigquery/mock_bigquery"
	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/kitagry/bqls/langserver/internal/source"
	"github.com/sirupsen/logrus"
	"github.com/sourcegraph/jsonrpc2"
)

// fakeConn records every Notify call so tests can assert on what the
// scheduler pushed to the client.
type fakeConn struct {
	mu        sync.Mutex
	notifies  []fakeNotify
	notifiedC chan struct{}
	nextIndex int
}

type fakeNotify struct {
	method string
	params any
}

func newFakeConn() *fakeConn {
	return &fakeConn{notifiedC: make(chan struct{}, 10)}
}

func (f *fakeConn) Notify(ctx context.Context, method string, params any, opts ...jsonrpc2.CallOption) error {
	f.mu.Lock()
	f.notifies = append(f.notifies, fakeNotify{method: method, params: params})
	f.mu.Unlock()
	f.notifiedC <- struct{}{}
	return nil
}

func (f *fakeConn) Close() error { return nil }

// waitForNotify returns notifications in the order they were sent (not
// necessarily the order waitForNotify is called relative to when they
// arrive): each call waits for at least one more notification to exist,
// then returns the next unread one. This matters because two notifications
// can be sent back-to-back fast enough that both land before the first
// waitForNotify call wakes up.
func (f *fakeConn) waitForNotify(t *testing.T) fakeNotify {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		f.mu.Lock()
		if f.nextIndex < len(f.notifies) {
			notify := f.notifies[f.nextIndex]
			f.nextIndex++
			f.mu.Unlock()
			return notify
		}
		f.mu.Unlock()

		select {
		case <-f.notifiedC:
		case <-deadline:
			t.Fatal("timed out waiting for notification")
		}
	}
}

// assertNoMoreNotify fails the test if another (unread) notification shows
// up within d.
func (f *fakeConn) assertNoMoreNotify(t *testing.T, d time.Duration) {
	t.Helper()
	deadline := time.After(d)
	for {
		f.mu.Lock()
		if f.nextIndex < len(f.notifies) {
			notify := f.notifies[f.nextIndex]
			f.mu.Unlock()
			t.Errorf("unexpected extra notification: %+v", notify)
			return
		}
		f.mu.Unlock()

		select {
		case <-f.notifiedC:
		case <-deadline:
			return
		}
	}
}

func TestHandler_scheduleVirtualTextDocument(t *testing.T) {
	ctrl := gomock.NewController(t)
	bqClient := mock_bigquery.NewMockClient(ctrl)
	bqClient.EXPECT().GetTableMetadata(gomock.Any(), "p", "d", "t").Return(&bigquery.TableMetadata{
		FullID: "p:d.t",
		Type:   bigquery.ViewTable,
		Schema: bigquery.Schema{{Name: "id", Type: bigquery.IntegerFieldType}},
	}, nil)

	logger := logrus.New()
	conn := newFakeConn()
	h := &Handler{
		logger:                     logger,
		conn:                       conn,
		project:                    source.NewProjectWithBQClient("/", bqClient, logger),
		virtualTextDocumentRequest: make(chan lsp.DocumentURI, 3),
	}
	go h.scheduleVirtualTextDocument()

	uri := lsp.DocumentURI("bqls://project/p/dataset/d/table/t")
	h.virtualTextDocumentRequest <- uri

	// Details and preview are published as two separate notifications, with
	// details arriving first since it doesn't need to wait for preview rows.
	detailsNotify := conn.waitForNotify(t)
	if detailsNotify.method != "bqls/publishVirtualTextDocument" {
		t.Fatalf("notify method = %q, want %q", detailsNotify.method, "bqls/publishVirtualTextDocument")
	}
	detailsParams, ok := detailsNotify.params.(lsp.PublishVirtualTextDocumentParams)
	if !ok {
		t.Fatalf("notify params type = %T, want lsp.PublishVirtualTextDocumentParams", detailsNotify.params)
	}
	if detailsParams.TextDocument.URI != uri {
		t.Errorf("params.TextDocument.URI = %q, want %q", detailsParams.TextDocument.URI, uri)
	}
	if detailsParams.Kind != lsp.VirtualTextDocumentKindDetails {
		t.Errorf("params.Kind = %q, want %q", detailsParams.Kind, lsp.VirtualTextDocumentKindDetails)
	}
	if detailsParams.Error != "" {
		t.Errorf("params.Error = %q, want empty", detailsParams.Error)
	}
	if len(detailsParams.Contents) == 0 {
		t.Errorf("params.Contents is empty, want table metadata markdown")
	}

	previewNotify := conn.waitForNotify(t)
	previewParams, ok := previewNotify.params.(lsp.PublishVirtualTextDocumentParams)
	if !ok {
		t.Fatalf("notify params type = %T, want lsp.PublishVirtualTextDocumentParams", previewNotify.params)
	}
	if previewParams.Kind != lsp.VirtualTextDocumentKindPreview {
		t.Errorf("params.Kind = %q, want %q", previewParams.Kind, lsp.VirtualTextDocumentKindPreview)
	}
	if previewParams.Error != "" {
		t.Errorf("params.Error = %q, want empty", previewParams.Error)
	}
	if previewParams.Result == nil {
		t.Errorf("params.Result is nil, want a non-nil (possibly empty) QueryResult, since this is a ViewTable with no preview rows")
	}
}

func TestHandler_scheduleVirtualTextDocument_cancelsSupersededRequest(t *testing.T) {
	ctrl := gomock.NewController(t)
	bqClient := mock_bigquery.NewMockClient(ctrl)

	firstCalled := make(chan struct{})
	gomock.InOrder(
		bqClient.EXPECT().GetTableMetadata(gomock.Any(), "p", "d", "t").DoAndReturn(
			func(ctx context.Context, projectID, datasetID, tableID string) (*bigquery.TableMetadata, error) {
				close(firstCalled)
				<-ctx.Done() // block until superseded by the second request
				return nil, ctx.Err()
			},
		),
		bqClient.EXPECT().GetTableMetadata(gomock.Any(), "p", "d", "t").Return(&bigquery.TableMetadata{
			FullID: "p:d.t",
			Type:   bigquery.ViewTable,
			Schema: bigquery.Schema{{Name: "id", Type: bigquery.IntegerFieldType}},
		}, nil),
	)

	logger := logrus.New()
	conn := newFakeConn()
	h := &Handler{
		logger:                     logger,
		conn:                       conn,
		project:                    source.NewProjectWithBQClient("/", bqClient, logger),
		virtualTextDocumentRequest: make(chan lsp.DocumentURI, 3),
	}
	go h.scheduleVirtualTextDocument()

	uri := lsp.DocumentURI("bqls://project/p/dataset/d/table/t")

	h.virtualTextDocumentRequest <- uri
	select {
	case <-firstCalled:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for first fetch to start")
	}
	h.virtualTextDocumentRequest <- uri // supersedes the first, in-flight request

	// The canceled first request must have never gotten far enough to
	// publish anything (it was blocked inside GetTableMetadata); only the
	// second request's details+preview notifications should arrive.
	detailsNotify := conn.waitForNotify(t)
	detailsParams, ok := detailsNotify.params.(lsp.PublishVirtualTextDocumentParams)
	if !ok {
		t.Fatalf("notify params type = %T, want lsp.PublishVirtualTextDocumentParams", detailsNotify.params)
	}
	if detailsParams.Kind != lsp.VirtualTextDocumentKindDetails {
		t.Errorf("params.Kind = %q, want %q", detailsParams.Kind, lsp.VirtualTextDocumentKindDetails)
	}
	if detailsParams.Error != "" {
		t.Errorf("params.Error = %q, want empty (only the second, superseding request should be published)", detailsParams.Error)
	}
	if len(detailsParams.Contents) == 0 {
		t.Errorf("params.Contents is empty, want table metadata markdown from the second request")
	}

	previewNotify := conn.waitForNotify(t)
	previewParams, ok := previewNotify.params.(lsp.PublishVirtualTextDocumentParams)
	if !ok {
		t.Fatalf("notify params type = %T, want lsp.PublishVirtualTextDocumentParams", previewNotify.params)
	}
	if previewParams.Kind != lsp.VirtualTextDocumentKindPreview {
		t.Errorf("params.Kind = %q, want %q", previewParams.Kind, lsp.VirtualTextDocumentKindPreview)
	}

	// Give the canceled first request a moment to settle and confirm no
	// extra (third) notification arrives.
	conn.assertNoMoreNotify(t, 200*time.Millisecond)
}
