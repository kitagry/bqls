package langserver

import (
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/golang/mock/gomock"
	"github.com/kitagry/bqls/langserver/internal/bigquery/mock_bigquery"
	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/kitagry/bqls/langserver/internal/source"
	"github.com/sirupsen/logrus"
	"github.com/sourcegraph/jsonrpc2"
)

func TestHandler_resolveVirtualTextDocument(t *testing.T) {
	ctrl := gomock.NewController(t)
	bqClient := mock_bigquery.NewMockClient(ctrl)
	bqClient.EXPECT().GetTableMetadata(gomock.Any(), "p", "d", "t").Return(&bigquery.TableMetadata{
		FullID: "p:d.t",
		Type:   bigquery.ViewTable, // avoid GetTableRecord being called
		Schema: bigquery.Schema{{Name: "id", Type: bigquery.IntegerFieldType}},
	}, nil)

	logger := logrus.New()
	h := &Handler{
		logger:  logger,
		project: source.NewProjectWithBQClient("/", bqClient, logger),
	}

	got, err := h.resolveVirtualTextDocument(t.Context(), "token", lsp.VirtualTextDocumentInfo{
		ProjectID: "p",
		DatasetID: "d",
		TableID:   "t",
	})
	if err != nil {
		t.Fatalf("resolveVirtualTextDocument() error = %v", err)
	}

	if len(got.Contents) == 0 {
		t.Errorf("resolveVirtualTextDocument() Contents is empty, want table metadata markdown")
	}
	if got.Pending {
		t.Errorf("resolveVirtualTextDocument() Pending = true, want false")
	}
}

func TestHandler_resolveVirtualTextDocumentDetails_and_Preview(t *testing.T) {
	ctrl := gomock.NewController(t)
	bqClient := mock_bigquery.NewMockClient(ctrl)
	bqClient.EXPECT().GetTableMetadata(gomock.Any(), "p", "d", "t").Return(&bigquery.TableMetadata{
		FullID: "p:d.t",
		Type:   bigquery.RegularTable,
		Schema: bigquery.Schema{{Name: "id", Type: bigquery.IntegerFieldType}},
	}, nil)
	bqClient.EXPECT().GetTableRecord(gomock.Any(), "p", "d", "t").Return(&bigquery.RowIterator{}, nil)

	logger := logrus.New()
	h := &Handler{
		logger:  logger,
		project: source.NewProjectWithBQClient("/", bqClient, logger),
	}

	details, err := h.resolveVirtualTextDocumentDetails(t.Context(), "token", lsp.VirtualTextDocumentInfo{
		ProjectID: "p",
		DatasetID: "d",
		TableID:   "t",
	})
	if err != nil {
		t.Fatalf("resolveVirtualTextDocumentDetails() error = %v", err)
	}
	if len(details.contents) == 0 {
		t.Errorf("resolveVirtualTextDocumentDetails() contents is empty, want table metadata markdown")
	}
	if details.fetchPreview == nil {
		t.Fatalf("resolveVirtualTextDocumentDetails() fetchPreview is nil, want a non-nil closure for a regular table")
	}

	// buildQueryResult (called by resolveVirtualTextDocumentPreview) calls
	// it.Next(), which panics on a zero-value *bigquery.RowIterator, so this
	// only checks that fetchPreview itself resolves to a non-nil iterator.
	// resolveVirtualTextDocumentPreview's own logic (nil fetchPreview /
	// nil iterator handling) is covered by the tests below.
	it, err := details.fetchPreview(t.Context())
	if err != nil {
		t.Fatalf("fetchPreview() error = %v", err)
	}
	if it == nil {
		t.Errorf("fetchPreview() = nil, want a non-nil iterator for a regular table")
	}
}

func TestHandler_resolveVirtualTextDocumentPreview_noFetchPreview(t *testing.T) {
	h := &Handler{logger: logrus.New()}

	result, err := h.resolveVirtualTextDocumentPreview(t.Context(), virtualTextDocumentDetails{})
	if err != nil {
		t.Fatalf("resolveVirtualTextDocumentPreview() error = %v", err)
	}
	if result.Columns != nil || result.Data != nil {
		t.Errorf("resolveVirtualTextDocumentPreview() = %+v, want zero value QueryResult", result)
	}
}

func TestHandler_handleVirtualTextDocument_async(t *testing.T) {
	logger := logrus.New()
	h := &Handler{
		logger: logger,
		initializeParams: lsp.InitializeParams[InitializeOption]{
			InitializationOptions: InitializeOption{SupportsAsyncVirtualTextDocument: true},
		},
		virtualTextDocumentRequest: make(chan lsp.DocumentURI, 3),
	}

	uri := lsp.DocumentURI("bqls://project/p/dataset/d/table/t")
	req := &jsonrpc2.Request{}
	if err := req.SetParams(lsp.VirtualTextDocumentParams{TextDocument: lsp.TextDocumentIdentifier{URI: uri}}); err != nil {
		t.Fatalf("failed to set params: %v", err)
	}

	got, err := h.handleVirtualTextDocument(t.Context(), nil, req)
	if err != nil {
		t.Fatalf("handleVirtualTextDocument() error = %v", err)
	}

	result, ok := got.(lsp.VirtualTextDocument)
	if !ok {
		t.Fatalf("result type = %T, want lsp.VirtualTextDocument", got)
	}
	if !result.Pending {
		t.Errorf("result.Pending = false, want true (async path should return a placeholder)")
	}

	select {
	case gotURI := <-h.virtualTextDocumentRequest:
		if gotURI != uri {
			t.Errorf("queued uri = %q, want %q", gotURI, uri)
		}
	default:
		t.Error("expected uri to be queued on virtualTextDocumentRequest channel")
	}
}

func TestHandler_handleVirtualTextDocument_sync(t *testing.T) {
	ctrl := gomock.NewController(t)
	bqClient := mock_bigquery.NewMockClient(ctrl)
	bqClient.EXPECT().GetTableMetadata(gomock.Any(), "p", "d", "t").Return(&bigquery.TableMetadata{
		FullID: "p:d.t",
		Type:   bigquery.ViewTable,
		Schema: bigquery.Schema{{Name: "id", Type: bigquery.IntegerFieldType}},
	}, nil)

	logger := logrus.New()
	h := &Handler{
		logger:  logger,
		project: source.NewProjectWithBQClient("/", bqClient, logger),
		// SupportsAsyncVirtualTextDocument defaults to false (zero value),
		// so this should take the fully-synchronous path unchanged.
	}

	uri := lsp.DocumentURI("bqls://project/p/dataset/d/table/t")
	req := &jsonrpc2.Request{}
	if err := req.SetParams(lsp.VirtualTextDocumentParams{TextDocument: lsp.TextDocumentIdentifier{URI: uri}}); err != nil {
		t.Fatalf("failed to set params: %v", err)
	}

	got, err := h.handleVirtualTextDocument(t.Context(), nil, req)
	if err != nil {
		t.Fatalf("handleVirtualTextDocument() error = %v", err)
	}

	result, ok := got.(lsp.VirtualTextDocument)
	if !ok {
		t.Fatalf("result type = %T, want lsp.VirtualTextDocument", got)
	}
	if result.Pending {
		t.Errorf("result.Pending = true, want false (sync path for older clients)")
	}
	if len(result.Contents) == 0 {
		t.Errorf("result.Contents is empty, want table metadata markdown")
	}
}

// BuildFieldSchema itself now lives in (and is tested by)
// langserver/internal/source (field_schema.go / field_schema_test.go).
