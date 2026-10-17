package langserver

import (
	"encoding/json"
	"fmt"
	"testing"

	bq "cloud.google.com/go/bigquery"
	"github.com/golang/mock/gomock"
	"github.com/kitagry/bqls/langserver/internal/bigquery/mock_bigquery"
	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/kitagry/bqls/langserver/internal/source"
	"github.com/sirupsen/logrus"
	"github.com/sourcegraph/jsonrpc2"
)

func TestHandler_didSave_RefreshesDiagnosticsAfterTransientTableFetchFailure(t *testing.T) {
	const uri lsp.DocumentURI = "file:///a.sql"
	ctrl := gomock.NewController(t)
	bqClient := mock_bigquery.NewMockClient(ctrl)
	bqClient.EXPECT().GetDefaultProject().Return("p").AnyTimes()
	gomock.InOrder(
		bqClient.EXPECT().GetTableMetadata(gomock.Any(), "p", "d", "t").
			Return(nil, fmt.Errorf("transient network error")),
		bqClient.EXPECT().GetTableMetadata(gomock.Any(), "p", "d", "t").
			Return(&bq.TableMetadata{
				Type:   bq.RegularTable,
				Schema: bq.Schema{{Name: "x", Type: bq.IntegerFieldType}},
			}, nil).AnyTimes(),
	)
	p := source.NewProjectWithBQClient("/", bqClient, logrus.New())
	if err := p.UpdateFile(uri, "SELECT x FROM p.d.t", 1); err != nil {
		t.Fatal(err)
	}
	h := &Handler{
		project:           p,
		logger:            logrus.New(),
		diagnosticRequest: make(chan lsp.DocumentURI, 1),
		dryrunRequest:     make(chan lsp.DocumentURI, 1),
	}
	if failed, _ := h.diagnose(t.Context(), uri); len(failed[uri]) == 0 {
		t.Fatal("diagnose() reported nothing, want the failed table fetch to surface as a diagnostic")
	}

	raw := json.RawMessage(fmt.Sprintf(`{"textDocument":{"uri":%q}}`, uri))
	if _, err := h.handleTextDocumentDidSave(t.Context(), nil, &jsonrpc2.Request{Params: &raw}); err != nil {
		t.Fatalf("handleTextDocumentDidSave() error = %v", err)
	}
	got, _ := h.diagnose(t.Context(), uri)

	if len(got[uri]) != 0 {
		t.Errorf("diagnose() after didSave reported %+v, want no diagnostics once the table fetch succeeds", got[uri])
	}
}
