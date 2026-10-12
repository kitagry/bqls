package source_test

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/google/go-cmp/cmp"
	"github.com/kitagry/bqls/langserver/internal/bigquery/mock_bigquery"
	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/kitagry/bqls/langserver/internal/source"
	"github.com/sirupsen/logrus"
)

func TestProject_Complete_FiltersByPrefixAfterMultibyteCharacters(t *testing.T) {
	ctrl := gomock.NewController(t)
	bqClient := mock_bigquery.NewMockClient(ctrl)
	bqClient.EXPECT().GetDefaultProject().Return("project").AnyTimes()
	bqClient.EXPECT().ListTables(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	p := source.NewProjectWithBQClient("/", bqClient, logrus.New())
	if err := p.UpdateFile("a.sql", "DECLARE my_variable INT64;\n/* あいう */ SELECT my_v", 1); err != nil {
		t.Fatal(err)
	}

	// LSP clients send Character in UTF-16 code units: each of あいう is one
	// unit, so the end of "/* あいう */ SELECT my_v" is 21 (26 in bytes).
	items, err := p.Complete(t.Context(), "a.sql", lsp.Position{Line: 1, Character: 21})
	if err != nil {
		t.Fatal(err)
	}

	got := make([]string, len(items))
	for i, item := range items {
		got[i] = item.NewText
	}
	want := []string{"my_variable", "FROM "}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("completion items diff (-want +got):\n%s", diff)
	}
}
