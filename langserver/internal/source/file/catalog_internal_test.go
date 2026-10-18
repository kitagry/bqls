package file

import (
	"testing"

	bq "cloud.google.com/go/bigquery"
	googlesql "github.com/goccy/go-googlesql"
	"github.com/golang/mock/gomock"
	"github.com/kitagry/bqls/langserver/internal/bigquery/mock_bigquery"
)

func TestCatalog_EnsureTableReportsAliasErrors(t *testing.T) {
	ctrl := gomock.NewController(t)
	bqClient := mock_bigquery.NewMockClient(ctrl)
	bqClient.EXPECT().GetDefaultProject().Return("p").AnyTimes()
	bqClient.EXPECT().GetTableMetadata(gomock.Any(), "p", "d", "t").
		Return(&bq.TableMetadata{Schema: bq.Schema{{Name: "x", Type: bq.IntegerFieldType}}}, nil).AnyTimes()
	c := NewCatalog(bqClient)

	// Occupies the name of the backticked alias so that registering it traps.
	conflict, err := googlesql.NewSimpleTable("D.T", 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.catalog.AddTable2("D.T", conflict); err != nil {
		t.Fatal(err)
	}

	if err := c.EnsureTable([]string{"d", "t"}); err == nil {
		t.Fatal("EnsureTable() = nil, want an error for the failed alias registration")
	}
	// The failed alias must not be remembered as registered, or a later call
	// would silently report success.
	if err := c.EnsureTable([]string{"d", "t"}); err == nil {
		t.Error("second EnsureTable() = nil, want the alias registration to be retried and fail again")
	}
}
