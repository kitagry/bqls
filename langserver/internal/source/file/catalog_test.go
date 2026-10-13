package file_test

import (
	"errors"
	"fmt"
	"slices"
	"testing"

	bq "cloud.google.com/go/bigquery"
	googlesql "github.com/goccy/go-googlesql"
	"github.com/golang/mock/gomock"
	"github.com/google/go-cmp/cmp"
	"github.com/kitagry/bqls/langserver/internal/bigquery"
	"github.com/kitagry/bqls/langserver/internal/bigquery/mock_bigquery"
	"github.com/kitagry/bqls/langserver/internal/source/file"
	"github.com/sirupsen/logrus"
)

func TestCatalog_AddTable(t *testing.T) {
	type column struct {
		Name     string
		Pseudo   bool
		Writable bool
	}
	dummyErr := fmt.Errorf("dummy error")
	tests := map[string]struct {
		path               []string
		createMockBigQuery func(ctrl *gomock.Controller) bigquery.Client

		expectError     error
		expectTableName string
		expectColumns   []column
	}{
		"Normal case": {
			path: []string{"project.dataset.table"},
			createMockBigQuery: func(ctrl *gomock.Controller) bigquery.Client {
				bqClient := mock_bigquery.NewMockClient(ctrl)
				bqClient.EXPECT().GetTableMetadata(gomock.Any(), "project", "dataset", "table").Return(&bq.TableMetadata{
					Schema: bq.Schema{
						{
							Name: "name",
							Type: bq.StringFieldType,
						},
					},
				}, nil)
				return bqClient
			},

			expectTableName: "project.dataset.table",
			expectColumns:   []column{{Name: "name", Writable: true}},
		},
		"Expect error": {
			path: []string{"project.dataset.table"},
			createMockBigQuery: func(ctrl *gomock.Controller) bigquery.Client {
				bqClient := mock_bigquery.NewMockClient(ctrl)
				bqClient.EXPECT().GetTableMetadata(gomock.Any(), "project", "dataset", "table").Return(nil, dummyErr)
				return bqClient
			},

			expectError: dummyErr,
		},
		"Wildcard table": {
			path: []string{"project.dataset.table*"},
			createMockBigQuery: func(ctrl *gomock.Controller) bigquery.Client {
				bqClient := mock_bigquery.NewMockClient(ctrl)
				bqClient.EXPECT().GetTableMetadata(gomock.Any(), "project", "dataset", "table*").Return(&bq.TableMetadata{
					Schema: bq.Schema{
						{
							Name: "name",
							Type: bq.StringFieldType,
						},
					},
				}, nil)
				return bqClient
			},
			expectTableName: "project.dataset.table*",
			expectColumns: []column{
				{Name: "name", Writable: true},
				{Name: "_TABLE_SUFFIX", Pseudo: true},
			},
		},
		"partitiontime table": {
			path: []string{"project.dataset.table"},
			createMockBigQuery: func(ctrl *gomock.Controller) bigquery.Client {
				bqClient := mock_bigquery.NewMockClient(ctrl)
				bqClient.EXPECT().GetTableMetadata(gomock.Any(), "project", "dataset", "table").Return(&bq.TableMetadata{
					Schema: bq.Schema{
						{
							Name: "name",
							Type: bq.StringFieldType,
						},
					},
					TimePartitioning: &bq.TimePartitioning{
						Type: bq.DayPartitioningType,
					},
				}, nil)
				return bqClient
			},
			expectTableName: "project.dataset.table",
			expectColumns: []column{
				{Name: "name", Writable: true},
				{Name: "_PARTITIONTIME", Pseudo: true, Writable: true},
				{Name: "_PARTITIONDATE", Pseudo: true},
			},
		},
	}

	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			mockCtrl := gomock.NewController(t)
			defer mockCtrl.Finish()

			bqClient := tt.createMockBigQuery(mockCtrl)
			catalog := file.NewCatalog(bqClient)

			err := catalog.EnsureTable(tt.path)
			if !errors.Is(err, tt.expectError) {
				t.Fatalf("error: got %v, want %v", err, tt.expectError)
			}
			if err != nil {
				return
			}

			tables, err := catalog.CatalogNode().Tables()
			if err != nil {
				t.Fatal(err)
			}
			var table googlesql.TableNode
			for _, tbl := range tables {
				if name, _ := tbl.Name(); name == tt.expectTableName {
					table = tbl
				}
			}
			if table == nil {
				t.Fatalf("table %s is not registered in the catalog", tt.expectTableName)
			}

			n, err := table.NumColumns()
			if err != nil {
				t.Fatal(err)
			}
			got := make([]column, 0, n)
			for i := range n {
				col, err := table.GetColumn(i)
				if err != nil {
					t.Fatal(err)
				}
				name, err := col.Name()
				if err != nil {
					t.Fatal(err)
				}
				pseudo, err := col.IsPseudoColumn()
				if err != nil {
					t.Fatal(err)
				}
				writable, err := col.IsWritableColumn()
				if err != nil {
					t.Fatal(err)
				}
				got = append(got, column{Name: name, Pseudo: pseudo, Writable: writable})
			}
			if diff := cmp.Diff(tt.expectColumns, got); diff != "" {
				t.Errorf("columns diff (-want +got):\n%s", diff)
			}
		})
	}
}

func TestCatalog_shouldNotGetSchemaTwice(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	bqClient := mock_bigquery.NewMockClient(ctrl)
	bqClient.EXPECT().GetTableMetadata(gomock.Any(), "project", "dataset", "table").Return(&bq.TableMetadata{
		Schema: bq.Schema{
			{
				Name: "name",
				Type: bq.StringFieldType,
			},
		},
	}, nil).Times(1)
	catalog := file.NewCatalog(bqClient)

	err := catalog.EnsureTable([]string{"project.dataset.table"})
	if err != nil {
		t.Fatal(err)
	}

	err = catalog.EnsureTable([]string{"project.dataset.table"})
	if err != nil {
		t.Fatal(err)
	}
}

func TestCatalog_PartitionPseudoColumns(t *testing.T) {
	tables := map[string]*bq.TableMetadata{
		"daily": {
			TimePartitioning: &bq.TimePartitioning{Type: bq.DayPartitioningType},
			Schema:           bq.Schema{{Name: "x", Type: bq.IntegerFieldType}},
		},
		"untyped": {
			TimePartitioning: &bq.TimePartitioning{},
			Schema:           bq.Schema{{Name: "x", Type: bq.IntegerFieldType}},
		},
		"hourly": {
			TimePartitioning: &bq.TimePartitioning{Type: bq.HourPartitioningType},
			Schema:           bq.Schema{{Name: "x", Type: bq.IntegerFieldType}},
		},
		"by_col": {
			TimePartitioning: &bq.TimePartitioning{Type: bq.DayPartitioningType, Field: "dt"},
			Schema:           bq.Schema{{Name: "x", Type: bq.IntegerFieldType}, {Name: "dt", Type: bq.DateFieldType}},
		},
		"plain": {
			Schema: bq.Schema{{Name: "x", Type: bq.IntegerFieldType}},
		},
	}
	parse := func(t *testing.T, src string) file.ParsedFile {
		t.Helper()
		ctrl := gomock.NewController(t)
		bqClient := mock_bigquery.NewMockClient(ctrl)
		bqClient.EXPECT().GetDefaultProject().Return("p").AnyTimes()
		bqClient.EXPECT().GetTableMetadata(gomock.Any(), "p", "d", gomock.Any()).
			DoAndReturn(func(_ any, _, _, table string) (*bq.TableMetadata, error) {
				if m, ok := tables[table]; ok {
					return m, nil
				}
				return nil, fmt.Errorf("table %s not found", table)
			}).AnyTimes()
		return file.NewAnalyzer(logrus.New(), bqClient).ParseFile("file:///a.sql", src)
	}

	t.Run("referenced by name on daily ingestion-time tables", func(t *testing.T) {
		for _, table := range []string{"daily", "untyped"} {
			parsed := parse(t, "SELECT _PARTITIONDATE FROM p.d."+table+" WHERE _PARTITIONDATE = '2020-01-01'")
			if len(parsed.Errors) > 0 {
				t.Errorf("%s: unexpected errors: %v", table, parsed.Errors)
			}
		}
	})

	t.Run("absent elsewhere", func(t *testing.T) {
		for _, table := range []string{"hourly", "by_col", "plain"} {
			parsed := parse(t, "SELECT _PARTITIONDATE FROM p.d."+table)
			if len(parsed.Errors) == 0 {
				t.Errorf("%s: _PARTITIONDATE resolved, want an error", table)
			}
		}
	})

	t.Run("excluded from SELECT *", func(t *testing.T) {
		parsed := parse(t, "SELECT * FROM p.d.daily")
		if len(parsed.Errors) > 0 {
			t.Fatalf("unexpected errors: %v", parsed.Errors)
		}
		stmt, err := parsed.RNode[0].ResolvedStatement()
		if err != nil {
			t.Fatal(err)
		}
		cols, err := stmt.(*googlesql.ResolvedQueryStmt).OutputColumnList()
		if err != nil {
			t.Fatal(err)
		}
		var got []string
		for _, c := range cols {
			name, err := c.Name()
			if err != nil {
				t.Fatal(err)
			}
			got = append(got, name)
		}
		if diff := cmp.Diff([]string{"x"}, got); diff != "" {
			t.Errorf("SELECT * columns diff (-want +got):\n%s", diff)
		}
	})

	t.Run("_PARTITIONTIME referenced by name on ingestion-time tables", func(t *testing.T) {
		for _, table := range []string{"daily", "untyped", "hourly"} {
			parsed := parse(t, "SELECT _PARTITIONTIME FROM p.d."+table)
			if len(parsed.Errors) > 0 {
				t.Errorf("%s: unexpected errors: %v", table, parsed.Errors)
			}
		}
	})

	t.Run("_PARTITIONTIME absent elsewhere", func(t *testing.T) {
		for _, table := range []string{"by_col", "plain"} {
			parsed := parse(t, "SELECT _PARTITIONTIME FROM p.d."+table)
			if len(parsed.Errors) == 0 {
				t.Errorf("%s: _PARTITIONTIME resolved, want an error", table)
			}
		}
	})

	t.Run("_PARTITIONTIME writable", func(t *testing.T) {
		// The tables are also read so that the DML target is preloaded into the catalog.
		for _, src := range []string{
			"INSERT INTO p.d.daily (_PARTITIONTIME, x) SELECT CURRENT_TIMESTAMP(), x FROM p.d.daily",
			"UPDATE p.d.daily SET _PARTITIONTIME = CURRENT_TIMESTAMP() WHERE x IN (SELECT x FROM p.d.daily)",
		} {
			parsed := parse(t, src)
			if len(parsed.Errors) > 0 {
				t.Errorf("%s: unexpected errors: %v", src, parsed.Errors)
			}
		}
	})

	t.Run("not writable", func(t *testing.T) {
		parsed := parse(t, "INSERT p.d.daily (x, _PARTITIONDATE) VALUES (1, '2020-01-01')")
		var msgs []string
		for _, e := range parsed.Errors {
			msgs = append(msgs, e.Msg)
		}
		if diff := cmp.Diff([]string{"Cannot INSERT value on non-writable column: _PARTITIONDATE"}, msgs); diff != "" {
			t.Errorf("errors diff (-want +got):\n%s", diff)
		}
	})
}

func TestCatalog_TableSuffixPseudoColumn(t *testing.T) {
	parse := func(t *testing.T, src string) file.ParsedFile {
		t.Helper()
		ctrl := gomock.NewController(t)
		bqClient := mock_bigquery.NewMockClient(ctrl)
		bqClient.EXPECT().GetDefaultProject().Return("p").AnyTimes()
		bqClient.EXPECT().GetTableMetadata(gomock.Any(), "p", "d", "t_*").Return(&bq.TableMetadata{
			Schema: bq.Schema{{Name: "x", Type: bq.IntegerFieldType}},
		}, nil).AnyTimes()
		return file.NewAnalyzer(logrus.New(), bqClient).ParseFile("file:///a.sql", src)
	}

	t.Run("referenced by name", func(t *testing.T) {
		parsed := parse(t, "SELECT _TABLE_SUFFIX FROM `p.d.t_*` WHERE _TABLE_SUFFIX = '20200101'")
		if len(parsed.Errors) > 0 {
			t.Errorf("unexpected errors: %v", parsed.Errors)
		}
	})

	t.Run("excluded from SELECT *", func(t *testing.T) {
		parsed := parse(t, "SELECT * FROM `p.d.t_*`")
		if len(parsed.Errors) > 0 {
			t.Fatalf("unexpected errors: %v", parsed.Errors)
		}
		stmt, err := parsed.RNode[0].ResolvedStatement()
		if err != nil {
			t.Fatal(err)
		}
		cols, err := stmt.(*googlesql.ResolvedQueryStmt).OutputColumnList()
		if err != nil {
			t.Fatal(err)
		}
		var got []string
		for _, c := range cols {
			name, err := c.Name()
			if err != nil {
				t.Fatal(err)
			}
			got = append(got, name)
		}
		if diff := cmp.Diff([]string{"x"}, got); diff != "" {
			t.Errorf("SELECT * columns diff (-want +got):\n%s", diff)
		}
	})
}

func TestCatalog_PreloadDMLTargets(t *testing.T) {
	tests := map[string]struct {
		src string
		// tables in p.d that must be fetched; any other GetTableMetadata call fails the test.
		tables []string
		// exact error messages to tolerate because they come from bugs outside
		// DML target preloading.
		tolerate []string
	}{
		"INSERT INTO ... SELECT": {
			src:    "INSERT INTO p.d.t (x) SELECT 1",
			tables: []string{"t"},
		},
		"INSERT without INTO": {
			src:    "INSERT p.d.t (x) VALUES (1)",
			tables: []string{"t"},
		},
		"INSERT with backticked path": {
			src:    "INSERT `p.d.t` (x) VALUES (1)",
			tables: []string{"t"},
		},
		"INSERT with dataset-qualified path": {
			src:    "INSERT d.t (x) VALUES (1)",
			tables: []string{"t"},
			// The table is fetched with the default project, but the analyzer
			// cannot resolve 2-part names yet, even in FROM (#405).
			tolerate: []string{"Table not found: d.t"},
		},
		"UPDATE with alias": {
			src:    "UPDATE p.d.t AS u SET x = 1 WHERE true",
			tables: []string{"t"},
		},
		"UPDATE with FROM": {
			src:    "UPDATE p.d.t SET x = s.x FROM p.d.s s WHERE true",
			tables: []string{"t", "s"},
		},
		"DELETE FROM": {
			src:    "DELETE FROM p.d.t WHERE true",
			tables: []string{"t"},
		},
		"DELETE without FROM": {
			src:    "DELETE p.d.t WHERE true",
			tables: []string{"t"},
		},
		"MERGE USING table": {
			src:    "MERGE p.d.t T USING p.d.s S ON T.x = S.x WHEN MATCHED THEN UPDATE SET x = S.x",
			tables: []string{"t", "s"},
		},
		"MERGE USING subquery": {
			src:    "MERGE p.d.t T USING (SELECT 1 AS x) S ON T.x = S.x WHEN NOT MATCHED THEN INSERT (x) VALUES (S.x)",
			tables: []string{"t"},
		},
		"DML inside BEGIN END": {
			src:    "BEGIN\n  DELETE FROM p.d.t WHERE true;\nEND;",
			tables: []string{"t"},
			// BEGIN...END itself is reported as unsupported (#403).
			tolerate: []string{"Statement not supported: BeginEndBlock"},
		},
		"nested DML targets are array columns": {
			src:    "UPDATE p.d.t SET (DELETE FROM arr WHERE true), (INSERT d.arr VALUES (1)) WHERE true",
			tables: []string{"t"},
			// Nested DML is analyzed standalone and fails to resolve the array
			// column (#402).
			tolerate: []string{"Table not found: arr", "Table not found: d.arr", "Unrecognized name: d"},
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			bqClient := mock_bigquery.NewMockClient(ctrl)
			bqClient.EXPECT().GetDefaultProject().Return("p").AnyTimes()
			for _, table := range tt.tables {
				bqClient.EXPECT().GetTableMetadata(gomock.Any(), "p", "d", table).
					Return(&bq.TableMetadata{Schema: bq.Schema{
						{Name: "x", Type: bq.IntegerFieldType},
						{Name: "arr", Type: bq.IntegerFieldType, Repeated: true},
					}}, nil).MinTimes(1)
			}

			parsed := file.NewAnalyzer(logrus.New(), bqClient).ParseFile("file:///a.sql", tt.src)
			for _, e := range parsed.Errors {
				if !slices.Contains(tt.tolerate, e.Msg) {
					t.Errorf("unexpected error: %s", e.Msg)
				}
			}
		})
	}
}
