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

func TestCatalog_CaseInsensitiveCacheKeys(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	meta := &bq.TableMetadata{Schema: bq.Schema{{Name: "name", Type: bq.StringFieldType}}}
	bqClient := mock_bigquery.NewMockClient(ctrl)
	bqClient.EXPECT().GetTableMetadata(gomock.Any(), "Project", "dataset", "a").Return(meta, nil).Times(1)
	bqClient.EXPECT().GetTableMetadata(gomock.Any(), "project", "dataset", "b").Return(meta, nil).Times(1)
	catalog := file.NewCatalog(bqClient)

	if err := catalog.EnsureTable([]string{"Project.dataset.a"}); err != nil {
		t.Fatal(err)
	}
	// SimpleCatalog matches names case-insensitively, so "project" must reuse
	// the "Project" sub-catalog instead of colliding with it.
	if err := catalog.EnsureTable([]string{"project.dataset.b"}); err != nil {
		t.Fatal(err)
	}
	// A different casing of a loaded table is the same catalog entry, so it is
	// neither fetched nor registered again.
	if err := catalog.EnsureTable([]string{"project.DATASET.A"}); err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.FindTableMetadata("project.DATASET.A"); err != nil {
		t.Errorf("metadata must be found regardless of casing: %v", err)
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
		},
		"nested DML targets are array columns": {
			src:    "UPDATE p.d.t SET (DELETE FROM arr WHERE true), (INSERT d.arr VALUES (1)) WHERE true",
			tables: []string{"t"},
			// d.arr is not a column of the outer table.
			tolerate: []string{"Unrecognized name: d"},
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

func TestCatalog_TwoPartTableNames(t *testing.T) {
	schema := bq.Schema{{Name: "x", Type: bq.IntegerFieldType}}
	parse := func(t *testing.T, src string) file.ParsedFile {
		t.Helper()
		ctrl := gomock.NewController(t)
		bqClient := mock_bigquery.NewMockClient(ctrl)
		bqClient.EXPECT().GetDefaultProject().Return("p").AnyTimes()
		bqClient.EXPECT().GetTableMetadata(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			DoAndReturn(func(_ any, project, dataset, table string) (*bq.TableMetadata, error) {
				switch project + "." + dataset + "." + table {
				case "p.d.t", "p.d.t_*", "p.other.t", "other.x.t", "p.D.T", "p.D.u":
					return &bq.TableMetadata{Schema: schema}, nil
				}
				return nil, fmt.Errorf("table %s.%s.%s not found", project, dataset, table)
			}).AnyTimes()
		return file.NewAnalyzer(logrus.New(), bqClient).ParseFile("file:///a.sql", src)
	}

	tests := map[string]string{
		"unquoted":                           "SELECT x FROM d.t",
		"backticked":                         "SELECT x FROM `d.t`",
		"wildcard":                           "SELECT x FROM `d.t_*` WHERE _TABLE_SUFFIX = '1'",
		"both 3-part and 2-part":             "SELECT a.x FROM p.d.t a JOIN d.t b ON a.x = b.x",
		"2-part before 3-part":               "SELECT a.x FROM d.t a JOIN p.d.t b ON a.x = b.x",
		"dataset named like another project": "SELECT a.x FROM other.x.t a JOIN other.t b ON a.x = b.x",
		"dataset named like a project, 2-part first": "SELECT a.x FROM other.t a JOIN other.x.t b ON a.x = b.x",
		"repeated unquoted":                          "SELECT a.x FROM d.t a JOIN d.t b ON a.x = b.x",
		"repeated backticked":                        "SELECT a.x FROM `d.t` a JOIN `d.t` b ON a.x = b.x",
		// SimpleCatalog matches names case-insensitively, so these must not
		// register a second table or sub-catalog for the other casing (#435).
		"mixed case 2-part":                      "SELECT a.x FROM d.t a JOIN D.T b ON a.x = b.x",
		"mixed case 3-part":                      "SELECT a.x FROM p.d.t a JOIN p.D.T b ON a.x = b.x",
		"mixed case dataset, other table":        "SELECT a.x FROM p.d.t a JOIN p.D.u b ON a.x = b.x",
		"mixed case 2-part dataset, other table": "SELECT a.x FROM d.t a JOIN D.u b ON a.x = b.x",
		"multiple statements":                    "SELECT x FROM d.t;\nSELECT x FROM d.t;",
		"DML target also read":                   "INSERT d.t (x) SELECT x FROM d.t",
	}
	for name, src := range tests {
		t.Run(name, func(t *testing.T) {
			parsed := parse(t, src)
			if len(parsed.Errors) > 0 {
				t.Errorf("unexpected errors: %v", parsed.Errors)
			}
		})
	}
}

func TestCatalog_PartiallyBacktickedPaths(t *testing.T) {
	parse := func(t *testing.T, src string) file.ParsedFile {
		t.Helper()
		ctrl := gomock.NewController(t)
		bqClient := mock_bigquery.NewMockClient(ctrl)
		bqClient.EXPECT().GetDefaultProject().Return("p").AnyTimes()
		bqClient.EXPECT().GetTableMetadata(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			DoAndReturn(func(_ any, project, dataset, table string) (*bq.TableMetadata, error) {
				switch project + "." + dataset + "." + table {
				case "p.d.t", "P.d.t":
					return &bq.TableMetadata{Schema: bq.Schema{{Name: "x", Type: bq.IntegerFieldType}}}, nil
				}
				return nil, fmt.Errorf("table %s.%s.%s not found", project, dataset, table)
			}).AnyTimes()
		return file.NewAnalyzer(logrus.New(), bqClient).ParseFile("file:///a.sql", src)
	}

	tests := map[string]string{
		"project and dataset backticked":    "SELECT x FROM `p.d`.t",
		"dataset and table backticked":      "SELECT x FROM p.`d.t`",
		"each part backticked":              "SELECT x FROM `p`.`d`.`t`",
		"2-part table backticked":           "SELECT x FROM d.`t`",
		"2-part dataset backticked":         "SELECT x FROM `d`.t",
		"every form in one query":           "SELECT a.x FROM `p.d`.t a JOIN p.`d.t` b ON a.x = b.x JOIN `p.d.t` c ON a.x = c.x JOIN p.d.t e ON a.x = e.x",
		"mixed case partially backticked":   "SELECT a.x FROM `p.d`.t a JOIN `P.d`.T b ON a.x = b.x",
		"partially backticked DML target":   "INSERT `p.d`.t (x) SELECT x FROM p.`d.t`",
		"partially backticked after 2-part": "SELECT a.x FROM d.t a JOIN `p.d`.t b ON a.x = b.x",
	}
	for name, src := range tests {
		t.Run(name, func(t *testing.T) {
			parsed := parse(t, src)
			if len(parsed.Errors) > 0 {
				t.Errorf("unexpected errors: %v", parsed.Errors)
			}
		})
	}
}

func TestCatalog_InformationSchema(t *testing.T) {
	jobs := bq.Schema{{Name: "job_id", Type: bq.StringFieldType}}
	tables := bq.Schema{{Name: "table_name", Type: bq.StringFieldType}}
	schemata := bq.Schema{{Name: "schema_name", Type: bq.StringFieldType}}
	// Keyed by the dry-run query, so that a wrong split of the path fails the test.
	schemas := map[string]bq.Schema{
		"SELECT * FROM `p`.`region-us`.`INFORMATION_SCHEMA`.`JOBS`":     jobs,
		"SELECT * FROM `p`.`region-us`.`information_schema`.`jobs`":     jobs,
		"SELECT * FROM `other`.`region-us`.`INFORMATION_SCHEMA`.`JOBS`": jobs,
		"SELECT * FROM `p`.`d`.`INFORMATION_SCHEMA`.`TABLES`":           tables,
		"SELECT * FROM `other`.`d`.`INFORMATION_SCHEMA`.`TABLES`":       tables,
		"SELECT * FROM `p`.`INFORMATION_SCHEMA`.`SCHEMATA`":             schemata,
	}
	parse := func(t *testing.T, src string) file.ParsedFile {
		t.Helper()
		ctrl := gomock.NewController(t)
		// GetTableMetadata is not expected, because INFORMATION_SCHEMA views have no table metadata.
		bqClient := mock_bigquery.NewMockClient(ctrl)
		bqClient.EXPECT().GetDefaultProject().Return("p").AnyTimes()
		bqClient.EXPECT().DryRunSchema(gomock.Any(), gomock.Any()).
			DoAndReturn(func(_ any, query string) (bq.Schema, error) {
				if s, ok := schemas[query]; ok {
					return s, nil
				}
				return nil, fmt.Errorf("unexpected query: %s", query)
			}).AnyTimes()
		return file.NewAnalyzer(logrus.New(), bqClient).ParseFile("file:///a.sql", src)
	}

	tests := map[string]string{
		"region qualified":                 "SELECT job_id FROM `region-us`.INFORMATION_SCHEMA.JOBS",
		"region qualified, backticked":     "SELECT job_id FROM `region-us.INFORMATION_SCHEMA.JOBS`",
		"project and region qualified":     "SELECT job_id FROM other.`region-us`.INFORMATION_SCHEMA.JOBS",
		"project and region, backticked":   "SELECT job_id FROM `other.region-us.INFORMATION_SCHEMA.JOBS`",
		"project and region, partially":    "SELECT job_id FROM `other.region-us`.INFORMATION_SCHEMA.JOBS",
		"lowercase":                        "SELECT job_id FROM `region-us`.information_schema.jobs",
		"dataset qualified":                "SELECT table_name FROM d.INFORMATION_SCHEMA.TABLES",
		"project and dataset qualified":    "SELECT table_name FROM other.d.INFORMATION_SCHEMA.TABLES",
		"default project, fully qualified": "SELECT table_name FROM p.d.INFORMATION_SCHEMA.TABLES",
		"unqualified":                      "SELECT schema_name FROM INFORMATION_SCHEMA.SCHEMATA",
		"same view by two names":           "SELECT a.table_name FROM d.INFORMATION_SCHEMA.TABLES a JOIN p.d.INFORMATION_SCHEMA.TABLES b USING (table_name)",
	}
	for name, src := range tests {
		t.Run(name, func(t *testing.T) {
			parsed := parse(t, src)
			if len(parsed.Errors) > 0 {
				t.Errorf("unexpected errors: %v", parsed.Errors)
			}
		})
	}

	t.Run("unknown column", func(t *testing.T) {
		parsed := parse(t, "SELECT nope FROM `region-us`.INFORMATION_SCHEMA.JOBS")
		var msgs []string
		for _, e := range parsed.Errors {
			msgs = append(msgs, e.Msg)
		}
		if diff := cmp.Diff([]string{"Unrecognized name: nope"}, msgs); diff != "" {
			t.Errorf("errors diff (-want +got):\n%s", diff)
		}
	})

	t.Run("metadata is found by the table name", func(t *testing.T) {
		parsed := parse(t, "SELECT job_id FROM `region-us`.INFORMATION_SCHEMA.JOBS")
		meta, ok := parsed.TableMetadata("p.region-us.INFORMATION_SCHEMA.JOBS")
		if !ok {
			t.Fatal("metadata not found")
		}
		if diff := cmp.Diff(jobs, meta.Schema); diff != "" {
			t.Errorf("schema diff (-want +got):\n%s", diff)
		}
	})
}

func TestAnalyzer_GetTableMetadataFromPath_InformationSchema(t *testing.T) {
	ctrl := gomock.NewController(t)
	bqClient := mock_bigquery.NewMockClient(ctrl)
	bqClient.EXPECT().GetDefaultProject().Return("p").AnyTimes()
	schema := bq.Schema{{Name: "job_id", Type: bq.StringFieldType}}
	bqClient.EXPECT().DryRunSchema(gomock.Any(), "SELECT * FROM `p`.`region-us`.`INFORMATION_SCHEMA`.`JOBS`").Return(schema, nil).Times(2)

	analyzer := file.NewAnalyzer(logrus.New(), bqClient)
	for _, path := range []string{"region-us.INFORMATION_SCHEMA.JOBS", "p.region-us.INFORMATION_SCHEMA.JOBS"} {
		meta, err := analyzer.GetTableMetadataFromPath(t.Context(), path)
		if err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		if diff := cmp.Diff(schema, meta.Schema); diff != "" {
			t.Errorf("%s: schema diff (-want +got):\n%s", path, diff)
		}
	}
}
