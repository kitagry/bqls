package lint_test

import (
	"context"
	"fmt"
	"testing"

	bq "cloud.google.com/go/bigquery"
	"github.com/golang/mock/gomock"
	"github.com/kitagry/bqls/langserver/internal/bigquery/mock_bigquery"
	"github.com/kitagry/bqls/langserver/internal/lint"
	"github.com/kitagry/bqls/langserver/internal/source/file"
	"github.com/sirupsen/logrus"
)

var requiredTables = map[string]*bq.TableMetadata{
	"p.d.t": {Type: bq.RegularTable, Schema: bq.Schema{
		{Name: "id", Type: bq.IntegerFieldType, Required: true},
		{Name: "v", Type: bq.IntegerFieldType},
	}},
	"p.d.st": {Type: bq.RegularTable, Schema: bq.Schema{
		{Name: "n", Type: bq.NumericFieldType, Required: true},
		{Name: "s", Type: bq.RecordFieldType, Schema: bq.Schema{
			{Name: "f", Type: bq.IntegerFieldType, Required: true},
			{Name: "g", Type: bq.IntegerFieldType},
		}},
		{Name: "rs", Type: bq.RecordFieldType, Required: true, Schema: bq.Schema{
			{Name: "f", Type: bq.IntegerFieldType, Required: true},
		}},
	}},
	"p.d.view": {Type: bq.ViewTable, Schema: bq.Schema{
		{Name: "id", Type: bq.IntegerFieldType, Required: true},
	}},
	"p.d.src": {Type: bq.RegularTable, Schema: bq.Schema{
		{Name: "id", Type: bq.IntegerFieldType, Required: true},
		{Name: "v", Type: bq.IntegerFieldType},
		{Name: "w", Type: bq.IntegerFieldType},
	}},
}

func lookupRequiredTable(_ context.Context, path string) (*bq.TableMetadata, error) {
	m, ok := requiredTables[path]
	if !ok {
		return nil, fmt.Errorf("table %s not found", path)
	}
	return m, nil
}

func parseRequired(t *testing.T, src string) file.ParsedFile {
	t.Helper()
	ctrl := gomock.NewController(t)
	bqClient := mock_bigquery.NewMockClient(ctrl)
	bqClient.EXPECT().GetDefaultProject().Return("p").AnyTimes()
	bqClient.EXPECT().GetTableMetadata(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, project, dataset, table string) (*bq.TableMetadata, error) {
			return lookupRequiredTable(ctx, project+"."+dataset+"."+table)
		}).AnyTimes()
	parsed := file.NewAnalyzer(logrus.New(), bqClient).ParseFile("file:///a.sql", src)
	if len(parsed.Errors) > 0 {
		t.Fatalf("unexpected analysis errors: %v", parsed.Errors)
	}
	return parsed
}

var insertNullIntoRequiredEnabled = lint.Config{Rules: map[string]bool{"insert-null-into-required": true}}

func requiredNullMessage(column, table, because string) string {
	return fmt.Sprintf("`%s` of `%s` is REQUIRED, but %s; BigQuery fails the statement at run time once it writes a NULL there", column, table, because)
}

func nullableSource(column, table string) string {
	return fmt.Sprintf("this value comes from `%s` of `%s`, which is NULLABLE", column, table)
}

func optionalSide(join string) string {
	return fmt.Sprintf("this value comes from the side of a %s JOIN that is NULL when no row matches", join)
}

func TestInsertNullIntoRequired(t *testing.T) {
	type report struct {
		// text is the exact reported source text.
		text    string
		message string
	}
	tests := map[string]struct {
		src  string
		want []report
	}{
		"values null": {
			src:  "INSERT INTO p.d.t (id, v) VALUES (NULL, 1)",
			want: []report{{text: "NULL", message: requiredNullMessage("id", "p.d.t", "this value is NULL")}},
		},
		"values cast null": {
			src:  "INSERT INTO p.d.t (id) VALUES (CAST(NULL AS INT64))",
			want: []report{{text: "CAST(NULL AS INT64)", message: requiredNullMessage("id", "p.d.t", "this value is NULL")}},
		},
		"values without a column list": {
			src:  "INSERT INTO p.d.t VALUES (1, NULL), (NULL, NULL)",
			want: []report{{text: "NULL", message: requiredNullMessage("id", "p.d.t", "this value is NULL")}},
		},
		"values in another column order": {
			src:  "INSERT INTO p.d.t (v, id) VALUES (1, NULL)",
			want: []report{{text: "NULL", message: requiredNullMessage("id", "p.d.t", "this value is NULL")}},
		},

		"values into a nullable column": {src: "INSERT INTO p.d.t (id, v) VALUES (1, NULL)"},
		"values literal":                {src: "INSERT INTO p.d.t (id) VALUES (1)"},
		"values function call":          {src: "INSERT INTO p.d.t (id) VALUES (IFNULL(NULL, 1))"},
		// Only a regular table declares its modes.
		"view": {src: "INSERT INTO p.d.view (id) VALUES (NULL)"},

		"select null": {
			src:  "INSERT INTO p.d.t (id) SELECT NULL",
			want: []report{{text: "NULL", message: requiredNullMessage("id", "p.d.t", "this value is NULL")}},
		},
		"select nullable column": {
			src:  "INSERT INTO p.d.t (id) SELECT v FROM p.d.src",
			want: []report{{text: "v", message: requiredNullMessage("id", "p.d.t", nullableSource("v", "p.d.src"))}},
		},
		"select without a column list": {
			src:  "INSERT INTO p.d.t SELECT w, v FROM p.d.src",
			want: []report{{text: "w", message: requiredNullMessage("id", "p.d.t", nullableSource("w", "p.d.src"))}},
		},
		"select aliased column": {
			src:  "INSERT INTO p.d.t (id, v) SELECT s.v AS id, s.id AS v FROM p.d.src AS s",
			want: []report{{text: "s.v AS id", message: requiredNullMessage("id", "p.d.t", nullableSource("v", "p.d.src"))}},
		},
		"select through a cte": {
			src:  "INSERT INTO p.d.t (id) WITH c AS (SELECT v FROM p.d.src) SELECT v FROM c",
			want: []report{{text: "v", message: requiredNullMessage("id", "p.d.t", nullableSource("v", "p.d.src"))}},
		},
		"select through a derived table": {
			src:  "INSERT INTO p.d.t (id) SELECT k FROM (SELECT v AS k FROM p.d.src)",
			want: []report{{text: "k", message: requiredNullMessage("id", "p.d.t", nullableSource("v", "p.d.src"))}},
		},
		"select group by key": {
			src:  "INSERT INTO p.d.t (id) SELECT v FROM p.d.src GROUP BY v",
			want: []report{{text: "v", message: requiredNullMessage("id", "p.d.t", nullableSource("v", "p.d.src"))}},
		},
		"select filter on another column": {
			src:  "INSERT INTO p.d.t (id) SELECT v FROM p.d.src WHERE w > 0",
			want: []report{{text: "v", message: requiredNullMessage("id", "p.d.t", nullableSource("v", "p.d.src"))}},
		},
		"select multiline": {
			src:  "INSERT INTO p.d.t (id)\nSELECT\n  v\nFROM p.d.src",
			want: []report{{text: "v", message: requiredNullMessage("id", "p.d.t", nullableSource("v", "p.d.src"))}},
		},
		// Without a select list item to point at, the target is reported.
		"select star": {
			src:  "INSERT INTO p.d.t (id) SELECT * FROM (SELECT v FROM p.d.src)",
			want: []report{{text: "p.d.t", message: requiredNullMessage("id", "p.d.t", nullableSource("v", "p.d.src"))}},
		},

		"select required column":      {src: "INSERT INTO p.d.t (id) SELECT id FROM p.d.src"},
		"select into nullable column": {src: "INSERT INTO p.d.t (id, v) SELECT id, v FROM p.d.src"},
		"select ifnull":               {src: "INSERT INTO p.d.t (id) SELECT IFNULL(v, 0) FROM p.d.src"},
		"select coalesce":             {src: "INSERT INTO p.d.t (id) SELECT COALESCE(v, w, 0) FROM p.d.src"},
		"select aggregate":            {src: "INSERT INTO p.d.t (id) SELECT MAX(v) FROM p.d.src"},
		"select arithmetic":           {src: "INSERT INTO p.d.t (id) SELECT v + 1 FROM p.d.src"},
		"select is not null":          {src: "INSERT INTO p.d.t (id) SELECT v FROM p.d.src WHERE v IS NOT NULL"},
		"select comparison":           {src: "INSERT INTO p.d.t (id) SELECT v FROM p.d.src WHERE v > 0"},
		"select is not null outside a cte": {
			src: "INSERT INTO p.d.t (id) WITH c AS (SELECT v FROM p.d.src) SELECT v FROM c WHERE v IS NOT NULL",
		},
		"select inner join on the column": {
			src: "INSERT INTO p.d.t (id) SELECT a.v FROM p.d.src AS a JOIN p.d.src AS b ON a.v = b.id",
		},
		"select having on the column": {
			src: "INSERT INTO p.d.t (id) SELECT v FROM p.d.src GROUP BY v HAVING v IS NOT NULL",
		},
		"select qualify on the column": {
			src: "INSERT INTO p.d.t (id) SELECT v FROM p.d.src WHERE TRUE QUALIFY ROW_NUMBER() OVER (PARTITION BY w) = 1 AND v IS NOT NULL",
		},
		"select union": {src: "INSERT INTO p.d.t (id) SELECT v FROM p.d.src UNION ALL SELECT id FROM p.d.src"},
		"select from a view": {
			src: "INSERT INTO p.d.t (id) SELECT id FROM p.d.view",
		},
		"select unnest": {src: "INSERT INTO p.d.t (id) SELECT x FROM UNNEST([1, NULL]) AS x"},

		"left join optional side": {
			src:  "INSERT INTO p.d.t (id) SELECT b.id FROM p.d.src AS a LEFT JOIN p.d.src AS b ON a.v = b.id",
			want: []report{{text: "b.id", message: requiredNullMessage("id", "p.d.t", optionalSide("LEFT"))}},
		},
		"right join optional side": {
			src:  "INSERT INTO p.d.t (id) SELECT a.id FROM p.d.src AS a RIGHT JOIN p.d.src AS b ON a.v = b.id",
			want: []report{{text: "a.id", message: requiredNullMessage("id", "p.d.t", optionalSide("RIGHT"))}},
		},
		"full join both sides": {
			src: "INSERT INTO p.d.t (id, v) SELECT a.id, b.id FROM p.d.src AS a FULL JOIN p.d.src AS b ON a.v = b.id",
			want: []report{
				{text: "a.id", message: requiredNullMessage("id", "p.d.t", optionalSide("FULL"))},
			},
		},
		"left join filter inside the optional side": {
			src:  "INSERT INTO p.d.t (id) SELECT b.id FROM p.d.src AS a LEFT JOIN (SELECT id FROM p.d.src WHERE id > 0) AS b ON a.v = b.id",
			want: []report{{text: "b.id", message: requiredNullMessage("id", "p.d.t", optionalSide("LEFT"))}},
		},
		"left join filter on the preserved side": {
			src:  "INSERT INTO p.d.t (id) SELECT b.id FROM p.d.src AS a LEFT JOIN p.d.src AS b ON a.v = b.id WHERE a.w > 0",
			want: []report{{text: "b.id", message: requiredNullMessage("id", "p.d.t", optionalSide("LEFT"))}},
		},
		"left join through a cte": {
			src:  "INSERT INTO p.d.t (id) WITH c AS (SELECT b.id FROM p.d.src AS a LEFT JOIN p.d.src AS b ON a.v = b.id) SELECT id FROM c",
			want: []report{{text: "id", message: requiredNullMessage("id", "p.d.t", optionalSide("LEFT"))}},
		},
		// Another outer join's ON does not drop the NULL-extended rows either.
		"left join chained": {
			src:  "INSERT INTO p.d.t (id) SELECT b.id FROM p.d.src AS a LEFT JOIN p.d.src AS b ON a.v = b.id LEFT JOIN p.d.src AS c ON b.id = c.id",
			want: []report{{text: "b.id", message: requiredNullMessage("id", "p.d.t", optionalSide("LEFT"))}},
		},
		"left join nullable column": {
			src:  "INSERT INTO p.d.t (id) SELECT b.v FROM p.d.src AS a LEFT JOIN p.d.src AS b ON a.id = b.id",
			want: []report{{text: "b.v", message: requiredNullMessage("id", "p.d.t", optionalSide("LEFT"))}},
		},

		"left join preserved side": {
			src: "INSERT INTO p.d.t (id) SELECT a.id FROM p.d.src AS a LEFT JOIN p.d.src AS b ON a.v = b.id",
		},
		"left join is not null": {
			src: "INSERT INTO p.d.t (id) SELECT b.id FROM p.d.src AS a LEFT JOIN p.d.src AS b ON a.v = b.id WHERE b.id IS NOT NULL",
		},
		"left join filter on another optional column": {
			src: "INSERT INTO p.d.t (id) SELECT b.id FROM p.d.src AS a LEFT JOIN p.d.src AS b ON a.v = b.id WHERE b.w > 0",
		},
		"left join filter outside a cte": {
			src: "INSERT INTO p.d.t (id) WITH c AS (SELECT b.id FROM p.d.src AS a LEFT JOIN p.d.src AS b ON a.v = b.id) SELECT id FROM c WHERE id IS NOT NULL",
		},
		"left join filter on a computed column": {
			src: "INSERT INTO p.d.t (id) WITH c AS (SELECT b.id, b.w + 1 AS x FROM p.d.src AS a LEFT JOIN p.d.src AS b ON a.v = b.id) SELECT id FROM c WHERE x > 0",
		},
		"left join then inner join": {
			src: "INSERT INTO p.d.t (id) SELECT b.id FROM p.d.src AS a LEFT JOIN p.d.src AS b ON a.v = b.id JOIN p.d.src AS c ON b.id = c.id",
		},
		"left join coalesce": {
			src: "INSERT INTO p.d.t (id) SELECT COALESCE(b.id, 0) FROM p.d.src AS a LEFT JOIN p.d.src AS b ON a.v = b.id",
		},
		"left join using key": {
			src: "INSERT INTO p.d.t (id) SELECT id FROM p.d.src AS a LEFT JOIN p.d.src AS b USING (id)",
		},
		"update set null": {
			src:  "UPDATE p.d.t SET id = NULL WHERE TRUE",
			want: []report{{text: "NULL", message: requiredNullMessage("id", "p.d.t", "this value is NULL")}},
		},
		"update set nullable column of the target": {
			src:  "UPDATE p.d.t SET id = v WHERE TRUE",
			want: []report{{text: "v", message: requiredNullMessage("id", "p.d.t", nullableSource("v", "p.d.t"))}},
		},
		"update from a nullable column": {
			src:  "UPDATE p.d.t AS t SET id = s.v FROM p.d.src AS s WHERE t.id = s.id",
			want: []report{{text: "s.v", message: requiredNullMessage("id", "p.d.t", nullableSource("v", "p.d.src"))}},
		},

		"update set nullable column":   {src: "UPDATE p.d.t SET v = NULL WHERE TRUE"},
		"update where is not null":     {src: "UPDATE p.d.t SET id = v WHERE v IS NOT NULL"},
		"update from where comparison": {src: "UPDATE p.d.t AS t SET id = s.v FROM p.d.src AS s WHERE t.id = s.id AND s.v > 0"},
		"update ifnull":                {src: "UPDATE p.d.t SET id = IFNULL(v, 0) WHERE TRUE"},
		"merge insert null": {
			src:  "MERGE p.d.t AS t USING p.d.src AS s ON t.id = s.id WHEN NOT MATCHED THEN INSERT (id) VALUES (NULL)",
			want: []report{{text: "NULL", message: requiredNullMessage("id", "p.d.t", "this value is NULL")}},
		},
		"merge insert nullable column": {
			src:  "MERGE p.d.t AS t USING p.d.src AS s ON t.id = s.id WHEN NOT MATCHED THEN INSERT (v, id) VALUES (s.id, s.v)",
			want: []report{{text: "s.v", message: requiredNullMessage("id", "p.d.t", nullableSource("v", "p.d.src"))}},
		},
		"merge update nullable column": {
			src:  "MERGE p.d.t AS t USING p.d.src AS s ON t.id = s.id WHEN MATCHED THEN UPDATE SET id = s.v",
			want: []report{{text: "s.v", message: requiredNullMessage("id", "p.d.t", nullableSource("v", "p.d.src"))}},
		},
		"merge using a subquery": {
			src:  "MERGE p.d.t AS t USING (SELECT id AS k, v FROM p.d.src) AS s ON t.id = s.k WHEN NOT MATCHED THEN INSERT (id) VALUES (s.v)",
			want: []report{{text: "s.v", message: requiredNullMessage("id", "p.d.t", nullableSource("v", "p.d.src"))}},
		},
		"merge update by source": {
			src:  "MERGE p.d.t AS t USING p.d.src AS s ON t.id = s.id WHEN NOT MATCHED BY SOURCE THEN UPDATE SET id = NULL",
			want: []report{{text: "NULL", message: requiredNullMessage("id", "p.d.t", "this value is NULL")}},
		},

		"merge on the column": {
			src: "MERGE p.d.t AS t USING p.d.src AS s ON t.id = s.v WHEN MATCHED THEN UPDATE SET id = s.v",
		},
		"merge when condition on the column": {
			src: "MERGE p.d.t AS t USING p.d.src AS s ON t.id = s.id WHEN MATCHED AND s.v IS NOT NULL THEN UPDATE SET id = s.v",
		},
		"merge insert required column": {
			src: "MERGE p.d.t AS t USING p.d.src AS s ON t.id = s.w WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v)",
		},
		"cast nullable column": {
			src:  "INSERT INTO p.d.st (n) SELECT v FROM p.d.src",
			want: []report{{text: "v", message: requiredNullMessage("n", "p.d.st", nullableSource("v", "p.d.src"))}},
		},
		"cast is not null": {
			src: "INSERT INTO p.d.st (n) SELECT v FROM p.d.src WHERE v IS NOT NULL",
		},

		"struct field null": {
			src:  "INSERT INTO p.d.st (s) VALUES (STRUCT(NULL, 1))",
			want: []report{{text: "NULL", message: requiredNullMessage("s.f", "p.d.st", "this value is NULL")}},
		},
		"struct field from a nullable column": {
			src:  "INSERT INTO p.d.st (s) SELECT STRUCT(v AS f, 1 AS g) FROM p.d.src",
			want: []report{{text: "v", message: requiredNullMessage("s.f", "p.d.st", nullableSource("v", "p.d.src"))}},
		},
		"struct field through a cast": {
			src:  "INSERT INTO p.d.st (s) SELECT STRUCT(v, w) FROM p.d.src",
			want: []report{{text: "v", message: requiredNullMessage("s.f", "p.d.st", nullableSource("v", "p.d.src"))}},
		},
		"struct field through a cte": {
			src:  "INSERT INTO p.d.st (s) WITH c AS (SELECT STRUCT(v AS f, 1 AS g) AS x FROM p.d.src) SELECT x FROM c",
			want: []report{{text: "v", message: requiredNullMessage("s.f", "p.d.st", nullableSource("v", "p.d.src"))}},
		},
		"required struct null": {
			src:  "INSERT INTO p.d.st (rs) VALUES (NULL)",
			want: []report{{text: "NULL", message: requiredNullMessage("rs", "p.d.st", "this value is NULL")}},
		},
		"update struct field null": {
			src:  "UPDATE p.d.st SET s = STRUCT(NULL, 1) WHERE TRUE",
			want: []report{{text: "NULL", message: requiredNullMessage("s.f", "p.d.st", "this value is NULL")}},
		},

		"nullable struct null":       {src: "INSERT INTO p.d.st (s) VALUES (NULL)"},
		"struct nullable field null": {src: "INSERT INTO p.d.st (s) VALUES (STRUCT(1, NULL))"},
		"struct field is not null": {
			src: "INSERT INTO p.d.st (s) WITH c AS (SELECT STRUCT(v AS f, 1 AS g) AS x FROM p.d.src) SELECT x FROM c WHERE x.f IS NOT NULL",
		},
		"struct from a table column": {
			src: "INSERT INTO p.d.st (s) SELECT s FROM p.d.st",
		},
		"select pipe syntax": {
			src:  "INSERT INTO p.d.t (id) FROM p.d.src |> SELECT v",
			want: []report{{text: "p.d.t", message: requiredNullMessage("id", "p.d.t", nullableSource("v", "p.d.src"))}},
		},
		"select if":            {src: "INSERT INTO p.d.t (id) SELECT IF(w > 0, v, 0) FROM p.d.src"},
		"values default":       {src: "INSERT INTO p.d.t (id) VALUES (DEFAULT)"},
		"values variable":      {src: "DECLARE x INT64;\nINSERT INTO p.d.t (id) VALUES (x)"},
		"update nested target": {src: "UPDATE p.d.st SET s.f = NULL WHERE TRUE"},
		"full join using key": {
			src: "INSERT INTO p.d.t (id) SELECT id FROM p.d.src AS a FULL JOIN p.d.src AS b USING (id)",
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			parsed := parseRequired(t, tt.src)
			got := lint.Run(t.Context(), parsed, insertNullIntoRequiredEnabled, lint.Env{LookupTable: lookupRequiredTable}, lint.InsertNullIntoRequired)

			var gotReports []report
			for _, d := range got {
				if d.Rule != "insert-null-into-required" || d.Severity != lint.SeverityWarning || len(d.SuggestedFixes) != 0 {
					t.Errorf("unexpected diagnostic metadata: %+v", d)
				}
				gotReports = append(gotReports, report{text: textInRange(tt.src, d), message: d.Message})
			}
			if fmt.Sprint(gotReports) != fmt.Sprint(tt.want) {
				t.Errorf("reports = %+v, want %+v", gotReports, tt.want)
			}
		})
	}
}

func TestInsertNullIntoRequiredWithoutMetadata(t *testing.T) {
	parsed := parseRequired(t, "INSERT INTO p.d.t (id) VALUES (NULL)")
	if got := lint.Run(t.Context(), parsed, insertNullIntoRequiredEnabled, lint.Env{}, lint.InsertNullIntoRequired); len(got) != 0 {
		t.Errorf("got %+v, want no report without metadata", got)
	}
}

func TestInsertNullIntoRequiredDisabledByDefault(t *testing.T) {
	parsed := parseRequired(t, "INSERT INTO p.d.t (id) VALUES (NULL)")
	if got := lint.Run(t.Context(), parsed, lint.Config{}, lint.Env{LookupTable: lookupRequiredTable}, lint.InsertNullIntoRequired); len(got) != 0 {
		t.Errorf("got %+v, want no report by default", got)
	}
}
