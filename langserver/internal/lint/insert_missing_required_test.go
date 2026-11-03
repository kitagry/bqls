package lint_test

import (
	"fmt"
	"testing"

	"github.com/kitagry/bqls/langserver/internal/lint"
)

func missingRequiredMessage(column, table string) string {
	return fmt.Sprintf("the column list leaves out `%s`, which is REQUIRED in `%s` and has no default value; BigQuery fails the statement at run time once it writes a row", column, table)
}

func TestInsertMissingRequired(t *testing.T) {
	type report struct {
		// text is the exact reported source text.
		text    string
		message string
	}
	tests := map[string]struct {
		src  string
		want []report
	}{
		"values": {
			src:  "INSERT INTO p.d.t (v) VALUES (1)",
			want: []report{{text: "(v)", message: missingRequiredMessage("id", "p.d.t")}},
		},
		"select": {
			src:  "INSERT INTO p.d.t (v) SELECT v FROM p.d.src",
			want: []report{{text: "(v)", message: missingRequiredMessage("id", "p.d.t")}},
		},
		"several missing columns": {
			src: "INSERT INTO p.d.st (s) VALUES (NULL)",
			want: []report{
				{text: "(s)", message: missingRequiredMessage("n", "p.d.st")},
				{text: "(s)", message: missingRequiredMessage("rs", "p.d.st")},
			},
		},
		"merge insert": {
			src:  "MERGE p.d.t AS t USING p.d.src AS s ON t.id = s.id WHEN NOT MATCHED THEN INSERT (v) VALUES (s.v)",
			want: []report{{text: "(v)", message: missingRequiredMessage("id", "p.d.t")}},
		},
		"inside a script block": {
			src:  "BEGIN\n  INSERT INTO p.d.t (v) VALUES (1);\nEND",
			want: []report{{text: "(v)", message: missingRequiredMessage("id", "p.d.t")}},
		},
		"merge with several insert clauses": {
			src:  "MERGE p.d.t AS t USING p.d.src AS s ON t.id = s.id WHEN NOT MATCHED AND s.w > 0 THEN INSERT (id) VALUES (s.id) WHEN NOT MATCHED THEN INSERT (v) VALUES (s.v)",
			want: []report{{text: "(v)", message: missingRequiredMessage("id", "p.d.t")}},
		},

		"all required columns listed":    {src: "INSERT INTO p.d.t (id) VALUES (1)"},
		"columns in another order":       {src: "INSERT INTO p.d.t (v, id) VALUES (1, 2)"},
		"quoted column name":             {src: "INSERT INTO p.d.t (`id`) VALUES (1)"},
		"column name in another case":    {src: "INSERT INTO p.d.t (ID) VALUES (1)"},
		"required column with a default": {src: "INSERT INTO p.d.def (id) VALUES (1)"},
		"values without a column list":   {src: "INSERT INTO p.d.t VALUES (1, 2)"},
		"select without a column list":   {src: "INSERT INTO p.d.t SELECT id, v FROM p.d.src"},
		// Only a regular table declares its modes.
		"view":                        {src: "INSERT INTO p.d.view (id) VALUES (1)"},
		"merge insert all columns":    {src: "MERGE p.d.t AS t USING p.d.src AS s ON t.id = s.id WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v)"},
		"merge insert row":            {src: "MERGE p.d.t AS t USING (SELECT id, v FROM p.d.src) AS s ON t.id = s.id WHEN NOT MATCHED THEN INSERT ROW"},
		"merge insert without a list": {src: "MERGE p.d.t AS t USING p.d.src AS s ON t.id = s.id WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.v)"},
		"merge update":                {src: "MERGE p.d.t AS t USING p.d.src AS s ON t.id = s.id WHEN MATCHED THEN UPDATE SET v = s.v"},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			parsed := parseRequired(t, tt.src)
			got := lint.Run(t.Context(), parsed, lint.Config{}, lint.Env{LookupTable: lookupRequiredTable}, lint.InsertMissingRequired)

			var gotReports []report
			for _, d := range got {
				if d.Rule != "insert-missing-required" || d.Severity != lint.SeverityWarning || len(d.SuggestedFixes) != 0 {
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

func TestInsertMissingRequiredWithoutMetadata(t *testing.T) {
	parsed := parseRequired(t, "INSERT INTO p.d.t (v) VALUES (1)")
	if got := lint.Run(t.Context(), parsed, lint.Config{}, lint.Env{}, lint.InsertMissingRequired); len(got) != 0 {
		t.Errorf("got %+v, want no report without metadata", got)
	}
}
