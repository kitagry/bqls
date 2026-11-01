package lint_test

import (
	"strings"
	"testing"

	"github.com/kitagry/bqls/langserver/internal/lint"
)

func leftJoinAsInnerMessage(join string) string {
	return "this " + join + " behaves as an INNER JOIN: the WHERE clause rejects the NULL columns of its unmatched rows; move the condition into the ON clause to keep those rows, or write INNER JOIN"
}

func TestLeftJoinAsInner(t *testing.T) {
	tests := map[string]struct {
		src string
		// want is the exact text of each reported range, in order.
		want []string
	}{
		"equal":                {src: "SELECT a.x FROM p.d.t AS a LEFT JOIN p.d.t AS b ON a.x = b.x WHERE b.x = 1", want: []string{"LEFT JOIN"}},
		"constant on the left": {src: "SELECT a.x FROM p.d.t AS a LEFT JOIN p.d.t AS b ON a.x = b.x WHERE 1 = b.x", want: []string{"LEFT JOIN"}},
		"not equal":            {src: "SELECT a.x FROM p.d.t AS a LEFT JOIN p.d.t AS b ON a.x = b.x WHERE b.x != 1", want: []string{"LEFT JOIN"}},
		"less":                 {src: "SELECT a.x FROM p.d.t AS a LEFT JOIN p.d.t AS b ON a.x = b.x WHERE b.x < 1", want: []string{"LEFT JOIN"}},
		"greater or equal":     {src: "SELECT a.x FROM p.d.t AS a LEFT JOIN p.d.t AS b ON a.x = b.x WHERE b.x >= 1", want: []string{"LEFT JOIN"}},
		"compared with the other side": {
			src:  "SELECT a.x FROM p.d.t AS a LEFT JOIN p.d.t AS b ON a.x = b.x WHERE b.x > a.x",
			want: []string{"LEFT JOIN"},
		},
		"in list":     {src: "SELECT a.x FROM p.d.t AS a LEFT JOIN p.d.t AS b ON a.x = b.x WHERE b.x IN (1, 2)", want: []string{"LEFT JOIN"}},
		"not in list": {src: "SELECT a.x FROM p.d.t AS a LEFT JOIN p.d.t AS b ON a.x = b.x WHERE b.x NOT IN (1, 2)", want: []string{"LEFT JOIN"}},
		"between":     {src: "SELECT a.x FROM p.d.t AS a LEFT JOIN p.d.t AS b ON a.x = b.x WHERE b.x BETWEEN 1 AND 2", want: []string{"LEFT JOIN"}},
		"like":        {src: "SELECT a.x FROM p.d.t AS a LEFT JOIN p.d.t AS b ON a.x = b.x WHERE CAST(b.x AS STRING) LIKE '1%'", want: []string{"LEFT JOIN"}},
		"is not null": {src: "SELECT a.x FROM p.d.t AS a LEFT JOIN p.d.t AS b ON a.x = b.x WHERE b.x IS NOT NULL", want: []string{"LEFT JOIN"}},
		"is true":     {src: "SELECT a.x FROM p.d.t AS a LEFT JOIN p.d.t AS b ON a.x = b.x WHERE (b.x = 1) IS TRUE", want: []string{"LEFT JOIN"}},
		"is false":    {src: "SELECT a.x FROM p.d.t AS a LEFT JOIN p.d.t AS b ON a.x = b.x WHERE (b.x = 1) IS FALSE", want: []string{"LEFT JOIN"}},
		"not":         {src: "SELECT a.x FROM p.d.t AS a LEFT JOIN p.d.t AS b ON a.x = b.x WHERE NOT b.x = 1", want: []string{"LEFT JOIN"}},
		"bare boolean": {
			src:  "SELECT a.x FROM p.d.t AS a LEFT JOIN (SELECT x, x > 0 AS f FROM p.d.t) AS b ON a.x = b.x WHERE b.f",
			want: []string{"LEFT JOIN"},
		},
		"arithmetic": {src: "SELECT a.x FROM p.d.t AS a LEFT JOIN p.d.t AS b ON a.x = b.x WHERE b.x + 1 > 2", want: []string{"LEFT JOIN"}},
		"cast":       {src: "SELECT a.x FROM p.d.t AS a LEFT JOIN p.d.t AS b ON a.x = b.x WHERE CAST(b.x AS STRING) = '1'", want: []string{"LEFT JOIN"}},
		"struct field": {
			src:  "SELECT a.x FROM p.d.t AS a LEFT JOIN (SELECT x, STRUCT(x AS y) AS s FROM p.d.t) AS b ON a.x = b.x WHERE b.s.y = 1",
			want: []string{"LEFT JOIN"},
		},
		"one conjunct of and": {
			src:  "SELECT a.x FROM p.d.t AS a LEFT JOIN p.d.t AS b ON a.x = b.x WHERE a.x > 0 AND b.x = 1",
			want: []string{"LEFT JOIN"},
		},
		"nested and": {
			src:  "SELECT a.x FROM p.d.t AS a LEFT JOIN p.d.t AS b ON a.x = b.x WHERE a.x > 0 AND (a.x < 9 AND b.x = 1)",
			want: []string{"LEFT JOIN"},
		},
		"two conjuncts report once": {
			src:  "SELECT a.x FROM p.d.t AS a LEFT JOIN p.d.t AS b ON a.x = b.x WHERE b.x = 1 AND b.x > 0",
			want: []string{"LEFT JOIN"},
		},
		"left outer join": {src: "SELECT a.x FROM p.d.t AS a LEFT OUTER JOIN p.d.t AS b ON a.x = b.x WHERE b.x = 1", want: []string{"LEFT OUTER JOIN"}},
		"right join":      {src: "SELECT b.x FROM p.d.t AS a RIGHT JOIN p.d.t AS b ON a.x = b.x WHERE a.x = 1", want: []string{"RIGHT JOIN"}},
		"subquery on the optional side": {
			src:  "SELECT a.x FROM p.d.t AS a LEFT JOIN (SELECT x AS y FROM p.d.t) AS b ON a.x = b.y WHERE b.y = 1",
			want: []string{"LEFT JOIN"},
		},
		"later inner join": {
			src:  "SELECT a.x FROM p.d.t AS a LEFT JOIN p.d.t AS b ON a.x = b.x JOIN p.d.t AS c ON a.x = c.x WHERE b.x = 1",
			want: []string{"LEFT JOIN"},
		},
		"later left join": {
			src:  "SELECT a.x FROM p.d.t AS a LEFT JOIN p.d.t AS b ON a.x = b.x LEFT JOIN p.d.t AS c ON a.x = c.x WHERE b.x = 1",
			want: []string{"LEFT JOIN"},
		},
		"only the filtered one of two joins": {
			src:  "SELECT a.x FROM p.d.t AS a LEFT JOIN p.d.t AS b ON a.x = b.x LEFT JOIN p.d.t AS c ON a.x = c.x WHERE c.x = 1",
			want: []string{"LEFT JOIN"},
		},
		"inside a cte": {
			src:  "WITH c AS (SELECT a.x FROM p.d.t AS a LEFT JOIN p.d.t AS b ON a.x = b.x WHERE b.x = 1) SELECT x FROM c",
			want: []string{"LEFT JOIN"},
		},
		"inside a subquery": {
			src:  "SELECT x FROM (SELECT a.x FROM p.d.t AS a LEFT JOIN p.d.t AS b ON a.x = b.x WHERE b.x = 1)",
			want: []string{"LEFT JOIN"},
		},
		// Only the outer join is reached; the WHERE is not directly above the inner one.
		"nested left join inside the optional side": {
			src:  "SELECT a.x FROM p.d.t AS a LEFT JOIN (p.d.t AS b LEFT JOIN p.d.t AS c ON b.x = c.x) ON a.x = b.x WHERE c.x = 1",
			want: []string{"LEFT JOIN"},
		},
		"multiline": {
			src:  "SELECT a.x\nFROM p.d.t AS a\nLEFT JOIN p.d.t AS b\n  ON a.x = b.x\nWHERE b.x = 1",
			want: []string{"LEFT JOIN"},
		},

		// The condition can pass the NULL row, or the join is out of scope.
		"is null":                          {src: "SELECT a.x FROM p.d.t AS a LEFT JOIN p.d.t AS b ON a.x = b.x WHERE b.x IS NULL"},
		"is not true":                      {src: "SELECT a.x FROM p.d.t AS a LEFT JOIN p.d.t AS b ON a.x = b.x WHERE (b.x = 1) IS NOT TRUE"},
		"not is not null":                  {src: "SELECT a.x FROM p.d.t AS a LEFT JOIN p.d.t AS b ON a.x = b.x WHERE NOT b.x IS NOT NULL"},
		"coalesce":                         {src: "SELECT a.x FROM p.d.t AS a LEFT JOIN p.d.t AS b ON a.x = b.x WHERE COALESCE(b.x, 0) = 0"},
		"ifnull":                           {src: "SELECT a.x FROM p.d.t AS a LEFT JOIN p.d.t AS b ON a.x = b.x WHERE IFNULL(b.x, 0) = 0"},
		"if":                               {src: "SELECT a.x FROM p.d.t AS a LEFT JOIN p.d.t AS b ON a.x = b.x WHERE IF(b.x IS NULL, TRUE, b.x = 1)"},
		"or with is null":                  {src: "SELECT a.x FROM p.d.t AS a LEFT JOIN p.d.t AS b ON a.x = b.x WHERE b.x = 1 OR b.x IS NULL"},
		"or":                               {src: "SELECT a.x FROM p.d.t AS a LEFT JOIN p.d.t AS b ON a.x = b.x WHERE b.x = 1 OR a.x = 1"},
		"is distinct from":                 {src: "SELECT a.x FROM p.d.t AS a LEFT JOIN p.d.t AS b ON a.x = b.x WHERE b.x IS DISTINCT FROM 1"},
		"in with b in list":                {src: "SELECT a.x FROM p.d.t AS a LEFT JOIN p.d.t AS b ON a.x = b.x WHERE a.x IN (b.x, 1)"},
		"in subquery":                      {src: "SELECT a.x FROM p.d.t AS a LEFT JOIN p.d.t AS b ON a.x = b.x WHERE b.x IN (SELECT x FROM p.d.t)"},
		"preserved side":                   {src: "SELECT a.x FROM p.d.t AS a LEFT JOIN p.d.t AS b ON a.x = b.x WHERE a.x = 1"},
		"using column":                     {src: "SELECT x FROM p.d.t AS a LEFT JOIN p.d.t AS b USING (x) WHERE x = 1"},
		"condition in on":                  {src: "SELECT a.x FROM p.d.t AS a LEFT JOIN p.d.t AS b ON a.x = b.x AND b.x = 1"},
		"right join on the preserved side": {src: "SELECT b.x FROM p.d.t AS a RIGHT JOIN p.d.t AS b ON a.x = b.x WHERE b.x = 1"},
		"inner join":                       {src: "SELECT a.x FROM p.d.t AS a JOIN p.d.t AS b ON a.x = b.x WHERE b.x = 1"},
		"full join":                        {src: "SELECT a.x FROM p.d.t AS a FULL JOIN p.d.t AS b ON a.x = b.x WHERE b.x = 1"},
		"left unnest":                      {src: "SELECT a.x FROM p.d.t AS a LEFT JOIN UNNEST([1, 2]) AS u WHERE u = 1"},
		"having":                           {src: "SELECT b.x FROM p.d.t AS a LEFT JOIN p.d.t AS b ON a.x = b.x GROUP BY b.x HAVING b.x = 1"},
		"no where":                         {src: "SELECT a.x FROM p.d.t AS a LEFT JOIN p.d.t AS b ON a.x = b.x"},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			parsed := parseWithTable(t, tt.src)
			got := lint.Run(t.Context(), parsed, lint.Config{}, lint.Env{}, lint.LeftJoinAsInner)

			var gotText []string
			for _, d := range got {
				text := textInRange(tt.src, d)
				join := strings.ToUpper(strings.Fields(text)[0]) + " JOIN"
				if d.Rule != "left-join-as-inner" || d.Message != leftJoinAsInnerMessage(join) || d.Severity != lint.SeverityWarning || len(d.SuggestedFixes) != 0 {
					t.Errorf("unexpected diagnostic metadata: %+v", d)
				}
				gotText = append(gotText, text)
			}
			if strings.Join(gotText, "|") != strings.Join(tt.want, "|") {
				t.Errorf("reported ranges = %q, want %q", gotText, tt.want)
			}
		})
	}
}

func TestLeftJoinAsInnerSuppressed(t *testing.T) {
	src := "SELECT a.x\nFROM p.d.t AS a\n-- bqls:ignore left-join-as-inner\nLEFT JOIN p.d.t AS b ON a.x = b.x\nWHERE b.x = 1"
	parsed := parseWithTable(t, src)
	if got := lint.Run(t.Context(), parsed, lint.Config{}, lint.Env{}, lint.LeftJoinAsInner); len(got) != 0 {
		t.Errorf("got %+v, want suppressed", got)
	}
}

func TestLeftJoinAsInnerConfig(t *testing.T) {
	src := "SELECT a.x FROM p.d.t AS a LEFT JOIN p.d.t AS b ON a.x = b.x WHERE b.x = 1"
	parsed := parseWithTable(t, src)
	off := lint.Config{Rules: map[string]bool{"left-join-as-inner": false}}
	if got := lint.Run(t.Context(), parsed, off, lint.Env{}, lint.LeftJoinAsInner); len(got) != 0 {
		t.Errorf("disabled rule reported %+v", got)
	}
	if got := lint.Run(t.Context(), parsed, lint.Config{}, lint.Env{}, lint.LeftJoinAsInner); len(got) != 1 {
		t.Errorf("rule should be enabled by default, got %+v", got)
	}
}
