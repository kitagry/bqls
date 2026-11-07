package lint_test

import (
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/kitagry/bqls/langserver/internal/lint"
)

// The rule count and rule table are written by hand, so these tests catch
// docs left behind when a rule is added or removed.

func TestReadmeRuleCountMatchesAnalyzers(t *testing.T) {
	readme, err := os.ReadFile("../../../README.md")
	if err != nil {
		t.Fatal(err)
	}
	m := regexp.MustCompile(`\*\*Lint\*\* \| (\d+) rules`).FindSubmatch(readme)
	if m == nil {
		t.Fatal(`README.md has no "**Lint** | N rules" row in its feature table`)
	}
	got, err := strconv.Atoi(string(m[1]))
	if err != nil {
		t.Fatal(err)
	}
	if want := len(lint.Analyzers); got != want {
		t.Errorf("README.md says %d rules, but lint.Analyzers has %d", got, want)
	}
}

func TestDocsRuleTableMatchesAnalyzers(t *testing.T) {
	doc, err := os.ReadFile("../../../docs/lint.md")
	if err != nil {
		t.Fatal(err)
	}
	documented := ruleTableNames(t, string(doc))

	registered := make(map[string]bool, len(lint.Analyzers))
	for _, a := range lint.Analyzers {
		registered[a.Name] = true
	}

	var missing, extra []string
	for name := range registered {
		if !documented[name] {
			missing = append(missing, name)
		}
	}
	for name := range documented {
		if !registered[name] {
			extra = append(extra, name)
		}
	}
	sort.Strings(missing)
	sort.Strings(extra)
	if len(missing) > 0 {
		t.Errorf("docs/lint.md rule table lacks registered rules: %v", missing)
	}
	if len(extra) > 0 {
		t.Errorf("docs/lint.md rule table lists unregistered rules: %v", extra)
	}
}

// ruleTableNames returns the rule names in the table between "## Rules" and
// the first rule section, counting each row once.
func ruleTableNames(t *testing.T, doc string) map[string]bool {
	t.Helper()
	_, after, ok := strings.Cut(doc, "\n## Rules\n")
	if !ok {
		t.Fatal(`docs/lint.md has no "## Rules" section`)
	}
	table, _, _ := strings.Cut(after, "\n### ")

	row := regexp.MustCompile("(?m)^\\| \\[`([a-z0-9-]+)`\\]\\(#")
	names := make(map[string]bool)
	for _, m := range row.FindAllStringSubmatch(table, -1) {
		if names[m[1]] {
			t.Errorf("docs/lint.md rule table lists %s more than once", m[1])
		}
		names[m[1]] = true
	}
	if len(names) == 0 {
		t.Fatal("docs/lint.md rule table has no rule rows")
	}
	return names
}
