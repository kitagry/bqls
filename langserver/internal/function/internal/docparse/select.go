package docparse

import (
	"bufio"
	"fmt"
	"io"
	"path"
	"slices"
	"strings"
)

// Page is the functions parsed from one GoogleSQL reference file.
type Page struct {
	Name      string // file name without extension, e.g. "string_functions"
	Functions []Function
}

// Report lists the names that differ between the GoogleSQL docs and BigQuery.
type Report struct {
	// NotInBigQuery are documented in GoogleSQL but absent from the BigQuery list, so they are dropped.
	NotInBigQuery []string
	// NotInDocs are in the BigQuery list but have no GoogleSQL documentation.
	NotInDocs []string
}

// ParseFunctionList reads the BigQuery function list: one "NAME<TAB>URL" per
// line, with blank lines and lines starting with "#" ignored.
func ParseFunctionList(r io.Reader) (map[string]string, error) {
	list := map[string]string{}
	sc := bufio.NewScanner(r)
	for n := 1; sc.Scan(); n++ {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name, url, ok := strings.Cut(line, "\t")
		if !ok || name == "" || strings.TrimSpace(url) == "" {
			return nil, fmt.Errorf("line %d: want \"NAME<TAB>URL\", got %q", n, line)
		}
		list[name] = strings.TrimSpace(url)
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	if len(list) == 0 {
		return nil, fmt.Errorf("function list is empty")
	}
	return list, nil
}

// Select keeps only the functions that exist in the BigQuery list and uses the
// BigQuery documentation URL for them. BigQuery functions without GoogleSQL
// documentation are appended with only a name and URL. When several pages document the same
// name, the page BigQuery uses for it wins, otherwise the first one. The result
// is ordered by the first appearance of each name.
func Select(pages []Page, list map[string]string) ([]Function, Report) {
	var order []string
	candidates := map[string][]Function{}
	candidatePage := map[string][]string{}
	for _, p := range pages {
		for _, f := range p.Functions {
			if _, seen := candidates[f.Name]; !seen {
				order = append(order, f.Name)
			}
			candidates[f.Name] = append(candidates[f.Name], f)
			candidatePage[f.Name] = append(candidatePage[f.Name], p.Name)
		}
	}

	var result []Function
	var report Report
	for _, name := range order {
		url, ok := list[name]
		if !ok {
			report.NotInBigQuery = append(report.NotInBigQuery, name)
			continue
		}
		bqPage, _, _ := strings.Cut(path.Base(url), "#")
		pick := 0
		if i := slices.Index(candidatePage[name], bqPage); i >= 0 {
			pick = i
		}
		f := candidates[name][pick]
		f.URL = url
		result = append(result, f)
	}
	for name := range list {
		if _, ok := candidates[name]; !ok {
			report.NotInDocs = append(report.NotInDocs, name)
		}
	}
	slices.Sort(report.NotInDocs)
	// Keep BigQuery functions that GoogleSQL does not document, with the link only,
	// so that completion still offers them and hover can point to the BigQuery docs.
	for _, name := range report.NotInDocs {
		result = append(result, Function{Name: name, URL: list[name]})
	}
	return result, report
}
