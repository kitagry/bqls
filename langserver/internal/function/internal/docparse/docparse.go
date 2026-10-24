// Package docparse extracts built-in function references from the GoogleSQL
// Markdown documentation (https://github.com/google/googlesql/tree/master/docs).
package docparse

import (
	"html"
	"regexp"
	"strings"
)

// BigQueryDocsBase is where GoogleSQL doc links are redirected to, so that
// hover text points at the documentation BigQuery users actually read.
const BigQueryDocsBase = "https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/"

const googleSQLDocsBase = "https://github.com/google/googlesql/blob/master/docs/"

type Function struct {
	Name        string
	Method      string
	Description string
	ExampleSQLs []string
	URL         string
}

var (
	functionHeading = regexp.MustCompile("^## `([^`]+)`\\s*$")
	anyH2           = regexp.MustCompile(`^## `)
	boldHeading     = regexp.MustCompile(`^\*\*[^*]+\*\*\s*$`)
	refDefinition   = regexp.MustCompile(`^\[([^\]]+)\]:\s*(\S+)\s*$`)
	mdlintComment   = regexp.MustCompile(`^<!--\s*mdlint .*-->\s*$`)
	refLink         = regexp.MustCompile(`\[([^\[\]]+)\]\[([^\[\]]*)\]`)
	listAnchor      = regexp.MustCompile(`md#([^"]+)"><code>([^<]+)</code>`)
	htmlSyntax      = regexp.MustCompile(`^<code>(.*)</code>\s*$`)
	codeSpan        = regexp.MustCompile("`[^`]*`")
	blankRuns       = regexp.MustCompile(`\n{3,}`)
)

// Parse extracts every function defined with a "## `NAME`" heading in a
// GoogleSQL function reference page. pageURL is the BigQuery documentation
// page for the same file and is used to build per-function URLs.
func Parse(src []byte, pageURL string) ([]Function, error) {
	lines := strings.Split(string(src), "\n")

	anchors := map[string]string{}
	refs := map[string]string{}
	inFence := false
	for _, line := range lines {
		if isFence(line) {
			inFence = !inFence
			continue
		}
		if inFence {
			continue
		}
		for _, m := range listAnchor.FindAllStringSubmatch(line, -1) {
			anchors[m[2]] = m[1]
		}
		if m := refDefinition.FindStringSubmatch(line); m != nil {
			refs[strings.ToLower(m[1])] = m[2]
		}
	}

	var functions []Function
	var name string
	var section []string
	flush := func() {
		if name != "" {
			functions = append(functions, parseSection(name, section, pageURL, anchors, refs))
		}
	}

	inFence = false
	for _, line := range lines {
		if isFence(line) {
			inFence = !inFence
		}
		if !inFence && anyH2.MatchString(line) {
			flush()
			name, section = "", nil
			if m := functionHeading.FindStringSubmatch(line); m != nil {
				name = m[1]
			}
			continue
		}
		section = append(section, line)
	}
	flush()
	return functions, nil
}

func isFence(line string) bool {
	return strings.HasPrefix(strings.TrimSpace(line), "```")
}

func parseSection(name string, lines []string, pageURL string, anchors, refs map[string]string) Function {
	anchor, ok := anchors[name]
	if !ok {
		anchor = strings.ToLower(strings.ReplaceAll(name, ".", ""))
	}
	f := Function{Name: name, URL: pageURL + "#" + anchor}

	const (
		stateMethod = iota
		stateOther
		stateDescription
		stateExamples
	)
	state := stateMethod
	var description []string
	var block []string
	inFence := false
	var methods []string

	for _, line := range lines {
		if isFence(line) {
			inFence = !inFence
			if inFence {
				block = nil
				if state == stateDescription {
					description = append(description, line)
				}
				continue
			}
			// closing fence
			code := strings.TrimSpace(strings.Join(block, "\n"))
			switch {
			case state == stateMethod:
				methods = append(methods, code)
			case state == stateExamples:
				f.ExampleSQLs = append(f.ExampleSQLs, code)
			case state == stateDescription:
				description = append(description, line)
			}
			continue
		}
		if inFence {
			block = append(block, line)
			if state == stateDescription {
				description = append(description, line)
			}
			continue
		}

		if boldHeading.MatchString(line) {
			switch strings.TrimSpace(line) {
			case "**Description**":
				state = stateDescription
			case "**Example**", "**Examples**":
				state = stateExamples
			default:
				state = stateOther
			}
			continue
		}

		if state == stateMethod {
			if m := htmlSyntax.FindStringSubmatch(line); m != nil {
				methods = append(methods, html.UnescapeString(m[1]))
				continue
			}
			if strings.HasPrefix(line, "<a id=") || strings.HasPrefix(line, "<pre") || line == "</pre>" {
				continue
			}
		}
		// Some functions put the description right after the syntax blocks
		// without a "**Description**" heading.
		if state == stateMethod && len(methods) > 0 && strings.TrimSpace(line) != "" {
			state = stateDescription
		}
		if state != stateDescription {
			continue
		}
		if refDefinition.MatchString(line) || mdlintComment.MatchString(strings.TrimSpace(line)) {
			continue
		}
		description = append(description, expandRefs(line, pageURL, refs))
	}

	f.Method = strings.Join(methods, "\n\n")
	desc := blankRuns.ReplaceAllString(strings.Join(description, "\n"), "\n\n")
	f.Description = strings.TrimSpace(desc)
	return f
}

// expandRefs turns "[text][ref]" into "[text](url)" unless it sits inside an
// inline code span.
func expandRefs(line, pageURL string, refs map[string]string) string {
	spans := codeSpan.FindAllStringIndex(line, -1)
	inCode := func(i int) bool {
		for _, sp := range spans {
			if i >= sp[0] && i < sp[1] {
				return true
			}
		}
		return false
	}

	var b strings.Builder
	last := 0
	for _, loc := range refLink.FindAllStringSubmatchIndex(line, -1) {
		if inCode(loc[0]) {
			continue
		}
		text := line[loc[2]:loc[3]]
		label := line[loc[4]:loc[5]]
		if label == "" {
			label = text
		}
		url, ok := refs[strings.ToLower(label)]
		if !ok {
			continue
		}
		b.WriteString(line[last:loc[0]])
		b.WriteString("[" + text + "](" + resolveURL(url, pageURL) + ")")
		last = loc[1]
	}
	b.WriteString(line[last:])
	return b.String()
}

func resolveURL(url, pageURL string) string {
	if strings.HasPrefix(url, "#") {
		return pageURL + url
	}
	if rest, ok := strings.CutPrefix(url, googleSQLDocsBase); ok {
		file, frag, hasFrag := strings.Cut(rest, "#")
		file = strings.TrimSuffix(file, ".md")
		if hasFrag {
			return BigQueryDocsBase + file + "#" + frag
		}
		return BigQueryDocsBase + file
	}
	return url
}
