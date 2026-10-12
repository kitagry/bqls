package lint

import (
	"strings"
)

const ignoreDirective = "bqls:ignore"

// Suppressions maps a 0-based line number to the rules suppressed on it.
type Suppressions map[int]suppression

type suppression struct {
	all   bool
	rules map[string]bool
}

// Suppressed reports whether diagnostics of rule starting on line are suppressed.
func (s Suppressions) Suppressed(rule string, line int) bool {
	sup, ok := s[line]
	if !ok {
		return false
	}
	return sup.all || sup.rules[rule]
}

func (s Suppressions) add(line int, rules []string) {
	sup := s[line]
	if len(rules) == 0 {
		sup.all = true
	}
	if sup.rules == nil {
		sup.rules = make(map[string]bool)
	}
	for _, r := range rules {
		sup.rules[r] = true
	}
	s[line] = sup
}

// ParseSuppressions scans src for `bqls:ignore` comment directives.
//
// A directive in a comment that follows code on the same line suppresses that
// line; a directive in a comment alone on its line suppresses the next line.
// Without rule names, every rule is suppressed.
func ParseSuppressions(src string) Suppressions {
	result := make(Suppressions)

	line := 0
	lineHasCode := false
	i := 0
	for i < len(src) {
		c := src[i]
		switch {
		case c == '\n':
			line++
			lineHasCode = false
			i++
		case c == '-' && strings.HasPrefix(src[i:], "--"), c == '#':
			end := strings.IndexByte(src[i:], '\n')
			if end < 0 {
				end = len(src) - i
			}
			body := strings.TrimPrefix(src[i:i+end], "#")
			body = strings.TrimPrefix(body, "--")
			addDirective(result, body, line, lineHasCode, line)
			i += end
		case c == '/' && strings.HasPrefix(src[i:], "/*"):
			end := strings.Index(src[i+2:], "*/")
			var body string
			next := i
			if end < 0 {
				body = src[i+2:]
				next = len(src)
			} else {
				body = src[i+2 : i+2+end]
				next = i + 2 + end + 2
			}
			endLine := line + strings.Count(src[i:next], "\n")
			addDirective(result, body, line, lineHasCode, endLine)
			line = endLine
			i = next
		case c == '\'' || c == '"' || c == '`':
			next := skipQuoted(src, i)
			line += strings.Count(src[i:next], "\n")
			lineHasCode = true
			i = next
		case c == ' ' || c == '\t' || c == '\r':
			i++
		default:
			lineHasCode = true
			i++
		}
	}
	return result
}

func addDirective(s Suppressions, body string, startLine int, trailing bool, endLine int) {
	rules, ok := parseDirective(body)
	if !ok {
		return
	}
	if trailing {
		s.add(startLine, rules)
		return
	}
	s.add(endLine+1, rules)
}

func parseDirective(body string) ([]string, bool) {
	body = strings.TrimSpace(body)
	rest, ok := strings.CutPrefix(body, ignoreDirective)
	if !ok {
		return nil, false
	}
	if rest != "" && !strings.ContainsAny(rest[:1], " \t,") {
		return nil, false
	}

	var rules []string
	for _, f := range strings.FieldsFunc(rest, func(r rune) bool {
		return r == ',' || r == ' ' || r == '\t'
	}) {
		// Allow an explanation after the rule list, e.g. `-- bqls:ignore rule -- reason`.
		if strings.HasPrefix(f, "--") {
			break
		}
		rules = append(rules, f)
	}
	return rules, true
}

// skipQuoted returns the offset just after the string literal or quoted
// identifier starting at src[start].
//
// Backslashes always skip the next byte: in GoogleSQL even raw strings cannot
// be terminated by an escaped quote, so raw/bytes prefixes need no special case.
func skipQuoted(src string, start int) int {
	q := src[start]
	delim := src[start : start+1]
	if q != '`' && strings.HasPrefix(src[start:], strings.Repeat(delim, 3)) {
		delim = strings.Repeat(delim, 3)
	}
	i := start + len(delim)
	for i < len(src) {
		if src[i] == '\\' {
			i += 2
			continue
		}
		if strings.HasPrefix(src[i:], delim) {
			return i + len(delim)
		}
		i++
	}
	return len(src)
}
