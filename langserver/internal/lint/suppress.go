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
			if !lineHasCode && codeFollowsOnLine(src[next:]) {
				// `/* bqls:ignore */ SELECT ...` targets the code on its own line.
				addDirective(result, body, endLine, true, endLine)
			} else {
				addDirective(result, body, line, lineHasCode, endLine)
			}
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

func codeFollowsOnLine(rest string) bool {
	if i := strings.IndexByte(rest, '\n'); i >= 0 {
		rest = rest[:i]
	}
	rest = strings.TrimSpace(rest)
	return rest != "" && !strings.HasPrefix(rest, "--") && !strings.HasPrefix(rest, "#") && !strings.HasPrefix(rest, "/*")
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
	rules, _, ok := scanRules(rest)
	return rules, ok
}

// scanRules parses the rule list that follows `bqls:ignore` and returns the
// byte offset in rest just after the last rule name.
func scanRules(rest string) ([]string, int, bool) {
	if rest != "" && !strings.ContainsAny(rest[:1], " \t,") {
		return nil, 0, false
	}
	var rules []string
	end := 0
	i := 0
	for i < len(rest) {
		if strings.ContainsRune(" \t,", rune(rest[i])) {
			i++
			continue
		}
		j := i
		for j < len(rest) && !strings.ContainsRune(" \t,", rune(rest[j])) {
			j++
		}
		tok := rest[i:j]
		// Allow an explanation after the rule list, e.g. `-- bqls:ignore rule -- reason`.
		if strings.HasPrefix(tok, "--") {
			break
		}
		rules = append(rules, tok)
		end = j
		i = j
	}
	return rules, end, true
}

// IgnoreLine is a line consisting only of a `bqls:ignore` comment.
type IgnoreLine struct {
	// Rules is empty when every rule is ignored.
	Rules []string
	// RulesEnd is the byte offset in the line where another rule name can be
	// appended.
	RulesEnd int
}

// ParseIgnoreLine reports whether line is a standalone `bqls:ignore`
// directive, which ParseSuppressions applies to the following line.
func ParseIgnoreLine(line string) (IgnoreLine, bool) {
	trimmed := strings.TrimLeft(line, " \t")
	bodyStart := len(line) - len(trimmed)
	var body string
	switch {
	case strings.HasPrefix(trimmed, "--"):
		bodyStart += 2
		body = line[bodyStart:]
	case strings.HasPrefix(trimmed, "#"):
		bodyStart++
		body = line[bodyStart:]
	case strings.HasPrefix(trimmed, "/*"):
		end := strings.Index(trimmed, "*/")
		if end < 0 || strings.TrimSpace(trimmed[end+2:]) != "" {
			return IgnoreLine{}, false
		}
		bodyStart += 2
		body = trimmed[2:end]
	default:
		return IgnoreLine{}, false
	}

	afterSpace := strings.TrimLeft(body, " \t")
	rest, ok := strings.CutPrefix(afterSpace, ignoreDirective)
	if !ok {
		return IgnoreLine{}, false
	}
	rules, end, ok := scanRules(rest)
	if !ok {
		return IgnoreLine{}, false
	}
	directiveStart := bodyStart + len(body) - len(afterSpace)
	return IgnoreLine{Rules: rules, RulesEnd: directiveStart + len(ignoreDirective) + end}, true
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
