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
	scan(src, func(c comment) {
		if c.block && !c.lineHasCode && codeFollowsOnLine(src[c.end:]) {
			// `/* bqls:ignore */ SELECT ...` targets the code on its own line.
			addDirective(result, c.body, c.endLine, true, c.endLine)
			return
		}
		addDirective(result, c.body, c.startLine, c.lineHasCode, c.endLine)
	})
	return result
}

// LineState tells whether a line's edges are outside string literals and
// comments, i.e. whether text inserted there would be read as code.
type LineState struct {
	// StartsInCode is false when the line begins inside a string literal or
	// block comment opened on an earlier line.
	StartsInCode bool
	// EndsInCode is false when the line ends inside a string literal or a
	// comment.
	EndsInCode bool
}

// LineStates returns the LineState of every line in src.
func LineStates(src string) []LineState {
	return scan(src, func(comment) {})
}

// HasComment reports whether src contains a comment. Comment markers inside
// string literals and quoted identifiers are not comments.
func HasComment(src string) bool {
	found := false
	scan(src, func(comment) { found = true })
	return found
}

type comment struct {
	body      string
	startLine int
	endLine   int
	// lineHasCode reports whether code precedes the comment on startLine.
	lineHasCode bool
	block       bool
	// end is the offset just after the comment.
	end int
}

// scan lexes src, calling onComment for each comment, and returns the state of
// every line.
func scan(src string, onComment func(comment)) []LineState {
	states := make([]LineState, strings.Count(src, "\n")+1)
	for l := range states {
		states[l] = LineState{StartsInCode: true, EndsInCode: true}
	}
	span := func(from, to int, closed bool) {
		for l := from; l < to; l++ {
			states[l].EndsInCode = false
			states[l+1].StartsInCode = false
		}
		if !closed {
			states[to].EndsInCode = false
		}
	}

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
			states[line].EndsInCode = false
			onComment(comment{body: body, startLine: line, endLine: line, lineHasCode: lineHasCode, end: i + end})
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
			span(line, endLine, end >= 0)
			onComment(comment{body: body, startLine: line, endLine: endLine, lineHasCode: lineHasCode, block: true, end: next})
			if endLine > line {
				// Code before the comment is on an earlier line, not on the one where it ends.
				lineHasCode = false
			}
			line = endLine
			i = next
		case c == '\'' || c == '"' || c == '`':
			next, closed := skipQuoted(src, i)
			endLine := line + strings.Count(src[i:next], "\n")
			span(line, endLine, closed)
			line = endLine
			lineHasCode = true
			i = next
		case c == ' ' || c == '\t' || c == '\r':
			i++
		default:
			lineHasCode = true
			i++
		}
	}
	return states
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
// identifier starting at src[start], and whether it is terminated.
//
// Backslashes always skip the next byte: in GoogleSQL even raw strings cannot
// be terminated by an escaped quote, so raw/bytes prefixes need no special case.
func skipQuoted(src string, start int) (int, bool) {
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
			return i + len(delim), true
		}
		i++
	}
	return len(src), false
}
