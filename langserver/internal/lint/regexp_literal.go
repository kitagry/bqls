package lint

import (
	"errors"
	"fmt"
	"regexp"
	"regexp/syntax"
	"strconv"
	"strings"

	googlesql "github.com/goccy/go-googlesql"
	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/kitagry/bqls/langserver/internal/source/file"
)

var RegexpLiteral = &Analyzer{
	Name:           "regexp-literal",
	Doc:            "reports constant regular expressions that BigQuery rejects when evaluated",
	DefaultEnabled: true,
	Run:            runRegexpLiteral,
}

// regexpFunctions lists the functions whose second argument is an RE2
// pattern. REGEXP_SUBSTR resolves to regexp_extract.
var regexpFunctions = map[string]bool{
	"regexp_contains":    true,
	"regexp_extract":     true,
	"regexp_extract_all": true,
	"regexp_instr":       true,
	"regexp_replace":     true,
}

// singleGroupFunctions fail on a pattern with more than one capturing group.
var singleGroupFunctions = map[string]bool{
	"regexp_extract":     true,
	"regexp_extract_all": true,
	"regexp_instr":       true,
}

func runRegexpLiteral(pass *Pass) {
	for _, output := range pass.File.RNode {
		for _, call := range file.ListResolvedAstNode[*googlesql.ResolvedFunctionCall](output) {
			name, ok := functionName(call)
			if !ok || !regexpFunctions[name] {
				continue
			}
			args, err := call.ArgumentList()
			if err != nil || len(args) < 2 {
				continue
			}
			lit, re, invalid := checkRegexpPattern(pass, args[1])
			if invalid {
				continue
			}
			if singleGroupFunctions[name] && re != nil && re.MaxCap() > 1 {
				pass.Report(Diagnostic{
					Range:          lit.loc,
					Message:        fmt.Sprintf("The regular expression %s has %d capturing groups, but REGEXP_EXTRACT, REGEXP_EXTRACT_ALL, REGEXP_SUBSTR and REGEXP_INSTR allow at most one, so the query fails when this is evaluated; make the other groups non-capturing with (?:...)", lit.src, re.MaxCap()),
					SuggestedFixes: nonCapturingFixes(pass, lit, re.MaxCap()),
				})
			}
			if name == "regexp_replace" && len(args) >= 3 {
				checkRegexpReplacement(pass, args[2], lit, re)
			}
		}
	}
}

// checkRegexpPattern reports expr when it is an invalid pattern literal, and
// returns it parsed when it is a valid one.
func checkRegexpPattern(pass *Pass, expr googlesql.ResolvedExprNode) (lit stringLiteral, re *syntax.Regexp, invalid bool) {
	lit, ok := writtenStringLiteral(pass, expr)
	if !ok {
		return stringLiteral{}, nil, false
	}
	re, err := syntax.Parse(lit.value, syntax.Perl)
	if err == nil {
		return lit, re, false
	}
	var serr *syntax.Error
	if !errors.As(err, &serr) || !reportedByRE2(serr) {
		return stringLiteral{}, nil, false
	}
	pass.Report(Diagnostic{
		Range:   lit.loc,
		Message: fmt.Sprintf("Invalid regular expression %s: %s: `%s`, so the query fails when this is evaluated", lit.src, serr.Code, serr.Expr),
	})
	return stringLiteral{}, nil, true
}

// checkRegexpReplacement applies RE2's CheckRewriteString, which BigQuery
// runs before replacing anything: a backslash must be followed by a digit or
// a backslash, and \N must name a group of pattern. pattern is checked only
// when re is not nil.
func checkRegexpReplacement(pass *Pass, expr googlesql.ResolvedExprNode, pattern stringLiteral, re *syntax.Regexp) {
	lit, ok := writtenStringLiteral(pass, expr)
	if !ok {
		return
	}
	group := 0
	for i := 0; i < len(lit.value); i++ {
		if lit.value[i] != '\\' {
			continue
		}
		i++
		if i < len(lit.value) && lit.value[i] == '\\' {
			continue
		}
		if i == len(lit.value) || lit.value[i] < '0' || lit.value[i] > '9' {
			pass.Report(Diagnostic{
				Range:   lit.loc,
				Message: fmt.Sprintf(`In the replacement %s, a backslash must be followed by a digit or another backslash (write \\ for a literal backslash), so the query fails when this is evaluated`, lit.src),
			})
			return
		}
		group = max(group, int(lit.value[i]-'0'))
	}
	if re != nil && group > re.MaxCap() {
		pass.Report(Diagnostic{
			Range:   lit.loc,
			Message: fmt.Sprintf(`The replacement %s refers to group \%d, but the regular expression %s has %s, so the query fails when this is evaluated`, lit.src, group, pattern.src, capturingGroups(re.MaxCap())),
		})
	}
}

func capturingGroups(n int) string {
	switch n {
	case 0:
		return "no capturing groups"
	case 1:
		return "1 capturing group"
	}
	return fmt.Sprintf("%d capturing groups", n)
}

// reportedByRE2 reports whether BigQuery's RE2 is known to reject the
// pattern too. Go's regexp/syntax is also RE2, but it lacks \C, ships its own
// Unicode tables and enforces its own size limits.
func reportedByRE2(err *syntax.Error) bool {
	switch err.Code {
	case syntax.ErrInvalidEscape:
		return err.Expr != `\C`
	case syntax.ErrInvalidCharRange:
		return !strings.HasPrefix(err.Expr, `\p`) && !strings.HasPrefix(err.Expr, `\P`)
	case syntax.ErrLarge, syntax.ErrNestingDepth, syntax.ErrInvalidUTF8:
		return false
	}
	return true
}

type stringLiteral struct {
	// value is the literal's value. BYTES are read as Latin-1, one rune per
	// byte, as RE2 reads BYTES patterns.
	value string
	// raw is the value as stored: UTF-8 for STRING, the bytes for BYTES.
	raw string
	src string
	// start is the offset of src in FixedSrc.
	start int
	loc   lsp.Range
}

// writtenStringLiteral returns expr when it is a STRING or BYTES literal in
// the source. Declared variables are analyzed as substituted literals, so the
// source text decides.
func writtenStringLiteral(pass *Pass, expr googlesql.ResolvedExprNode) (stringLiteral, bool) {
	lit, ok := expr.(*googlesql.ResolvedLiteral)
	if !ok {
		return stringLiteral{}, false
	}
	loc, err := lit.GetParseLocationRangeOrNULL()
	if err != nil || loc == nil {
		return stringLiteral{}, false
	}
	src, ok := pass.File.ExtractSQL(loc)
	if !ok {
		return stringLiteral{}, false
	}
	if _, _, ok := literalQuote(src); !ok {
		return stringLiteral{}, false
	}
	r, ok := pass.File.ToLspRange(loc)
	if !ok {
		return stringLiteral{}, false
	}
	v, err := lit.Value()
	if err != nil || v == nil {
		return stringLiteral{}, false
	}
	typ, err := lit.Type()
	if err != nil || typ == nil {
		return stringLiteral{}, false
	}
	result := stringLiteral{src: src, start: file.ParseLocStart(loc), loc: r}
	if isBytes, err := typ.IsBytes(); err == nil && isBytes {
		if result.raw, err = v.BytesValue(); err != nil {
			return stringLiteral{}, false
		}
		runes := make([]rune, len(result.raw))
		for i := range len(result.raw) {
			runes[i] = rune(result.raw[i])
		}
		result.value = string(runes)
	} else {
		if result.raw, err = v.StringValue(); err != nil {
			return stringLiteral{}, false
		}
		result.value = result.raw
	}
	return result, true
}

// nonCapturingFixes offers, for each of the n capturing groups, a fix that
// keeps only that group and makes the others non-capturing. A fix is left out
// when a group it changes is opened with escape sequences.
func nonCapturingFixes(pass *Pass, lit stringLiteral, n int) []SuggestedFix {
	end := lit.start + len(lit.src)
	if end > len(pass.File.FixedSrc) || pass.File.FixedSrc[lit.start:end] != lit.src {
		return nil
	}
	units, ok := literalUnits(lit.src)
	if !ok {
		return nil
	}
	var value strings.Builder
	var unitOf []literalUnit
	for _, u := range units {
		value.WriteString(u.value)
		for range len(u.value) {
			unitOf = append(unitOf, u)
		}
	}
	groups := capturingGroupOpenings(lit.raw)
	if value.String() != lit.raw || len(groups) != n {
		return nil
	}
	// edits[i] makes group i non-capturing: "(" becomes "(?:", and
	// "(?P<name>" / "(?<name>" become "(?:". Written units are one byte of
	// source each, so a run of them maps to a contiguous span.
	edits := make([]*lsp.TextEdit, n)
	for i, g := range groups {
		from, to := g.start, g.end
		if to-from > 1 {
			from++
		}
		if !allWritten(unitOf[from:to]) {
			continue
		}
		start, end := unitOf[from].start, unitOf[to-1].end
		if to-g.start == 1 {
			start = end
		}
		if r, ok := pass.File.OffsetsToLspRange(lit.start+start, lit.start+end); ok {
			edits[i] = &lsp.TextEdit{Range: r, NewText: "?:"}
		}
	}
	var fixes []SuggestedFix
	for keep := range n {
		fix := SuggestedFix{Title: fmt.Sprintf("Make every capturing group except group %d non-capturing", keep+1)}
		for i, e := range edits {
			if i == keep {
				continue
			}
			if e == nil {
				fix.Edits = nil
				break
			}
			fix.Edits = append(fix.Edits, *e)
		}
		if fix.Edits != nil {
			fixes = append(fixes, fix)
		}
	}
	return fixes
}

func allWritten(units []literalUnit) bool {
	for _, u := range units {
		if !u.written {
			return false
		}
	}
	return true
}

// literalUnit is one character or escape sequence of a literal's body.
type literalUnit struct {
	// start and end are offsets in the literal's source.
	start, end int
	value      string
	// written is false for an escape sequence. A written unit is one byte.
	written bool
}

var literalPrefix = regexp.MustCompile(`^(?i:[rb]{0,2})`)

// literalQuote returns the prefix (r, b, rb, ...) and the quote of a STRING or
// BYTES literal.
func literalQuote(src string) (prefix, quote string, ok bool) {
	prefix = literalPrefix.FindString(src)
	for _, q := range []string{`'''`, `"""`, `'`, `"`} {
		if len(src) >= len(prefix)+2*len(q) && strings.HasPrefix(src[len(prefix):], q) && strings.HasSuffix(src, q) {
			return prefix, q, true
		}
	}
	return "", "", false
}

// literalUnits splits the body of a STRING or BYTES literal into the units
// its value is made of.
func literalUnits(src string) ([]literalUnit, bool) {
	prefix, quote, ok := literalQuote(src)
	if !ok {
		return nil, false
	}
	isRaw := strings.ContainsAny(prefix, "rR")
	start, end := len(prefix)+len(quote), len(src)-len(quote)
	var units []literalUnit
	for i := start; i < end; {
		if isRaw || src[i] != '\\' {
			units = append(units, literalUnit{start: i, end: i + 1, value: src[i : i+1], written: true})
			i++
			continue
		}
		value, n, ok := literalEscape(src[i:end])
		if !ok {
			return nil, false
		}
		units = append(units, literalUnit{start: i, end: i + n, value: value})
		i += n
	}
	return units, true
}

var simpleEscapes = map[byte]string{
	'a': "\a", 'b': "\b", 'f': "\f", 'n': "\n", 'r': "\r", 't': "\t", 'v': "\v",
	'\\': `\`, '?': "?", '"': `"`, '\'': "'", '`': "`",
}

// literalEscape decodes the escape sequence that s starts with and returns
// its value and length.
func literalEscape(s string) (string, int, bool) {
	if len(s) < 2 {
		return "", 0, false
	}
	if v, ok := simpleEscapes[s[1]]; ok {
		return v, 2, true
	}
	digitsAt, digits, base := 2, 0, 16
	switch s[1] {
	case 'x', 'X':
		digits = 2
	case 'u':
		digits = 4
	case 'U':
		digits = 8
	case '0', '1', '2', '3':
		digitsAt, digits, base = 1, 3, 8
	default:
		return "", 0, false
	}
	n := digitsAt + digits
	if len(s) < n {
		return "", 0, false
	}
	c, err := strconv.ParseUint(s[digitsAt:n], base, 32)
	if err != nil {
		return "", 0, false
	}
	if s[1] == 'u' || s[1] == 'U' {
		return string(rune(c)), n, true
	}
	return string([]byte{byte(c)}), n, true
}

type groupOpening struct {
	// start is the offset of "(", and end the offset after "(", "(?P<name>"
	// or "(?<name>".
	start, end int
}

// capturingGroupOpenings finds the capturing groups of a pattern that parses,
// skipping escapes, character classes and \Q...\E.
func capturingGroupOpenings(pattern string) []groupOpening {
	var groups []groupOpening
	for i := 0; i < len(pattern); i++ {
		switch pattern[i] {
		case '\\':
			if strings.HasPrefix(pattern[i:], `\Q`) {
				e := strings.Index(pattern[i:], `\E`)
				if e < 0 {
					return groups
				}
				i += e + 1
				continue
			}
			i++
		case '[':
			i = classEnd(pattern, i)
		case '(':
			rest := pattern[i+1:]
			switch {
			case strings.HasPrefix(rest, "?P<"), strings.HasPrefix(rest, "?<"):
				if e := strings.IndexByte(rest, '>'); e >= 0 {
					groups = append(groups, groupOpening{i, i + 2 + e})
				}
			case !strings.HasPrefix(rest, "?"):
				groups = append(groups, groupOpening{i, i + 1})
			}
		}
	}
	return groups
}

// classEnd returns the offset of the "]" that closes the character class
// opened at start. A "]" right after "[" or "[^" is a literal.
func classEnd(pattern string, start int) int {
	i := start + 1
	if i < len(pattern) && pattern[i] == '^' {
		i++
	}
	if i < len(pattern) && pattern[i] == ']' {
		i++
	}
	for ; i < len(pattern); i++ {
		switch {
		case pattern[i] == '\\':
			i++
		case strings.HasPrefix(pattern[i:], "[:"):
			if e := strings.Index(pattern[i+2:], ":]"); e >= 0 {
				i += e + 3
			}
		case pattern[i] == ']':
			return i
		}
	}
	return i
}
