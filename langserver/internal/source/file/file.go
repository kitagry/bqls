package file

import (
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"unicode"

	bq "cloud.google.com/go/bigquery"
	googlesql "github.com/goccy/go-googlesql"
	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/kitagry/bqls/langserver/internal/source/bqparser"
)

var lastDotRegex = regexp.MustCompile(`[\w.]+\.[\s;]`)

var zetaSQLLocationRegex = regexp.MustCompile(`\[at (\d+):(\d+)\]`)

var notBoolConditionErrorRegex = regexp.MustCompile(`^Cannot coerce expression (?s:(.*)) to type BOOL$`)

var nonNullArgumentErrorRegex = regexp.MustCompile(`^(Argument \d+ to|The argument to) \S+ must be non-NULL$`)

// operatorSignatureErrorRegex captures the operator and the argument types listed in one of these forms:
//
//	No matching signature for operator = for argument types: INT64, STRING
//	No matching signature for operator BETWEEN\n  Argument types: INT64, STRING, STRING
//	No matching signature for operator IN for argument types literal INT64 and {STRING}
var operatorSignatureErrorRegex = regexp.MustCompile(`^No matching signature for operator ([^\n]*?)(?: for argument types:? |\n\s*Argument types: )([^\n]*)`)

var bitwiseOperatorErrorRegex = regexp.MustCompile(`^Bitwise operator \S+ requires two integer/BYTES arguments of the same type, but saw (\S+) and (\S+)$`)

var literalCastErrorRegex = regexp.MustCompile(`^Could not cast literal .* to type \S+$`)

var logicalOperandErrorRegex = regexp.MustCompile(`^No matching signature for operator (AND|OR|NOT)\n(?s:.*?)Argument (\d+): Unable to coerce type \S+ to expected type BOOL`)

type ParsedFile struct {
	URI lsp.DocumentURI
	Src string
	// FixedSrc matches the byte offsets in Node, RNode, and ParseTree.
	FixedSrc string

	// googlesql AST node
	Node *googlesql.ASTScript
	// RNode holds the outputs of the statements whose analysis succeeded, in
	// Node's statement order.
	RNode []*googlesql.AnalyzerOutput
	// Statements holds the result of every statement analyzed in the final
	// pass, so that a failed statement does not shift which output belongs to
	// which statement. When the retry limit is reached, the final pass applies
	// no fix-up, so the statement it would fix up keeps its error.
	Statements []StatementResult

	ParseTree *bqparser.Node

	FixOffsets []FixOffset
	// edits are the rewrites from Src to FixedSrc, in the order they were made.
	edits []sourceEdit
	// Errors are positioned in Src, unlike the offsets of the nodes.
	Errors []Error

	catalog *Catalog
}

// TableMetadata returns the metadata the analysis already fetched for path,
// so that callers can avoid asking BigQuery for the same table again.
func (p ParsedFile) TableMetadata(path string) (*bq.TableMetadata, bool) {
	if p.catalog == nil {
		return nil, false
	}
	meta, err := p.catalog.FindTableMetadata(path)
	return meta, err == nil
}

// StatementResult is the analysis result of a single statement. Output is nil
// when the analysis failed or the statement produces no output, such as SET.
type StatementResult struct {
	Stmt   googlesql.ASTStatementNode
	Output *googlesql.AnalyzerOutput
	Err    *Error
}

func (p ParsedFile) Close() {}

func (p ParsedFile) TermOffset(pos lsp.Position) int {
	return toFixedOffset(p.edits, positionToByteOffset(p.Src, pos))
}

// SrcOffset is TermOffset in Src, for looking at what the user wrote where a
// fix-up rewrote it.
func (p ParsedFile) SrcOffset(pos lsp.Position) int {
	return positionToByteOffset(p.Src, pos)
}

func (p ParsedFile) ExtractSQL(locationRange *googlesql.ParseLocationRange) (string, bool) {
	if locationRange == nil {
		return "", false
	}

	startOffset := toSrcOffset(p.edits, parseLocStart(locationRange))
	endOffset := toSrcEnd(p.edits, parseLocEnd(locationRange))
	if startOffset < 0 || endOffset < 0 || startOffset > endOffset || endOffset > len(p.Src) {
		return "", false
	}

	return p.Src[startOffset:endOffset], true
}

func (p ParsedFile) ToLspRange(r *googlesql.ParseLocationRange) (lsp.Range, bool) {
	if r == nil {
		return lsp.Range{}, false
	}

	return p.OffsetsToLspRange(parseLocStart(r), parseLocEnd(r))
}

// OffsetsToLspRange converts byte offsets in FixedSrc to an LSP range in Src.
func (p ParsedFile) OffsetsToLspRange(startOff, endOff int) (lsp.Range, bool) {
	if startOff < 0 || endOff < 0 || startOff > endOff || endOff > len(p.FixedSrc) {
		return lsp.Range{}, false
	}

	return lsp.Range{
		Start: p.toLspPoint(toSrcOffset(p.edits, startOff)),
		End:   p.toLspPoint(toSrcEnd(p.edits, endOff)),
	}, true
}

// toLspPoint converts a byte offset in Src to an LSP position.
func (p ParsedFile) toLspPoint(offset int) lsp.Position {
	offset = min(max(offset, 0), len(p.Src))
	toEndText := p.Src[:offset]
	line := strings.Count(toEndText, "\n")
	newLineInd := strings.LastIndex(toEndText, "\n")
	var char int
	if newLineInd == -1 {
		char = len(toEndText)
	} else {
		char = len(toEndText[newLineInd:]) - 1
	}
	return lsp.Position{
		Line:      line,
		Character: char,
	}
}

func (p ParsedFile) FindTargetStatementNode(termOffset int) (googlesql.ASTStatementNode, bool) {
	stmts := collectStatements(p.Node)

	if len(stmts) == 1 {
		return stmts[0], true
	}

	for _, stmt := range stmts {
		if containsOffset(stmt, termOffset) {
			return stmt, true
		}
	}

	return nil, false
}

// isInsideScriptControl reports whether the innermost statement containing
// offset is a script statement such as SET or IF.
func (p ParsedFile) isInsideScriptControl(offset int) bool {
	inside := false
	// collectStatements is in pre-order, so the last statement containing the
	// offset is the innermost one.
	for _, s := range collectStatements(p.Node) {
		if containsOffset(s, offset) {
			kind, _ := s.NodeKind()
			inside = isScriptControlStatement(kind)
		}
	}
	return inside
}

func containsOffset(stmt googlesql.ASTStatementNode, offset int) bool {
	loc, _ := stmt.GetParseLocationRange()
	if loc == nil {
		return false
	}
	startOffset := parseLocStart(loc)
	endOffset := parseLocEnd(loc)
	if startOffset < 0 || endOffset < 0 {
		return false
	}
	return startOffset <= offset && offset <= endOffset
}

func (p *ParsedFile) FindTargetAnalyzeOutput(termOffset int) (*googlesql.AnalyzerOutput, bool) {
	// A script statement such as SET has no analyzer output of its own, and
	// returning another statement's output would resolve names in the wrong scope.
	if p.isInsideScriptControl(termOffset) {
		return nil, false
	}

	// Candidates come from the AST rather than Statements, because Statements
	// can miss trailing statements and would make another statement look like
	// the only candidate.
	candidates := make([]googlesql.ASTStatementNode, 0)
	for _, s := range collectStatements(p.Node) {
		kind, _ := s.NodeKind()
		if kind == googlesql.ASTNodeKindAstVariableDeclaration || isScriptControlStatement(kind) {
			continue
		}
		candidates = append(candidates, s)
	}

	var target googlesql.ASTStatementNode
	if len(candidates) == 1 {
		target = candidates[0]
	} else {
		for _, s := range candidates {
			if containsOffset(s, termOffset) {
				target = s
				break
			}
		}
	}
	if target == nil {
		return nil, false
	}

	// Each walk wraps AST nodes in new Go values, so statements are matched by
	// their location instead of by identity.
	start, end := locStart(target), locEnd(target)
	for _, s := range p.Statements {
		if locStart(s.Stmt) == start && locEnd(s.Stmt) == end {
			return s.Output, s.Output != nil
		}
	}
	return nil, false
}

func (p *ParsedFile) FindIncompleteColumnName(pos lsp.Position) string {
	// Errors and their TermLength are in Src, so the cursor is not mapped.
	targetTerm := positionToByteOffset(p.Src, pos)

	for _, err := range p.Errors {
		startOffset := positionToByteOffset(p.Src, err.Position)
		if startOffset <= targetTerm && targetTerm <= startOffset+err.TermLength {
			return err.IncompleteColumnName
		}
	}
	return ""
}

type FixOffset struct {
	Offset int
	Length int

	// edits is exactly what the fix-up rewrote, and offsets are mapped only
	// through it. Offset and Length cannot be inverted exactly, because fix-ups
	// anchor Offset at either the start or the end of what they rewrote, and
	// the rewritten text cannot be recovered by diffing either, since a
	// replacement may repeat the text around it.
	edits []sourceEdit
}

// sourceEdit is one rewrite of the source: the bytes [start, start+oldLen)
// were replaced with newLen bytes.
type sourceEdit struct {
	start  int
	oldLen int
	newLen int
}

// toSrcOffset maps a byte offset in the text after edits back to the text
// before them. Edits build on each other, so they are undone in reverse.
// At a deletion, the offset maps after the deleted text, as the start of
// whatever follows it.
func toSrcOffset(edits []sourceEdit, offset int) int {
	return undoEdits(edits, offset, false)
}

// toSrcEnd is toSrcOffset for the end of a term. At a deletion, the end maps
// before the deleted text, so that the term does not swallow it.
func toSrcEnd(edits []sourceEdit, offset int) int {
	return undoEdits(edits, offset, true)
}

func undoEdits(edits []sourceEdit, offset int, isEnd bool) int {
	for i := len(edits) - 1; i >= 0; i-- {
		e := edits[i]
		end := e.start + e.newLen
		switch {
		case offset > end, offset == end && !(isEnd && e.newLen == 0):
			offset += e.oldLen - e.newLen
		case offset > e.start+e.oldLen:
			// Inside text a fix-up added; there is no original counterpart.
			offset = e.start + e.oldLen
		}
	}
	return offset
}

// toFixedOffset maps a byte offset in the text before edits to the text after
// them. An offset at an insertion stays before the inserted text, so that the
// cursor keeps pointing at what the user wrote.
func toFixedOffset(edits []sourceEdit, offset int) int {
	for _, e := range edits {
		switch {
		case offset <= e.start:
		case offset >= e.start+e.oldLen:
			offset += e.newLen - e.oldLen
		default:
			// Inside rewritten text, which may be shorter than the original.
			offset = e.start + min(offset-e.start, e.newLen)
		}
	}
	return offset
}

// insideInsertedText reports whether offset in text, the text after edits,
// points at text a fix-up inserted rather than at text the user wrote. An
// error there is caused by the fix-up, which only inserts where the user has
// not finished typing, so the user's own error is already reported at the
// insertion point.
func insideInsertedText(text string, edits []sourceEdit, offset int) bool {
	offset = min(max(offset, 0), len(text))
	// An error at the end of the script is about the last code before it,
	// which may be inserted text followed by the user's trailing comment.
	codeEnd := len(strings.TrimRightFunc(text[:beforeTrailingComments(text, offset)], unicode.IsSpace))
	return insertedAt(edits, offset) || insertedAt(edits, codeEnd)
}

func insertedAt(edits []sourceEdit, offset int) bool {
	for i := len(edits) - 1; i >= 0; i-- {
		e := edits[i]
		end := e.start + e.newLen
		switch {
		case e.oldLen == 0 && e.newLen > 0 && e.start <= offset && offset <= end:
			return true
		case offset > end:
			offset += e.oldLen - e.newLen
		case offset > e.start+e.oldLen:
			offset = e.start + e.oldLen
		}
	}
	return false
}

// errorToSrc converts err, whose position is in errSrc (src after edits), to
// a position in src.
func errorToSrc(src, errSrc string, edits []sourceEdit, err Error) Error {
	start, ok := errorByteOffset(errSrc, err.Position)
	if !ok {
		return err
	}
	srcStart := toSrcOffset(edits, start)
	pos, ok := byteOffsetToPosition(src, srcStart)
	if !ok {
		return err
	}
	if err.TermLength > 0 {
		err.TermLength = toSrcEnd(edits, start+err.TermLength) - srcStart
	}
	err.Position = pos
	return err
}

type Error struct {
	Msg                  string
	Position             lsp.Position
	TermLength           int
	IncompleteColumnName string
	Severity             lsp.DiagnosticSeverity
}

func (e Error) Error() string {
	return fmt.Sprintf("%d:%d: %s", e.Position.Line, e.Position.Character, e.Msg)
}

func parseZetaSQLError(src string, err error) Error {
	errStr := err.Error()

	// extract position information like "... [at 1:28]". A message can carry
	// several locations (e.g. "... [at 1:8]; ... [at 2:3]"), and the first one
	// belongs to the error itself.
	loc := zetaSQLLocationRegex.FindStringSubmatchIndex(errStr)
	if loc == nil {
		return Error{Msg: errStr}
	}
	line, err1 := strconv.Atoi(errStr[loc[2]:loc[3]])
	col, err2 := strconv.Atoi(errStr[loc[4]:loc[5]])
	if err1 != nil || err2 != nil {
		return Error{Msg: errStr}
	}
	pos := lsp.Position{Line: line - 1, Character: zetaSQLColumnToByteOffset(src, line-1, col)}

	// Trim position information
	errStr = strings.TrimSpace(errStr[:loc[0]])
	return Error{Msg: errStr, Position: pos}
}

// addInformationToNotBoolConditionError marks the condition quoted in the
// error as its term.
func addInformationToNotBoolConditionError(parsedErr Error) Error {
	m := notBoolConditionErrorRegex.FindStringSubmatch(parsedErr.Msg)
	if m == nil {
		return parsedErr
	}
	parsedErr.TermLength = len(m[1])
	return parsedErr
}

// notBoolConditionErrorInSrc quotes the condition from src, since the analyzed
// text may have declared variables replaced with their values.
func notBoolConditionErrorInSrc(src string, srcErr Error) Error {
	start, ok := errorByteOffset(src, srcErr.Position)
	if !ok || srcErr.TermLength <= 0 || start+srcErr.TermLength > len(src) {
		return srcErr
	}
	srcErr.Msg = fmt.Sprintf("Cannot coerce expression %s to type BOOL", src[start:start+srcErr.TermLength])
	return srcErr
}

// zetaSQLLocation formats a byte offset in src as GoogleSQL's "[at line:column]",
// the inverse of zetaSQLColumnToByteOffset.
func zetaSQLLocation(src string, offset int) string {
	lineStart := strings.LastIndex(src[:offset], "\n") + 1
	line := strings.Count(src[:lineStart], "\n") + 1
	col := 1
	for _, r := range src[lineStart:offset] {
		if r == '\t' {
			col = ((col-1)/8+1)*8 + 1
			continue
		}
		col++
	}
	return fmt.Sprintf("[at %d:%d]", line, col)
}

// zetaSQLColumnToByteOffset converts a 1-based GoogleSQL column into a byte
// offset within the line. GoogleSQL counts columns in code points and expands
// tabs to the next multiple of 8, so the column cannot be used as a byte offset
// directly.
func zetaSQLColumnToByteOffset(src string, line, col int) int {
	lines := strings.Split(src, "\n")
	if line < 0 || line >= len(lines) {
		return col - 1
	}

	current := 1
	for i, r := range lines[line] {
		next := current + 1
		if r == '\t' {
			next = ((current-1)/8+1)*8 + 1
		}
		if col < next {
			return i
		}
		current = next
	}
	return len(lines[line])
}

// fixDot replaces the last dot of a word with a comma. and change word to same length of 1.
// SELECT aaa. FROM table
//
// becomes
//
// SELECT true FROM table
func fixDot(src string) (fixedSrc string, errs []Error, fixOffsets []FixOffset) {
	fixedBuilder := strings.Builder{}
	currentOffset := 0
	// currentOffset counts the rewritten text, so the growth so far is subtracted
	// to locate a word in src.
	growth := 0
	errs = make([]Error, 0, 1)
	fixOffsets = make([]FixOffset, 0, 1)
	// Words are searched in code, which has the same byte layout as src, so
	// that a word ending with a dot inside a block comment is left alone.
	codeLines := strings.Split(blankBlockComments(src), "\n")
	lineNo := 0
	for line := range strings.SplitSeq(src, "\n") {
		code := codeLines[lineNo]
		lineNo++
		ind := len(code)
		// NOTE: to skip comments
		if i := strings.Index(code, "--"); i != -1 {
			ind = i
		}
		if i := strings.Index(code, "//"); i != -1 && i < ind {
			ind = i
		}

		// src is a word that ends with a dot.
		loc := lastDotRegex.FindIndex([]byte(code[:ind] + " "))
		if len(loc) != 2 {
			fixedBuilder.WriteString(line + "\n")
			currentOffset += len(line) + 1
			continue
		}

		for len(loc) == 2 {
			// sql.Rawtext[loc[1]] is a space, a semicolon, or end of file.
			loc[0] = hyphenatedTablePathStart(code, loc[0])
			srcStart := currentOffset + loc[0]
			targetWord := line[loc[0] : loc[1]-1]
			line = line[:loc[0]] + "true" + line[loc[1]-1:]
			code = code[:loc[0]] + "true" + code[loc[1]-1:]
			pos, _ := byteOffsetToPosition(src, srcStart-growth)
			errs = append(errs, Error{
				Msg:                  fmt.Sprintf("Unrecognized name: %s", targetWord),
				Position:             pos,
				TermLength:           len(targetWord),
				IncompleteColumnName: targetWord,
			})
			fixOffsets = append(fixOffsets, FixOffset{
				Offset: srcStart + len(targetWord),
				Length: len("true") - len(targetWord),
				edits:  []sourceEdit{{start: srcStart, oldLen: len(targetWord), newLen: len("true")}},
			})
			growth += len("true") - len(targetWord)

			oldLoc := loc
			ind -= len(targetWord) - len("true")
			loc = lastDotRegex.FindIndex([]byte(code[:ind] + " "))
			if len(loc) == 2 && loc[0] == oldLoc[0] {
				break
			}
		}
		fixedBuilder.WriteString(line + "\n")
		currentOffset += len(line) + 1
	}

	// Every line, including the last one, was written with "\n", which is one
	// more than src has.
	fixed := strings.TrimSuffix(fixedBuilder.String(), "\n")
	return fixed, errs, fixOffsets
}

// blankBlockComments replaces each byte inside /* */ comments, other than line
// breaks, with a space, so that the result keeps the byte offsets and lines of
// src. Quoted text and line comments are skipped, so that "/*" inside them does
// not open a comment.
func blankBlockComments(src string) string {
	b := []byte(src)
	for i := 0; i < len(src); {
		switch {
		case src[i] == '\'' || src[i] == '"' || src[i] == '`':
			i, _ = bqparser.SkipQuoted(src, i)
		case strings.HasPrefix(src[i:], "/*"):
			end := bqparser.SkipComment(src, i)
			for ; i < end; i++ {
				if b[i] != '\n' && b[i] != '\r' {
					b[i] = ' '
				}
			}
		default:
			if next := bqparser.SkipComment(src, i); next != i {
				i = next
			} else {
				i++
			}
		}
	}
	return string(b)
}

// hyphenatedTablePathStart extends the start of a word ending with a dot back over
// a hyphenated project ID, such as `FROM my-project.dataset.`. An unquoted
// identifier can contain hyphens only in a table path, so the word is extended
// only after FROM or JOIN; otherwise `1-t.` would be taken as one word.
func hyphenatedTablePathStart(line string, start int) int {
	i := start
	for i > 0 && (line[i-1] == '-' || line[i-1] == '.' || isIdentifierByte(line[i-1])) {
		i--
	}
	if i == start {
		return start
	}

	words := strings.Fields(line[:i])
	if len(words) == 0 {
		return start
	}
	lastWord := words[len(words)-1]
	if !strings.EqualFold(lastWord, "FROM") && !strings.EqualFold(lastWord, "JOIN") {
		return start
	}
	return i
}

// fix SELECT list must not be empty
//
//	SELECT FROM table
//
// becomes
//
//	SELECT 1 FROM table
func fixSelectListMustNotBeEmptyStatement(src string, parsedErr Error) (fixedSrc string, err Error, fixOffsets []FixOffset) {
	errOffset, ok := errorByteOffset(src, parsedErr.Position)
	if !ok {
		return src, parsedErr, nil
	}
	return src[:errOffset] + "1 " + src[errOffset:], parsedErr, []FixOffset{
		{
			Offset: errOffset,
			Length: len("1 "),
			edits:  []sourceEdit{{start: errOffset, newLen: len("1 ")}},
		},
	}
}

func fixUnexpectedEndOfScript(src string, parsedErr Error) (fixedSrc string, err Error, fixOffsets []FixOffset) {
	targets := []struct {
		keywords []string
		fixFunx  func(src string, err Error, keyword []string) (string, Error, []FixOffset)
	}{
		{
			keywords: []string{
				"GROUP BY", "ORDER BY", "GROUP", "ORDER", "AND", "OR",
				"UNION ALL", "UNION DISTINCT", "UNION", "INTERSECT DISTINCT", "EXCEPT DISTINCT",
			},
			fixFunx: fixUnexpectedEndOfScriptWithDeletion,
		},
		{
			keywords: []string{"WHERE", "ON"},
			fixFunx:  fixUnexpectedEndOfScriptWithCondition,
		},
	}

	for _, target := range targets {
		if _, ok := trailingKeyword(src[:beforeTrailingComments(src, len(src))], target.keywords); ok {
			return target.fixFunx(src, parsedErr, target.keywords)
		}
	}

	return src, parsedErr, nil
}

// trailingKeyword returns where the keyword that code ends with starts.
// Keywords are case-insensitive and their words may be separated by any
// whitespace, but they must not be the end of a longer word.
func trailingKeyword(code string, keywords []string) (int, bool) {
	for _, keyword := range keywords {
		if start, ok := keywordSuffixStart(code, keyword); ok {
			return start, true
		}
	}
	return 0, false
}

func keywordSuffixStart(code, keyword string) (int, bool) {
	start := len(code)
	words := strings.Fields(keyword)
	for i := len(words) - 1; i >= 0; i-- {
		end := len(strings.TrimRightFunc(code[:start], unicode.IsSpace))
		start = end - len(words[i])
		if start < 0 || !strings.EqualFold(code[start:end], words[i]) {
			return 0, false
		}
		if start > 0 && isIdentifierByte(code[start-1]) {
			return 0, false
		}
	}
	return start, true
}

// endInsertionPoint returns where a fix-up inserts text to complete what the
// user is typing at offset: after the whitespace that follows it, so that a
// cursor there stays before the inserted text, and before any comment.
func endInsertionPoint(src string, offset int) int {
	for offset < len(src) && unicode.IsSpace(rune(src[offset])) {
		offset++
	}
	return beforeTrailingComments(src, offset)
}

// beforeTrailingComments returns where the comments that precede offset, with
// only whitespace between them and offset, start, or offset when there are
// none. Text a fix-up puts at the end of the script must go there, because
// after a line comment it would be part of the comment.
func beforeTrailingComments(src string, offset int) int {
	commentStart := -1
	for i := 0; i < offset; {
		switch {
		case unicode.IsSpace(rune(src[i])):
			i++
		case src[i] == '\'' || src[i] == '"' || src[i] == '`':
			i, _ = bqparser.SkipQuoted(src, i)
			commentStart = -1
		default:
			if next := bqparser.SkipComment(src, i); next != i {
				if commentStart == -1 {
					commentStart = i
				}
				i = next
			} else {
				i++
				commentStart = -1
			}
		}
	}
	if commentStart == -1 {
		return offset
	}
	return commentStart
}

// `WITH t1 AS (SELECT 1)` return error Expected "(" or "," or keyword SELECT but got end of script
func fixOnlyWithClauseSyntaxError(src string, parsedErr Error) (fixedSrc string, err Error, fixOffsets []FixOffset) {
	errOffset, ok := errorByteOffset(src, parsedErr.Position)
	if !ok {
		return src, parsedErr, nil
	}
	errOffset = endInsertionPoint(src, errOffset)
	return src[:errOffset] + " SELECT 1 " + src[errOffset:], parsedErr, []FixOffset{
		{
			Offset: errOffset,
			Length: len(" SELECT 1 "),
			edits:  []sourceEdit{{start: errOffset, newLen: len(" SELECT 1 ")}},
		},
	}
}

// fixMissingSelectListComma inserts the comma that the user has not typed yet after a
// word in front of an existing select list item on the next line.
//
//	SELECT id,
//	  na
//	  COALESCE(name, '')
//
// becomes
//
//	SELECT id,
//	  na,
//	  COALESCE(name, '')
func fixMissingSelectListComma(src string, parsedErr Error) (fixedSrc string, fixOffsets []FixOffset) {
	errOffset, ok := errorByteOffset(src, parsedErr.Position)
	if !ok {
		return src, nil
	}
	start, end, ok := bqparser.SelectListItemOnNextLine(src, uint(errOffset))
	if !ok || isReservedKeyword(src[start:end]) {
		return src, nil
	}
	insertAt := int(end)
	return src[:insertAt] + "," + src[insertAt:], []FixOffset{{
		Offset: insertAt,
		Length: len(","),
		edits:  []sourceEdit{{start: insertAt, newLen: len(",")}},
	}}
}

// aliasOnNextLine reports whether the expression spanning [start, end) is a whole select
// column whose alias, written without AS, is on a following line.
func aliasOnNextLine(src string, node googlesql.ASTNode, start, end int) bool {
	col, ok := SearchAstNode[*googlesql.ASTSelectColumn](node, start)
	if !ok {
		return false
	}
	expr, err := col.Expression()
	if err != nil || expr == nil || locStart(expr) != start || locEnd(expr) != end {
		return false
	}
	alias, err := col.Alias()
	if err != nil || alias == nil {
		return false
	}
	aliasStart := locStart(alias)
	if aliasStart < end || aliasStart > len(src) {
		return false
	}
	// An alias written with AS is meant as one, and dropping it would leave AS
	// at the start of the next column.
	if id, err := alias.Identifier(); err != nil || id == nil || locStart(id) != aliasStart {
		return false
	}
	gap := src[end:aliasStart]
	return strings.TrimSpace(gap) == "" && strings.Contains(gap, "\n")
}

// The analyzer's options, because some keywords such as QUALIFY are reserved only
// with the language features enabled.
var reservedKeywordOptions = sync.OnceValues(newLanguageOptions)

// isReservedKeyword reports false when the keywords cannot be looked up, so that the
// caller treats word as an identifier as before.
func isReservedKeyword(word string) bool {
	opts, err := reservedKeywordOptions()
	if err != nil {
		return false
	}
	reserved, err := opts.IsReservedKeyword(strings.ToUpper(word))
	return err == nil && reserved
}

// pipeOperatorKeywords are the words that can follow "|>" before the operator
// takes its arguments.
var pipeOperatorKeywords = map[string]bool{
	"AGGREGATE": true, "ALL": true, "AS": true, "ASSERT": true, "BY": true,
	"CALL": true, "CROSS": true, "DISTINCT": true, "DROP": true, "EXCEPT": true,
	"EXTEND": true, "FULL": true, "GROUP": true, "INNER": true, "INTERSECT": true,
	"JOIN": true, "LEFT": true, "LIMIT": true, "ORDER": true, "OUTER": true,
	"PIVOT": true, "RENAME": true, "RIGHT": true, "SELECT": true, "SET": true,
	"TABLESAMPLE": true, "UNION": true, "UNPIVOT": true, "WHERE": true, "WINDOW": true,
}

// fix the pipe operator that has no complete operation by deleting it
//
//	FROM table |> SELECT
//
// becomes
//
//	FROM table
func fixIncompletePipeOperator(src string, parsedErr Error) (fixedSrc string, fixOffsets []FixOffset) {
	errOffset, ok := errorByteOffset(src, parsedErr.Position)
	if !ok {
		return src, nil
	}

	pipeOffset := -1
	for _, offset := range pipeOperatorOffsets(src) {
		if offset < errOffset {
			pipeOffset = offset
		}
	}
	if pipeOffset == -1 {
		return src, nil
	}

	// Keywords are only taken from the line of the pipe operator, because a
	// keyword on a following line may start the next statement.
	deleteEnd := pipeOffset + len("|>")
	for {
		wordStart := deleteEnd
		for wordStart < len(src) && (src[wordStart] == ' ' || src[wordStart] == '\t') {
			wordStart++
		}
		wordEnd := wordStart
		for wordEnd < len(src) && isIdentifierByte(src[wordEnd]) {
			wordEnd++
		}
		if wordEnd == wordStart || !pipeOperatorKeywords[strings.ToUpper(src[wordStart:wordEnd])] {
			break
		}
		deleteEnd = wordEnd
	}

	// Only delete the operator when what follows can end the query, so that
	// the rest of the file is kept as it is.
	next := skipSpacesAndComments(src, deleteEnd)
	endsQuery := next == len(src) || src[next] == ';' || src[next] == ')' || strings.HasPrefix(src[next:], "|>")
	if !endsQuery || errOffset > next {
		return src, nil
	}

	fixedSrc = src[:pipeOffset] + src[deleteEnd:]
	return fixedSrc, []FixOffset{
		{
			Offset: deleteEnd,
			Length: pipeOffset - deleteEnd,
			edits:  []sourceEdit{{start: pipeOffset, oldLen: deleteEnd - pipeOffset}},
		},
	}
}

// fix the pipe operator that follows a WITH clause by reading the last CTE, so
// that the pipe operators can be analyzed while the user is typing them
//
//	WITH a AS (SELECT 1) |> SELECT
//
// becomes
//
//	WITH a AS (SELECT 1) FROM a |> SELECT
func fixPipeOperatorAfterWithClause(src string, parsedErr Error) (fixedSrc string, fixOffsets []FixOffset) {
	errOffset, ok := errorByteOffset(src, parsedErr.Position)
	if !ok || !strings.HasPrefix(src[errOffset:], "|>") {
		return src, nil
	}
	name, ok := lastCTEName(src[:errOffset])
	if !ok {
		return src, nil
	}
	insert := "FROM " + name + " "
	return src[:errOffset] + insert + src[errOffset:], []FixOffset{
		{
			Offset: errOffset,
			Length: len(insert),
			edits:  []sourceEdit{{start: errOffset, newLen: len(insert)}},
		},
	}
}

// lastCTEName returns the name of the CTE "name AS (query)" that code ends with.
func lastCTEName(code string) (string, bool) {
	type token struct {
		text string
		// match is the index of the matching "(" for ")".
		match int
	}
	var tokens []token
	var opens []int
	for i := 0; i < len(code); {
		switch {
		case unicode.IsSpace(rune(code[i])):
			i++
		case code[i] == '\'' || code[i] == '"' || code[i] == '`':
			end, _ := bqparser.SkipQuoted(code, i)
			tokens = append(tokens, token{text: code[i:end]})
			i = end
		case code[i] == '(':
			opens = append(opens, len(tokens))
			tokens = append(tokens, token{text: "("})
			i++
		case code[i] == ')':
			match := -1
			if len(opens) > 0 {
				match = opens[len(opens)-1]
				opens = opens[:len(opens)-1]
			}
			tokens = append(tokens, token{text: ")", match: match})
			i++
		case isIdentifierByte(code[i]):
			end := i
			for end < len(code) && isIdentifierByte(code[end]) {
				end++
			}
			tokens = append(tokens, token{text: code[i:end]})
			i = end
		default:
			if next := bqparser.SkipComment(code, i); next != i {
				i = next
			} else {
				tokens = append(tokens, token{text: code[i : i+1]})
				i++
			}
		}
	}

	if len(tokens) == 0 || tokens[len(tokens)-1].text != ")" {
		return "", false
	}
	open := tokens[len(tokens)-1].match
	if open < 2 || !strings.EqualFold(tokens[open-1].text, "AS") {
		return "", false
	}
	name := tokens[open-2].text
	if name[0] != '`' && !isIdentifierByte(name[0]) {
		return "", false
	}
	return name, true
}

func isIdentifierByte(b byte) bool {
	return b == '_' || ('a' <= b && b <= 'z') || ('A' <= b && b <= 'Z') || ('0' <= b && b <= '9')
}

// pipeOperatorOffsets returns the offsets of "|>" outside of comments and
// quoted strings or identifiers.
func pipeOperatorOffsets(src string) []int {
	var offsets []int
	for i := 0; i < len(src); {
		switch {
		case strings.HasPrefix(src[i:], "|>"):
			offsets = append(offsets, i)
			i += len("|>")
		case src[i] == '\'' || src[i] == '"' || src[i] == '`':
			i, _ = bqparser.SkipQuoted(src, i)
		default:
			if next := bqparser.SkipComment(src, i); next != i {
				i = next
			} else {
				i++
			}
		}
	}
	return offsets
}

func skipSpacesAndComments(src string, start int) int {
	i := start
	for i < len(src) {
		if unicode.IsSpace(rune(src[i])) {
			i++
			continue
		}
		next := bqparser.SkipComment(src, i)
		if next == i {
			break
		}
		i = next
	}
	return i
}

// fix Unexpected end of script with deletion
//
//	SELECT * FROM table GROUP BY
//	SELECT * FROM table UNION ALL
//
// becomes
//
//	SELECT * FROM table
func fixUnexpectedEndOfScriptWithDeletion(src string, parsedErr Error, targetUnexpectedEndKeyword []string) (fixedSrc string, err Error, fixOffsets []FixOffset) {
	errOffset, ok := errorByteOffset(src, parsedErr.Position)
	if !ok {
		return src, parsedErr, nil
	}

	code := src[:beforeTrailingComments(src, errOffset)]
	targetOffset, ok := trailingKeyword(code, targetUnexpectedEndKeyword)
	if !ok {
		return src, parsedErr, nil
	}

	// Only the end is trimmed, so that the text before the keyword keeps its offsets.
	kept := strings.TrimRightFunc(src[:targetOffset], unicode.IsSpace)
	// A pipe operator left without its operation is another syntax error, so
	// it is removed together with the keyword.
	if trimmed := strings.TrimSuffix(kept, "|>"); trimmed != kept {
		kept = strings.TrimRightFunc(trimmed, unicode.IsSpace)
	}
	fixedSrc = kept + src[len(code):]
	return fixedSrc, parsedErr, []FixOffset{
		{
			Offset: errOffset,
			Length: -(len(code) - len(kept)),
			edits:  []sourceEdit{{start: len(kept), oldLen: len(code) - len(kept)}},
		},
	}
}

// fix Unexpected end of script with condition
//
//	SELECT * FROM table WHERE
//
// becomes
//
//	SELECT * FROM table WHERE 1=1
func fixUnexpectedEndOfScriptWithCondition(src string, parsedErr Error, targetUnexpectedEndKeyword []string) (fixedSrc string, err Error, fixOffsets []FixOffset) {
	errOffset, ok := errorByteOffset(src, parsedErr.Position)
	if !ok {
		return src, parsedErr, nil
	}

	errOffset = endInsertionPoint(src, errOffset)
	if _, ok := trailingKeyword(src[:errOffset], targetUnexpectedEndKeyword); !ok {
		return src, parsedErr, nil
	}

	insertStr := " 1=1"
	fixedSrc = src[:errOffset] + insertStr + src[errOffset:]
	return fixedSrc, parsedErr, []FixOffset{
		{
			Offset: errOffset,
			Length: len(insertStr),
			edits:  []sourceEdit{{start: errOffset, newLen: len(insertStr)}},
		},
	}
}

func addInformationToUnrecognizedNameError(src string, parsedErr Error) Error {
	ind := strings.Index(parsedErr.Msg, "Unrecognized name: ")
	unrecognizedName := strings.TrimSpace(parsedErr.Msg[ind+len("Unrecognized name: "):])

	// For the folowing error message:
	// 	INVALID_ARGUMENT: Unrecognized name: invalid_column; Did you mean valid_column?
	if ind := strings.Index(unrecognizedName, ";"); ind != -1 {
		unrecognizedName = unrecognizedName[:ind]
	}

	parsedErr.TermLength = len(unrecognizedName)
	parsedErr.IncompleteColumnName = unrecognizedName
	return parsedErr
}

func fixDeclarationError(src string, parsedErr Error, defaultVal string) (fixedSrc string, fixOffsets []FixOffset) {
	errOffset, ok := errorByteOffset(src, parsedErr.Position)
	if !ok || errOffset == 0 || errOffset == len(src) {
		return src, nil
	}

	replaceOffset := errOffset + parsedErr.TermLength - len(parsedErr.IncompleteColumnName)
	if replaceOffset < 0 || replaceOffset+len(parsedErr.IncompleteColumnName) > len(src) {
		return src, nil
	}
	rest := src[replaceOffset+len(parsedErr.IncompleteColumnName):]
	defaultVal = keepApartFromDot(defaultVal, rest)
	fixedSrc = src[:replaceOffset] + defaultVal + rest
	fixOffset := FixOffset{
		Offset: errOffset,
		Length: -parsedErr.TermLength + len(defaultVal),
		edits:  []sourceEdit{{start: replaceOffset, oldLen: len(parsedErr.IncompleteColumnName), newLen: len(defaultVal)}},
	}

	return fixedSrc, []FixOffset{fixOffset}
}

// fix field name <name> error.
//
//	SELECT t.unexist_column FROM table AS t
//
// becomes
//
//	SELECT 1 FROM table AS t
func fixErrorToLiteral(src string, parsedErr Error) (fixedSrc string, fixOffsets []FixOffset) {
	return fixErrorToExpression(src, parsedErr, "1")
}

func fixErrorToExpression(src string, parsedErr Error, expr string) (fixedSrc string, fixOffsets []FixOffset) {
	errOffset, ok := errorByteOffset(src, parsedErr.Position)
	if !ok || errOffset == 0 || errOffset == len(src) {
		return src, nil
	}

	replaceOffset := errOffset + parsedErr.TermLength - len(parsedErr.IncompleteColumnName)
	if replaceOffset < 0 || replaceOffset+len(parsedErr.IncompleteColumnName) > len(src) {
		return src, nil
	}
	// The fields of an unknown name are unknown too. Leaving them would also make `1.x`,
	// where `1.` is lexed as a floating point literal.
	replaceEnd := replaceOffset + len(parsedErr.IncompleteColumnName)
	replaceEnd += fieldAccessLen(src[replaceEnd:])
	fixedSrc = src[:replaceOffset] + expr + src[replaceEnd:]
	fixOffset := FixOffset{
		Offset: errOffset,
		Length: -(replaceEnd - replaceOffset) + len(expr),
		edits:  []sourceEdit{{start: replaceOffset, oldLen: replaceEnd - replaceOffset, newLen: len(expr)}},
	}

	return fixedSrc, []FixOffset{fixOffset}
}

// fieldAccessLen returns the length of the field accesses such as `.x.y` or `.*` that
// rest starts with. It returns 0 if a subscript follows them, since the placeholder
// would then be subscripted.
func fieldAccessLen(rest string) int {
	n := fieldPathLen(rest)
	if strings.HasPrefix(rest[n:], "[") {
		return 0
	}
	return n
}

func fieldPathLen(rest string) int {
	i := 0
	for strings.HasPrefix(rest[i:], ".") {
		j := i + len(".")
		switch {
		case strings.HasPrefix(rest[j:], "*"):
			return j + len("*")
		case strings.HasPrefix(rest[j:], "`"):
			end, closed := bqparser.SkipQuoted(rest, j)
			if !closed {
				return i
			}
			j = end
		default:
			for j < len(rest) && isIdentifierByte(rest[j]) {
				j++
			}
			if j == i+len(".") {
				return i
			}
		}
		i = j
	}
	return i
}

// keepApartFromDot parenthesizes expr if rest starts with a dot, because `1.` would be
// lexed as a floating point literal.
func keepApartFromDot(expr, rest string) string {
	if !strings.HasPrefix(rest, ".") {
		return expr
	}
	return "(" + expr + ")"
}

// fixSubstitutedNullArgument replaces the NULL that a fix-up substituted for an unknown
// function argument with 1, for the arguments such as the offset of LAG that must be non-NULL.
// A NULL that the user wrote is left as is, because the error is the user's.
func fixSubstitutedNullArgument(src, fixedSrc string, edits []sourceEdit, parsedErr Error) (string, []FixOffset) {
	errOffset, ok := errorByteOffset(fixedSrc, parsedErr.Position)
	if !ok {
		return fixedSrc, nil
	}
	if _, ok := substitutedPlaceholder(src, fixedSrc, edits, errOffset, "NULL"); !ok {
		return fixedSrc, nil
	}
	return fixErrorToLiteral(fixedSrc, Error{Position: parsedErr.Position, TermLength: len("NULL"), IncompleteColumnName: "NULL"})
}

// substitutedPlaceholder reports whether placeholder at offset in fixedSrc was substituted
// by a fix-up rather than written by the user, and returns the text it was substituted for.
func substitutedPlaceholder(src, fixedSrc string, edits []sourceEdit, offset int, placeholder string) (string, bool) {
	end := offset + len(placeholder)
	if offset < 0 || end > len(fixedSrc) || !strings.EqualFold(fixedSrc[offset:end], placeholder) {
		return "", false
	}
	srcStart, srcEnd := toSrcOffset(edits, offset), toSrcEnd(edits, end)
	if srcStart < 0 || srcEnd > len(src) || srcStart > srcEnd || strings.EqualFold(src[srcStart:srcEnd], placeholder) {
		return "", false
	}
	return src[srcStart:srcEnd], true
}

// fixSubstitutedOperatorOperand replaces the `1` that a fix-up substituted for an unknown
// operand of an operator such as `=`, IN or BETWEEN with a value of the type of the other
// operands, taken from the error message. A literal NULL does not work here, because
// GoogleSQL rejects it as an operand of a comparison.
//
// declared maps the lowercased names of declared variables to their dummy values. A `1`
// substituted for a declared INT64 variable stands for a value of the declared type, so
// the error about it is the user's and is left as is.
func fixSubstitutedOperatorOperand(src, fixedSrc string, edits []sourceEdit, node googlesql.ASTNode, parsedErr Error, declared map[string]string) (string, []FixOffset) {
	m := operatorSignatureErrorRegex.FindStringSubmatch(parsedErr.Msg)
	if m == nil {
		return fixedSrc, nil
	}
	op := m[1]
	typeName, ok := operandTypeForOperator(op, m[2])
	if !ok {
		return fixedSrc, nil
	}
	errOffset, ok := errorByteOffset(fixedSrc, parsedErr.Position)
	if !ok {
		return fixedSrc, nil
	}

	// The error is at the start of a binary expression but at the keyword of IN or BETWEEN,
	// so the innermost operator of the kind in the message around it that has a substituted
	// operand is the one. Operators of other kinds may start at the same offset, as `=` in
	// `1 = 1 AND 1`.
	var placeholders []googlesql.ASTExpressionNode
	Walk(node, func(n googlesql.ASTNode) error { //nolint
		operands := operatorOperands(n, op)
		if len(operands) == 0 || locStart(n) > errOffset || errOffset >= locEnd(n) {
			return nil
		}
		var found []googlesql.ASTExpressionNode
		for _, operand := range operands {
			start := locStart(operand)
			if locEnd(operand)-start != len("1") {
				continue
			}
			original, ok := substitutedPlaceholder(src, fixedSrc, edits, start, "1")
			if _, isDeclared := declared[strings.ToLower(original)]; ok && !isDeclared {
				found = append(found, operand)
			}
		}
		if len(found) > 0 {
			placeholders = found
		}
		return nil
	})
	if len(placeholders) == 0 {
		return fixedSrc, nil
	}

	dummy := getDummyValueForTypeName(typeName)
	var fixOffsets []FixOffset
	// From the last one, so that each edit keeps the offsets of the ones before it.
	for _, p := range slices.Backward(placeholders) {
		start := locStart(p)
		fixedSrc = fixedSrc[:start] + dummy + fixedSrc[start+len("1"):]
		fixOffsets = append(fixOffsets, FixOffset{
			Offset: start,
			Length: len(dummy) - len("1"),
			edits:  []sourceEdit{{start: start, oldLen: len("1"), newLen: len(dummy)}},
		})
	}
	return fixedSrc, fixOffsets
}

// operandTypeForOperator returns the type that a placeholder operand of op must have,
// given the argument types listed in the operator signature error.
func operandTypeForOperator(op, argumentTypes string) (string, bool) {
	switch op {
	case "AND", "OR", "NOT":
		return "BOOL", true
	}
	typeName, ok := operandTypeOtherThanPlaceholder(argumentTypes)
	if !ok || op != "IN UNNEST" {
		return typeName, ok
	}
	// The placeholder is compared with the elements of the array.
	elem, ok := strings.CutPrefix(typeName, "ARRAY<")
	if !ok {
		return "", false
	}
	return strings.CutSuffix(elem, ">")
}

// operandTypeOtherThanPlaceholder returns the type in the argument types of an operator
// signature error other than INT64, the type of the `1` placeholder. It fails unless
// exactly one such type is listed, since the operands then do not agree on a type anyway.
func operandTypeOtherThanPlaceholder(argumentTypes string) (string, bool) {
	return onlyTypeOtherThan(argumentTypeList(argumentTypes), "INT64")
}

// argumentTypeList returns the argument types listed in an operator signature error.
func argumentTypeList(argumentTypes string) []string {
	// IN lists the types as `literal INT64 and {STRING, INT64}`.
	list := splitTypeList(strings.NewReplacer(" and {", ", ", "{", "", "}", "").Replace(argumentTypes))
	for i, typ := range list {
		list[i] = strings.TrimPrefix(typ, "literal ")
	}
	return list
}

// onlyTypeOtherThan returns the one type among types other than placeholderType. It fails
// unless exactly one such type is found, since the operands then do not agree on a type anyway.
func onlyTypeOtherThan(types []string, placeholderType string) (string, bool) {
	var typeName string
	for _, name := range types {
		if name == placeholderType || name == typeName {
			continue
		}
		if typeName != "" {
			return "", false
		}
		typeName = name
	}
	return typeName, typeName != ""
}

// operatorOperands returns the operands of node if it is an operator of the kind that op,
// the operator named in a signature error, refers to and its operands must agree on a type,
// which for AND, OR and NOT is BOOL. Arithmetic operators are left out, because their
// operands may differ in type, as in `date - 1`.
func operatorOperands(node googlesql.ASTNode, op string) []googlesql.ASTExpressionNode {
	var operands []googlesql.ASTExpressionNode
	add := func(e googlesql.ASTExpressionNode, err error) {
		if err == nil && e != nil {
			operands = append(operands, e)
		}
	}
	switch n := node.(type) {
	case *googlesql.ASTBinaryExpression:
		switch op {
		case "AND", "OR", "NOT", "IN", "IN UNNEST", "BETWEEN":
			return nil
		}
		switch binOp, _ := n.Op(); binOp {
		case googlesql.ASTBinaryExpressionEnums_OpEq, googlesql.ASTBinaryExpressionEnums_OpNe,
			googlesql.ASTBinaryExpressionEnums_OpNe2, googlesql.ASTBinaryExpressionEnums_OpLt,
			googlesql.ASTBinaryExpressionEnums_OpLe, googlesql.ASTBinaryExpressionEnums_OpGt,
			googlesql.ASTBinaryExpressionEnums_OpGe, googlesql.ASTBinaryExpressionEnums_OpLike,
			googlesql.ASTBinaryExpressionEnums_OpDistinct:
			add(n.Lhs())
			add(n.Rhs())
		}
	case *googlesql.ASTBetweenExpression:
		if op == "BETWEEN" {
			add(n.Lhs())
			add(n.Low())
			add(n.High())
		}
	case *googlesql.ASTInExpression:
		switch op {
		case "IN UNNEST":
			add(n.Lhs())
		case "IN":
			add(n.Lhs())
			if list, err := n.InList(); err == nil && list != nil {
				num, _ := list.NumChildren()
				for i := int32(0); i < num; i++ {
					add(list.List(i))
				}
			}
		}
	case *googlesql.ASTAndExpr:
		if op == "AND" {
			num, _ := n.NumChildren()
			for i := int32(0); i < num; i++ {
				add(n.Conjuncts(i))
			}
		}
	case *googlesql.ASTOrExpr:
		if op == "OR" {
			num, _ := n.NumChildren()
			for i := int32(0); i < num; i++ {
				add(n.Disjuncts(i))
			}
		}
	case *googlesql.ASTUnaryExpression:
		if unaryOp, _ := n.Op(); op == "NOT" && unaryOp == googlesql.ASTUnaryExpressionEnums_OpNot {
			add(n.Operand())
		}
	}
	return operands
}

// fixSubstitutedStructField replaces the placeholders that fix-ups substituted for unknown
// fields of a struct constructor compared by an operator, as x in `(x, 1) = ('a', 1)`, with
// a value of the type of the field at the same position in the STRUCT types of the error
// message. The fields are found by where the fix-ups substituted text, not by the text of
// the placeholder, which differs between fix-ups.
//
// A STRUCT type whose number of fields differs from the constructor tells nothing about
// a field, so the error about such a comparison is left as the user's.
func fixSubstitutedStructField(src, fixedSrc string, edits []sourceEdit, node googlesql.ASTNode, parsedErr Error, declared map[string]string) (string, []FixOffset) {
	m := operatorSignatureErrorRegex.FindStringSubmatch(parsedErr.Msg)
	if m == nil {
		return fixedSrc, nil
	}
	op := m[1]
	argumentTypes := argumentTypeList(m[2])
	if op == "IN UNNEST" {
		// The struct is compared with the elements of the array.
		for i, typ := range argumentTypes {
			if elem, ok := strings.CutPrefix(typ, "ARRAY<"); ok {
				argumentTypes[i] = strings.TrimSuffix(elem, ">")
			}
		}
	}
	errOffset, ok := errorByteOffset(fixedSrc, parsedErr.Position)
	if !ok {
		return fixedSrc, nil
	}

	// As in fixSubstitutedOperatorOperand, the innermost operator around the error is the one.
	var fields []substitutedStructField
	Walk(node, func(n googlesql.ASTNode) error { //nolint
		operands := operatorOperands(n, op)
		if len(operands) == 0 || locStart(n) > errOffset || errOffset >= locEnd(n) {
			return nil
		}
		var found []substitutedStructField
		for _, operand := range operands {
			found = append(found, substitutedStructFields(src, edits, operand, nil, declared)...)
		}
		if len(found) > 0 {
			fields = found
		}
		return nil
	})

	var fixOffsets []FixOffset
	// From the last one, so that each edit keeps the offsets of the ones before it.
	for _, f := range slices.Backward(fields) {
		placeholderType, ok := placeholderTypes[strings.ToUpper(fixedSrc[f.start:f.end])]
		if !ok {
			continue
		}
		typeName, ok := structFieldType(argumentTypes, f.path, placeholderType)
		if !ok {
			continue
		}
		dummy := getDummyValueForTypeName(typeName)
		if fixedSrc[f.start:f.end] == dummy {
			continue
		}
		fixedSrc = fixedSrc[:f.start] + dummy + fixedSrc[f.end:]
		fixOffsets = append(fixOffsets, FixOffset{
			Offset: f.start,
			Length: len(dummy) - (f.end - f.start),
			edits:  []sourceEdit{{start: f.start, oldLen: f.end - f.start, newLen: len(dummy)}},
		})
	}
	return fixedSrc, fixOffsets
}

// placeholderTypes maps the placeholders that fix-ups substitute for an unknown name, such
// as `true` in WHERE, to their types, which the error message lists for them.
var placeholderTypes = map[string]string{
	"1":    "INT64",
	"TRUE": "BOOL",
}

// substitutedStructField is a field of a struct constructor that a fix-up substituted,
// spanning [start, end) of the fixed source.
type substitutedStructField struct {
	start, end int
	// path leads from the outermost constructor to the field.
	path []structFieldPosition
}

type structFieldPosition struct {
	index, numFields int
}

// substitutedStructFields returns the fields that fix-ups substituted in the struct
// constructors of expr, at any depth. path leads to expr, and is empty for an operand,
// which fixSubstitutedOperatorOperand handles.
func substitutedStructFields(src string, edits []sourceEdit, expr googlesql.ASTExpressionNode, path []structFieldPosition, declared map[string]string) []substitutedStructField {
	var fieldExprs []googlesql.ASTExpressionNode
	switch e := expr.(type) {
	case *googlesql.ASTStructConstructorWithParens:
		num, _ := e.NumChildren()
		for i := int32(0); i < num; i++ {
			if f, err := e.FieldExpressions(i); err == nil && f != nil {
				fieldExprs = append(fieldExprs, f)
			}
		}
	case *googlesql.ASTStructConstructorWithKeyword:
		if typ, _ := e.StructType(); typ != nil {
			// The fields are coerced to the written types, so no operator signature error is about them.
			return nil
		}
		num, _ := e.NumChildren()
		for i := int32(0); i < num; i++ {
			if arg, err := e.Fields(i); err == nil && arg != nil {
				if f, err := arg.Expression(); err == nil && f != nil {
					fieldExprs = append(fieldExprs, f)
				}
			}
		}
	default:
		if len(path) == 0 {
			return nil
		}
		start, end := locStart(expr), locEnd(expr)
		original, ok := substitutedText(src, edits, start, end)
		// A placeholder substituted for a declared variable stands for a value of the declared type.
		if _, isDeclared := declared[strings.ToLower(original)]; !ok || isDeclared {
			return nil
		}
		return []substitutedStructField{{start: start, end: end, path: path}}
	}

	var fields []substitutedStructField
	for i, f := range fieldExprs {
		fieldPath := append(slices.Clone(path), structFieldPosition{index: i, numFields: len(fieldExprs)})
		fields = append(fields, substitutedStructFields(src, edits, f, fieldPath, declared)...)
	}
	return fields
}

// substitutedText reports whether [start, end) of the text after edits lies in text that
// an edit substituted for what the user wrote, and returns what the user wrote there.
func substitutedText(src string, edits []sourceEdit, start, end int) (string, bool) {
	for i := len(edits) - 1; i >= 0; i-- {
		e := edits[i]
		if e.oldLen > 0 && e.start <= start && end <= e.start+e.newLen {
			srcStart, srcEnd := toSrcOffset(edits[:i], e.start), toSrcEnd(edits[:i], e.start+e.oldLen)
			if srcStart < 0 || srcEnd > len(src) || srcStart > srcEnd {
				return "", false
			}
			return src[srcStart:srcEnd], true
		}
		start, end = toSrcOffset(edits[i:i+1], start), toSrcEnd(edits[i:i+1], end)
	}
	return "", false
}

// structFieldType returns the type of the field at path in the STRUCT types among
// argumentTypes other than placeholderType, the type of the placeholder in the field. As in
// operandTypeOtherThanPlaceholder, it fails unless exactly one such type is found.
func structFieldType(argumentTypes []string, path []structFieldPosition, placeholderType string) (string, bool) {
	var fieldTypes []string
	for _, typ := range argumentTypes {
		if field, ok := structFieldTypeAt(typ, path); ok {
			fieldTypes = append(fieldTypes, field)
		}
	}
	return onlyTypeOtherThan(fieldTypes, placeholderType)
}

func structFieldTypeAt(typ string, path []structFieldPosition) (string, bool) {
	for _, p := range path {
		fields, ok := structFieldTypes(typ)
		if !ok || len(fields) != p.numFields {
			return "", false
		}
		typ = fields[p.index]
	}
	return typ, true
}

// structFieldTypes returns the types of the fields of a STRUCT type, without the names of the fields.
func structFieldTypes(typ string) ([]string, bool) {
	inner, ok := strings.CutPrefix(typ, "STRUCT<")
	if !ok {
		return nil, false
	}
	inner, ok = strings.CutSuffix(inner, ">")
	if !ok {
		return nil, false
	}
	fields := splitTypeList(inner)
	for i, f := range fields {
		// A named field is listed as `a STRING`, and a type has spaces only inside its brackets.
		if sp := strings.IndexByte(f, ' '); sp >= 0 && (strings.IndexByte(f, '<') < 0 || sp < strings.IndexByte(f, '<')) {
			fields[i] = f[sp+1:]
		}
	}
	return fields, true
}

// splitTypeList splits a comma-separated list of types, such as the argument types in an
// operator signature error, leaving the commas inside a type such as STRUCT<INT64, STRING>.
func splitTypeList(types string) []string {
	var list []string
	depth, start := 0, 0
	for i := 0; i <= len(types); i++ {
		if i < len(types) {
			switch types[i] {
			case '<', '(':
				depth++
			case '>', ')':
				depth--
			}
			if types[i] != ',' || depth > 0 {
				continue
			}
		}
		list = append(list, strings.TrimSpace(types[start:i]))
		start = i + 1
	}
	return list
}

// fixSubstitutedArithmeticOperand recovers from an error of an arithmetic, concatenation
// or bitwise operator, one of whose operands is a placeholder that a fix-up substituted
// for an unknown name. Unlike comparisons, the operands of these operators may differ in
// type, as in `date - 1` or `timestamp + INTERVAL 1 DAY`, so the placeholder is replaced
// with a value of the type that the first signature in the error that fits the other
// operand expects. If no signature fits, as for `x + 'a'`, the operator fails whatever
// the unknown name is, so the whole expression is replaced with the placeholder `1`,
// which the fix-ups of the enclosing expression then handle like any other placeholder.
//
// declared maps the lowercased names of declared variables to their dummy values, which
// stand for values of the declared types, so the errors about them are the user's.
func fixSubstitutedArithmeticOperand(src, fixedSrc string, edits []sourceEdit, node googlesql.ASTNode, parsedErr Error, declared map[string]string) (string, []FixOffset) {
	errOffset, ok := errorByteOffset(fixedSrc, parsedErr.Position)
	if !ok {
		return fixedSrc, nil
	}
	m := operatorSignatureErrorRegex.FindStringSubmatch(parsedErr.Msg)
	isLiteralCastError := literalCastErrorRegex.MatchString(parsedErr.Msg)

	var expr googlesql.ASTNode
	var placeholder googlesql.ASTExpressionNode
	// The index of the placeholder operand.
	var k int
	// The innermost operator around the error is the one, which the Walk visits last.
	Walk(node, func(n googlesql.ASTNode) error { //nolint
		bin, ok := n.(*googlesql.ASTBinaryExpression)
		if !ok || locStart(n) > errOffset || errOffset >= locEnd(n) {
			return nil
		}
		binOp, _ := bin.Op()
		op, ok := arithmeticOperators[binOp]
		if !ok || m != nil && m[1] != op {
			return nil
		}
		lhs, err := bin.Lhs()
		if err != nil {
			return nil
		}
		rhs, err := bin.Rhs()
		if err != nil {
			return nil
		}
		// An operator error is at the start of the operator, and a literal cast error at the
		// literal operand. An error deeper inside an operand, as `name + 1` in
		// `x + (name + 1)`, is the user's, which must not be folded away with the placeholder.
		if isLiteralCastError {
			if errOffset != locStart(lhs) && errOffset != locStart(rhs) {
				return nil
			}
		} else if errOffset != locStart(n) {
			return nil
		}
		for i, operand := range []googlesql.ASTExpressionNode{lhs, rhs} {
			original, ok := substitutedText(src, edits, locStart(operand), locEnd(operand))
			// A placeholder substituted for a declared variable stands for a value of the declared type.
			if _, isDeclared := declared[strings.ToLower(original)]; ok && !isDeclared {
				expr, placeholder, k = n, operand, i
				return nil
			}
		}
		return nil
	})
	if expr == nil {
		return fixedSrc, nil
	}

	// Only the INT64 placeholder is given a type. A placeholder that already has one may
	// stand in an operator other than the one in the error, as `1` in `1 + 1 + ts`, so it
	// is folded into its expression instead of being given types back and forth.
	var typeName string
	if m != nil {
		typeName = arithmeticOperandType(m[1], m[2], parsedErr.Msg, k)
	} else if bm := bitwiseOperatorErrorRegex.FindStringSubmatch(parsedErr.Msg); bm != nil && bm[1+k] == "INT64" && bm[2-k] != "INT64" {
		typeName = bm[2-k]
	}
	if typeName == "" {
		return replaceSpan(fixedSrc, locStart(expr), locEnd(expr), "1")
	}
	return replaceSpan(fixedSrc, locStart(placeholder), locEnd(placeholder), getDummyValueForTypeName(typeName))
}

// arithmeticOperators maps the arithmetic, concatenation and bitwise operators to their
// names in the errors.
var arithmeticOperators = map[googlesql.ASTBinaryExpressionEnums_Op]string{
	googlesql.ASTBinaryExpressionEnums_OpPlus:       "+",
	googlesql.ASTBinaryExpressionEnums_OpMinus:      "-",
	googlesql.ASTBinaryExpressionEnums_OpMultiply:   "*",
	googlesql.ASTBinaryExpressionEnums_OpDivide:     "/",
	googlesql.ASTBinaryExpressionEnums_OpConcatOp:   "||",
	googlesql.ASTBinaryExpressionEnums_OpBitwiseAnd: "&",
	googlesql.ASTBinaryExpressionEnums_OpBitwiseOr:  "|",
	googlesql.ASTBinaryExpressionEnums_OpBitwiseXor: "^",
}

var signatureLineRegex = regexp.MustCompile(`(?m)^\s*Signature: (.*)$`)

// arithmeticOperandType returns the type of the k-th operand of the binary operator op in
// the first signature listed in the error message whose other operand has the type of the
// other argument, or "" if none or if the k-th argument is not the INT64 of a placeholder.
func arithmeticOperandType(op, argumentTypes, msg string, k int) string {
	args := argumentTypeList(argumentTypes)
	if len(args) != 2 {
		return ""
	}
	for _, sm := range signatureLineRegex.FindAllStringSubmatch(msg, -1) {
		lhs, rhs, ok := strings.Cut(sm[1], " "+op+" ")
		if !ok {
			continue
		}
		sig := [2]string{lhs, rhs}
		if args[k] == "INT64" && sig[1-k] == args[1-k] && sig[k] != args[k] {
			return sig[k]
		}
	}
	return ""
}

// replaceSpan replaces [start, end) of fixedSrc with text.
func replaceSpan(fixedSrc string, start, end int, text string) (string, []FixOffset) {
	return fixedSrc[:start] + text + fixedSrc[end:], []FixOffset{{
		Offset: start,
		Length: len(text) - (end - start),
		edits:  []sourceEdit{{start: start, oldLen: end - start, newLen: len(text)}},
	}}
}

// isWholeFunctionArgument reports whether the path spanning [start, end) is by itself an
// argument of a function call, CAST or EXTRACT.
func isWholeFunctionArgument(node googlesql.ASTNode, start, end int) bool {
	path, ok := SearchAstNode[*googlesql.ASTPathExpression](node, start)
	if !ok || locStart(path) != start || locEnd(path) != end {
		return false
	}
	parent, err := path.Parent()
	if err != nil {
		return false
	}
	switch parent.(type) {
	case *googlesql.ASTFunctionCall, *googlesql.ASTCastExpression, *googlesql.ASTExtractExpression:
		return !insideTablePartitionBy(parent)
	}
	return false
}

// insideTablePartitionBy reports whether node is in the PARTITION BY of a table, which
// rejects a constant expression such as `DATE(NULL)`, unlike the one of a window.
func insideTablePartitionBy(node googlesql.ASTNode) bool {
	for n := node; n != nil; {
		parent, err := n.Parent()
		if err != nil || parent == nil {
			return false
		}
		if _, ok := n.(*googlesql.ASTPartitionBy); ok {
			_, isWindow := parent.(*googlesql.ASTWindowSpecification)
			return !isWindow
		}
		n = parent
	}
	return false
}

// fix error in where clause.
//
//	SELECT * FROM table WHERE unexist_column = 1
//
// becomes
//
//	SELECT * FROM table WHERE true
func fixErrorForWhereStatement(src string, node googlesql.ASTStatementNode, parsedErr Error) (fixedSrc string, fixOffsets []FixOffset) {
	errOffset, ok := errorByteOffset(src, parsedErr.Position)
	if !ok || errOffset == 0 || errOffset == len(src) {
		return src, nil
	}

	var fixOffset FixOffset
	usedCondition := false
	if cond, ok := innermostCondition(node, errOffset); ok && insideInnermostCondition(node, cond, errOffset) && !containsAnalyticFunction(cond) {
		// e.x.) SELECT * FROM table WHERE unexist_column = 1
		loc, _ := cond.GetParseLocationRange()
		startOffset := parseLocStart(loc)
		endOffset := parseLocEnd(loc)
		if startOffset >= 0 && endOffset >= 0 {
			length := endOffset - startOffset
			fixedSrc = src[:startOffset] + "true" + src[endOffset:]
			fixOffset = FixOffset{
				Offset: startOffset,
				Length: len("true") - length,
				edits:  []sourceEdit{{start: startOffset, oldLen: length, newLen: len("true")}},
			}
			usedCondition = true
		}
	}
	if !usedCondition {
		incompleteOffset := errOffset + parsedErr.TermLength - len(parsedErr.IncompleteColumnName)
		if isArithmeticOperand(node, incompleteOffset) {
			// e.x.) SELECT * FROM table WHERE STARTS_WITH(unexist_column || 'a', 'b')
			// `true` fails the operator, whereas the fix-ups of arithmetic operators give `1` a type.
			return fixErrorToLiteral(src, parsedErr)
		}
		// e.x.) SELECT * FROM table WHERE unexist_column
		replacedLength := len(parsedErr.IncompleteColumnName)
		if incompleteOffset < 0 || incompleteOffset+replacedLength > len(src) {
			return src, nil
		}
		// As in fixErrorToExpression, `true.x` would fail to access a field of BOOL.
		replacedLength += fieldAccessLen(src[incompleteOffset+replacedLength:])
		fixedSrc = src[:incompleteOffset] + "true" + src[incompleteOffset+replacedLength:]
		fixOffset = FixOffset{
			Offset: incompleteOffset,
			Length: len("true") - replacedLength,
			edits:  []sourceEdit{{start: incompleteOffset, oldLen: replacedLength, newLen: len("true")}},
		}
	}

	return fixedSrc, []FixOffset{fixOffset}
}

// innermostCondition returns the innermost condition around offset, which
// fixErrorForWhereStatement replaces with `true` as a whole.
func innermostCondition(node googlesql.ASTNode, offset int) (googlesql.ASTNode, bool) {
	var cond googlesql.ASTNode
	// The Walk visits the enclosing nodes from the outermost one.
	Walk(node, func(n googlesql.ASTNode) error { //nolint
		if isCondition(n) && locStart(n) <= offset && offset < locEnd(n) {
			cond = n
		}
		return nil
	})
	return cond, cond != nil
}

// isCondition reports whether n is an expression that yields BOOL whatever the types of
// its operands are. An arithmetic expression is not, as `x + 1` in `x + 1 > 0`.
func isCondition(n googlesql.ASTNode) bool {
	switch n := n.(type) {
	case *googlesql.ASTBinaryExpression:
		op, _ := n.Op()
		_, isArithmetic := arithmeticOperators[op]
		return !isArithmetic
	}
	return false
}

// isArithmeticExpression reports whether n is an arithmetic, concatenation or bitwise expression.
func isArithmeticExpression(n googlesql.ASTNode) bool {
	switch n := n.(type) {
	case *googlesql.ASTBinaryExpression:
		op, _ := n.Op()
		_, ok := arithmeticOperators[op]
		return ok
	case *googlesql.ASTBitwiseShiftExpression:
		return true
	}
	return false
}

// isArithmeticOperand reports whether the path starting at offset is an operand of an
// arithmetic, concatenation or bitwise operator.
func isArithmeticOperand(node googlesql.ASTNode, offset int) bool {
	path, ok := SearchAstNode[*googlesql.ASTPathExpression](node, offset)
	if !ok || locStart(path) != offset {
		return false
	}
	parent, err := path.Parent()
	return err == nil && isArithmeticExpression(parent)
}

// insideInnermostCondition reports whether expr lies within the innermost WHERE/ON/HAVING/QUALIFY
// condition containing offset. Otherwise expr belongs to an outer query, e.g. it
// compares against a subquery, and rewriting it would drop the subquery being typed.
func insideInnermostCondition(node googlesql.ASTNode, expr googlesql.ASTNode, offset int) bool {
	clauseStart, clauseEnd := -1, -1
	if where, ok := SearchAstNode[*googlesql.ASTWhereClause](node, offset); ok {
		clauseStart, clauseEnd = locStart(where), locEnd(where)
	}
	if on, ok := SearchAstNode[*googlesql.ASTOnClause](node, offset); ok && locStart(on) > clauseStart {
		clauseStart, clauseEnd = locStart(on), locEnd(on)
	}
	if having, ok := SearchAstNode[*googlesql.ASTHaving](node, offset); ok && locStart(having) > clauseStart {
		clauseStart, clauseEnd = locStart(having), locEnd(having)
	}
	if qualify, ok := SearchAstNode[*googlesql.ASTQualify](node, offset); ok && locStart(qualify) > clauseStart {
		clauseStart, clauseEnd = locStart(qualify), locEnd(qualify)
	}
	if clauseStart < 0 {
		return false
	}
	return clauseStart <= locStart(expr) && locEnd(expr) <= clauseEnd
}

// containsAnalyticFunction reports whether expr contains an analytic function, e.g.
// `QUALIFY ROW_NUMBER() OVER (PARTITION BY unknown_col) = 1`. Rewriting such expr would
// drop the analytic function, without which QUALIFY fails to analyze. The ones in a
// subquery are not counted, since the enclosing clause does not depend on them.
func containsAnalyticFunction(expr googlesql.ASTNode) bool {
	if expr == nil {
		return false
	}
	switch expr.(type) {
	case *googlesql.ASTAnalyticFunctionCall:
		return true
	case *googlesql.ASTQuery:
		return false
	}
	n, err := expr.NumChildren()
	if err != nil {
		return false
	}
	for i := int32(0); i < n; i++ {
		child, err := expr.Child(i)
		if err == nil && child != nil && containsAnalyticFunction(child) {
			return true
		}
	}
	return false
}

// addInformationToNotBoolClauseError fills IncompleteColumnName only when the whole
// WHERE/ON condition is a bare path, because only then is the user plausibly mid-way
// through typing a column name (as opposed to e.g. `WHERE 1 + 1`).
func addInformationToNotBoolClauseError(src string, node googlesql.ASTNode, parsedErr Error) Error {
	errOffset, ok := errorByteOffset(src, parsedErr.Position)
	if !ok {
		return parsedErr
	}

	// The message names the failing clause; searching by it avoids matching an outer
	// WHERE when the error is in a JOIN ON nested inside it (and vice versa).
	var expr googlesql.ASTExpressionNode
	switch {
	case strings.HasPrefix(parsedErr.Msg, "WHERE clause"):
		if where, ok := SearchAstNode[*googlesql.ASTWhereClause](node, errOffset); ok {
			expr, _ = where.Expression()
		}
	case strings.HasPrefix(parsedErr.Msg, "JOIN ON clause"):
		if on, ok := SearchAstNode[*googlesql.ASTOnClause](node, errOffset); ok {
			expr, _ = on.Expression()
		}
	}
	pathExpr, ok := expr.(*googlesql.ASTPathExpression)
	if !ok {
		return parsedErr
	}

	start, end := locStart(pathExpr), locEnd(pathExpr)
	if start != errOffset || end < start || end > len(src) {
		return parsedErr
	}
	parsedErr.TermLength = end - start
	parsedErr.IncompleteColumnName = src[start:end]
	return parsedErr
}

// addInformationToLogicalOperandError fills IncompleteColumnName when the operand of
// AND/OR/NOT that failed to coerce to BOOL is a bare path inside WHERE/ON, for the same
// reason as addInformationToNotBoolClauseError. GoogleSQL reports the error at the start
// of the whole operator expression, so the position is moved to the operand.
func addInformationToLogicalOperandError(src string, node googlesql.ASTNode, parsedErr Error) Error {
	m := logicalOperandErrorRegex.FindStringSubmatch(parsedErr.Msg)
	if m == nil {
		return parsedErr
	}
	operator := m[1]
	argIndex, err := strconv.Atoi(m[2])
	if err != nil || argIndex < 1 {
		return parsedErr
	}

	errOffset, ok := errorByteOffset(src, parsedErr.Position)
	if !ok {
		return parsedErr
	}
	_, inWhere := SearchAstNode[*googlesql.ASTWhereClause](node, errOffset)
	_, inOn := SearchAstNode[*googlesql.ASTOnClause](node, errOffset)
	if !inWhere && !inOn {
		return parsedErr
	}

	var operand googlesql.ASTExpressionNode
	Walk(node, func(n googlesql.ASTNode) error { //nolint
		if locStart(n) != errOffset {
			return nil
		}
		switch n := n.(type) {
		case *googlesql.ASTAndExpr:
			if operator == "AND" {
				operand, _ = n.Conjuncts(int32(argIndex - 1))
			}
		case *googlesql.ASTOrExpr:
			if operator == "OR" {
				operand, _ = n.Disjuncts(int32(argIndex - 1))
			}
		case *googlesql.ASTUnaryExpression:
			if op, _ := n.Op(); operator == "NOT" && op == googlesql.ASTUnaryExpressionEnums_OpNot {
				operand, _ = n.Operand()
			}
		}
		return nil
	})
	pathExpr, ok := operand.(*googlesql.ASTPathExpression)
	if !ok {
		return parsedErr
	}

	start, end := locStart(pathExpr), locEnd(pathExpr)
	if start < 0 || end < start || end > len(src) {
		return parsedErr
	}
	pos, ok := byteOffsetToPosition(src, start)
	if !ok {
		return parsedErr
	}
	parsedErr.Position = pos
	parsedErr.TermLength = end - start
	parsedErr.IncompleteColumnName = src[start:end]
	return parsedErr
}

// fixNotBoolClauseError replaces only the bare path itself; searching for an enclosing
// binary expression (as fixErrorForWhereStatement does) could rewrite an outer one.
func fixNotBoolClauseError(src string, parsedErr Error) (fixedSrc string, fixOffsets []FixOffset) {
	start, ok := errorByteOffset(src, parsedErr.Position)
	end := start + parsedErr.TermLength
	if !ok || end > len(src) {
		return src, nil
	}
	fixedSrc = src[:start] + "true" + src[end:]
	return fixedSrc, []FixOffset{{
		Offset: start,
		Length: len("true") - parsedErr.TermLength,
		edits:  []sourceEdit{{start: start, oldLen: parsedErr.TermLength, newLen: len("true")}},
	}}
}

// addInformationToPipeColumnNotFoundError covers the column name that pipe
// SET, DROP or RENAME could not find. The length is taken from the identifier
// because the message does not keep its quoting.
func addInformationToPipeColumnNotFoundError(src string, node googlesql.ASTNode, parsedErr Error) Error {
	errOffset, ok := errorByteOffset(src, parsedErr.Position)
	if !ok {
		return parsedErr
	}
	ident, ok := SearchAstNode[*googlesql.ASTIdentifier](node, errOffset)
	if !ok {
		return parsedErr
	}
	start, end := locStart(ident), locEnd(ident)
	if start != errOffset || end <= start || end > len(src) {
		return parsedErr
	}
	parsedErr.TermLength = end - start
	return parsedErr
}

// fixPipeColumnNotFound removes the pipe SET, DROP or RENAME item that names
// a column the input does not have, so that the rest of the statement is
// still analyzed. When the item is the only one, the whole operator is removed.
//
//	FROM t |> DROP x, unknown |> WHERE y
//
// becomes
//
//	FROM t |> DROP x |> WHERE y
func fixPipeColumnNotFound(src string, node googlesql.ASTNode, parsedErr Error) (fixedSrc string, fixOffsets []FixOffset) {
	errOffset, ok := errorByteOffset(src, parsedErr.Position)
	if !ok {
		return src, nil
	}
	ident, _, isInput, ok := PipeColumnIdentifier(node, errOffset)
	if !ok || !isInput || locStart(ident) != errOffset {
		return src, nil
	}
	parent, err := ident.Parent()
	if err != nil {
		return src, nil
	}

	start := locStart(ident)
	end := locEnd(ident)
	if _, isDropList := parent.(*googlesql.ASTIdentifierList); !isDropList {
		end = locEnd(parent)
	}
	if start < 0 || end <= start || end > len(src) {
		return src, nil
	}

	switch prev := strings.TrimRightFunc(src[:start], unicode.IsSpace); {
	case strings.HasSuffix(prev, ","):
		start = len(prev) - 1
	case strings.HasPrefix(strings.TrimLeftFunc(src[end:], unicode.IsSpace), ","):
		rest := strings.TrimLeftFunc(src[end:], unicode.IsSpace)[len(","):]
		end = len(src) - len(strings.TrimLeftFunc(rest, unicode.IsSpace))
	default:
		pipes := pipeOperatorOffsets(src[:start])
		if len(pipes) == 0 {
			return src, nil
		}
		start = len(strings.TrimRightFunc(src[:pipes[len(pipes)-1]], unicode.IsSpace))
	}

	fixedSrc = src[:start] + src[end:]
	return fixedSrc, []FixOffset{{
		Offset: start,
		Length: -(end - start),
		edits:  []sourceEdit{{start: start, oldLen: end - start, newLen: 0}},
	}}
}

func addInformationToNotExistInStructError(src string, node googlesql.ASTNode, parsedErr Error) Error {
	ind := strings.Index(parsedErr.Msg, "Field name ")
	if ind == -1 {
		return parsedErr
	}
	notExistColumn := parsedErr.Msg[ind+len("Field name "):]
	if end := strings.Index(notExistColumn, " "); end != -1 {
		notExistColumn = notExistColumn[:end]
	}

	errOffset, ok := errorByteOffset(src, parsedErr.Position)
	if !ok || errOffset == 0 || errOffset == len(src) || errOffset+len(notExistColumn) > len(src) {
		return parsedErr
	}

	parsedErr.TermLength = len(notExistColumn)

	firstIndex := pathStartAfter(node, strings.LastIndexFunc(src[:errOffset], unicode.IsSpace)+1, errOffset)
	// A field access on a non-path expression, e.g. `STRUCT(1 AS a).b`, may contain
	// whitespace, so the text after the last whitespace would be only part of it.
	if dot, ok := SearchAstNode[*googlesql.ASTDotIdentifier](node, errOffset); ok {
		if start := locStart(dot); start >= 0 && start < firstIndex {
			firstIndex = start
		}
	}
	parsedErr.IncompleteColumnName = src[firstIndex : errOffset+len(notExistColumn)]
	return parsedErr
}

func addInformationToNotFoundInsideTableError(src string, node googlesql.ASTNode, parsedErr Error) Error {
	ind := strings.Index(parsedErr.Msg, "Name ")
	if ind == -1 {
		return parsedErr
	}
	notExistColumn := parsedErr.Msg[ind+len("Name "):]
	if end := strings.Index(notExistColumn, " "); end != -1 {
		notExistColumn = notExistColumn[:end]
	}

	errOffset, ok := errorByteOffset(src, parsedErr.Position)
	if !ok || errOffset == 0 || errOffset == len(src) || errOffset+len(notExistColumn) > len(src) {
		return parsedErr
	}

	parsedErr.TermLength = len(notExistColumn)

	firstIndex := pathStartAfter(node, strings.LastIndexFunc(src[:errOffset], unicode.IsSpace)+1, errOffset)
	parsedErr.IncompleteColumnName = src[firstIndex : errOffset+len(notExistColumn)]
	return parsedErr
}

// pathStartAfter returns the start of the path containing errOffset when it is after
// firstIndex. The path may follow a delimiter without whitespace, e.g. `NULLIF(t.a`,
// and the fix-up must not replace the delimiter along with it.
func pathStartAfter(node googlesql.ASTNode, firstIndex, errOffset int) int {
	if path, ok := SearchAstNode[*googlesql.ASTPathExpression](node, errOffset); ok {
		if start := locStart(path); start > firstIndex {
			return start
		}
	}
	return firstIndex
}

func positionToByteOffset(sql string, position lsp.Position) int {
	// Split on "\n" only so that "\r" in CRLF line endings is counted as part
	// of the line it terminates.
	lines := strings.Split(sql, "\n")

	var offset int
	for i := 0; i < position.Line && i < len(lines); i++ {
		offset += len(lines[i]) + 1
	}
	offset += position.Character
	return offset
}

// errorByteOffset is positionToByteOffset for positions that come from error
// messages. Fix-up functions slice the source with the result, so a position
// outside of src is reported as not ok instead of being trusted.
func errorByteOffset(src string, position lsp.Position) (int, bool) {
	lines := strings.Split(src, "\n")
	if position.Line < 0 || position.Line >= len(lines) {
		return 0, false
	}
	if position.Character < 0 || position.Character > len(lines[position.Line]) {
		return 0, false
	}
	return positionToByteOffset(src, position), true
}

func byteOffsetToPosition(sql string, offset int) (lsp.Position, bool) {
	lines := strings.Split(sql, "\n")

	line := 0
	for _, l := range lines {
		if offset < len(l)+1 {
			return lsp.Position{
				Line:      line,
				Character: offset,
			}, true
		}

		line++
		offset -= len(l) + 1
	}

	return lsp.Position{}, false
}
