package file

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"unicode"

	bq "cloud.google.com/go/bigquery"
	googlesql "github.com/goccy/go-googlesql"
	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/kitagry/bqls/langserver/internal/source/bqparser"
)

var lastDotRegex = regexp.MustCompile(`[\w.]+\.\s`)

var zetaSQLLocationRegex = regexp.MustCompile(`\[at (\d+):(\d+)\]`)

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
	for line := range strings.SplitSeq(src, "\n") {
		ind := len(line)
		// NOTE: to skip comments
		// TODO: handle multi-line comments
		if i := strings.Index(line, "--"); i != -1 {
			ind = i
		}
		if i := strings.Index(line, "//"); i != -1 && i < ind {
			ind = i
		}

		// src is a word that ends with a dot.
		loc := lastDotRegex.FindIndex([]byte(line[:ind] + " "))
		if len(loc) != 2 {
			fixedBuilder.WriteString(line + "\n")
			currentOffset += len(line) + 1
			continue
		}

		for len(loc) == 2 {
			// sql.Rawtext[loc[1]] is a space or end of file.
			srcStart := currentOffset + loc[0]
			targetWord := line[loc[0] : loc[1]-1]
			line = line[:loc[0]] + "true" + line[loc[1]-1:]
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
			loc = lastDotRegex.FindIndex([]byte(line[:ind] + " "))
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
			i = skipQuoted(src, i)
			commentStart = -1
		default:
			if next := skipComment(src, i); next != i {
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
			i = skipQuoted(src, i)
		default:
			if next := skipComment(src, i); next != i {
				i = next
			} else {
				i++
			}
		}
	}
	return offsets
}

// skipQuoted returns the offset after the quoted string or identifier
// starting at src[start].
func skipQuoted(src string, start int) int {
	quote := src[start : start+1]
	if strings.HasPrefix(src[start:], strings.Repeat(quote, 3)) {
		quote = strings.Repeat(quote, 3)
	}
	for i := start + len(quote); i < len(src); i++ {
		if src[i] == '\\' {
			i++
			continue
		}
		if strings.HasPrefix(src[i:], quote) {
			return i + len(quote)
		}
	}
	return len(src)
}

// skipComment returns the offset after the comment starting at src[start],
// or start when no comment starts there.
func skipComment(src string, start int) int {
	switch {
	case strings.HasPrefix(src[start:], "--"), strings.HasPrefix(src[start:], "#"):
		if end := strings.IndexByte(src[start:], '\n'); end != -1 {
			return start + end + 1
		}
		return len(src)
	case strings.HasPrefix(src[start:], "/*"):
		if end := strings.Index(src[start+len("/*"):], "*/"); end != -1 {
			return start + len("/*") + end + len("*/")
		}
		return len(src)
	}
	return start
}

func skipSpacesAndComments(src string, start int) int {
	i := start
	for i < len(src) {
		if unicode.IsSpace(rune(src[i])) {
			i++
			continue
		}
		next := skipComment(src, i)
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
	fixedSrc = src[:replaceOffset] + defaultVal + src[replaceOffset+len(parsedErr.IncompleteColumnName):]
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
	fixedSrc = src[:replaceOffset] + expr + src[replaceOffset+len(parsedErr.IncompleteColumnName):]
	fixOffset := FixOffset{
		Offset: errOffset,
		Length: -len(parsedErr.IncompleteColumnName) + len(expr),
		edits:  []sourceEdit{{start: replaceOffset, oldLen: len(parsedErr.IncompleteColumnName), newLen: len(expr)}},
	}

	return fixedSrc, []FixOffset{fixOffset}
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
	usedBinaryExpr := false
	if binExpr, ok := SearchAstNode[*googlesql.ASTBinaryExpression](node, errOffset); ok && insideInnermostCondition(node, binExpr, errOffset) {
		// e.x.) SELECT * FROM table WHERE unexist_column = 1
		loc, _ := binExpr.GetParseLocationRange()
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
			usedBinaryExpr = true
		}
	}
	if !usedBinaryExpr {
		// e.x.) SELECT * FROM table WHERE unexist_column
		incompleteOffset := errOffset + parsedErr.TermLength - len(parsedErr.IncompleteColumnName)
		replacedLength := len(parsedErr.IncompleteColumnName)
		if incompleteOffset < 0 || incompleteOffset+replacedLength > len(src) {
			return src, nil
		}
		fixedSrc = src[:incompleteOffset] + "true" + src[incompleteOffset+replacedLength:]
		fixOffset = FixOffset{
			Offset: incompleteOffset,
			Length: len("true") - replacedLength,
			edits:  []sourceEdit{{start: incompleteOffset, oldLen: replacedLength, newLen: len("true")}},
		}
	}

	return fixedSrc, []FixOffset{fixOffset}
}

// insideInnermostCondition reports whether expr lies within the innermost WHERE/ON
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
	if clauseStart < 0 {
		return false
	}
	return clauseStart <= locStart(expr) && locEnd(expr) <= clauseEnd
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

func addInformationToNotExistInStructError(src string, parsedErr Error) Error {
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

	firstIndex := strings.LastIndexFunc(src[:errOffset], unicode.IsSpace) + 1
	parsedErr.IncompleteColumnName = src[firstIndex : errOffset+len(notExistColumn)]
	return parsedErr
}

func addInformationToNotFoundInsideTableError(src string, parsedErr Error) Error {
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

	firstIndex := strings.LastIndexFunc(src[:errOffset], unicode.IsSpace) + 1
	parsedErr.IncompleteColumnName = src[firstIndex : errOffset+len(notExistColumn)]
	return parsedErr
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
