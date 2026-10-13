package file

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"unicode"

	googlesql "github.com/goccy/go-googlesql"
	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/kitagry/bqls/langserver/internal/source/bqparser"
)

var lastDotRegex = regexp.MustCompile(`[\w.]+\.\s`)

var zetaSQLLocationRegex = regexp.MustCompile(`\[at (\d+):(\d+)\]`)

type ParsedFile struct {
	URI lsp.DocumentURI
	Src string
	// FixedSrc matches the byte offsets in Node, RNode, and ParseTree.
	FixedSrc string

	// googlesql AST node
	Node *googlesql.ASTScript
	// index is Node's statement order
	RNode []*googlesql.AnalyzerOutput

	ParseTree *bqparser.Node

	FixOffsets []FixOffset
	Errors     []Error
}

func (p ParsedFile) Close() {}

func (p ParsedFile) TermOffset(pos lsp.Position) int {
	termOffset := positionToByteOffset(p.Src, pos)
	return p.fixTermOffsetForNode(termOffset)
}

func (p ParsedFile) fixTermOffsetForNode(termOffset int) int {
	for _, fo := range p.FixOffsets {
		if termOffset > fo.Offset+fo.Length {
			termOffset += fo.Length
		}
	}
	return termOffset
}

func (p ParsedFile) ExtractSQL(locationRange *googlesql.ParseLocationRange) (string, bool) {
	if locationRange == nil {
		return "", false
	}

	startOffset := p.fixTermOffsetForSQL(parseLocStart(locationRange))
	endOffset := p.fixTermOffsetForSQL(parseLocEnd(locationRange))
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
		Start: p.toLspPoint(startOff),
		End:   p.toLspPoint(endOff),
	}, true
}

func (p ParsedFile) toLspPoint(byteOffset int) lsp.Position {
	offset := p.fixTermOffsetForSQL(byteOffset)

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

func (p ParsedFile) fixTermOffsetForSQL(termOffset int) int {
	for _, fo := range p.FixOffsets {
		if termOffset > fo.Offset+fo.Length {
			termOffset -= fo.Length
		}
	}
	return termOffset
}

func (p ParsedFile) FindTargetStatementNode(termOffset int) (googlesql.ASTStatementNode, bool) {
	stmts := collectStatementsFromNode(p.Node)

	if len(stmts) == 1 {
		return stmts[0], true
	}

	for _, stmt := range stmts {
		loc, _ := stmt.GetParseLocationRange()
		if loc == nil {
			continue
		}
		startOffset := parseLocStart(loc)
		endOffset := parseLocEnd(loc)
		if startOffset < 0 || endOffset < 0 {
			continue
		}
		if startOffset <= termOffset && termOffset <= endOffset {
			return stmt, true
		}
	}

	return nil, false
}

func (p ParsedFile) findTargetStatementNodeIndex(termOffset int) (int, bool) {
	stmts := collectStatementsFromNode(p.Node)
	// Filter out statements that never produce an analyzer output so that
	// indices stay aligned with RNode.
	filtered := make([]googlesql.ASTStatementNode, 0, len(stmts))
	insideScriptControl := false
	for _, s := range stmts {
		kind, _ := s.NodeKind()
		// stmts is in pre-order, so the last statement containing the cursor
		// is the innermost one.
		if containsOffset(s, termOffset) {
			insideScriptControl = isScriptControlStatement(kind)
		}
		if kind == googlesql.ASTNodeKindAstVariableDeclaration || isScriptControlStatement(kind) {
			continue
		}
		filtered = append(filtered, s)
	}

	// A script statement such as SET has no analyzer output of its own, and
	// returning another statement's output would resolve names in the wrong scope.
	if insideScriptControl {
		return -1, false
	}

	if len(filtered) == 1 {
		return 0, true
	}

	for i, stmt := range filtered {
		if containsOffset(stmt, termOffset) {
			return i, true
		}
	}

	return -1, false
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
	index, ok := p.findTargetStatementNodeIndex(termOffset)
	if !ok {
		return nil, false
	}

	if index >= len(p.RNode) {
		return nil, false
	}

	return p.RNode[index], true
}

func (p *ParsedFile) FindIncompleteColumnName(pos lsp.Position) string {
	targetTerm := p.TermOffset(pos)

	for _, err := range p.Errors {
		startOffset := positionToByteOffset(p.Src, err.Position)
		startOffset = p.fixTermOffsetForNode(startOffset)
		if startOffset <= targetTerm && targetTerm <= startOffset+err.TermLength {
			return err.IncompleteColumnName
		}
	}
	return ""
}

// collectStatementsFromNode collects all statement nodes from the AST.
func collectStatementsFromNode(node *googlesql.ASTScript) []googlesql.ASTStatementNode {
	stmts := make([]googlesql.ASTStatementNode, 0)
	Walk(node, func(n googlesql.ASTNode) error { //nolint
		if n == nil {
			return nil
		}
		isStmt, _ := n.IsStatement()
		if isStmt {
			if sn, ok := n.(googlesql.ASTStatementNode); ok {
				stmts = append(stmts, sn)
			}
		}
		return nil
	})
	return stmts
}

type FixOffset struct {
	Offset int
	Length int
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
			pos, _ := byteOffsetToPosition(src, srcStart)
			errs = append(errs, Error{
				Msg:                  fmt.Sprintf("Unrecognized name: %s", targetWord),
				Position:             pos,
				TermLength:           len(targetWord),
				IncompleteColumnName: targetWord,
			})
			fixOffsets = append(fixOffsets, FixOffset{
				Offset: srcStart + len(targetWord),
				Length: len("true") - len(targetWord),
			})

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
		},
	}
}

func fixUnexpectedEndOfScript(src string, parsedErr Error) (fixedSrc string, err Error, fixOffsets []FixOffset) {
	targets := []struct {
		keywords []string
		fixFunx  func(src string, err Error, keyword []string) (string, Error, []FixOffset)
	}{
		{
			keywords: []string{"GROUP BY", "ORDER BY", "AND", "OR"},
			fixFunx:  fixUnexpectedEndOfScriptWithDeletion,
		},
		{
			keywords: []string{"WHERE", "ON"},
			fixFunx:  fixUnexpectedEndOfScriptWithCondition,
		},
	}

	oneLineSrc := strings.Join(strings.Fields(src), " ")
	for _, target := range targets {
		targetIndex := -1
		for i, keyword := range target.keywords {
			if strings.HasSuffix(oneLineSrc, keyword) {
				targetIndex = i
				break
			}
		}

		if targetIndex != -1 {
			return target.fixFunx(src, parsedErr, target.keywords)
		}
	}

	return src, parsedErr, nil
}

// `WITH t1 AS (SELECT 1)` return error Expected "(" or "," or keyword SELECT but got end of script
func fixOnlyWithClauseSyntaxError(src string, parsedErr Error) (fixedSrc string, err Error, fixOffsets []FixOffset) {
	errOffset, ok := errorByteOffset(src, parsedErr.Position)
	if !ok {
		return src, parsedErr, nil
	}
	return src[:errOffset] + " SELECT 1 " + src[errOffset:], parsedErr, []FixOffset{
		{
			Offset: errOffset,
			Length: len(" SELECT 1 "),
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

	next := skipSpacesAndComments(src, deleteEnd)
	if next != len(src) || errOffset > next {
		return src, nil
	}

	fixedSrc = src[:pipeOffset] + src[deleteEnd:]
	return fixedSrc, []FixOffset{
		{
			Offset: deleteEnd,
			Length: pipeOffset - deleteEnd,
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
//
// becomes
//
//	SELECT * FROM table
func fixUnexpectedEndOfScriptWithDeletion(src string, parsedErr Error, targetUnexpectedEndKeyword []string) (fixedSrc string, err Error, fixOffsets []FixOffset) {
	errOffset, ok := errorByteOffset(src, parsedErr.Position)
	if !ok {
		return src, parsedErr, nil
	}

	oneLineSrc := strings.Join(strings.Fields(src), " ")
	targetIndex := -1
	for i, keyword := range targetUnexpectedEndKeyword {
		if strings.HasSuffix(oneLineSrc, keyword) {
			targetIndex = i
			break
		}
	}

	if targetIndex == -1 {
		return src, parsedErr, nil
	}

	targetKeyword := targetUnexpectedEndKeyword[targetIndex]
	targetOffset := strings.LastIndex(src[:errOffset], strings.Split(targetKeyword, " ")[0])
	if targetOffset == -1 {
		return src, parsedErr, nil
	}

	fixedSrc = strings.TrimSpace(src[:targetOffset])
	// A pipe operator left without its operation is another syntax error, so
	// it is removed together with the keyword.
	if trimmed := strings.TrimSuffix(fixedSrc, "|>"); trimmed != fixedSrc {
		fixedSrc = strings.TrimRightFunc(trimmed, unicode.IsSpace)
	}
	return fixedSrc, parsedErr, []FixOffset{
		{
			Offset: errOffset,
			Length: -(errOffset - len(fixedSrc)),
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

	oneLineSrc := strings.Join(strings.Fields(src), " ")
	targetIndex := -1
	for i, keyword := range targetUnexpectedEndKeyword {
		if strings.HasSuffix(oneLineSrc, keyword) {
			targetIndex = i
			break
		}
	}

	if targetIndex == -1 {
		return src, parsedErr, nil
	}

	insertStr := " 1=1"
	fixedSrc = src[:errOffset] + insertStr + src[errOffset:]
	return fixedSrc, parsedErr, []FixOffset{
		{
			Offset: errOffset,
			Length: len(insertStr),
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

	fixOffset := FixOffset{Offset: errOffset, Length: 0}
	usedBinaryExpr := false
	if binExpr, ok := SearchAstNode[*googlesql.ASTBinaryExpression](node, errOffset); ok {
		// e.x.) SELECT * FROM table WHERE unexist_column = 1
		loc, _ := binExpr.GetParseLocationRange()
		startOffset := parseLocStart(loc)
		endOffset := parseLocEnd(loc)
		if startOffset >= 0 && endOffset >= 0 {
			length := endOffset - startOffset
			fixedSrc = src[:startOffset] + "true" + src[endOffset:]
			fixOffset.Length = len("true") - length
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
		fixOffset.Length = len("true") - replacedLength
	}

	return fixedSrc, []FixOffset{fixOffset}
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
