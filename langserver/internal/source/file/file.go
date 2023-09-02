package file

import (
	"bufio"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/goccy/go-zetasql"
	"github.com/goccy/go-zetasql/ast"
	"github.com/goccy/go-zetasql/types"
	"github.com/kitagry/bqls/langserver/internal/lsp"
)

var lastDotRegex = regexp.MustCompile(`[\w.]+\.\s`)

type ParsedFile struct {
	URI string
	Src string

	Node ast.ScriptNode
	// index is Node's statement order
	RNode []*zetasql.AnalyzerOutput

	FixOffsets []FixOffset
	Errors     []Error
}

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

func (p ParsedFile) ExtractSQL(locationRange *types.ParseLocationRange) (string, bool) {
	if locationRange == nil {
		return "", false
	}

	startOffset := p.fixTermOFfsetForSQL(locationRange.Start().ByteOffset())
	endOffset := p.fixTermOFfsetForSQL(locationRange.End().ByteOffset())

	return p.Src[startOffset:endOffset], true
}

func (p ParsedFile) fixTermOFfsetForSQL(termOffset int) int {
	for _, fo := range p.FixOffsets {
		if termOffset > fo.Offset+fo.Length {
			termOffset -= fo.Length
		}
	}
	return termOffset
}

func (p ParsedFile) FindTargetStatementNode(termOffset int) (ast.StatementNode, bool) {
	stmts := make([]ast.StatementNode, 0)
	ast.Walk(p.Node, func(n ast.Node) error {
		if n == nil {
			return nil
		}
		if n.IsStatement() {
			stmts = append(stmts, n)
		}
		return nil
	})

	if len(stmts) == 1 {
		return stmts[0], true
	}

	for _, stmt := range stmts {
		loc := stmt.ParseLocationRange()
		if loc == nil {
			continue
		}
		startOffset := loc.Start().ByteOffset()
		endOffset := loc.End().ByteOffset()
		if startOffset <= termOffset && termOffset <= endOffset {
			return stmt, true
		}
	}

	return nil, false
}

func (p ParsedFile) findTargetStatementNodeIndex(termOffset int) (int, bool) {
	stmts := make([]ast.StatementNode, 0)
	ast.Walk(p.Node, func(n ast.Node) error {
		if n == nil {
			return nil
		}
		// Currently VariableDeclarationNode can't be analyzed.
		// So, skip it.
		_, isVariableDeclaration := n.(*ast.VariableDeclarationNode)
		if n.IsStatement() && !isVariableDeclaration {
			stmts = append(stmts, n)
		}
		return nil
	})

	if len(stmts) == 1 {
		return 0, true
	}

	for i, stmt := range stmts {
		loc := stmt.ParseLocationRange()
		if loc == nil {
			continue
		}
		startOffset := loc.Start().ByteOffset()
		endOffset := loc.End().ByteOffset()
		if startOffset <= termOffset && termOffset <= endOffset {
			return i, true
		}
	}

	return -1, false
}

func (p *ParsedFile) FindTargetAnalyzeOutput(termOffset int) (*zetasql.AnalyzerOutput, bool) {
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

type FixOffset struct {
	Offset int
	Length int
}

type Error struct {
	Msg                  string
	Position             lsp.Position
	TermLength           int
	IncompleteColumnName string
}

func (e Error) Error() string {
	return fmt.Sprintf("%d:%d: %s", e.Position.Line, e.Position.Character, e.Msg)
}

func parseZetaSQLError(err error) Error {
	errStr := err.Error()
	if !strings.Contains(errStr, "[at ") {
		return Error{Msg: errStr}
	}

	// extract position information like "... [at 1:28]"
	positionInd := strings.Index(errStr, "[at ")
	location := errStr[positionInd+4 : len(errStr)-1]
	locationSep := strings.Split(location, ":")
	line, _ := strconv.Atoi(locationSep[0])
	col, _ := strconv.Atoi(locationSep[1])
	pos := lsp.Position{Line: line - 1, Character: col - 1}

	// Trim position information
	errStr = strings.TrimSpace(errStr[:positionInd])
	return Error{Msg: errStr, Position: pos}
}

// fixDot replaces the last dot of a word with a comma. and change word to same length of 1.
// SELECT aaa. FROM table
//
// becomes
//
// SELECT true FROM table
func fixDot(src string) (fixedSrc string, errs []Error, fixOffsets []FixOffset) {
	// src is a word that ends with a dot.
	loc := lastDotRegex.FindIndex([]byte(src + " "))
	if len(loc) != 2 {
		return src, make([]Error, 0), nil
	}

	errs = make([]Error, 0, 1)
	fixOffsets = make([]FixOffset, 0, 1)
	for len(loc) == 2 {
		// sql.Rawtext[loc[1]] is a space or end of file.
		targetWord := src[loc[0] : loc[1]-1]
		src = src[:loc[0]] + "true" + src[loc[1]-1:]
		pos, _ := byteOffsetToPosition(src, loc[0])
		errs = append(errs, Error{
			Msg:                  fmt.Sprintf("INVALID_ARGUMENT: Unrecognized name: %s", targetWord),
			Position:             pos,
			TermLength:           len(targetWord),
			IncompleteColumnName: targetWord,
		})
		fixOffsets = append(fixOffsets, FixOffset{
			Offset: loc[0] + len(targetWord),
			Length: len("true") - len(targetWord),
		})

		oldLoc := loc
		loc = lastDotRegex.FindIndex([]byte(src))
		if len(loc) == 2 && loc[0] == oldLoc[0] {
			break
		}
	}
	return src, errs, fixOffsets
}

// fix SELECT list must not be empty
//
//	SELECT FROM table
//
// becomes
//
//	SELECT 1 FROM table
func fixSelectListMustNotBeEmptyStatement(src string, parsedErr Error) (fixedSrc string, err Error, fixOffsets []FixOffset) {
	errOffset := positionToByteOffset(src, parsedErr.Position)
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

// fix Unexpected end of script with deletion
//
//	SELECT * FROM table GROUP BY
//
// becomes
//
//	SELECT * FROM table
func fixUnexpectedEndOfScriptWithDeletion(src string, parsedErr Error, targetUnexpectedEndKeyword []string) (fixedSrc string, err Error, fixOffsets []FixOffset) {
	errOffset := positionToByteOffset(src, parsedErr.Position)

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
	errOffset := positionToByteOffset(src, parsedErr.Position)

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
	errOffset := positionToByteOffset(src, parsedErr.Position)
	if errOffset == 0 || errOffset == len(src) {
		return src, nil
	}

	replaceOffset := errOffset + parsedErr.TermLength - len(parsedErr.IncompleteColumnName)
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
	errOffset := positionToByteOffset(src, parsedErr.Position)
	if errOffset == 0 || errOffset == len(src) {
		return src, nil
	}

	replaceOffset := errOffset + parsedErr.TermLength - len(parsedErr.IncompleteColumnName)
	fixedSrc = src[:replaceOffset] + "1" + src[replaceOffset+len(parsedErr.IncompleteColumnName):]
	fixOffset := FixOffset{
		Offset: errOffset,
		Length: -len(parsedErr.IncompleteColumnName) + len("1"),
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
func fixErrorForWhereStatement(src string, node ast.StatementNode, parsedErr Error) (fixedSrc string, fixOffsets []FixOffset) {
	errOffset := positionToByteOffset(src, parsedErr.Position)
	if errOffset == 0 || errOffset == len(src) {
		return src, nil
	}

	fixOffset := FixOffset{Offset: errOffset, Length: 0}
	if node, ok := SearchAstNode[*ast.BinaryExpressionNode](node, errOffset); ok {
		// e.x.) SELECT * FROM table WHERE unexist_column = 1
		loc := node.ParseLocationRange()
		startOffset := loc.Start().ByteOffset()
		endOffset := loc.End().ByteOffset()
		length := endOffset - startOffset
		fixedSrc = src[:startOffset] + "true" + src[endOffset:]
		fixOffset.Length = len("true") - length
	} else {
		// e.x.) SELECT * FROM table WHERE unexist_column
		incompleteOffset := errOffset + parsedErr.TermLength - len(parsedErr.IncompleteColumnName)
		replacedLength := len(parsedErr.IncompleteColumnName)
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
	notExistColumn = notExistColumn[:strings.Index(notExistColumn, " ")]

	errOffset := positionToByteOffset(src, parsedErr.Position)
	if errOffset == 0 || errOffset == len(src) {
		return parsedErr
	}

	parsedErr.TermLength = len(notExistColumn)

	firstIndex := strings.LastIndex(src[:errOffset], " ") + 1
	parsedErr.IncompleteColumnName = src[firstIndex : errOffset+len(notExistColumn)]
	return parsedErr
}

func addInformationToNotFoundInsideTableError(src string, parsedErr Error) Error {
	ind := strings.Index(parsedErr.Msg, "INVALID_ARGUMENT: Name ")
	if ind == -1 {
		return parsedErr
	}
	notExistColumn := parsedErr.Msg[ind+len("INVALID_ARGUMENT: Name "):]
	notExistColumn = notExistColumn[:strings.Index(notExistColumn, " ")]

	errOffset := positionToByteOffset(src, parsedErr.Position)
	if errOffset == 0 || errOffset == len(src) {
		return parsedErr
	}

	parsedErr.TermLength = len(notExistColumn)

	firstIndex := strings.LastIndex(src[:errOffset], " ") + 1
	parsedErr.IncompleteColumnName = src[firstIndex : errOffset+len(notExistColumn)]
	return parsedErr
}

func positionToByteOffset(sql string, position lsp.Position) int {
	buf := bufio.NewScanner(strings.NewReader(sql))
	buf.Split(bufio.ScanLines)

	var offset int
	for i := 0; i < position.Line; i++ {
		buf.Scan()
		offset += len([]byte(buf.Text())) + 1
	}
	offset += position.Character
	return offset
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
