package source

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/goccy/go-zetasql"
	"github.com/goccy/go-zetasql/ast"
	"github.com/goccy/go-zetasql/types"
	"github.com/kitagry/bqls/langserver/internal/lsp"
)

var lastDotRegex = regexp.MustCompile(`\w+\.\s`)

type ParsedFile struct {
	URI string
	Src string

	Node ast.ScriptNode
	// index is Node's statement order
	RNode []*zetasql.AnalyzerOutput

	FixOffsets []FixOffset
	Errors     []Error
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
		if n.IsStatement() {
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
	targetTerm := positionToByteOffset(p.Src, pos)
	targetTerm = p.fixTermOffsetForNode(targetTerm)

	for _, err := range p.Errors {
		startOffset := positionToByteOffset(p.Src, err.Position)
		startOffset = p.fixTermOffsetForNode(startOffset)
		if startOffset <= targetTerm && targetTerm <= startOffset+err.TermLength {
			return err.IncompleteColumnName
		}
	}
	return ""
}

func (p *Project) ParseFile(uri string, src string) ParsedFile {
	fixedSrc, errs, fixOffsets := fixDot(src)

	var node ast.ScriptNode
	rnode := make([]*zetasql.AnalyzerOutput, 0)
	for _retry := 0; _retry < 10; _retry++ {
		var err error
		var fo []FixOffset
		node, err = zetasql.ParseScript(fixedSrc, zetasql.NewParserOptions(), zetasql.ErrorMessageOneLine)
		if err != nil {
			pErr := parseZetaSQLError(err)
			if strings.Contains(pErr.Msg, "SELECT list must not be empty") {
				fixedSrc, pErr, fo = fixSelectListMustNotBeEmptyStatement(fixedSrc, pErr)
			}
			if strings.Contains(pErr.Msg, "Unexpected end of script") {
				fixedSrc, pErr, fo = fixUnexpectedEndOfScript(fixedSrc, pErr)
			}
			errs = append(errs, pErr)
			if len(fo) > 0 {
				// retry
				fixOffsets = append(fixOffsets, fo...)
				continue
			}
		}

		stmts := make([]ast.StatementNode, 0)
		ast.Walk(node, func(n ast.Node) error {
			if n == nil {
				return nil
			}
			if n.IsStatement() {
				stmts = append(stmts, n)
			}
			return nil
		})

		for _, s := range stmts {
			output, err := p.analyzeStatement(fixedSrc, s)
			if err == nil {
				rnode = append(rnode, output)
				continue
			}

			pErr := parseZetaSQLError(err)

			isUnrecognizedNameError := strings.Contains(pErr.Msg, "Unrecognized name: ")
			isNotExistInStructError := strings.Contains(pErr.Msg, "does not exist in STRUCT")
			isNotFoundInsideError := strings.Contains(pErr.Msg, "not found inside")
			isTableNotFoundError := strings.Contains(pErr.Msg, "Table not found: ")

			// add information to Error
			switch {
			case isUnrecognizedNameError:
				pErr = addInformationToUnrecognizedNameError(fixedSrc, pErr)
			case isNotExistInStructError:
				pErr = addInformationToNotExistInStructError(fixedSrc, pErr)
			case isNotFoundInsideError:
				pErr = addInformationToNotFoundInsideTableError(fixedSrc, pErr)
			case isTableNotFoundError:
				ind := strings.Index(pErr.Msg, "Table not found: ")
				table := strings.TrimSpace(pErr.Msg[ind+len("Table not found: "):])
				pErr.TermLength = len(table)
				pErr.IncompleteColumnName = table
			}
			errs = append(errs, pErr)

			// fix src
			if isUnrecognizedNameError || isNotExistInStructError || isNotFoundInsideError {
				errTermOffset := positionToByteOffset(fixedSrc, pErr.Position)
				if _, ok := searchAstNode[*ast.SelectListNode](node, errTermOffset); ok {
					fixedSrc, fo = fixErrorToLiteral(fixedSrc, pErr)
				}
				if _, ok := searchAstNode[*ast.WhereClauseNode](node, errTermOffset); ok {
					fixedSrc, fo = fixErrorForWhereStatement(fixedSrc, s, pErr)
				}
				if _, ok := searchAstNode[*ast.GroupByNode](node, errTermOffset); ok {
					fixedSrc, fo = fixErrorToLiteral(fixedSrc, pErr)
				}
				if _, ok := searchAstNode[*ast.OrderByNode](node, errTermOffset); ok {
					fixedSrc, fo = fixErrorToLiteral(fixedSrc, pErr)
				}
			}

			if len(fo) > 0 {
				fixOffsets = append(fixOffsets, fo...)
				goto retry
			}
		}
		break
	retry:
	}

	return ParsedFile{
		URI:        uri,
		Src:        src,
		Node:       node,
		RNode:      rnode,
		FixOffsets: fixOffsets,
		Errors:     errs,
	}
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
// SELECT 111, FROM table
func fixDot(src string) (fixedSrc string, errs []Error, fixOffsets []FixOffset) {
	loc := lastDotRegex.FindIndex([]byte(src))
	if len(loc) != 2 {
		return src, nil, nil
	}

	errs = make([]Error, 0, 1)
	fixOffsets = make([]FixOffset, 0, 1)
	for len(loc) == 2 {
		// sql.Rawtext[loc[1]] is a space.
		targetWord := src[loc[0] : loc[1]-1]
		src = src[:loc[0]] + strings.Repeat("1", len(targetWord)) + src[loc[1]-1:]
		pos, _ := byteOffsetToPosition(src, loc[0])
		errs = append(errs, Error{
			Msg:                  fmt.Sprintf("INVALID_ARGUMENT: Unrecognized name: %s", targetWord),
			Position:             pos,
			TermLength:           len(targetWord),
			IncompleteColumnName: targetWord,
		})

		loc = lastDotRegex.FindIndex([]byte(src))
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

// fix Unexpected end of script
//
//	SELECT * FROM table WHERE
//
// becomes
//
//	SELECT * FROM table
func fixUnexpectedEndOfScript(src string, parsedErr Error) (fixedSrc string, err Error, fixOffsets []FixOffset) {
	targetUnexpectedEndKeyword := []string{"WHERE", "GROUP BY", "ORDER BY"}
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
	if node, ok := searchAstNode[*ast.BinaryExpressionNode](node, errOffset); ok {
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
