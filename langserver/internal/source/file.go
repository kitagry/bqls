package source

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/goccy/go-zetasql"
	"github.com/goccy/go-zetasql/ast"
	"github.com/kitagry/bqls/langserver/internal/lsp"
)

var lastDotRegex = regexp.MustCompile(`\w+\.\s`)

type ParsedFile struct {
	URI string
	Src string

	Node  ast.ScriptNode
	RNode map[ast.StatementNode]*zetasql.AnalyzerOutput

	Fixed  bool
	Errors []Error
}

func (p *Project) ParseFile(uri string, src string) ParsedFile {
	fixedSrc, errs, dotFixed := fixDot(src)

	var node ast.ScriptNode
	rnode := make(map[ast.StatementNode]*zetasql.AnalyzerOutput)
	var analyzeErrFixed bool
	for _retry := 0; _retry < 10; _retry++ {
		var err error
		var fixed bool
		node, err = zetasql.ParseScript(fixedSrc, zetasql.NewParserOptions(), zetasql.ErrorMessageOneLine)
		if err != nil {
			pErr := parseZetaSQLError(err)
			if strings.Contains(pErr.Msg, "SELECT list must not be empty") {
				fixedSrc, pErr, fixed = fixSelectListMustNotBeEmptyStatement(fixedSrc, pErr)
			}
			errs = append(errs, pErr)
			if fixed {
				// retry
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
				rnode[s] = output
				break
			}

			pErr := parseZetaSQLError(err)
			switch {
			case strings.Contains(pErr.Msg, "Unrecognized name: "):
				fixedSrc, pErr, fixed = fixUnrecognizedNameStatement(fixedSrc, pErr)
			case strings.Contains(pErr.Msg, "does not exist in STRUCT"):
				fixedSrc, pErr, fixed = fixFieldDoesNotExistInStructStatement(fixedSrc, pErr)
			case strings.Contains(pErr.Msg, "not found inside"):
				fixedSrc, pErr, fixed = fixNotFoundIndsideTableStatement(fixedSrc, pErr)
			}
			errs = append(errs, pErr)
			if fixed {
				analyzeErrFixed = true
				goto retry
			}
		}
		break
	retry:
	}

	return ParsedFile{
		URI:    uri,
		Src:    src,
		Node:   node,
		RNode:  rnode,
		Fixed:  dotFixed || analyzeErrFixed,
		Errors: errs,
	}
}

type Error struct {
	Msg        string
	Position   lsp.Position
	TermLength int
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
func fixDot(src string) (fixedSrc string, errs []Error, fixed bool) {
	loc := lastDotRegex.FindIndex([]byte(src))
	if len(loc) != 2 {
		return src, nil, false
	}

	errs = make([]Error, 0, 1)
	for len(loc) == 2 {
		// sql.Rawtext[loc[1]] is a space.
		targetWord := src[loc[0] : loc[1]-1]
		src = src[:loc[0]] + strings.Repeat("1", len(targetWord)-1) + "," + src[loc[1]-1:]
		pos, _ := byteOffsetToPosition(src, loc[0])
		errs = append(errs, Error{
			Msg:        fmt.Sprintf("INVALID_ARGUMENT: Unrecognized name: %s", targetWord),
			Position:   pos,
			TermLength: len(targetWord),
		})

		loc = lastDotRegex.FindIndex([]byte(src))
	}
	return src, errs, true
}

// fix SELECT list must not be empty
//
//	SELECT FROM table
//
// becomes
//
//	SELECT 1 FROM table
func fixSelectListMustNotBeEmptyStatement(src string, parsedErr Error) (fixedSrc string, err Error, fixed bool) {
	errOffset := positionToByteOffset(src, parsedErr.Position)
	return src[:errOffset] + "1 " + src[errOffset:], parsedErr, true
}

// fix Unrecognized name: <name> error.
//
//	SELECT unexist_column FROM table
//
// becomes
//
//	SELECT 11111111111111 FROM table
func fixUnrecognizedNameStatement(src string, parsedErr Error) (fixedSrc string, err Error, fixed bool) {
	ind := strings.Index(parsedErr.Msg, "Unrecognized name: ")

	unrecognizedName := strings.TrimSpace(parsedErr.Msg[ind+len("Unrecognized name: "):])
	errOffset := positionToByteOffset(src, parsedErr.Position)
	if errOffset == 0 || errOffset == len(src) {
		return src, parsedErr, false
	}

	fixedSrc = src[:errOffset] + strings.Repeat("1", len(unrecognizedName)) + src[errOffset+len(unrecognizedName):]
	parsedErr.TermLength = len(unrecognizedName)

	return fixedSrc, parsedErr, true
}

// fix field name <name> error.
//
//	SELECT struct.unexist_column FROM table
//
// becomes
//
//	SELECT 111111111111111111111 FROM table
func fixFieldDoesNotExistInStructStatement(src string, parsedErr Error) (fixedSrc string, err Error, fixed bool) {
	ind := strings.Index(parsedErr.Msg, "Field name ")
	if ind == -1 {
		return src, parsedErr, false
	}
	notExistColumn := parsedErr.Msg[ind+len("Field name "):]
	notExistColumn = notExistColumn[:strings.Index(notExistColumn, " ")]

	errOffset := positionToByteOffset(src, parsedErr.Position)
	if errOffset == 0 || errOffset == len(src) {
		return src, parsedErr, false
	}

	parsedErr.TermLength = len(notExistColumn)

	firstIndex := strings.LastIndex(src[:errOffset], " ") + 1
	if firstIndex == 0 {
		fixedSrc = src[:errOffset] + "*," + src[errOffset+len(notExistColumn):]
		return fixedSrc, parsedErr, true
	}
	structLen := len(src[firstIndex:errOffset])
	fixedSrc = src[:firstIndex] + strings.Repeat("1", structLen+len(notExistColumn)) + src[errOffset+len(notExistColumn):]

	return fixedSrc, parsedErr, true
}

// fix field name <name> error.
//
//	SELECT t.unexist_column FROM table AS t
//
// becomes
//
//	SELECT 1111111111111111 FROM table AS t
func fixNotFoundIndsideTableStatement(src string, parsedErr Error) (fixedSrc string, err Error, fixed bool) {
	ind := strings.Index(parsedErr.Msg, "INVALID_ARGUMENT: Name ")
	if ind == -1 {
		return src, parsedErr, false
	}
	notExistColumn := parsedErr.Msg[ind+len("INVALID_ARGUMENT: Name "):]
	notExistColumn = notExistColumn[:strings.Index(notExistColumn, " ")]

	errOffset := positionToByteOffset(src, parsedErr.Position)
	if errOffset == 0 || errOffset == len(src) {
		return src, parsedErr, false
	}

	parsedErr.TermLength = len(notExistColumn)

	firstIndex := strings.LastIndex(src[:errOffset], " ") + 1
	if firstIndex == 0 {
		fixedSrc = src[:errOffset] + "*," + src[errOffset+len(notExistColumn):]
		return fixedSrc, parsedErr, true
	}
	structLen := len(src[firstIndex:errOffset])
	fixedSrc = src[:firstIndex] + strings.Repeat("1", structLen+len(notExistColumn)) + src[errOffset+len(notExistColumn):]

	return fixedSrc, parsedErr, true
}
