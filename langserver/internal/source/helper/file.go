package helper

import (
	"errors"
	"strings"
	"unicode/utf16"

	"github.com/kitagry/bqls/langserver/internal/lsp"
)

var ErrNoPosition = errors.New("no position")

func GetLspPosition(files map[lsp.DocumentURI]string) (formattedFiles map[lsp.DocumentURI]string, path lsp.DocumentURI, position lsp.Position, err error) {
	formattedFiles = make(map[lsp.DocumentURI]string)
	for filePath, file := range files {
		if ind := cursorIndex(file); ind != -1 {
			file = file[:ind] + file[ind+1:]
			path = filePath
			position = IndexToPosition(file, ind)
		}
		formattedFiles[filePath] = file
	}

	if path == "" {
		return nil, "", lsp.Position{}, ErrNoPosition
	}

	return
}

// cursorIndex returns the index of the first "|" that marks the cursor. A "|"
// directly followed by ">" is the pipe operator, so it is not the cursor.
func cursorIndex(file string) int {
	offset := 0
	for {
		ind := strings.Index(file[offset:], "|")
		if ind == -1 {
			return -1
		}
		ind += offset
		if !strings.HasPrefix(file[ind+1:], ">") {
			return ind
		}
		offset = ind + 1
	}
}

func IndexToPosition(file string, index int) lsp.Position {
	col, row := 0, 0
	lines := strings.Split(file, "\n")
	for _, line := range lines {
		if index <= len(line) {
			col = index
			break
		}
		index -= len(line) + 1
		row++
	}
	return lsp.Position{Line: row, Character: col}
}

// GetUTF16LspPosition is GetLspPosition with the cursor Character counted
// in UTF-16 code units, i.e. what an LSP client would send.
func GetUTF16LspPosition(files map[lsp.DocumentURI]string) (formattedFiles map[lsp.DocumentURI]string, path lsp.DocumentURI, position lsp.Position, err error) {
	formattedFiles, path, position, err = GetLspPosition(files)
	if err != nil {
		return
	}
	position = BytePositionToUTF16Position(formattedFiles[path], position)
	return
}

// PositionConverter converts positions between byte columns, which the
// source package works with, and UTF-16 code unit columns, which LSP clients
// use by default. It splits src once so that converting many positions of
// the same file stays linear.
type PositionConverter struct {
	lines []string
}

func NewPositionConverter(src string) PositionConverter {
	return PositionConverter{lines: strings.Split(src, "\n")}
}

// ToUTF16 converts a byte-column position. A column past the end of its line
// is clamped to the line end; a line outside src is returned unchanged.
func (c PositionConverter) ToUTF16(pos lsp.Position) lsp.Position {
	if pos.Line < 0 || pos.Line >= len(c.lines) {
		return pos
	}
	units := 0
	for i, r := range c.lines[pos.Line] {
		if i >= pos.Character {
			break
		}
		units += utf16.RuneLen(r)
	}
	return lsp.Position{Line: pos.Line, Character: units}
}

// FromUTF16 converts a UTF-16-column position. A column past the end of its
// line is clamped to the line end; a line outside src is returned unchanged.
func (c PositionConverter) FromUTF16(pos lsp.Position) lsp.Position {
	if pos.Line < 0 || pos.Line >= len(c.lines) {
		return pos
	}
	line := c.lines[pos.Line]
	units := 0
	for i, r := range line {
		if units >= pos.Character {
			return lsp.Position{Line: pos.Line, Character: i}
		}
		units += utf16.RuneLen(r)
	}
	return lsp.Position{Line: pos.Line, Character: len(line)}
}

func (c PositionConverter) RangeToUTF16(r lsp.Range) lsp.Range {
	return lsp.Range{Start: c.ToUTF16(r.Start), End: c.ToUTF16(r.End)}
}

// UTF16PositionToBytePosition converts a position received from an LSP
// client into the byte-column position the source package works with.
func UTF16PositionToBytePosition(src string, pos lsp.Position) lsp.Position {
	return NewPositionConverter(src).FromUTF16(pos)
}

// BytePositionToUTF16Position converts a byte-column position into the
// UTF-16-column position an LSP client expects.
func BytePositionToUTF16Position(src string, pos lsp.Position) lsp.Position {
	return NewPositionConverter(src).ToUTF16(pos)
}
