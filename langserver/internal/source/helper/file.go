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
		if ind := strings.Index(file, "|"); ind != -1 {
			file = strings.Replace(file, "|", "", 1)
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

// UTF16PositionToBytePosition converts a position received from an LSP
// client, whose Character is counted in UTF-16 code units, into one whose
// Character is a byte offset within the line, which is what the source
// package works with.
func UTF16PositionToBytePosition(src string, pos lsp.Position) lsp.Position {
	lines := strings.Split(src, "\n")
	if pos.Line < 0 || pos.Line >= len(lines) {
		return pos
	}

	line := lines[pos.Line]
	units := 0
	for i, r := range line {
		if units >= pos.Character {
			return lsp.Position{Line: pos.Line, Character: i}
		}
		units += utf16.RuneLen(r)
	}
	return lsp.Position{Line: pos.Line, Character: len(line)}
}
