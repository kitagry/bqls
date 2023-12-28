package helper

import (
	"errors"
	"strings"

	"github.com/kitagry/bqls/langserver/internal/lsp"
)

var ErrNoPosition = errors.New("no position")

func GetLspPosition(files map[string]string) (formattedFiles map[string]string, path string, position lsp.Position, err error) {
	formattedFiles = make(map[string]string)
	for filePath, file := range files {
		if ind := strings.Index(file, "|"); ind != -1 {
			file = strings.Replace(file, "|", "", 1)
			path = filePath
			position = IndexToPosition(file, ind)
		}
		formattedFiles[path] = file
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
