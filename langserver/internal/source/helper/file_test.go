package helper_test

import (
	"errors"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/kitagry/bqls/langserver/internal/source/helper"
)

func TestGetLspPosition(t *testing.T) {
	tests := map[string]struct {
		files         map[lsp.DocumentURI]string
		expectedFiles map[lsp.DocumentURI]string
		expectedPath  lsp.DocumentURI
		expectedPos   lsp.Position
		expectedErr   error
	}{
		"position exists": {
			files:         map[lsp.DocumentURI]string{"file.sql": "SELECT |* FROM table"},
			expectedFiles: map[lsp.DocumentURI]string{"file.sql": "SELECT * FROM table"}, // remove "|"
			expectedPath:  "file.sql",
			expectedPos:   lsp.Position{Line: 0, Character: 7},
		},
		"position exists in multiline": {
			files: map[lsp.DocumentURI]string{"file.sql": `SELECT
  * |FROM table`},
			expectedFiles: map[lsp.DocumentURI]string{"file.sql": `SELECT
  * FROM table`}, // remove "|"
			expectedPath: "file.sql",
			expectedPos:  lsp.Position{Line: 1, Character: 4},
		},
		"pipe operator is not a position": {
			files:         map[lsp.DocumentURI]string{"file.sql": "FROM table |> SELECT |"},
			expectedFiles: map[lsp.DocumentURI]string{"file.sql": "FROM table |> SELECT "},
			expectedPath:  "file.sql",
			expectedPos:   lsp.Position{Line: 0, Character: 21},
		},
		"position right after pipe operator": {
			files:         map[lsp.DocumentURI]string{"file.sql": "FROM table |>| WHERE x = 1"},
			expectedFiles: map[lsp.DocumentURI]string{"file.sql": "FROM table |> WHERE x = 1"},
			expectedPath:  "file.sql",
			expectedPos:   lsp.Position{Line: 0, Character: 13},
		},
		"no position with pipe operator": {
			files:         map[lsp.DocumentURI]string{"file.sql": "FROM table |> SELECT *"},
			expectedFiles: nil,
			expectedPath:  "",
			expectedPos:   lsp.Position{},
			expectedErr:   helper.ErrNoPosition,
		},
		"no position": {
			files:         map[lsp.DocumentURI]string{"file.sql": "SELECT * FROM table"},
			expectedFiles: nil,
			expectedPath:  "",
			expectedPos:   lsp.Position{},
			expectedErr:   helper.ErrNoPosition,
		},
	}

	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			gotFiles, gotPath, gotPos, gotErr := helper.GetLspPosition(tt.files)
			if diff := cmp.Diff(gotFiles, tt.expectedFiles); diff != "" {
				t.Errorf("files mismatch (-got +want):\n%s", diff)
			}

			if gotPath != tt.expectedPath {
				t.Errorf("path mismatch: got %s, want %s", gotPath, tt.expectedPath)
			}

			if gotPos != tt.expectedPos {
				t.Errorf("position mismatch: got %v, want %v", gotPos, tt.expectedPos)
			}

			if !errors.Is(gotErr, tt.expectedErr) {
				t.Errorf("error mismatch: got %v, want %v", gotErr, tt.expectedErr)
			}
		})
	}
}

func TestUTF16PositionToBytePosition(t *testing.T) {
	tests := map[string]struct {
		src  string
		pos  lsp.Position
		want lsp.Position
	}{
		"ascii only": {
			src:  "SELECT x",
			pos:  lsp.Position{Line: 0, Character: 7},
			want: lsp.Position{Line: 0, Character: 7},
		},
		"after multibyte characters": {
			src:  "SELECT 1;\n/* あいう */ x",
			pos:  lsp.Position{Line: 1, Character: 11},
			want: lsp.Position{Line: 1, Character: 17},
		},
		"after surrogate pair": {
			src:  "'😀' x",
			pos:  lsp.Position{Line: 0, Character: 5},
			want: lsp.Position{Line: 0, Character: 7},
		},
		"beyond the end of the line": {
			src:  "'あ'",
			pos:  lsp.Position{Line: 0, Character: 10},
			want: lsp.Position{Line: 0, Character: 5},
		},
		"beyond the last line": {
			src:  "SELECT 1",
			pos:  lsp.Position{Line: 3, Character: 2},
			want: lsp.Position{Line: 3, Character: 2},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			got := helper.UTF16PositionToBytePosition(tt.src, tt.pos)
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("UTF16PositionToBytePosition(%q, %v) diff (-want +got):\n%s", tt.src, tt.pos, diff)
			}
		})
	}
}
