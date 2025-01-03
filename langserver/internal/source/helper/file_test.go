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
