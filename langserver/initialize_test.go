package langserver

import (
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"sort"
	"strconv"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/kitagry/bqls/langserver/internal/lsp"
)

func TestInitializeOption_SupportsAsyncVirtualTextDocument(t *testing.T) {
	tests := map[string]struct {
		json     string
		expected bool
	}{
		"flag set to true": {
			json:     `{"project_id": "p", "location": "l", "supports_async_virtual_text_document": true}`,
			expected: true,
		},
		"flag omitted defaults to false": {
			json:     `{"project_id": "p", "location": "l"}`,
			expected: false,
		},
	}

	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			var got InitializeOption
			if err := json.Unmarshal([]byte(tt.json), &got); err != nil {
				t.Fatalf("failed to unmarshal: %v", err)
			}
			if got.SupportsAsyncVirtualTextDocument != tt.expected {
				t.Errorf("SupportsAsyncVirtualTextDocument = %v, want %v", got.SupportsAsyncVirtualTextDocument, tt.expected)
			}
		})
	}
}

func TestHandler_buildInitializeResult_ServerInfo(t *testing.T) {
	h := &Handler{version: "v1.2.3"}

	result := h.buildInitializeResult()

	if result.ServerInfo == nil {
		t.Fatal("ServerInfo is nil, want non-nil")
	}
	if result.ServerInfo.Name != "bqls" {
		t.Errorf("ServerInfo.Name = %q, want %q", result.ServerInfo.Name, "bqls")
	}
	if result.ServerInfo.Version != "v1.2.3" {
		t.Errorf("ServerInfo.Version = %q, want %q", result.ServerInfo.Version, "v1.2.3")
	}
}

func TestServerInfo_JSON(t *testing.T) {
	tests := map[string]struct {
		info lsp.ServerInfo
		want string
	}{
		"with version": {
			info: lsp.ServerInfo{Name: "bqls", Version: "v1.2.3"},
			want: `{"name":"bqls","version":"v1.2.3"}`,
		},
		"version omitted when empty": {
			info: lsp.ServerInfo{Name: "bqls"},
			want: `{"name":"bqls"}`,
		},
	}

	for n, tt := range tests {
		t.Run(n, func(t *testing.T) {
			got, err := json.Marshal(tt.info)
			if err != nil {
				t.Fatalf("failed to marshal: %v", err)
			}
			if string(got) != tt.want {
				t.Errorf("json = %s, want %s", got, tt.want)
			}
		})
	}
}

// Parses execute_command.go so that adding a new case to the dispatcher
// without advertising it (or vice versa) fails this test.
func TestHandler_buildInitializeResult_AdvertisesAllHandledCommands(t *testing.T) {
	handled := handledExecuteCommands(t)

	h := &Handler{}
	result := h.buildInitializeResult()
	if result.Capabilities.ExecuteCommandProvider == nil {
		t.Fatal("ExecuteCommandProvider is nil, want non-nil")
	}
	advertised := result.Capabilities.ExecuteCommandProvider.Commands

	sort.Strings(advertised)
	if diff := cmp.Diff(handled, advertised); diff != "" {
		t.Errorf("advertised commands mismatch handled commands (-handled +advertised):\n%s", diff)
	}
}

func handledExecuteCommands(t *testing.T) []string {
	t.Helper()

	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "execute_command.go", nil, 0)
	if err != nil {
		t.Fatalf("failed to parse execute_command.go: %v", err)
	}

	consts := make(map[string]string)
	var dispatcher *ast.FuncDecl
	for _, decl := range file.Decls {
		switch d := decl.(type) {
		case *ast.GenDecl:
			if d.Tok != token.CONST {
				continue
			}
			for _, spec := range d.Specs {
				vs := spec.(*ast.ValueSpec)
				for i, name := range vs.Names {
					if lit, ok := vs.Values[i].(*ast.BasicLit); ok && lit.Kind == token.STRING {
						v, err := strconv.Unquote(lit.Value)
						if err != nil {
							t.Fatalf("failed to unquote %s: %v", lit.Value, err)
						}
						consts[name.Name] = v
					}
				}
			}
		case *ast.FuncDecl:
			if d.Name.Name == "handleWorkspaceExecuteCommand" {
				dispatcher = d
			}
		}
	}
	if dispatcher == nil {
		t.Fatal("handleWorkspaceExecuteCommand not found in execute_command.go")
	}

	var handled []string
	ast.Inspect(dispatcher.Body, func(n ast.Node) bool {
		cc, ok := n.(*ast.CaseClause)
		if !ok {
			return true
		}
		for _, expr := range cc.List {
			ident, ok := expr.(*ast.Ident)
			if !ok {
				t.Fatalf("unexpected case expression %T in handleWorkspaceExecuteCommand", expr)
			}
			v, ok := consts[ident.Name]
			if !ok {
				t.Fatalf("case %s is not a string constant declared in execute_command.go", ident.Name)
			}
			handled = append(handled, v)
		}
		return true
	})
	if len(handled) == 0 {
		t.Fatal("no command cases found in handleWorkspaceExecuteCommand")
	}
	sort.Strings(handled)
	return handled
}
