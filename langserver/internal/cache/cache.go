package cache

import (
	"strconv"
	"strings"
	"sync"

	"github.com/goccy/go-zetasql"
	"github.com/goccy/go-zetasql/ast"
	"github.com/kitagry/bqls/langserver/internal/lsp"
)

type Policy struct {
	RawText string
	Node    ast.Node
	Errors  []Error
}

type Error struct {
	Msg      string
	Position lsp.Position
}

type GlobalCache struct {
	mu            sync.RWMutex
	pathToPlicies map[string]*Policy
}

func NewGlobalCache() *GlobalCache {
	g := &GlobalCache{pathToPlicies: make(map[string]*Policy)}

	return g
}

func (g *GlobalCache) Get(path string) *Policy {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.pathToPlicies[path]
}

func (g *GlobalCache) Put(path string, rawText string) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	policy, ok := g.pathToPlicies[path]
	if !ok {
		policy = &Policy{}
	}
	policy.RawText = rawText

	node, err := zetasql.ParseScript(rawText, zetasql.NewParserOptions(), zetasql.ErrorMessageOneLine)
	if err != nil {
		error := parseZetaSQLError(err)
		policy.Errors = []Error{error}
		g.pathToPlicies[path] = policy
		return nil
	}

	policy.Node = node
	policy.Errors = nil

	g.pathToPlicies[path] = policy
	return nil
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

func (g *GlobalCache) Delete(path string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	delete(g.pathToPlicies, path)
}
