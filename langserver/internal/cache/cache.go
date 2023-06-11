package cache

import (
	"strconv"
	"strings"
	"sync"

	"github.com/goccy/go-zetasql"
	"github.com/kitagry/bqls/langserver/internal/lsp"
)

type Error struct {
	Msg      string
	Position lsp.Position
}

type GlobalCache struct {
	mu        sync.RWMutex
	pathToSQL map[string]*SQL
}

func NewGlobalCache() *GlobalCache {
	g := &GlobalCache{pathToSQL: make(map[string]*SQL)}

	return g
}

func (g *GlobalCache) Get(path string) *SQL {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.pathToSQL[path]
}

func (g *GlobalCache) Put(path string, rawText string) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	sql, ok := g.pathToSQL[path]
	if !ok {
		sql = &SQL{}
	}
	sql.RawText = rawText

	node, err := zetasql.ParseScript(rawText, zetasql.NewParserOptions(), zetasql.ErrorMessageOneLine)
	if err != nil {
		error := parseZetaSQLError(err)
		sql.Errors = []Error{error}
		g.pathToSQL[path] = sql
		return nil
	}

	sql.Node = node
	sql.Errors = nil

	g.pathToSQL[path] = sql
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
	delete(g.pathToSQL, path)
}
