package cache

import (
	"sync"

	"github.com/goccy/go-zetasql"
)


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
		sql.Errors = []error{err}
		g.pathToSQL[path] = sql
		return nil
	}

	sql.Node = node
	sql.Errors = nil

	g.pathToSQL[path] = sql
	return nil
}

func (g *GlobalCache) Delete(path string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	delete(g.pathToSQL, path)
}
