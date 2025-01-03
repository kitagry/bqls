package cache

import (
	"sync"

	"github.com/kitagry/bqls/langserver/internal/lsp"
)

type GlobalCache struct {
	mu        sync.RWMutex
	pathToSQL map[lsp.DocumentURI]*SQL
}

func NewGlobalCache() *GlobalCache {
	g := &GlobalCache{pathToSQL: make(map[lsp.DocumentURI]*SQL)}

	return g
}

func (g *GlobalCache) Get(uri lsp.DocumentURI) *SQL {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.pathToSQL[uri]
}

func (g *GlobalCache) Put(uri lsp.DocumentURI, rawText string) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.pathToSQL[uri] = NewSQL(rawText)
	return nil
}

func (g *GlobalCache) Delete(uri lsp.DocumentURI) {
	g.mu.Lock()
	defer g.mu.Unlock()
	delete(g.pathToSQL, uri)
}
