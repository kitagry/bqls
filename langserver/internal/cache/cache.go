package cache

import (
	"sync"
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

	g.pathToSQL[path] = NewSQL(rawText)
	return nil
}

func (g *GlobalCache) Delete(path string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	delete(g.pathToSQL, path)
}
