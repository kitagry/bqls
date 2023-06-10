package cache

import (
	"bytes"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type Policy struct {
	RawText string
}

type GlobalCache struct {
	mu            sync.RWMutex
	pathToPlicies map[string]*Policy
}

func NewGlobalCache(rootPath string) (*GlobalCache, error) {
	g := &GlobalCache{pathToPlicies: make(map[string]*Policy)}

	regoFilePaths, err := loadRegoFiles(rootPath)
	if err != nil {
		return nil, err
	}

	for _, path := range regoFilePaths {
		err = g.putWithPath(path)
		if err != nil {
			return nil, err
		}
	}
	return g, nil
}

func NewGlobalCacheWithFiles(pathToText map[string]string) (*GlobalCache, error) {
	g := &GlobalCache{pathToPlicies: make(map[string]*Policy, len(pathToText))}

	for path, text := range pathToText {
		err := g.Put(path, text)
		if err != nil {
			return nil, err
		}
	}
	return g, nil
}

func loadRegoFiles(rootPath string) ([]string, error) {
	result := make([]string, 0)
	err := filepath.WalkDir(rootPath, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if d.IsDir() {
			return nil
		}

		if strings.HasSuffix(d.Name(), ".rego") {
			result = append(result, path)
		}
		return nil
	})
	return result, err
}

func (g *GlobalCache) Get(path string) *Policy {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.pathToPlicies[path]
}

func (g *GlobalCache) putWithPath(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	buf := new(bytes.Buffer)
	_, err = buf.ReadFrom(f)
	if err != nil {
		return err
	}

	return g.Put(path, buf.String())
}

func (g *GlobalCache) Put(path string, rawText string) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	policy, ok := g.pathToPlicies[path]
	if !ok {
		policy = &Policy{}
	}
	policy.RawText = rawText
	g.pathToPlicies[path] = policy
	return nil
}

func (g *GlobalCache) Delete(path string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	delete(g.pathToPlicies, path)
}
