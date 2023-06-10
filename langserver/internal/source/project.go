package source

import (
	"github.com/kitagry/bqls/langserver/internal/cache"
)

type Project struct {
	rootPath string
	cache    *cache.GlobalCache
}

type File struct {
	RawText string
	Version int
}

func NewProject(rootPath string) (*Project, error) {
	cache, err := cache.NewGlobalCache(rootPath)
	if err != nil {
		return nil, err
	}

	return &Project{
		rootPath: rootPath,
		cache:    cache,
	}, nil
}

func NewProjectWithFiles(files map[string]File) (*Project, error) {
	ff := make(map[string]string, len(files))
	for path, file := range files {
		ff[path] = file.RawText
	}

	cache, err := cache.NewGlobalCacheWithFiles(ff)
	if err != nil {
		return nil, err
	}

	return &Project{
		cache: cache,
	}, nil
}

func (p *Project) UpdateFile(path string, text string, version int) error {
	p.cache.Put(path, text)

	return nil
}

func (p *Project) GetFile(path string) (string, bool) {
	policy := p.cache.Get(path)
	if policy == nil {
		return "", false
	}
	return policy.RawText, true
}

func (p *Project) DeleteFile(path string) {
	p.cache.Delete(path)
}
