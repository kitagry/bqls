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
	cache := cache.NewGlobalCache()

	return &Project{
		rootPath: rootPath,
		cache:    cache,
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

func (p *Project) GetErrors(path string) map[string][]cache.Error {
	policy := p.cache.Get(path)
	if policy == nil {
		return nil
	}
	return map[string][]cache.Error{path: policy.Errors}
}
