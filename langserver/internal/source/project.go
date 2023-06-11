package source

import (
	"context"
	"fmt"

	"github.com/kitagry/bqls/langserver/internal/bigquery"
	"github.com/kitagry/bqls/langserver/internal/cache"
)

type Project struct {
	rootPath string
	cache    *cache.GlobalCache
	bqClient bigquery.Client
}

type File struct {
	RawText string
	Version int
}

func NewProject(ctx context.Context, rootPath string) (*Project, error) {
	cache := cache.NewGlobalCache()

	bqClient, err := bigquery.New(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create bigquery client: %w", err)
	}

	return &Project{
		rootPath: rootPath,
		cache:    cache,
		bqClient: bqClient,
	}, nil
}

func (p *Project) UpdateFile(path string, text string, version int) error {
	p.cache.Put(path, text)

	return nil
}

func (p *Project) GetFile(path string) (string, bool) {
	sql := p.cache.Get(path)
	if sql == nil {
		return "", false
	}
	return sql.RawText, true
}

func (p *Project) DeleteFile(path string) {
	p.cache.Delete(path)
}

func (p *Project) GetErrors(path string) map[string][]cache.Error {
	sql := p.cache.Get(path)
	if sql == nil {
		return nil
	}

	if len(sql.Errors) > 0 {
		return map[string][]cache.Error{path: sql.Errors}
	}

	return nil
}
