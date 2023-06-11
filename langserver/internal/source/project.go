package source

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/goccy/go-zetasql"
	"github.com/goccy/go-zetasql/types"
	"github.com/kitagry/bqls/langserver/internal/bigquery"
	"github.com/kitagry/bqls/langserver/internal/cache"
	"github.com/kitagry/bqls/langserver/internal/lsp"
)

type Project struct {
	rootPath string
	cache    *cache.GlobalCache
	bqClient bigquery.Client
	catalog  types.Catalog
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

	catalog := NewCatalog(bqClient)

	return &Project{
		rootPath: rootPath,
		cache:    cache,
		bqClient: bqClient,
		catalog:  catalog,
	}, nil
}

func (p *Project) Close() error {
	return p.bqClient.Close()
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

func (p *Project) GetErrors(path string) map[string][]Error {
	sql := p.cache.Get(path)
	if sql == nil {
		return nil
	}

	if len(sql.Errors) > 0 {
		errs := make([]Error, len(sql.Errors))
		for i, e := range sql.Errors {
			errs[i] = parseZetaSQLError(e)
		}
		return map[string][]Error{path: errs}
	}

	opts := zetasql.NewAnalyzerOptions()
	opts.SetErrorMessageMode(zetasql.ErrorMessageOneLine)
	_, err := zetasql.AnalyzeStatement(sql.RawText, p.catalog, opts)
	if err != nil {
		return map[string][]Error{path: {parseZetaSQLError(err)}}
	}

	return map[string][]Error{path: nil}
}

type Error struct {
	Msg      string
	Position lsp.Position
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
