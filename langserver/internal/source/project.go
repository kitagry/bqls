package source

import (
	"context"
	"fmt"
	"os/exec"
	"strings"

	bq "cloud.google.com/go/bigquery"
	"github.com/kitagry/bqls/langserver/internal/bigquery"
	"github.com/kitagry/bqls/langserver/internal/cache"
	"github.com/kitagry/bqls/langserver/internal/source/file"
	"github.com/sirupsen/logrus"
)

type Project struct {
	rootPath string
	logger   *logrus.Logger
	cache    *cache.GlobalCache
	bqClient bigquery.Client
	analyzer *file.Analyzer
}

type File struct {
	RawText string
	Version int
}

func NewProject(ctx context.Context, rootPath string, projectID string, logger *logrus.Logger) (*Project, error) {
	cache := cache.NewGlobalCache()

	if projectID == "" {
		out, err := exec.CommandContext(ctx, "gcloud", "config", "get", "project").Output()
		if err != nil {
			return nil, fmt.Errorf("You don't set Bigquery projectID. And fallback to run `gcloud config get project`, but got error: %w", err)
		}
		fields := strings.Fields(string(out))
		if len(fields) == 0 {
			return nil, fmt.Errorf("You don't set Bigquery projectID. And fallback to run `gcloud config get project`, but got empty output")
		}
		projectID = fields[0]
		logger.Infof("You don't set Bigquery projectID. And fallback to run `gcloud config get project`. set projectID: %s", projectID)
	}

	bqClient, err := bigquery.New(ctx, projectID, true)
	if err != nil {
		return nil, fmt.Errorf("failed to create bigquery client: %w", err)
	}

	analyzer := file.NewAnalyzer(bqClient)

	return &Project{
		rootPath: rootPath,
		logger:   logger,
		cache:    cache,
		bqClient: bqClient,
		analyzer: analyzer,
	}, nil
}

func NewProjectWithBQClient(rootPath string, bqClient bigquery.Client, logger *logrus.Logger) *Project {
	cache := cache.NewGlobalCache()
	analyzer := file.NewAnalyzer(bqClient)
	return &Project{
		rootPath: rootPath,
		logger:   logger,
		cache:    cache,
		bqClient: bqClient,
		analyzer: analyzer,
	}
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

func (p *Project) GetErrors(path string) map[string][]file.Error {
	sql := p.cache.Get(path)
	if sql == nil {
		return nil
	}

	parsedFile := p.analyzer.ParseFile(path, sql.RawText)
	if len(parsedFile.Errors) > 0 {
		return map[string][]file.Error{path: parsedFile.Errors}
	}

	return map[string][]file.Error{path: nil}
}

func (p *Project) Dryrun(ctx context.Context, path string) (*bq.JobStatus, error) {
	sql := p.cache.Get(path)
	if sql == nil {
		return nil, nil
	}

	dryrun := true
	result, err := p.bqClient.Run(ctx, sql.RawText, dryrun)
	if err != nil {
		return nil, err
	}

	return result.LastStatus(), nil
}

func (p *Project) Run(ctx context.Context, path string) (bigquery.BigqueryJob, error) {
	sql := p.cache.Get(path)
	if sql == nil {
		return nil, nil
	}

	dryrun := false
	result, err := p.bqClient.Run(ctx, sql.RawText, dryrun)
	if err != nil {
		return nil, err
	}

	return result, nil
}
