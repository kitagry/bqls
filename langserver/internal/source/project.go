package source

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strings"

	bq "cloud.google.com/go/bigquery"
	"github.com/kitagry/bqls/langserver/internal/bigquery"
	"github.com/kitagry/bqls/langserver/internal/cache"
	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/kitagry/bqls/langserver/internal/source/file"
	"github.com/sirupsen/logrus"
	"google.golang.org/api/iterator"
)

type Project struct {
	BigQueryProjectID string
	rootPath          string
	logger            *logrus.Logger
	cache             *cache.GlobalCache
	bqClient          bigquery.Client
	analyzer          *file.Analyzer
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

	analyzer := file.NewAnalyzer(logger, bqClient)

	return &Project{
		BigQueryProjectID: projectID,
		rootPath:          rootPath,
		logger:            logger,
		cache:             cache,
		bqClient:          bqClient,
		analyzer:          analyzer,
	}, nil
}

func NewProjectWithBQClient(rootPath string, bqClient bigquery.Client, logger *logrus.Logger) *Project {
	cache := cache.NewGlobalCache()
	analyzer := file.NewAnalyzer(logger, bqClient)
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

func (p *Project) ListDatasets(ctx context.Context, projectID string) ([]*bq.Dataset, error) {
	return p.bqClient.ListDatasets(ctx, projectID)
}

func (p *Project) ListTables(ctx context.Context, projectID, datasetID string) ([]*bq.Table, error) {
	return p.bqClient.ListTables(ctx, projectID, datasetID, true)
}

func (p *Project) GetJobInfo(ctx context.Context, projectID, jobID string) (lsp.VirtualTextDocument, error) {
	job, err := p.bqClient.JobFromProject(ctx, projectID, jobID)
	if err != nil {
		return lsp.VirtualTextDocument{}, err
	}

	// FIXME: region should be dynamic
	markedStrings, err := buildBigQueryJobMarkedString(projectID, "US", job)
	if err != nil {
		return lsp.VirtualTextDocument{}, err
	}

	it, err := job.Read(ctx)
	if err != nil {
		return lsp.VirtualTextDocument{}, err
	}
	queryResult, err := buildQueryResult(it)
	if err != nil {
		return lsp.VirtualTextDocument{}, err
	}

	return lsp.VirtualTextDocument{Contents: markedStrings, Result: queryResult}, nil
}

func buildBigQueryJobMarkedString(projectID, region string, job bigquery.BigqueryJob) ([]lsp.MarkedString, error) {
	var result []lsp.MarkedString

	sb := strings.Builder{}
	sb.WriteString(fmt.Sprintf("## Job %s\n", job.ID()))

	status := job.LastStatus()

	sb.WriteString("\n### Job info\n\n")
	sb.WriteString(fmt.Sprintf("* Created: %s\n", status.Statistics.CreationTime.Format("2006-01-02 15:04:05")))
	sb.WriteString(fmt.Sprintf("* Started: %s\n", status.Statistics.StartTime.Format("2006-01-02 15:04:05")))
	endTimeStr := status.Statistics.EndTime.Format("2006-01-02 15:04:05")
	if status.Statistics.EndTime.IsZero() {
		endTimeStr = "Not finished"
	}
	sb.WriteString(fmt.Sprintf("* Ended: %s\n", endTimeStr))

	if len(status.Errors) > 0 {
		sb.WriteString("* Errors:\n")
		for _, e := range status.Errors {
			sb.WriteString(fmt.Sprintf("  * %s\n", e.Message))
		}
	}

	sb.WriteString(fmt.Sprintf("* Bytes processed: %s\n", bytesConvert(status.Statistics.TotalBytesProcessed)))

	switch details := status.Statistics.Details.(type) {
	case *bq.QueryStatistics:
		sb.WriteString(fmt.Sprintf("* Bytes billed: %s\n", bytesConvert(details.TotalBytesBilled)))
		sb.WriteString(fmt.Sprintf("* Slot milliseconds: %d\n", details.SlotMillis))
	}

	sb.WriteString(fmt.Sprintf("\n[Query URL](https://console.cloud.google.com/bigquery?project=%s&ws=!1m5!1m4!1m3!1s%s!2s%s!3s%s)\n", projectID, projectID, job.ID(), region))

	result = append(result, lsp.MarkedString{
		Language: "markdown",
		Value:    sb.String(),
	})
	return result, nil
}

func (p *Project) GetTableInfo(ctx context.Context, projectID, datasetID, tableID string) (lsp.VirtualTextDocument, error) {
	tableMetadata, err := p.bqClient.GetTableMetadata(ctx, projectID, datasetID, tableID)
	if err != nil {
		return lsp.VirtualTextDocument{}, err
	}

	markedStrings, err := buildBigQueryTableMetadataMarkedString(tableMetadata)
	if err != nil {
		return lsp.VirtualTextDocument{}, err
	}

	it, err := p.bqClient.GetTableRecord(ctx, projectID, datasetID, tableID)
	if err != nil {
		return lsp.VirtualTextDocument{}, err
	}
	it.Schema = tableMetadata.Schema
	queryResult, err := buildQueryResult(it)
	if err != nil {
		return lsp.VirtualTextDocument{}, err
	}

	return lsp.VirtualTextDocument{Contents: markedStrings, Result: queryResult}, nil
}

func (p *Project) GetTablePreview(ctx context.Context, projectID, datasetID, tableID string) (*bq.RowIterator, error) {
	return p.bqClient.GetTableRecord(ctx, projectID, datasetID, tableID)
}

func buildQueryResult(it *bq.RowIterator) (lsp.QueryResult, error) {
	var result lsp.QueryResult

	for _, field := range it.Schema {
		result.Columns = append(result.Columns, field.Name)
	}

	for i := 0; i < 100; i++ {
		var values []bq.Value
		err := it.Next(&values)
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return result, err
		}

		result.Data = append(result.Data, values)
	}

	return result, nil
}
