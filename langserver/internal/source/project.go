package source

import (
	"context"
	"fmt"
	"strings"

	bq "cloud.google.com/go/bigquery"
	"github.com/kitagry/bqls/langserver/internal/bigquery"
	"github.com/kitagry/bqls/langserver/internal/cache"
	"github.com/kitagry/bqls/langserver/internal/lsp"
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

func NewProject(ctx context.Context, rootPath string, bqClient bigquery.Client, logger *logrus.Logger) *Project {
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

func (p *Project) UpdateFile(uri lsp.DocumentURI, text string, version int) error {
	p.cache.Put(uri, text)

	return nil
}

func (p *Project) GetFile(uri lsp.DocumentURI) (string, bool) {
	sql := p.cache.Get(uri)
	if sql == nil {
		return "", false
	}
	return sql.RawText, true
}

func (p *Project) DeleteFile(uri lsp.DocumentURI) {
	p.cache.Delete(uri)
}

func (p *Project) GetErrors(uri lsp.DocumentURI) map[lsp.DocumentURI][]file.Error {
	sql := p.cache.Get(uri)
	if sql == nil {
		return nil
	}

	parsedFile := p.analyzer.ParseFile(uri, sql.RawText)
	if len(parsedFile.Errors) > 0 {
		return map[lsp.DocumentURI][]file.Error{uri: parsedFile.Errors}
	}

	return map[lsp.DocumentURI][]file.Error{uri: nil}
}

func (p *Project) Dryrun(ctx context.Context, uri lsp.DocumentURI) (*bq.JobStatus, error) {
	sql := p.cache.Get(uri)
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

func (p *Project) Run(ctx context.Context, uri lsp.DocumentURI) (bigquery.BigqueryJob, error) {
	sql := p.cache.Get(uri)
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

func (p *Project) GetJobInfo(ctx context.Context, projectID, jobID string) ([]lsp.MarkedString, *bq.RowIterator, error) {
	job, err := p.bqClient.JobFromProject(ctx, projectID, jobID)
	if err != nil {
		return nil, nil, err
	}

	// FIXME: region should be dynamic
	markedStrings, err := buildBigQueryJobMarkedString(projectID, "US", job)
	if err != nil {
		return nil, nil, err
	}

	it, err := job.Read(ctx)
	if err != nil {
		return markedStrings, nil, nil
	}
	return markedStrings, it, nil
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

	config, err := job.Config()
	if err != nil {
		return result, nil
	}

	switch c := config.(type) {
	case *bq.QueryConfig:
		result = append(result, lsp.MarkedString{
			Language: "sql",
			Value:    c.Q,
		})
	}

	return result, nil
}

func (p *Project) GetTableInfo(ctx context.Context, projectID, datasetID, tableID string) ([]lsp.MarkedString, *bq.RowIterator, error) {
	tableMetadata, err := p.bqClient.GetTableMetadata(ctx, projectID, datasetID, tableID)
	if err != nil {
		return nil, nil, err
	}

	markedStrings, err := buildBigQueryTableMetadataMarkedString(tableMetadata)
	if err != nil {
		return nil, nil, err
	}

	if tableMetadata.Type != bq.RegularTable {
		return markedStrings, nil, nil
	}

	it, err := p.bqClient.GetTableRecord(ctx, projectID, datasetID, tableID)
	if err != nil {
		return markedStrings, nil, err
	}
	return markedStrings, it, nil
}

func (p *Project) GetTablePreview(ctx context.Context, projectID, datasetID, tableID string) (*bq.RowIterator, error) {
	return p.bqClient.GetTableRecord(ctx, projectID, datasetID, tableID)
}
