package bigquery

import (
	"context"
	"fmt"
	"os/exec"
	"strings"

	"cloud.google.com/go/bigquery"
	"github.com/sirupsen/logrus"
	"google.golang.org/api/cloudresourcemanager/v1"
	"google.golang.org/api/iterator"
)

const (
	defaultLocation = "US" // Default location for BigQuery if not specified
)

type Client interface {
	Close() error

	// GetDefaultProject returns the default project of the current user.
	GetDefaultProject() string

	// ListProjects lists all projects the current user has access to.
	ListProjects(ctx context.Context) ([]*cloudresourcemanager.Project, error)

	// ListDatasets lists all datasets in the specified project.
	ListDatasets(ctx context.Context, projectID string) ([]*bigquery.Dataset, error)

	// ListTables lists all tables in the specified dataset.
	ListTables(ctx context.Context, projectID, datasetID string) ([]*bigquery.Table, error)

	// GetDatasetMetadata returns the metadata of the specified dataset.
	GetDatasetMetadata(ctx context.Context, projectID, datasetID string) (*bigquery.DatasetMetadata, error)

	// GetTableMetadata returns the metadata of the specified table.
	GetTableMetadata(ctx context.Context, projectID, datasetID, tableID string) (*bigquery.TableMetadata, error)

	// GetTableRecord returns the row of the specified table.
	GetTableRecord(ctx context.Context, projectID, datasetID, tableID string) (*bigquery.RowIterator, error)

	// Run runs the specified query.
	Run(ctx context.Context, q string, dryrun bool) (BigqueryJob, error)

	// JobFromID returns the job with the specified ID.
	JobFromProject(ctx context.Context, projectID, jobID, location string) (BigqueryJob, error)

	// Jobs returns the iterator of all jobs.
	Jobs(ctx context.Context) *bigquery.JobIterator
}

type client struct {
	bqClient                    *bigquery.Client
	cloudresourcemanagerService *cloudresourcemanager.Service
}

func New(ctx context.Context, projectID, location string, withCache bool, looger *logrus.Logger) (Client, error) {
	if projectID == "" {
		var err error
		projectID, err = getDefaultProjectID(ctx, looger)
		if err != nil {
			return nil, fmt.Errorf("getDefaultProjectID: %w", err)
		}
	}

	cloudresourcemanagerService, err := cloudresourcemanager.NewService(ctx)
	if err != nil {
		return nil, fmt.Errorf("cloudresourcemanager.NewService: %w", err)
	}

	bqClient, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		return nil, fmt.Errorf("bigquery.NewClient: %w", err)
	}

	if location == "" {
		location = defaultLocation
	}
	bqClient.Location = location

	var client Client = &client{bqClient, cloudresourcemanagerService}
	if withCache {
		client, err = newCache(client)
		if err != nil {
			return nil, fmt.Errorf("newCache: %w", err)
		}
	}

	return client, nil
}

func getDefaultProjectID(ctx context.Context, logger *logrus.Logger) (projectID string, err error) {
	out, err := exec.CommandContext(ctx, "gcloud", "config", "get", "project", "--format=json").Output()
	if err != nil {
		return "", fmt.Errorf("You don't set Bigquery projectID. And fallback to run `gcloud config get project`, but got error: %w", err)
	}
	fields := strings.Fields(string(out))
	if len(fields) == 0 {
		return "", fmt.Errorf("You don't set Bigquery projectID. And fallback to run `gcloud config get project`, but got empty output")
	}
	for _, field := range fields {
		if strings.HasPrefix(field, "\"") && strings.HasSuffix(field, "\"") {
			projectID = strings.Trim(field, "\"")
			break
		}
	}
	logger.Infof("You don't set Bigquery projectID. And fallback to run `gcloud config get project`. set projectID: %s", projectID)
	return
}

func (c *client) Close() error {
	return c.bqClient.Close()
}

func (c *client) GetDefaultProject() string {
	return c.bqClient.Project()
}

func (c *client) ListProjects(ctx context.Context) ([]*cloudresourcemanager.Project, error) {
	caller := c.cloudresourcemanagerService.Projects.List().PageSize(1000).Context(ctx)

	list, err := caller.Do()
	if err != nil {
		return nil, fmt.Errorf("cloudresourcemanagerService.Projects.List: %w", err)
	}

	result := make([]*cloudresourcemanager.Project, 0, len(list.Projects))
	result = append(result, list.Projects...)

	for list.NextPageToken != "" {
		list, err = caller.PageToken(list.NextPageToken).Do()
		if err != nil {
			return nil, fmt.Errorf("cloudresourcemanagerService.Projects.List: %w", err)
		}

		result = append(result, list.Projects...)
	}

	return result, nil
}

func (c *client) ListDatasets(ctx context.Context, projectID string) ([]*bigquery.Dataset, error) {
	d := c.bqClient.Datasets(ctx)
	d.ProjectID = projectID

	datasets := make([]*bigquery.Dataset, 0)
	for {
		dt, err := d.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("fail to scan DatasetsInProject: %w", err)
		}

		datasets = append(datasets, dt)
	}
	return datasets, nil
}

func (c *client) ListTables(ctx context.Context, projectID, datasetID string) ([]*bigquery.Table, error) {
	dataset := c.bqClient.DatasetInProject(projectID, datasetID)

	it := dataset.Tables(ctx)
	it.PageInfo().MaxSize = 1000

	tables := make([]*bigquery.Table, 0)
	for {
		table, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("fail to scan DatasetInProject: %w", err)
		}

		if strings.HasPrefix(table.TableID, "LOAD_TEMP_") {
			continue
		}

		tables = append(tables, table)
	}

	tables = extractLatestSuffixTables(tables)

	return tables, nil
}

func (c *client) GetDatasetMetadata(ctx context.Context, projectID, datasetID string) (*bigquery.DatasetMetadata, error) {
	md, err := c.bqClient.DatasetInProject(projectID, datasetID).Metadata(ctx)
	if err != nil {
		return nil, fmt.Errorf("fail to get metadata: %w", err)
	}

	return md, nil
}

func (c *client) GetTableMetadata(ctx context.Context, projectID, datasetID, tableID string) (*bigquery.TableMetadata, error) {
	md, err := c.bqClient.DatasetInProject(projectID, datasetID).Table(tableID).Metadata(ctx)
	if err != nil {
		return nil, fmt.Errorf("fail to get metadata: %w", err)
	}

	return md, nil
}

func (c *client) GetTableRecord(ctx context.Context, projectID, datasetID, tableID string) (*bigquery.RowIterator, error) {
	md, err := c.bqClient.DatasetInProject(projectID, datasetID).Table(tableID).Metadata(ctx)
	if err != nil {
		return nil, fmt.Errorf("fail to get metadata: %w", err)
	}

	it, err := c.bqClient.DatasetInProject(projectID, datasetID).Table(tableID).Read(ctx), nil
	if err != nil {
		return nil, fmt.Errorf("failed to get iterator: %w", err)
	}
	it.Schema = md.Schema
	return it, nil
}

type BigqueryJob interface {
	ID() string
	Location() string
	ProjectID() string
	Read(context.Context) (*bigquery.RowIterator, error)
	LastStatus() *bigquery.JobStatus
	Config() (bigquery.JobConfig, error)
	URL() string
}

type bqJobWrapper struct {
	*bigquery.Job
}

func newJobWrapper(job *bigquery.Job) BigqueryJob {
	return &bqJobWrapper{job}
}

func (j *bqJobWrapper) URL() string {
	return fmt.Sprintf("https://console.cloud.google.com/bigquery?project=%s&ws=!1m5!1m4!1m3!1s%s!2s%s!3s%s", j.Job.ProjectID(), j.Job.ProjectID(), j.Job.ID(), j.Job.Location())
}

func (c *client) Run(ctx context.Context, q string, dryrun bool) (BigqueryJob, error) {
	query := c.bqClient.Query(q)
	query.DryRun = dryrun
	query.UseLegacySQL = false
	job, err := query.Run(ctx)
	if err != nil {
		return nil, fmt.Errorf("fail to run query: %w", err)
	}

	return newJobWrapper(job), nil
}

func (c *client) JobFromProject(ctx context.Context, projectID, jobID, location string) (BigqueryJob, error) {
	job, err := c.bqClient.JobFromProject(ctx, projectID, jobID, location)
	if err != nil {
		return nil, fmt.Errorf("fail to get job: %w", err)
	}
	return newJobWrapper(job), nil
}

func (c *client) Jobs(ctx context.Context) *bigquery.JobIterator {
	return c.bqClient.Jobs(ctx)
}
