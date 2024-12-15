package bigquery

import (
	"context"
	"fmt"
	"strings"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/cloudresourcemanager/v1"
	"google.golang.org/api/iterator"
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

	// GetTableMetadata returns the metadata of the specified table.
	GetTableMetadata(ctx context.Context, projectID, datasetID, tableID string) (*bigquery.TableMetadata, error)

	// GetTableRecord returns the row of the specified table.
	GetTableRecord(ctx context.Context, projectID, datasetID, tableID string) (*bigquery.RowIterator, error)

	// Run runs the specified query.
	Run(ctx context.Context, q string, dryrun bool) (BigqueryJob, error)

	// JobFromID returns the job with the specified ID.
	JobFromProject(ctx context.Context, projectID, id string) (BigqueryJob, error)

	// Jobs returns the iterator of all jobs.
	Jobs(ctx context.Context) *bigquery.JobIterator
}

type client struct {
	bqClient                    *bigquery.Client
	cloudresourcemanagerService *cloudresourcemanager.Service
}

func New(ctx context.Context, projectID string, withCache bool) (Client, error) {
	cloudresourcemanagerService, err := cloudresourcemanager.NewService(ctx)
	if err != nil {
		return nil, fmt.Errorf("cloudresourcemanager.NewService: %w", err)
	}

	bqClient, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		return nil, fmt.Errorf("bigquery.NewClient: %w", err)
	}

	var client Client = &client{bqClient, cloudresourcemanagerService}
	if withCache {
		client, err = newCache(client)
		if err != nil {
			return nil, fmt.Errorf("newCache: %w", err)
		}
	}

	return client, nil
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

func (c *client) GetTableMetadata(ctx context.Context, projectID, datasetID, tableID string) (*bigquery.TableMetadata, error) {
	md, err := c.bqClient.DatasetInProject(projectID, datasetID).Table(tableID).Metadata(ctx)
	if err != nil {
		return nil, fmt.Errorf("fail to get metadata: %w", err)
	}

	return md, nil
}

func (c *client) GetTableRecord(ctx context.Context, projectID, datasetID, tableID string) (*bigquery.RowIterator, error) {
	return c.bqClient.DatasetInProject(projectID, datasetID).Table(tableID).Read(ctx), nil
}

type BigqueryJob interface {
	ID() string
	Read(context.Context) (*bigquery.RowIterator, error)
	LastStatus() *bigquery.JobStatus
	Config() (bigquery.JobConfig, error)
}

func (c *client) Run(ctx context.Context, q string, dryrun bool) (BigqueryJob, error) {
	query := c.bqClient.Query(q)
	query.DryRun = dryrun
	query.UseLegacySQL = false
	job, err := query.Run(ctx)
	if err != nil {
		return nil, fmt.Errorf("fail to run query: %w", err)
	}

	return job, nil
}

func (c *client) JobFromProject(ctx context.Context, projectID, id string) (BigqueryJob, error) {
	return c.bqClient.JobFromProject(ctx, projectID, id, c.bqClient.Location)
}

func (c *client) Jobs(ctx context.Context) *bigquery.JobIterator {
	return c.bqClient.Jobs(ctx)
}
