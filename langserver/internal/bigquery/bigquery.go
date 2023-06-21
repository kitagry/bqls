package bigquery

import (
	"context"
	"fmt"

	"cloud.google.com/go/bigquery"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/cloudresourcemanager/v1"
	"google.golang.org/api/compute/v1"
	"google.golang.org/api/iterator"
)

type Client interface {
	Close() error

	// GetDefaultProject returns the default project of the current user.
	GetDefaultProject() string

	// ListProjects lists all projects the current user has access to.
	ListProjects(ctx context.Context) ([]*cloudresourcemanager.Project, error)

	// ListDatasets lists all datasets in the specified project.
	ListDatasets(ctx context.Context, projectID string) ([]string, error)

	// ListTables lists all tables in the specified dataset.
	ListTables(ctx context.Context, projectID, datasetID string) ([]*bigquery.Table, error)

	// GetTableMetadata returns the metadata of the specified table.
	GetTableMetadata(ctx context.Context, projectID, datasetID, tableID string) (*bigquery.TableMetadata, error)
}

type client struct {
	bqClient                    *bigquery.Client
	cloudresourcemanagerService *cloudresourcemanager.Service
}

func New(ctx context.Context, withCache bool) (Client, error) {
	cloudresourcemanagerService, err := cloudresourcemanager.NewService(ctx)
	if err != nil {
		return nil, fmt.Errorf("cloudresourcemanager.NewService: %w", err)
	}

	credentials, err := google.FindDefaultCredentials(ctx, compute.ComputeScope)
	if err != nil {
		return nil, fmt.Errorf("google.FindDefaultCredentials: %w", err)
	}

	bqClient, err := bigquery.NewClient(ctx, credentials.ProjectID)
	if err != nil {
		return nil, fmt.Errorf("bigquery.NewClient: %w", err)
	}

	var client Client = &client{bqClient, cloudresourcemanagerService}
	if withCache {
		client = newCache(client)
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
	results, err := c.cloudresourcemanagerService.Projects.List().Context(ctx).Do()
	if err != nil {
		return nil, fmt.Errorf("cloudresourcemanagerService.Projects.List: %w", err)
	}

	return results.Projects, nil
}

func (c *client) ListDatasets(ctx context.Context, projectID string) ([]string, error) {
	it := c.bqClient.DatasetsInProject(ctx, projectID)

	datasets := make([]string, 0)
	for {
		dt, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("fail to scan DatasetsInProject: %w", err)
		}

		datasets = append(datasets, dt.DatasetID)
	}
	return datasets, nil
}

func (c *client) ListTables(ctx context.Context, projectID, datasetID string) ([]*bigquery.Table, error) {
	dataset := c.bqClient.DatasetInProject(projectID, datasetID)

	it := dataset.Tables(ctx)

	tables := make([]*bigquery.Table, 0)
	for {
		table, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("fail to scan DatasetInProject: %w", err)
		}

		tables = append(tables, table)
	}
	return tables, nil
}

func (c *client) GetTableMetadata(ctx context.Context, projectID, datasetID, tableID string) (*bigquery.TableMetadata, error) {
	md, err := c.bqClient.DatasetInProject(projectID, datasetID).Table(tableID).Metadata(ctx)
	if err != nil {
		return nil, fmt.Errorf("fail to get metadata: %w", err)
	}

	return md, nil
}
