package bigquery

import (
	"context"
	"fmt"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/cloudresourcemanager/v1"
)

type cache struct {
	bqClient           Client
	projectCache       []*cloudresourcemanager.Project
	datasetCache       map[string][]string
	tableCache         map[string][]string
	tableMetadataCache map[string]*bigquery.TableMetadata
}

func NewWithCache(bqClient Client) *cache {
	return &cache{
		bqClient:     bqClient,
		projectCache: make([]*cloudresourcemanager.Project, 0),
		datasetCache: make(map[string][]string),
		tableCache:   make(map[string][]string),
	}
}

func (c *cache) Close() error {
	return c.bqClient.Close()
}

func (c *cache) GetDefaultProject() string {
	return c.bqClient.GetDefaultProject()
}

func (c *cache) ListProjects(ctx context.Context) ([]*cloudresourcemanager.Project, error) {
	if len(c.projectCache) > 0 {
		return c.projectCache, nil
	}

	result, err := c.bqClient.ListProjects(ctx)
	if err != nil {
		return nil, err
	}

	if result != nil {
		c.projectCache = result
	}
	return result, nil
}

func (c *cache) ListDatasets(ctx context.Context, projectID string) ([]string, error) {
	if len(c.datasetCache[projectID]) > 0 {
		return c.datasetCache[projectID], nil
	}

	result, err := c.bqClient.ListDatasets(ctx, projectID)
	if err != nil {
		return nil, err
	}

	if result != nil {
		c.datasetCache[projectID] = result
	}
	return result, nil
}

func (c *cache) ListTables(ctx context.Context, projectID, datasetID string) ([]string, error) {
	cacheKey := fmt.Sprintf("%s:%s", projectID, datasetID)
	if len(c.tableCache[cacheKey]) > 0 {
		return c.tableCache[cacheKey], nil
	}

	result, err := c.bqClient.ListTables(ctx, projectID, datasetID)
	if err != nil {
		return nil, err
	}

	if result != nil {
		c.tableCache[cacheKey] = result
	}
	return result, nil
}

func (c *cache) GetTableMetadata(ctx context.Context, projectID, datasetID, tableID string) (*bigquery.TableMetadata, error) {
	cacheKey := fmt.Sprintf("%s:%s:%s", projectID, datasetID, tableID)
	cache, ok := c.tableMetadataCache[cacheKey]
	if ok {
		return cache, nil
	}

	result, err := c.bqClient.GetTableMetadata(ctx, projectID, datasetID, tableID)
	if err != nil {
		return nil, err
	}

	if result != nil {
		c.tableMetadataCache[cacheKey] = result
	}
	return result, nil
}
