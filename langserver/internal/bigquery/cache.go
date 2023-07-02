package bigquery

import (
	"context"
	"errors"
	"fmt"
	"os"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/cloudresourcemanager/v1"
)

type cache struct {
	db                 *database
	bqClient           Client
	tableMetadataCache map[string]*bigquery.TableMetadata
}

func newCache(bqClient Client) (*cache, error) {
	db, err := newDB()
	if err != nil {
		return nil, err
	}

	err = db.Migrate()
	if err != nil {
		return nil, fmt.Errorf("Migrate: %w", err)
	}

	return &cache{
		db:                 db,
		bqClient:           bqClient,
		tableMetadataCache: make(map[string]*bigquery.TableMetadata),
	}, nil
}

func (c *cache) Close() error {
	var errs []error
	errs = append(errs, c.db.Close())
	errs = append(errs, c.bqClient.Close())
	return errors.Join(errs...)
}

func (c *cache) GetDefaultProject() string {
	return c.bqClient.GetDefaultProject()
}

func (c *cache) ListProjects(ctx context.Context) ([]*cloudresourcemanager.Project, error) {
	results, err := c.db.SelectProjects(ctx)
	if err == nil && len(results) > 0 {
		return results, nil
	}
	if err != nil {
		// TODO
		fmt.Fprintf(os.Stderr, "failed to select projects: %v\n", err)
	}

	result, err := c.bqClient.ListProjects(ctx)
	if err != nil {
		return nil, err
	}

	if len(result) > 0 {
		err := c.db.InsertProjects(ctx, result)
		if err != nil {
			// TODO
			fmt.Fprintf(os.Stderr, "failed to insert projects: %v\n", err)
		}
	}
	return result, nil
}

func (c *cache) ListDatasets(ctx context.Context, projectID string) ([]*bigquery.Dataset, error) {
	results, err := c.db.SelectDatasets(ctx, projectID)
	if err == nil && len(results) > 0 {
		return results, nil
	}
	if err != nil {
		// TODO
		fmt.Fprintf(os.Stderr, "failed to select datasets: %v\n", err)
	}

	result, err := c.bqClient.ListDatasets(ctx, projectID)
	if err != nil {
		return nil, err
	}

	if len(result) > 0 {
		err := c.db.InsertDatasets(ctx, result)
		if err != nil {
			// TODO
			fmt.Fprintf(os.Stderr, "failed to insert datasets: %v\n", err)
		}
	}
	return result, nil
}

func (c *cache) ListTables(ctx context.Context, projectID, datasetID string) ([]*bigquery.Table, error) {
	results, err := c.db.SelectTables(ctx, projectID, datasetID)
	if err == nil && len(results) > 0 {
		return results, nil
	}
	if err != nil {
		// TODO
		fmt.Fprintf(os.Stderr, "failed to select tables: %v\n", err)
	}

	result, err := c.bqClient.ListTables(ctx, projectID, datasetID)
	if err != nil {
		return nil, err
	}

	if len(result) > 0 {
		err := c.db.InsertTables(ctx, result)
		if err != nil {
			// TODO
			fmt.Fprintf(os.Stderr, "failed to insert tables: %v\n", err)
		}
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

func (c *cache) Run(ctx context.Context, q string, dryrun bool) (BigqueryJob, error) {
	return c.bqClient.Run(ctx, q, dryrun)
}
