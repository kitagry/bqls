package bigquery

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/cloudresourcemanager/v1"
)

type cache struct {
	db                 *database
	bqClient           Client
	tableMetadataCache map[string]*bigquery.TableMetadata

	onceListProjects *sync.Once
	onceListDatasets map[string]*sync.Once
	onceListTables   map[string]*sync.Once
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
		onceListProjects:   &sync.Once{},
		onceListDatasets:   make(map[string]*sync.Once),
		onceListTables:     make(map[string]*sync.Once),
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
		// recache the latest projects
		go c.onceListProjects.Do(func() {
			ctx := context.WithoutCancel(ctx)
			_, err := c.callListProjects(ctx)
			if err != nil {
				fmt.Fprintf(os.Stderr, "failed to recache projects: %v\n", err)
			}
		})
		return results, nil
	}
	if err != nil {
		// TODO
		fmt.Fprintf(os.Stderr, "failed to select projects: %v\n", err)
	}

	return c.callListProjects(ctx)
}

func (c *cache) callListProjects(ctx context.Context) ([]*cloudresourcemanager.Project, error) {
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
		if c.onceListDatasets[projectID] == nil {
			c.onceListDatasets[projectID] = &sync.Once{}
		}
		go c.onceListDatasets[projectID].Do(func() {
			ctx := context.WithoutCancel(ctx)
			_, err := c.callListDatasets(ctx, projectID)
			if err != nil {
				fmt.Fprintf(os.Stderr, "failed to recache datasets: %v\n", err)
			}
		})
		return results, nil
	}
	if err != nil {
		// TODO
		fmt.Fprintf(os.Stderr, "failed to select datasets: %v\n", err)
	}

	return c.callListDatasets(ctx, projectID)
}

func (c *cache) callListDatasets(ctx context.Context, projectID string) ([]*bigquery.Dataset, error) {
	result, err := c.bqClient.ListDatasets(ctx, projectID)
	if err != nil {
		return nil, err
	}

	if len(result) > 0 {
		err := c.db.ReplaceDatasets(ctx, projectID, result)
		if err != nil {
			// TODO
			fmt.Fprintf(os.Stderr, "failed to insert datasets: %v\n", err)
		}
	}
	return result, nil
}

func (c *cache) ListTables(ctx context.Context, projectID, datasetID string, onlyLatestSuffix bool) ([]*bigquery.Table, error) {
	results, err := c.db.SelectTables(ctx, projectID, datasetID)
	if err == nil && len(results) > 0 {
		key := fmt.Sprintf("%s.%s", projectID, datasetID)
		if c.onceListTables[key] == nil {
			c.onceListTables[key] = &sync.Once{}
		}

		go c.onceListTables[key].Do(func() {
			ctx := context.WithoutCancel(ctx)
			_, err := c.callListTables(ctx, projectID, datasetID, false)
			if err != nil {
				fmt.Fprintf(os.Stderr, "failed to recache tables: %v\n", err)
			}
		})

		if onlyLatestSuffix {
			return extractLatestSuffixTables(results), nil
		}
		return results, nil
	}
	if err != nil {
		// TODO
		fmt.Fprintf(os.Stderr, "failed to select tables: %v\n", err)
	}

	return c.callListTables(ctx, projectID, datasetID, onlyLatestSuffix)
}

func (c *cache) callListTables(ctx context.Context, projectID, datasetID string, onlyLatestSuffix bool) ([]*bigquery.Table, error) {
	// onlyLatestSuffix is ignored for cache.
	result, err := c.bqClient.ListTables(ctx, projectID, datasetID, false)
	if err != nil {
		return nil, err
	}

	if len(result) > 0 {
		err := c.db.ReplaceTables(ctx, projectID, datasetID, result)
		if err != nil {
			// TODO
			fmt.Fprintf(os.Stderr, "failed to insert tables: %v\n", err)
		}
	}

	if onlyLatestSuffix {
		result = extractLatestSuffixTables(result)
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

func (c *cache) GetTableRecord(ctx context.Context, projectID, datasetID, tableID string) (*bigquery.RowIterator, error) {
	return c.bqClient.GetTableRecord(ctx, projectID, datasetID, tableID)
}

func (c *cache) Run(ctx context.Context, q string, dryrun bool) (BigqueryJob, error) {
	return c.bqClient.Run(ctx, q, dryrun)
}
