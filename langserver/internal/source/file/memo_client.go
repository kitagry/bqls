package file

import (
	"context"
	"sync"

	bq "cloud.google.com/go/bigquery"
	"github.com/kitagry/bqls/langserver/internal/bigquery"
)

// memoClient remembers the metadata lookups of one ParseFile call, including
// failures. Every retry of ParseFile analyzes against a fresh catalog, which
// would otherwise fetch every table again (#457).
type memoClient struct {
	bigquery.Client
	mu       sync.Mutex
	tables   map[string]memoResult[*bq.TableMetadata]
	routines map[string]memoResult[*bq.RoutineMetadata]
	schemas  map[string]memoResult[bq.Schema]
}

type memoResult[T any] struct {
	value T
	err   error
}

func newMemoClient(c bigquery.Client) *memoClient {
	return &memoClient{
		Client:   c,
		tables:   make(map[string]memoResult[*bq.TableMetadata]),
		routines: make(map[string]memoResult[*bq.RoutineMetadata]),
		schemas:  make(map[string]memoResult[bq.Schema]),
	}
}

func (c *memoClient) GetTableMetadata(ctx context.Context, projectID, datasetID, tableID string) (*bq.TableMetadata, error) {
	return memoize(&c.mu, c.tables, projectID+":"+datasetID+":"+tableID, func() (*bq.TableMetadata, error) {
		return c.Client.GetTableMetadata(ctx, projectID, datasetID, tableID)
	})
}

func (c *memoClient) GetRoutineMetadata(ctx context.Context, projectID, datasetID, routineID string) (*bq.RoutineMetadata, error) {
	return memoize(&c.mu, c.routines, projectID+":"+datasetID+":"+routineID, func() (*bq.RoutineMetadata, error) {
		return c.Client.GetRoutineMetadata(ctx, projectID, datasetID, routineID)
	})
}

func (c *memoClient) DryRunSchema(ctx context.Context, query string) (bq.Schema, error) {
	return memoize(&c.mu, c.schemas, query, func() (bq.Schema, error) {
		return c.Client.DryRunSchema(ctx, query)
	})
}

func memoize[T any](mu *sync.Mutex, results map[string]memoResult[T], key string, fetch func() (T, error)) (T, error) {
	mu.Lock()
	defer mu.Unlock()
	if r, ok := results[key]; ok {
		return r.value, r.err
	}
	value, err := fetch()
	results[key] = memoResult[T]{value, err}
	return value, err
}
