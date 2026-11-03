package bigquery

import (
	"context"
	"sync"
	"time"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/cloudresourcemanager/v1"
)

// authGuard recreates the client after an authentication error, because the
// client keeps the credentials it was created with and would not pick up a
// new login otherwise.
type authGuard struct {
	projectID     string
	newClient     func(ctx context.Context) (Client, error)
	onAuthError   func(err error)
	retryInterval time.Duration

	mu sync.Mutex
	// current is nil while creating the client fails, and createErr tells why.
	current   *guardedClient
	createErr error
	createdAt time.Time
	// stale means the current client failed to authenticate, so it should be
	// recreated with the credentials of a new login.
	stale bool
	// notified keeps onAuthError from being called again until a call
	// succeeds.
	notified bool
}

// guardedClient counts the calls in flight so that a replaced client is
// closed once they finish.
type guardedClient struct {
	Client
	users   int
	retired bool
}

// NewAuthGuard returns a Client that reports authentication errors to
// onAuthError, once until a call succeeds, and recreates the client with
// newClient at most once per retryInterval until it authenticates again.
// Missing credentials don't fail it, so that the user can log in later.
func NewAuthGuard(ctx context.Context, projectID string, newClient func(ctx context.Context) (Client, error), onAuthError func(err error), retryInterval time.Duration) (Client, error) {
	g := &authGuard{
		projectID:     projectID,
		newClient:     newClient,
		onAuthError:   onAuthError,
		retryInterval: retryInterval,
		createdAt:     time.Now(),
	}
	c, err := newClient(ctx)
	if err != nil {
		if !IsAuthError(err) {
			return nil, err
		}
		// The guidance waits for the first API call, since the client may
		// not be able to handle notifications while it is initializing.
		g.createErr = err
		return g, nil
	}
	g.current = &guardedClient{Client: c}
	return g, nil
}

func (g *authGuard) acquire(ctx context.Context) (*guardedClient, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	var retired *guardedClient
	if (g.current == nil || g.stale) && time.Since(g.createdAt) >= g.retryInterval {
		g.createdAt = time.Now()
		c, err := g.newClient(ctx)
		if err != nil {
			g.createErr = err
		} else {
			retired = g.current
			g.current = &guardedClient{Client: c}
			g.createErr = nil
			g.stale = false
		}
	}
	if retired != nil {
		retired.retired = true
		if retired.users == 0 {
			_ = retired.Close()
		}
	}

	if g.current == nil {
		g.observe(nil, g.createErr)
		return nil, g.createErr
	}
	g.current.users++
	return g.current, nil
}

func (g *authGuard) release(c *guardedClient, err error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	c.users--
	if c.retired && c.users == 0 {
		_ = c.Close()
	}
	g.observe(c, err)
}

// observe must be called with g.mu held.
func (g *authGuard) observe(c *guardedClient, err error) {
	if !IsAuthError(err) {
		if err == nil {
			g.notified = false
		}
		return
	}
	// An error from a replaced client says nothing about the current one.
	if c == g.current {
		g.stale = true
	}
	if !g.notified {
		g.notified = true
		g.onAuthError(err)
	}
}

func call[T any](ctx context.Context, g *authGuard, f func(c Client) (T, error)) (T, error) {
	c, err := g.acquire(ctx)
	if err != nil {
		var zero T
		return zero, err
	}
	result, err := f(c.Client)
	g.release(c, err)
	return result, err
}

func (g *authGuard) Close() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.current == nil {
		return nil
	}
	return g.current.Close()
}

func (g *authGuard) GetDefaultProject() string {
	return g.projectID
}

func (g *authGuard) ListProjects(ctx context.Context) ([]*cloudresourcemanager.Project, error) {
	return call(ctx, g, func(c Client) ([]*cloudresourcemanager.Project, error) {
		return c.ListProjects(ctx)
	})
}

func (g *authGuard) ListDatasets(ctx context.Context, projectID string) ([]*bigquery.Dataset, error) {
	return call(ctx, g, func(c Client) ([]*bigquery.Dataset, error) {
		return c.ListDatasets(ctx, projectID)
	})
}

func (g *authGuard) ListTables(ctx context.Context, projectID, datasetID string) ([]*bigquery.Table, error) {
	return call(ctx, g, func(c Client) ([]*bigquery.Table, error) {
		return c.ListTables(ctx, projectID, datasetID)
	})
}

func (g *authGuard) GetDatasetMetadata(ctx context.Context, projectID, datasetID string) (*bigquery.DatasetMetadata, error) {
	return call(ctx, g, func(c Client) (*bigquery.DatasetMetadata, error) {
		return c.GetDatasetMetadata(ctx, projectID, datasetID)
	})
}

func (g *authGuard) GetTableMetadata(ctx context.Context, projectID, datasetID, tableID string) (*bigquery.TableMetadata, error) {
	return call(ctx, g, func(c Client) (*bigquery.TableMetadata, error) {
		return c.GetTableMetadata(ctx, projectID, datasetID, tableID)
	})
}

func (g *authGuard) GetRoutineMetadata(ctx context.Context, projectID, datasetID, routineID string) (*bigquery.RoutineMetadata, error) {
	return call(ctx, g, func(c Client) (*bigquery.RoutineMetadata, error) {
		return c.GetRoutineMetadata(ctx, projectID, datasetID, routineID)
	})
}

func (g *authGuard) DryRunSchema(ctx context.Context, query string) (bigquery.Schema, error) {
	return call(ctx, g, func(c Client) (bigquery.Schema, error) {
		return c.DryRunSchema(ctx, query)
	})
}

func (g *authGuard) GetTableRecord(ctx context.Context, projectID, datasetID, tableID string) (*bigquery.RowIterator, error) {
	return call(ctx, g, func(c Client) (*bigquery.RowIterator, error) {
		return c.GetTableRecord(ctx, projectID, datasetID, tableID)
	})
}

func (g *authGuard) Run(ctx context.Context, q string, dryrun bool) (BigqueryJob, error) {
	return call(ctx, g, func(c Client) (BigqueryJob, error) {
		return c.Run(ctx, q, dryrun)
	})
}

func (g *authGuard) JobFromProject(ctx context.Context, projectID, jobID, location string) (BigqueryJob, error) {
	return call(ctx, g, func(c Client) (BigqueryJob, error) {
		return c.JobFromProject(ctx, projectID, jobID, location)
	})
}

func (g *authGuard) Jobs(ctx context.Context) (*bigquery.JobIterator, error) {
	return call(ctx, g, func(c Client) (*bigquery.JobIterator, error) {
		return c.Jobs(ctx)
	})
}

func (g *authGuard) SearchTables(ctx context.Context, projectIDs []string, query string) ([]TableSearchResult, error) {
	return call(ctx, g, func(c Client) ([]TableSearchResult, error) {
		return c.SearchTables(ctx, projectIDs, query)
	})
}
