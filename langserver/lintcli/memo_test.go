package lintcli

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	bq "cloud.google.com/go/bigquery"
	"github.com/golang/mock/gomock"
	"github.com/google/go-cmp/cmp"
	"github.com/kitagry/bqls/langserver/internal/bigquery/mock_bigquery"
	"github.com/kitagry/bqls/langserver/internal/source/file"
	"google.golang.org/api/googleapi"
)

// barrier returns a function that blocks until n callers have called it, or
// fails after a timeout so that serialized callers do not hang the test.
func barrier(n int) func() error {
	var wg sync.WaitGroup
	wg.Add(n)
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	return func() error {
		wg.Done()
		select {
		case <-done:
			return nil
		case <-time.After(5 * time.Second):
			return errors.New("calls were not made concurrently")
		}
	}
}

func TestMemoClient_DifferentTablesAreFetchedConcurrently(t *testing.T) {
	c := mock_bigquery.NewMockClient(gomock.NewController(t))
	wait := barrier(2)
	c.EXPECT().GetTableMetadata(gomock.Any(), "p", "d", gomock.Any()).
		DoAndReturn(func(context.Context, string, string, string) (*bq.TableMetadata, error) {
			return &bq.TableMetadata{}, wait()
		}).Times(2)
	memo := newMemoClient(c)

	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i, table := range []string{"a", "b"} {
		wg.Go(func() {
			_, errs[i] = memo.GetTableMetadata(t.Context(), "p", "d", table)
		})
	}
	wg.Wait()
	if err := errors.Join(errs...); err != nil {
		t.Error(err)
	}
}

func TestMemoClient_SameTableIsFetchedOnce(t *testing.T) {
	c := mock_bigquery.NewMockClient(gomock.NewController(t))
	var calls atomic.Int32
	notFound := &googleapi.Error{Code: http.StatusNotFound, Message: "not found"}
	c.EXPECT().GetTableMetadata(gomock.Any(), "p", "d", "t").
		DoAndReturn(func(context.Context, string, string, string) (*bq.TableMetadata, error) {
			calls.Add(1)
			time.Sleep(10 * time.Millisecond)
			return nil, notFound
		}).AnyTimes()
	memo := newMemoClient(c)

	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			if _, err := memo.GetTableMetadata(t.Context(), "p", "d", "t"); !errors.Is(err, notFound) {
				t.Errorf("GetTableMetadata() error = %v, want the cached %v", err, notFound)
			}
		})
	}
	wg.Wait()
	if got := calls.Load(); got != 1 {
		t.Errorf("GetTableMetadata API calls = %d, want 1", got)
	}
}

func TestMemoClient_Prefetch(t *testing.T) {
	forbidden := &googleapi.Error{Code: http.StatusForbidden, Message: "access denied"}
	notFound := &googleapi.Error{Code: http.StatusNotFound, Message: "not found"}
	newMemo := func(t *testing.T) *memoClient {
		c := mock_bigquery.NewMockClient(gomock.NewController(t))
		c.EXPECT().GetTableMetadata(gomock.Any(), "p", "d", "denied").Return(nil, forbidden).Times(1)
		c.EXPECT().GetTableMetadata(gomock.Any(), "p", "d", "missing").Return(nil, notFound).Times(1)
		c.EXPECT().GetTableMetadata(gomock.Any(), "p", "d", "ok").Return(&bq.TableMetadata{Name: "ok"}, nil).Times(1)
		return newMemoClient(c)
	}
	ids := []file.TableID{{Project: "p", Dataset: "d", Table: "denied"}, {Project: "p", Dataset: "d", Table: "missing"}, {Project: "p", Dataset: "d", Table: "ok"}}

	t.Run("prefetched results are served from the cache", func(t *testing.T) {
		memo := newMemo(t)
		memo.prefetchTables(t.Context(), ids)
		md, err := memo.GetTableMetadata(t.Context(), "p", "d", "ok")
		if err != nil || md.Name != "ok" {
			t.Errorf("GetTableMetadata() = %v, %v, want the prefetched metadata", md, err)
		}
	})

	t.Run("failure is not recorded until the analysis looks the table up", func(t *testing.T) {
		memo := newMemo(t)
		memo.prefetchTables(t.Context(), ids)
		if got := memo.failures(); len(got) != 0 {
			t.Errorf("failures() after prefetch = %v, want none", got)
		}
		_, _ = memo.GetTableMetadata(t.Context(), "p", "d", "missing")
		_, _ = memo.GetTableMetadata(t.Context(), "p", "d", "denied")
		_, _ = memo.GetTableMetadata(t.Context(), "p", "d", "denied")
		if diff := cmp.Diff([]string{forbidden.Error()}, memo.failures()); diff != "" {
			t.Errorf("failures() mismatch (-want +got):\n%s", diff)
		}
	})
}

func TestMemoClient_PrefetchRunsConcurrently(t *testing.T) {
	c := mock_bigquery.NewMockClient(gomock.NewController(t))
	wait := barrier(3)
	var errs atomic.Int32
	c.EXPECT().GetTableMetadata(gomock.Any(), "p", "d", gomock.Any()).
		DoAndReturn(func(context.Context, string, string, string) (*bq.TableMetadata, error) {
			if err := wait(); err != nil {
				errs.Add(1)
			}
			return &bq.TableMetadata{}, nil
		}).Times(3)
	memo := newMemoClient(c)

	memo.prefetchTables(t.Context(), []file.TableID{{Project: "p", Dataset: "d", Table: "a"}, {Project: "p", Dataset: "d", Table: "b"}, {Project: "p", Dataset: "d", Table: "c"}})
	if errs.Load() > 0 {
		t.Error("prefetchTables() fetched the tables one at a time")
	}
}
