package bigquery_test

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	bq "cloud.google.com/go/bigquery"
	"github.com/golang/mock/gomock"
	"github.com/kitagry/bqls/langserver/internal/bigquery"
	"github.com/kitagry/bqls/langserver/internal/bigquery/mock_bigquery"
	"google.golang.org/api/googleapi"
)

var (
	errNoCredentials = errors.New("credentials: could not find default credentials. See https://cloud.google.com/docs/authentication/external/set-up-adc for more information")
	errUnauthorized  = &googleapi.Error{Code: http.StatusUnauthorized, Message: "Request had invalid authentication credentials."}
)

// clientFactory hands out the given clients and errors in order, the way
// creating the real client would succeed or fail depending on the current
// credentials.
type clientFactory struct {
	t       *testing.T
	results []factoryResult
	calls   int
}

type factoryResult struct {
	client bigquery.Client
	err    error
}

func (f *clientFactory) newClient(context.Context) (bigquery.Client, error) {
	f.t.Helper()
	if f.calls >= len(f.results) {
		f.t.Fatalf("the client was created %d times, want at most %d", f.calls+1, len(f.results))
	}
	r := f.results[f.calls]
	f.calls++
	return r.client, r.err
}

type authErrorRecorder struct {
	errs []error
}

func (r *authErrorRecorder) record(err error) {
	r.errs = append(r.errs, err)
}

func TestAuthGuard_StartsWithoutCredentials(t *testing.T) {
	ctrl := gomock.NewController(t)
	recovered := mock_bigquery.NewMockClient(ctrl)
	recovered.EXPECT().GetTableMetadata(gomock.Any(), "my-project", "my_dataset", "my_table").Return(&bq.TableMetadata{}, nil)
	factory := &clientFactory{t: t, results: []factoryResult{{err: errNoCredentials}, {err: errNoCredentials}, {client: recovered}}}
	recorder := &authErrorRecorder{}

	client, err := bigquery.NewAuthGuard(t.Context(), "my-project", factory.newClient, recorder.record, 0)
	if err != nil {
		t.Fatalf("NewAuthGuard should not fail on missing credentials, got %v", err)
	}
	if got := client.GetDefaultProject(); got != "my-project" {
		t.Errorf("GetDefaultProject() = %q, want %q", got, "my-project")
	}
	if len(recorder.errs) != 0 {
		t.Errorf("the guidance should wait for the first API call, got %v", recorder.errs)
	}

	_, err = client.GetTableMetadata(t.Context(), "my-project", "my_dataset", "my_table")
	if !errors.Is(err, errNoCredentials) {
		t.Errorf("GetTableMetadata() error = %v, want %v", err, errNoCredentials)
	}
	_, err = client.GetTableMetadata(t.Context(), "my-project", "my_dataset", "my_table")
	if err != nil {
		t.Errorf("GetTableMetadata() should succeed once the user logs in, got %v", err)
	}
	if len(recorder.errs) != 1 {
		t.Errorf("onAuthError should be called once, got %v", recorder.errs)
	}
}

func TestAuthGuard_FailsOnNonAuthError(t *testing.T) {
	errDisabled := &googleapi.Error{Code: http.StatusForbidden, Message: "Cloud Resource Manager API has not been used in project my-project before or it is disabled."}
	factory := &clientFactory{t: t, results: []factoryResult{{err: errDisabled}}}

	_, err := bigquery.NewAuthGuard(t.Context(), "my-project", factory.newClient, (&authErrorRecorder{}).record, 0)
	if !errors.Is(err, errDisabled) {
		t.Errorf("NewAuthGuard() error = %v, want %v", err, errDisabled)
	}
}

func TestAuthGuard_NotifiesOnceUntilRecovered(t *testing.T) {
	ctrl := gomock.NewController(t)
	expired := mock_bigquery.NewMockClient(ctrl)
	expired.EXPECT().ListDatasets(gomock.Any(), "my-project").Return(nil, errUnauthorized).Times(2)
	// It is closed when replaced by itself, and then by the recovered one.
	expired.EXPECT().Close().Return(nil).Times(2)
	recovered := mock_bigquery.NewMockClient(ctrl)
	gomock.InOrder(
		recovered.EXPECT().ListDatasets(gomock.Any(), "my-project").Return([]*bq.Dataset{}, nil),
		recovered.EXPECT().ListDatasets(gomock.Any(), "my-project").Return(nil, errUnauthorized),
	)
	factory := &clientFactory{t: t, results: []factoryResult{{client: expired}, {client: expired}, {client: recovered}}}
	recorder := &authErrorRecorder{}

	client, err := bigquery.NewAuthGuard(t.Context(), "my-project", factory.newClient, recorder.record, 0)
	if err != nil {
		t.Fatal(err)
	}

	for range 2 {
		_, _ = client.ListDatasets(t.Context(), "my-project")
	}
	if len(recorder.errs) != 1 {
		t.Fatalf("onAuthError should be called once while authentication keeps failing, got %v", recorder.errs)
	}

	if _, err := client.ListDatasets(t.Context(), "my-project"); err != nil {
		t.Fatalf("ListDatasets() should succeed with the recreated client, got %v", err)
	}
	_, _ = client.ListDatasets(t.Context(), "my-project")
	if len(recorder.errs) != 2 {
		t.Errorf("onAuthError should be called again once authentication fails after recovering, got %v", recorder.errs)
	}
}

func TestAuthGuard_WaitsRetryIntervalBeforeRecreating(t *testing.T) {
	ctrl := gomock.NewController(t)
	expired := mock_bigquery.NewMockClient(ctrl)
	expired.EXPECT().ListDatasets(gomock.Any(), "my-project").Return(nil, errUnauthorized).Times(3)
	factory := &clientFactory{t: t, results: []factoryResult{{client: expired}}}

	client, err := bigquery.NewAuthGuard(t.Context(), "my-project", factory.newClient, (&authErrorRecorder{}).record, time.Hour)
	if err != nil {
		t.Fatal(err)
	}

	for range 3 {
		_, _ = client.ListDatasets(t.Context(), "my-project")
	}
	if factory.calls != 1 {
		t.Errorf("the client should not be recreated within the retry interval, created %d times", factory.calls)
	}
}

func TestAuthGuard_IgnoresNonAuthErrors(t *testing.T) {
	ctrl := gomock.NewController(t)
	errDenied := &googleapi.Error{Code: http.StatusForbidden, Message: "Access Denied: Table my-project:my_dataset.my_table"}
	c := mock_bigquery.NewMockClient(ctrl)
	c.EXPECT().GetTableMetadata(gomock.Any(), "my-project", "my_dataset", "my_table").Return(nil, errDenied).Times(2)
	factory := &clientFactory{t: t, results: []factoryResult{{client: c}}}
	recorder := &authErrorRecorder{}

	client, err := bigquery.NewAuthGuard(t.Context(), "my-project", factory.newClient, recorder.record, 0)
	if err != nil {
		t.Fatal(err)
	}

	for range 2 {
		_, err = client.GetTableMetadata(t.Context(), "my-project", "my_dataset", "my_table")
		if !errors.Is(err, errDenied) {
			t.Errorf("GetTableMetadata() error = %v, want %v", err, errDenied)
		}
	}
	if len(recorder.errs) != 0 {
		t.Errorf("onAuthError should not be called for denied access, got %v", recorder.errs)
	}
	if factory.calls != 1 {
		t.Errorf("the client should not be recreated for denied access, created %d times", factory.calls)
	}
}
