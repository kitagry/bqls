package bigquery

import (
	"sort"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"google.golang.org/api/cloudresourcemanager/v1"
)

// newTestDB points XDG_CACHE_HOME at a fresh temp dir so each test gets its
// own cache.sqlite3 and tests don't interfere with each other or the real
// user cache.
func newTestDB(t *testing.T) *database {
	t.Helper()
	t.Setenv("XDG_CACHE_HOME", t.TempDir())

	db, err := newDB()
	if err != nil {
		t.Fatalf("newDB() failed: %v", err)
	}
	t.Cleanup(func() {
		db.Close()
	})

	if err := db.Migrate(); err != nil {
		t.Fatalf("Migrate() failed: %v", err)
	}

	return db
}

func TestNewDB(t *testing.T) {
	db := newTestDB(t)

	if err := db.db.PingContext(t.Context()); err != nil {
		t.Fatalf("expected to be able to ping db, got error: %v", err)
	}
}

func TestMigrate(t *testing.T) {
	db := newTestDB(t)

	wantTables := []string{"projects", "datasets", "tables"}
	for _, table := range wantTables {
		var name string
		err := db.db.QueryRowContext(t.Context(), "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?", table).Scan(&name)
		if err != nil {
			t.Errorf("expected table %q to exist, got error: %v", table, err)
		}
	}
}

func TestInsertAndSelectProjects(t *testing.T) {
	ctx := t.Context()
	db := newTestDB(t)

	projects := []*cloudresourcemanager.Project{
		{ProjectId: "project-1", Name: "Project One"},
		{ProjectId: "project-2", Name: "Project Two"},
	}

	if err := db.InsertProjects(ctx, projects); err != nil {
		t.Fatalf("InsertProjects() failed: %v", err)
	}

	got, err := db.SelectProjects(ctx)
	if err != nil {
		t.Fatalf("SelectProjects() failed: %v", err)
	}

	sort.Slice(got, func(i, j int) bool { return got[i].ProjectId < got[j].ProjectId })
	if diff := cmp.Diff(projects, got); diff != "" {
		t.Errorf("SelectProjects() result diff (-want, +got)\n%s", diff)
	}
}

func TestInsertProjects_ignoresDuplicates(t *testing.T) {
	ctx := t.Context()
	db := newTestDB(t)

	project := []*cloudresourcemanager.Project{{ProjectId: "project-1", Name: "Project One"}}

	if err := db.InsertProjects(ctx, project); err != nil {
		t.Fatalf("InsertProjects() first call failed: %v", err)
	}
	if err := db.InsertProjects(ctx, project); err != nil {
		t.Fatalf("InsertProjects() second call failed: %v", err)
	}

	got, err := db.SelectProjects(ctx)
	if err != nil {
		t.Fatalf("SelectProjects() failed: %v", err)
	}
	if diff := cmp.Diff(project, got); diff != "" {
		t.Errorf("SelectProjects() result diff (-want, +got)\n%s", diff)
	}
}

func TestReplaceDatasets(t *testing.T) {
	ctx := t.Context()
	db := newTestDB(t)

	if err := db.ReplaceDatasets(ctx, "project-1", []*bigquery.Dataset{
		{ProjectID: "project-1", DatasetID: "dataset-old"},
	}); err != nil {
		t.Fatalf("ReplaceDatasets() first call failed: %v", err)
	}

	want := []*bigquery.Dataset{
		{ProjectID: "project-1", DatasetID: "dataset-new-1"},
		{ProjectID: "project-1", DatasetID: "dataset-new-2"},
	}
	if err := db.ReplaceDatasets(ctx, "project-1", want); err != nil {
		t.Fatalf("ReplaceDatasets() second call failed: %v", err)
	}

	got, err := db.SelectDatasets(ctx, "project-1")
	if err != nil {
		t.Fatalf("SelectDatasets() failed: %v", err)
	}

	sort.Slice(got, func(i, j int) bool { return got[i].DatasetID < got[j].DatasetID })
	if diff := cmp.Diff(want, got, cmpopts.IgnoreUnexported(bigquery.Dataset{})); diff != "" {
		t.Errorf("SelectDatasets() result diff (-want, +got)\n%s", diff)
	}
}

func TestReplaceTables(t *testing.T) {
	ctx := t.Context()
	db := newTestDB(t)

	if err := db.ReplaceTables(ctx, "project-1", "dataset-1", []*bigquery.Table{
		{ProjectID: "project-1", DatasetID: "dataset-1", TableID: "table-old"},
	}); err != nil {
		t.Fatalf("ReplaceTables() first call failed: %v", err)
	}

	want := []*bigquery.Table{
		{ProjectID: "project-1", DatasetID: "dataset-1", TableID: "table-new-1"},
		{ProjectID: "project-1", DatasetID: "dataset-1", TableID: "table-new-2"},
	}
	if err := db.ReplaceTables(ctx, "project-1", "dataset-1", want); err != nil {
		t.Fatalf("ReplaceTables() second call failed: %v", err)
	}

	got, err := db.SelectTables(ctx, "project-1", "dataset-1")
	if err != nil {
		t.Fatalf("SelectTables() failed: %v", err)
	}

	sort.Slice(got, func(i, j int) bool { return got[i].TableID < got[j].TableID })
	if diff := cmp.Diff(want, got, cmpopts.IgnoreUnexported(bigquery.Table{})); diff != "" {
		t.Errorf("SelectTables() result diff (-want, +got)\n%s", diff)
	}
}
