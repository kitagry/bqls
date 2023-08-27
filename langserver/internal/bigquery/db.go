package bigquery

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"cloud.google.com/go/bigquery"
	_ "github.com/mattn/go-sqlite3"
	"google.golang.org/api/cloudresourcemanager/v1"
)

type database struct {
	db *sql.DB
}

func newDB() (*database, error) {
	path, err := bqCachePath()
	if err != nil {
		return nil, fmt.Errorf("bqCachePath: %w", err)
	}

	path = filepath.Join(path, "cache.sqlite3")
	db, err := sql.Open("sqlite3", fmt.Sprintf("file:%s", path))
	if err != nil {
		return nil, fmt.Errorf("sql.Open: %w", err)
	}
	return &database{db}, nil
}

func bqCachePath() (string, error) {
	cachePath, ok := cachePath()
	if !ok {
		return "", fmt.Errorf("cache path not found")
	}

	path := filepath.Join(cachePath, "bqls")
	fi, err := os.Stat(path)
	if err == nil {
		if !fi.IsDir() {
			return "", fmt.Errorf("cache path(%s) is not a directory", path)
		}
		return path, nil
	}

	if !os.IsNotExist(err) {
		return "", fmt.Errorf("failed to stat path(%s), %w", path, err)
	}

	err = os.Mkdir(path, os.ModePerm)
	if err != nil {
		return "", fmt.Errorf("failed to create directory(%s), %w", path, err)
	}

	return path, nil
}

func cachePath() (string, bool) {
	if path := os.Getenv("XDG_CACHE_HOME"); path != "" {
		return path, true
	}

	if path := os.Getenv("HOME"); path != "" {
		return filepath.Join(path, ".cache"), true
	}

	return "", false
}

func (db *database) Migrate() error {
	_, err := db.db.Exec(`CREATE TABLE IF NOT EXISTS projects (
		project_id TEXT PRIMARY KEY,
		name TEXT
	)`)
	if err != nil {
		return fmt.Errorf("failed to create projects table: %w", err)
	}

	_, err = db.db.Exec(`CREATE TABLE IF NOT EXISTS datasets (
		project_id TEXT,
		dataset_id TEXT,
		PRIMARY KEY (project_id, dataset_id)
	)`)
	if err != nil {
		return fmt.Errorf("failed to create datasets table: %w", err)
	}

	_, err = db.db.Exec(`CREATE TABLE IF NOT EXISTS tables (
		project_id TEXT,
		dataset_id TEXT,
		table_id TEXT,
		PRIMARY KEY (project_id, dataset_id, table_id)
	)`)
	if err != nil {
		return fmt.Errorf("failed to create tables table: %w", err)
	}

	return nil
}

func (db *database) Close() error {
	return db.db.Close()
}

func (db *database) SelectProjects(ctx context.Context) ([]*cloudresourcemanager.Project, error) {
	rows, err := db.db.QueryContext(ctx, "SELECT project_id, name FROM projects")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	results := make([]*cloudresourcemanager.Project, 0)
	for rows.Next() {
		var projectID, name string
		err := rows.Scan(&projectID, &name)
		if err != nil {
			return nil, fmt.Errorf("failed to scan project: %w", err)
		}

		results = append(results, &cloudresourcemanager.Project{
			ProjectId: projectID,
			Name:      name,
		})
	}
	return results, nil
}

func (db *database) InsertProjects(ctx context.Context, projects []*cloudresourcemanager.Project) error {
	query := "INSERT OR IGNORE INTO projects(project_id, name) VALUES "
	queryBuilder := &strings.Builder{}
	queryBuilder.Grow(len(query) + 7*len(projects))
	queryBuilder.WriteString(query)
	args := make([]any, 0)
	for _, p := range projects {
		queryBuilder.WriteString("(?, ?),")
		args = append(args, p.ProjectId, p.Name)
	}
	query = queryBuilder.String()[0 : queryBuilder.Len()-1]

	_, err := db.db.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("failed to insert projects: %w", err)
	}
	return nil
}

func (db *database) SelectDatasets(ctx context.Context, projectID string) ([]*bigquery.Dataset, error) {
	rows, err := db.db.QueryContext(ctx, "SELECT project_id, dataset_id FROM datasets WHERE project_id = ?", projectID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	results := make([]*bigquery.Dataset, 0)
	for rows.Next() {
		var projectID, datasetID string
		err := rows.Scan(&projectID, &datasetID)
		if err != nil {
			return nil, fmt.Errorf("failed to scan datasets: %w", err)
		}

		results = append(results, &bigquery.Dataset{
			ProjectID: projectID,
			DatasetID: datasetID,
		})
	}
	return results, nil
}

func (db *database) InsertDatasets(ctx context.Context, datasets []*bigquery.Dataset) error {
	query := "INSERT OR IGNORE INTO datasets(project_id, dataset_id) VALUES "
	queryBuilder := &strings.Builder{}
	queryBuilder.Grow(len(query) + 7*len(datasets))
	queryBuilder.WriteString(query)
	args := make([]any, 0)
	for _, d := range datasets {
		queryBuilder.WriteString("(?, ?),")
		args = append(args, d.ProjectID, d.DatasetID)
	}
	query = queryBuilder.String()[0 : queryBuilder.Len()-1]

	_, err := db.db.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("failed to insert datasets: %w", err)
	}
	return nil
}

func (db *database) SelectTables(ctx context.Context, projectID, datasetID string) ([]*bigquery.Table, error) {
	rows, err := db.db.QueryContext(ctx, "SELECT project_id, dataset_id, table_id FROM tables WHERE project_id = ? AND dataset_id = ?", projectID, datasetID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	results := make([]*bigquery.Table, 0)
	for rows.Next() {
		var projectID, datasetID, tableID string
		err := rows.Scan(&projectID, &datasetID, &tableID)
		if err != nil {
			return nil, fmt.Errorf("failed to scan tables: %w", err)
		}

		results = append(results, &bigquery.Table{
			ProjectID: projectID,
			DatasetID: datasetID,
			TableID:   tableID,
		})
	}
	return results, nil
}

func (db *database) InsertTables(ctx context.Context, tables []*bigquery.Table) error {
	query := "INSERT OR IGNORE INTO tables(project_id, dataset_id, table_id) VALUES "
	queryBuilder := &strings.Builder{}
	queryBuilder.Grow(len(query) + 10*len(tables))
	queryBuilder.WriteString(query)
	args := make([]any, 0)
	for _, t := range tables {
		queryBuilder.WriteString("(?, ?, ?),")
		args = append(args, t.ProjectID, t.DatasetID, t.TableID)
	}
	query = queryBuilder.String()[0 : queryBuilder.Len()-1]

	_, err := db.db.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("failed to insert tables: %w", err)
	}
	return nil
}
