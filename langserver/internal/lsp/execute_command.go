package lsp

type ExecuteQueryResult struct {
	TextDocument TextDocumentIdentifier `json:"textDocument"`
	Result       QueryResult            `json:"result"`
}

type ListDatasetsResult struct {
	Datasets []string `json:"datasets"`
}

type ListTablesResult struct {
	Tables []string `json:"tables"`
}

type ListJobHistoryResult struct {
	Jobs []JobHistory `json:"jobs"`
}

type JobHistory struct {
	TextDocument TextDocumentIdentifier `json:"textDocument"`

	ID    string `json:"id"`
	Owner string `json:"owner"`

	// Summary is a human-readable summary of the job.
	// When the job is a query job, it is the query string.
	Summary string `json:"summary"`
}

type SaveResultResult struct {
	URL string `json:"url"`
}

type SearchTablesResult struct {
	Tables []TableSearchResult `json:"tables"`
}

type ListProjectsResult struct {
	Projects []ProjectInfo `json:"projects"`
}

type ProjectInfo struct {
	ProjectID string `json:"projectId"`
	Name      string `json:"name"`
}

type TableSearchResult struct {
	ProjectID   string `json:"projectId"`
	DatasetID   string `json:"datasetId"`
	TableID     string `json:"tableId"`
	Description string `json:"description,omitempty"`
}
