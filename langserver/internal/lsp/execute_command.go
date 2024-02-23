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
