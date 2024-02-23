package langserver

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"

	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/sourcegraph/jsonrpc2"
)

const (
	CommandExecuteQuery     = "executeQuery"
	CommandListDatasets     = "listDatasets"
	CommandListTables       = "listTables"
	CommandListJobHistories = "listJobHistories"
)

func (h *Handler) handleTextDocumentCodeAction(ctx context.Context, conn *jsonrpc2.Conn, req *jsonrpc2.Request) (result any, err error) {
	if req.Params == nil {
		return nil, &jsonrpc2.Error{Code: jsonrpc2.CodeInvalidParams}
	}

	var params lsp.CodeActionParams
	if err := json.Unmarshal(*req.Params, &params); err != nil {
		return nil, err
	}

	commands := []lsp.Command{
		{
			Title:     "Execute Query",
			Command:   CommandExecuteQuery,
			Arguments: []any{params.TextDocument.URI},
		},
		{
			Title:   "List Personal Job Histories",
			Command: CommandListJobHistories,
		},
		{
			Title:     "List Project Job Histories",
			Command:   CommandListJobHistories,
			Arguments: []any{"--all-user"},
		},
	}
	return commands, nil
}

func (h *Handler) handleWorkspaceExecuteCommand(ctx context.Context, conn *jsonrpc2.Conn, req *jsonrpc2.Request) (result any, err error) {
	if req.Params == nil {
		return nil, &jsonrpc2.Error{Code: jsonrpc2.CodeInvalidParams}
	}

	var params lsp.ExecuteCommandParams
	if err := json.Unmarshal(*req.Params, &params); err != nil {
		return nil, err
	}

	switch params.Command {
	case CommandExecuteQuery:
		return h.commandExecuteQuery(ctx, params)
	case CommandListDatasets:
		return h.commandListDatasets(ctx, params)
	case CommandListTables:
		return h.commandListTables(ctx, params)
	case CommandListJobHistories:
		return h.commandListJobHistories(ctx, params)
	default:
		return nil, fmt.Errorf("unknown command: %s", params.Command)
	}
}

func (h *Handler) commandExecuteQuery(ctx context.Context, params lsp.ExecuteCommandParams) (*lsp.ExecuteQueryResult, error) {
	if len(params.Arguments) != 1 {
		return nil, fmt.Errorf("file uri arguments is not provided")
	}
	uri, ok := params.Arguments[0].(string)
	if !ok {
		return nil, fmt.Errorf("arguments should be string, but got %T", params.Arguments[0])
	}

	path := documentURIToURI(lsp.DocumentURI(uri))

	workDoneToken := lsp.ProgressToken("execute_query")
	h.workDoneProgressBegin(ctx, workDoneToken, lsp.WorkDoneProgressBegin{
		Title:   "Execute Query",
		Message: "Runing query...",
	})
	defer h.workDoneProgressEnd(ctx, workDoneToken, lsp.WorkDoneProgressEnd{})
	job, err := h.project.Run(ctx, path)
	if err != nil {
		return nil, err
	}

	return &lsp.ExecuteQueryResult{
		TextDocument: lsp.TextDocumentIdentifier{
			URI: lsp.NewJobVirtualTextDocumentURI(h.project.BigQueryProjectID, job.ID()),
		},
	}, nil
}

func (h *Handler) commandListDatasets(ctx context.Context, params lsp.ExecuteCommandParams) (*lsp.ListDatasetsResult, error) {
	projectID := h.initializeParams.InitializationOptions.ProjectID
	if len(params.Arguments) > 0 {
		var ok bool
		projectID, ok = params.Arguments[0].(string)
		if !ok {
			return nil, fmt.Errorf("arguments should be string, but got %T", params.Arguments[0])
		}
	}

	workDoneToken := lsp.ProgressToken("list_datasets")
	h.workDoneProgressBegin(ctx, workDoneToken, lsp.WorkDoneProgressBegin{
		Title:   "List datasets",
		Message: "Loading datasets...",
	})
	defer h.workDoneProgressEnd(ctx, workDoneToken, lsp.WorkDoneProgressEnd{})

	datasets, err := h.project.ListDatasets(ctx, projectID)
	if err != nil {
		return nil, err
	}

	results := make([]string, 0, len(datasets))
	for _, d := range datasets {
		results = append(results, d.DatasetID)
	}

	return &lsp.ListDatasetsResult{
		Datasets: results,
	}, nil
}

func (h *Handler) commandListTables(ctx context.Context, params lsp.ExecuteCommandParams) (*lsp.ListTablesResult, error) {
	projectID := h.initializeParams.InitializationOptions.ProjectID
	var datasetID string
	if len(params.Arguments) == 0 {
		return nil, fmt.Errorf("datasetID arguments is not provided")
	} else if len(params.Arguments) == 1 {
		var ok bool
		datasetID, ok = params.Arguments[0].(string)
		if !ok {
			return nil, fmt.Errorf("arguments should be string, but got %T", params.Arguments[0])
		}
	} else if len(params.Arguments) == 2 {
		var ok bool
		projectID, ok = params.Arguments[0].(string)
		if !ok {
			return nil, fmt.Errorf("arguments should be string, but got %T", params.Arguments[0])
		}

		datasetID, ok = params.Arguments[1].(string)
		if !ok {
			return nil, fmt.Errorf("arguments should be string, but got %T", params.Arguments[1])
		}
	}

	workDoneToken := lsp.ProgressToken("list_tables")
	h.workDoneProgressBegin(ctx, workDoneToken, lsp.WorkDoneProgressBegin{
		Title:   "List tables",
		Message: "Loading tables...",
	})
	defer h.workDoneProgressEnd(ctx, workDoneToken, lsp.WorkDoneProgressEnd{})

	tables, err := h.project.ListTables(ctx, projectID, datasetID)
	if err != nil {
		return nil, err
	}

	results := make([]string, 0, len(tables))
	for _, t := range tables {
		results = append(results, t.TableID)
	}

	return &lsp.ListTablesResult{
		Tables: results,
	}, nil
}

func (h *Handler) commandListJobHistories(ctx context.Context, params lsp.ExecuteCommandParams) (any, error) {
	f := flag.NewFlagSet("listJobHistory", flag.ContinueOnError)
	allUser := f.Bool("all-user", false, "list personal job histories")

	strArgs := make([]string, 0, len(params.Arguments))
	for _, a := range params.Arguments {
		strArgs = append(strArgs, fmt.Sprint(a))
	}
	err := f.Parse(strArgs)
	if err != nil {
		return nil, err
	}

	jobs, err := h.project.ListJobs(ctx, h.initializeParams.InitializationOptions.ProjectID, *allUser)
	if err != nil {
		return nil, err
	}
	return lsp.ListJobHistoryResult{Jobs: jobs}, nil
}
