package langserver

import (
	"context"
	"encoding/json"
	"fmt"

	"cloud.google.com/go/bigquery"
	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/sourcegraph/jsonrpc2"
	"google.golang.org/api/iterator"
)

const (
	CommandExecuteQuery = "executeQuery"
	CommandListDatasets = "listDatasets"
	CommandListTables   = "listTables"
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
	default:
		return nil, fmt.Errorf("unknown command: %s", params.Command)
	}
}

type ExecuteQueryResult struct {
	TextDocument lsp.TextDocumentIdentifier `json:"textDocument"`
	Result       QueryResult                `json:"result"`
}

type QueryResult struct {
	Columns []string           `json:"columns"`
	Data    [][]bigquery.Value `json:"data"`
}

func (h *Handler) commandExecuteQuery(ctx context.Context, params lsp.ExecuteCommandParams) (*ExecuteQueryResult, error) {
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

	h.workDoneProgressReport(ctx, workDoneToken, lsp.WorkDoneProgressReport{
		Message: "Fetching query result...",
	})
	it, err := job.Read(ctx)
	if err != nil {
		return nil, err
	}

	data := make([][]bigquery.Value, 0)
	for {
		var values []bigquery.Value
		err := it.Next(&values)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}

		data = append(data, values)
	}

	columns := make([]string, 0)
	for _, f := range it.Schema {
		columns = append(columns, f.Name)
	}

	return &ExecuteQueryResult{
		TextDocument: lsp.TextDocumentIdentifier{
			URI: newJobVirtualTextDocumentURI(h.project.BigQueryProjectID, job.ID()),
		},
		Result: QueryResult{
			Columns: columns,
			Data:    data,
		},
	}, nil
}

type ListDatasetsResult struct {
	Datasets []string `json:"datasets"`
}

func (h *Handler) commandListDatasets(ctx context.Context, params lsp.ExecuteCommandParams) (*ListDatasetsResult, error) {
	projectID := h.initializeParams.InitializationOptions.ProjectID
	if len(params.Arguments) > 0 {
		var ok bool
		projectID, ok = params.Arguments[0].(string)
		if !ok {
			return nil, fmt.Errorf("arguments should be string, but got %T", params.Arguments[0])
		}
	}

	datasets, err := h.project.ListDatasets(ctx, projectID)
	if err != nil {
		return nil, err
	}

	results := make([]string, 0, len(datasets))
	for _, d := range datasets {
		results = append(results, d.DatasetID)
	}

	return &ListDatasetsResult{
		Datasets: results,
	}, nil
}

type ListTablesResult struct {
	Tables []string `json:"tables"`
}

func (h *Handler) commandListTables(ctx context.Context, params lsp.ExecuteCommandParams) (*ListTablesResult, error) {
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

	tables, err := h.project.ListTables(ctx, projectID, datasetID)
	if err != nil {
		return nil, err
	}

	results := make([]string, 0, len(tables))
	for _, t := range tables {
		results = append(results, t.TableID)
	}

	return &ListTablesResult{
		Tables: results,
	}, nil
}
