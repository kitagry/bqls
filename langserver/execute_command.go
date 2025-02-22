package langserver

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"

	"cloud.google.com/go/bigquery"
	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/sourcegraph/jsonrpc2"
	"google.golang.org/api/iterator"
)

const (
	CommandExecuteQuery     = "executeQuery"
	CommandListDatasets     = "listDatasets"
	CommandListTables       = "listTables"
	CommandListJobHistories = "listJobHistories"
	CommandSaveResult       = "saveResult"
)

func (h *Handler) handleTextDocumentCodeAction(ctx context.Context, conn *jsonrpc2.Conn, req *jsonrpc2.Request) (result any, err error) {
	if req.Params == nil {
		return nil, &jsonrpc2.Error{Code: jsonrpc2.CodeInvalidParams}
	}

	var params lsp.CodeActionParams
	if err := json.Unmarshal(*req.Params, &params); err != nil {
		return nil, err
	}

	if params.TextDocument.URI.IsVirtualTextDocument() {
		commands := []lsp.Command{
			{
				Title:     "Save Result",
				Command:   CommandSaveResult,
				Arguments: []any{params.TextDocument.URI},
			},
		}
		return commands, nil
	}
	if params.TextDocument.URI.IsFile() && strings.HasSuffix(string(params.TextDocument.URI), ".sql") {
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
	return nil, nil
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
	case CommandSaveResult:
		return h.commandSaveResult(ctx, params)
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

	path := lsp.DocumentURI(uri)

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

// params.Arguments[0]: document uri
// params.Arguments[1]: file uri e.x) file:///path/to/file.csv
func (h *Handler) commandSaveResult(ctx context.Context, params lsp.ExecuteCommandParams) (any, error) {
	if len(params.Arguments) != 2 {
		return nil, fmt.Errorf("file uri arguments is not provided")
	}

	args0, ok := params.Arguments[0].(string)
	if !ok {
		return nil, fmt.Errorf("arguments should be string, but got %T", params.Arguments[0])
	}
	documentURI := lsp.DocumentURI(args0)
	if !documentURI.IsVirtualTextDocument() {
		return nil, fmt.Errorf("document uri should be virtual text document")
	}
	virtualTextDocumentInfo, err := documentURI.VirtualTextDocumentInfo()
	if err != nil {
		return nil, err
	}

	args1, ok := params.Arguments[1].(string)
	if !ok {
		return nil, fmt.Errorf("arguments should be string, but got %T", params.Arguments[1])
	}
	fileURI := lsp.DocumentURI(args1)
	filePath, err := fileURI.FilePath()
	if err != nil {
		return nil, err
	}
	if !strings.HasSuffix(filePath, ".csv") {
		return nil, fmt.Errorf("file uri should be csv file")
	}

	workDoneToken := lsp.ProgressToken("save result")
	h.workDoneProgressBegin(ctx, workDoneToken, lsp.WorkDoneProgressBegin{
		Title:   "Fetch virtual text document",
		Message: "Loading virtual text document info...",
	})
	defer h.workDoneProgressEnd(ctx, workDoneToken, lsp.WorkDoneProgressEnd{})

	var it *bigquery.RowIterator
	if virtualTextDocumentInfo.DatasetID != "" {
		_, it, err = h.project.GetTableInfo(ctx, virtualTextDocumentInfo.ProjectID, virtualTextDocumentInfo.DatasetID, virtualTextDocumentInfo.TableID)
	} else if virtualTextDocumentInfo.JobID != "" {
		_, it, err = h.project.GetJobInfo(ctx, virtualTextDocumentInfo.ProjectID, virtualTextDocumentInfo.JobID)
	} else {
		return nil, fmt.Errorf("invalid virtual text document uri")
	}
	if err != nil {
		return nil, err
	}

	f, err := os.Create(filePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	cw := csv.NewWriter(f)
	h.workDoneProgressReport(ctx, workDoneToken, lsp.WorkDoneProgressReport{
		Message: "Scanning rows...",
	})

	// write header
	schemas := make([]string, 0, len(it.Schema))
	for _, s := range it.Schema {
		schemas = append(schemas, s.Name)
	}
	err = cw.Write(schemas)
	if err != nil {
		return nil, err
	}

	for {
		var values []bigquery.Value
		err := it.Next(&values)
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, err
		}
		if values == nil {
			break
		}
		var record []string
		for _, v := range values {
			record = append(record, fmt.Sprint(v))
		}
		err = cw.Write(record)
		if err != nil {
			return nil, err
		}
	}

	cw.Flush()

	if err := cw.Error(); err != nil {
		return nil, err
	}

	return nil, nil
}
