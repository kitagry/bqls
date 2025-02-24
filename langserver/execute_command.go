package langserver

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

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
			URI: lsp.NewJobVirtualTextDocumentURI(job.ProjectID(), job.ID()),
		},
	}, nil
}

func (h *Handler) commandListDatasets(ctx context.Context, params lsp.ExecuteCommandParams) (*lsp.ListDatasetsResult, error) {
	projectID := h.bqClient.GetDefaultProject()
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

	datasets, err := h.bqClient.ListDatasets(ctx, projectID)
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
	projectID := h.bqClient.GetDefaultProject()
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

	tables, err := h.bqClient.ListTables(ctx, projectID, datasetID)
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

	jobs, err := h.project.ListJobs(ctx, h.bqClient.GetDefaultProject(), *allUser)
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
		it, err = h.bqClient.GetTableRecord(ctx, virtualTextDocumentInfo.ProjectID, virtualTextDocumentInfo.DatasetID, virtualTextDocumentInfo.TableID)
		if err != nil {
			return nil, fmt.Errorf("failed to get table record: %w", err)
		}
	} else if virtualTextDocumentInfo.JobID != "" {
		job, err := h.bqClient.JobFromProject(ctx, virtualTextDocumentInfo.ProjectID, virtualTextDocumentInfo.JobID)
		if err != nil {
			return nil, fmt.Errorf("failed to get job: %w", err)
		}

		it, err = job.Read(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to read job: %w", err)
		}
	} else {
		return nil, fmt.Errorf("invalid virtual text document uri")
	}

	f, err := os.Create(filePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	cw := csv.NewWriter(f)

	headers := make([]string, 0, len(it.Schema))
	for _, s := range it.Schema {
		headers = append(headers, s.Name)
	}
	err = cw.Write(headers)
	if err != nil {
		return nil, err
	}

	for {
		var record []bigquery.Value
		err := it.Next(&record)
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, err
		}

		row, err := formatCSV(record, it.Schema)
		if err != nil {
			return nil, err
		}

		err = cw.Write(row)
		if err != nil {
			return nil, fmt.Errorf("failed to write csv: %w", err)
		}
	}

	cw.Flush()

	if err := cw.Error(); err != nil {
		return nil, fmt.Errorf("failed to flush csv: %w", err)
	}

	return nil, nil
}

func formatCSV(record []bigquery.Value, schema bigquery.Schema) ([]string, error) {
	row := make([]string, 0, len(record))
	for i, v := range record {
		fieldSchema := schema[i]

		column, err := formatCSVSingleRecord(v, fieldSchema)
		if err != nil {
			return nil, fmt.Errorf("schema(name=%s, row=%d) failed: %w", fieldSchema.Name, i, err)
		}
		row = append(row, column)
	}
	return row, nil
}

func formatCSVSingleRecord(record bigquery.Value, fieldSchema *bigquery.FieldSchema) (string, error) {
	if record == nil {
		return "", nil
	}

	if fieldSchema.Repeated {
		record, ok := record.([]bigquery.Value)
		if !ok {
			return "", fmt.Errorf("record should be array, but got %T", record)
		}

		fieldSchema.Repeated = false
		defer func() {
			fieldSchema.Repeated = true
		}()
		columns := make([]string, 0, len(record))
		for i, r := range record {
			column, err := formatCSVSingleRecord(r, fieldSchema)
			if err != nil {
				return "", fmt.Errorf("failed to format record[%d]: %w", i, err)
			}
			columns = append(columns, column)
		}
		return fmt.Sprintf("[%s]", strings.Join(columns, ",")), nil
	}

	switch value := record.(type) {
	case int64:
		return fmt.Sprint(value), nil
	case float64:
		return fmt.Sprint(value), nil
	case bool:
		return fmt.Sprint(value), nil
	case string:
		return value, nil
	case []byte:
		return string(value), nil
	case time.Time:
		return value.Format(time.RFC3339), nil
	case []bigquery.Value:
		if fieldSchema.Schema == nil {
			return "", fmt.Errorf("schema should be provided for record")
		}
		return formatRecordJson(value, fieldSchema.Schema)
	default:
		return fmt.Sprint(record), nil
	}
}

func formatRecordJson(record []bigquery.Value, schema bigquery.Schema) (string, error) {
	if len(record) != len(schema) {
		return "", fmt.Errorf("record length(=%d) should be equal to schema length(=%d)", len(record), len(schema))
	}

	sb := &strings.Builder{}
	_, err := sb.WriteString("{")
	if err != nil {
		return "", err
	}

	writeSingleJson := func(key string, valueJson []byte) error {
		keyJson, err := json.Marshal(key)
		if err != nil {
			return fmt.Errorf("failed to encode key: %w", err)
		}
		_, err = sb.Write(keyJson)
		if err != nil {
			return err
		}
		_, err = sb.WriteString(":")
		if err != nil {
			return err
		}
		_, err = sb.Write(valueJson)
		if err != nil {
			return err
		}
		return nil
	}

	for i, v := range record {
		fieldSchema := schema[i]
		s, err := formatSingleRecordJson(v, fieldSchema)
		if err != nil {
			return "", fmt.Errorf("schema(name=%s, row=%d) failed: %w", fieldSchema.Name, i, err)
		}
		err = writeSingleJson(fieldSchema.Name, s)
		if err != nil {
			return "", err
		}

		if i != len(record)-1 {
			_, err = sb.WriteString(",")
			if err != nil {
				return "", err
			}
		}
	}

	_, err = sb.WriteString("}")
	if err != nil {
		return "", err
	}

	return sb.String(), nil
}

func formatSingleRecordJson(record bigquery.Value, schema *bigquery.FieldSchema) ([]byte, error) {
	if schema.Repeated {
		record, ok := record.([]bigquery.Value)
		if !ok {
			return nil, fmt.Errorf("record should be array, but got %T", record)
		}
		schema.Repeated = false
		defer func() {
			schema.Repeated = true
		}()
		columns := make([]string, 0, len(record))
		for i, r := range record {
			column, err := formatSingleRecordJson(r, schema)
			if err != nil {
				return nil, fmt.Errorf("failed to format record[%d]: %w", i, err)
			}
			columns = append(columns, string(column))
		}
		return bytes.Join([][]byte{[]byte("["), []byte(strings.Join(columns, ",")), []byte("]")}, nil), nil
	}

	switch value := record.(type) {
	case int64, float64, bool, string, []byte:
		return json.Marshal(value)
	case nil:
		return []byte("null"), nil
	case time.Time:
		return []byte(fmt.Sprintf(`"%s"`, value.Format(time.RFC3339))), nil
	case []bigquery.Value:
		if schema.Schema == nil {
			return nil, fmt.Errorf("schema should be provided for record")
		}
		result, err := formatRecordJson(value, schema.Schema)
		return []byte(result), err
	default:
		return nil, fmt.Errorf("unsupported type: %T", record)
	}
}
