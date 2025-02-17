package langserver

import (
	"context"
	"encoding/json"
	"errors"

	"cloud.google.com/go/bigquery"
	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/sourcegraph/jsonrpc2"
	"google.golang.org/api/iterator"
)

func (h *Handler) handleVirtualTextDocument(ctx context.Context, conn *jsonrpc2.Conn, req *jsonrpc2.Request) (any, error) {
	if req.Params == nil {
		return nil, &jsonrpc2.Error{Code: jsonrpc2.CodeInvalidParams}
	}

	var params lsp.VirtualTextDocumentParams
	if err := json.Unmarshal(*req.Params, &params); err != nil {
		return nil, err
	}

	virtualTextDocument, err := params.TextDocument.URI.VirtualTextDocumentInfo()
	if err != nil {
		return nil, err
	}

	workDoneToken := lsp.ProgressToken("virtual_text_document")
	h.workDoneProgressBegin(ctx, workDoneToken, lsp.WorkDoneProgressBegin{
		Title:   "Virtual text document",
		Message: "Loading virtual text document info...",
	})
	defer h.workDoneProgressEnd(ctx, workDoneToken, lsp.WorkDoneProgressEnd{})

	var markedStrings []lsp.MarkedString
	var it *bigquery.RowIterator
	if virtualTextDocument.TableID != "" {
		h.workDoneProgressReport(ctx, workDoneToken, lsp.WorkDoneProgressReport{
			Message: "Fetching table info...",
		})
		markedStrings, it, err = h.project.GetTableInfo(ctx, virtualTextDocument.ProjectID, virtualTextDocument.DatasetID, virtualTextDocument.TableID)
		if err != nil {
			return nil, err
		}
	}

	if virtualTextDocument.JobID != "" {
		h.workDoneProgressReport(ctx, workDoneToken, lsp.WorkDoneProgressReport{
			Message: "Fetching job info...",
		})
		markedStrings, it, err = h.project.GetJobInfo(ctx, virtualTextDocument.ProjectID, virtualTextDocument.JobID)
		if err != nil {
			return nil, err
		}
	}

	result := lsp.VirtualTextDocument{Contents: markedStrings}

	if it != nil {
		builtResult, err := buildQueryResult(it, 100)
		if err != nil {
			h.logger.Printf("failed to build query result: %v", err)
		}
		result.Result = builtResult
	}

	return result, nil
}

func buildQueryResult(it *bigquery.RowIterator, maxRowNum int) (lsp.QueryResult, error) {
	var result lsp.QueryResult

	for _, field := range it.Schema {
		result.Columns = append(result.Columns, field.Name)
	}

	for i := 0; i < maxRowNum; i++ {
		var values []bigquery.Value
		err := it.Next(&values)
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return result, err
		}

		result.Data = append(result.Data, values)
	}

	return result, nil
}
