package langserver

import (
	"context"
	"encoding/json"
	"errors"

	"cloud.google.com/go/bigquery"
	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/kitagry/bqls/langserver/internal/source"
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

	if h.initializeParams.InitializationOptions.SupportsAsyncVirtualTextDocument {
		h.virtualTextDocumentRequest <- params.TextDocument.URI
		return lsp.VirtualTextDocument{Pending: true}, nil
	}

	workDoneToken := lsp.ProgressToken("virtual_text_document")
	h.workDoneProgressBegin(ctx, workDoneToken, lsp.WorkDoneProgressBegin{
		Title:   "Virtual text document",
		Message: "Loading virtual text document info...",
	})
	defer h.workDoneProgressEnd(ctx, workDoneToken, lsp.WorkDoneProgressEnd{})

	return h.resolveVirtualTextDocument(ctx, workDoneToken, virtualTextDocument)
}

// virtualTextDocumentDetails holds the details (markdown contents) fetched
// for a virtual text document, plus a closure to lazily fetch the preview
// (query result rows) reusing whatever the details fetch already looked up
// (table metadata / a polled-to-completion job), so fetching them separately
// doesn't require re-fetching from BigQuery.
type virtualTextDocumentDetails struct {
	contents     []lsp.MarkedString
	schema       []lsp.FieldSchema
	fetchPreview func(context.Context) (*bigquery.RowIterator, error)
}

func (h *Handler) resolveVirtualTextDocumentDetails(ctx context.Context, workDoneToken lsp.ProgressToken, info lsp.VirtualTextDocumentInfo) (virtualTextDocumentDetails, error) {
	if info.TableID != "" {
		h.workDoneProgressReport(ctx, workDoneToken, lsp.WorkDoneProgressReport{
			Message: "Fetching table info...",
		})
		contents, schema, fetchPreview, err := h.project.GetTableDetails(ctx, info.ProjectID, info.DatasetID, info.TableID)
		if err != nil {
			return virtualTextDocumentDetails{}, err
		}
		return virtualTextDocumentDetails{contents: contents, schema: schema, fetchPreview: fetchPreview}, nil
	}

	if info.JobID != "" {
		h.workDoneProgressReport(ctx, workDoneToken, lsp.WorkDoneProgressReport{
			Message: "Fetching job info...",
		})
		contents, fetchPreview, err := h.project.GetJobDetails(ctx, info.ProjectID, info.JobID, info.Location)
		if err != nil {
			return virtualTextDocumentDetails{}, err
		}
		return virtualTextDocumentDetails{contents: contents, fetchPreview: fetchPreview}, nil
	}

	return virtualTextDocumentDetails{}, nil
}

func (h *Handler) resolveVirtualTextDocumentPreview(ctx context.Context, details virtualTextDocumentDetails) (lsp.QueryResult, error) {
	if details.fetchPreview == nil {
		return lsp.QueryResult{}, nil
	}

	it, err := details.fetchPreview(ctx)
	if err != nil {
		return lsp.QueryResult{}, err
	}
	if it == nil {
		return lsp.QueryResult{}, nil
	}

	return buildQueryResult(it, 100)
}

func (h *Handler) resolveVirtualTextDocument(ctx context.Context, workDoneToken lsp.ProgressToken, info lsp.VirtualTextDocumentInfo) (lsp.VirtualTextDocument, error) {
	details, err := h.resolveVirtualTextDocumentDetails(ctx, workDoneToken, info)
	if err != nil {
		return lsp.VirtualTextDocument{}, err
	}

	result, err := h.resolveVirtualTextDocumentPreview(ctx, details)
	if err != nil {
		h.logger.Printf("failed to build query result: %v", err)
	}

	return lsp.VirtualTextDocument{Contents: details.contents, Schema: details.schema, Result: result}, nil
}

func buildQueryResult(it *bigquery.RowIterator, maxRowNum int) (lsp.QueryResult, error) {
	var result lsp.QueryResult

	for _, field := range it.Schema {
		result.Columns = append(result.Columns, field.Name)
	}
	result.Schema = source.BuildFieldSchema(it.Schema)

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
