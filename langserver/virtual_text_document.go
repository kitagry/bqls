package langserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"cloud.google.com/go/bigquery"
	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/sourcegraph/jsonrpc2"
	"google.golang.org/api/iterator"
)

type VirtualTextDocumentInfo struct {
	ProjectID string
	DatasetID string
	TableID   string
	JobID     string
}

func ParseVirtualTextDocument(textDocument lsp.DocumentURI) (VirtualTextDocumentInfo, error) {
	suffix, ok := strings.CutPrefix(string(textDocument), "bqls://")
	if !ok {
		return VirtualTextDocumentInfo{}, errors.New("invalid text document URI")
	}

	result := VirtualTextDocumentInfo{}
	for suffix != "" {
		for _, prefix := range []string{"project/", "dataset/", "table/", "job/"} {
			after, ok := strings.CutPrefix(suffix, prefix)
			if !ok {
				continue
			}

			ind := strings.Index(after, "/")
			var val string
			if ind == -1 {
				val = after
				suffix = ""
			} else {
				val = after[:ind]
				suffix = after[ind+1:]
			}

			if prefix == "project/" {
				result.ProjectID = val
			} else if prefix == "dataset/" {
				result.DatasetID = val
			} else if prefix == "table/" {
				result.TableID = val
			} else if prefix == "job/" {
				result.JobID = val
			}
		}
	}

	if err := result.validate(); err != nil {
		return VirtualTextDocumentInfo{}, err
	}

	return result, nil
}

func (v VirtualTextDocumentInfo) validate() error {
	if v.ProjectID == "" {
		return errors.New("project ID is required")
	}

	if v.DatasetID != "" && v.TableID != "" {
		return nil
	}

	if v.JobID != "" {
		return nil
	}

	return fmt.Errorf("either dataset ID and table ID or job ID is required")
}

func (h *Handler) handleVirtualTextDocument(ctx context.Context, conn *jsonrpc2.Conn, req *jsonrpc2.Request) (result any, err error) {
	if req.Params == nil {
		return nil, &jsonrpc2.Error{Code: jsonrpc2.CodeInvalidParams}
	}

	var params lsp.VirtualTextDocumentParams
	if err := json.Unmarshal(*req.Params, &params); err != nil {
		return nil, err
	}

	virtualTextDocument, err := ParseVirtualTextDocument(params.TextDocument.URI)
	if err != nil {
		return nil, err
	}

	workDoneToken := lsp.ProgressToken("virtual_text_document")
	h.workDoneProgressBegin(ctx, workDoneToken, lsp.WorkDoneProgressBegin{
		Title:   "Virtual text document",
		Message: "Loading virtual text document info...",
	})
	defer h.workDoneProgressEnd(ctx, workDoneToken, lsp.WorkDoneProgressEnd{})

	if virtualTextDocument.TableID != "" {
		h.workDoneProgressReport(ctx, workDoneToken, lsp.WorkDoneProgressReport{
			Message: "Fetching table info...",
		})
		result, err := h.project.GetTableInfo(ctx, virtualTextDocument.ProjectID, virtualTextDocument.DatasetID, virtualTextDocument.TableID)
		if err != nil {
			return nil, err
		}

		return result, nil
	}

	if virtualTextDocument.JobID != "" {
		h.workDoneProgressReport(ctx, workDoneToken, lsp.WorkDoneProgressReport{
			Message: "Fetching job info...",
		})
		result, err := h.project.GetJobInfo(ctx, virtualTextDocument.ProjectID, virtualTextDocument.JobID)
		if err != nil {
			return nil, err
		}
		return result, nil
	}

	return nil, nil
}

func buildQueryResult(it *bigquery.RowIterator) (lsp.QueryResult, error) {
	var result lsp.QueryResult

	for _, field := range it.Schema {
		result.Columns = append(result.Columns, field.Name)
	}

	for {
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
