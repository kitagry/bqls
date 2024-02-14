package langserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/sourcegraph/jsonrpc2"
)

type VirtualTextDocumentParams struct {
	TextDocument lsp.TextDocumentIdentifier `json:"textDocument"`
}

type VirtualTextDocument struct {
	Contents []lsp.MarkedString `json:"contents"`
}

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

func newJobVirtualTextDocumentURI(projectID, jobID string) lsp.DocumentURI {
	return lsp.DocumentURI(fmt.Sprintf("bqls://project/%s/job/%s", projectID, jobID))
}

func newTableVirtualTextDocumentURI(projectID, datasetID, tableID string) lsp.DocumentURI {
	return lsp.DocumentURI(fmt.Sprintf("bqls://project/%s/dataset/%s/table/%s", projectID, datasetID, tableID))
}

func (h *Handler) handleVirtualTextDocument(ctx context.Context, conn *jsonrpc2.Conn, req *jsonrpc2.Request) (result any, err error) {
	if req.Params == nil {
		return nil, &jsonrpc2.Error{Code: jsonrpc2.CodeInvalidParams}
	}

	var params VirtualTextDocumentParams
	if err := json.Unmarshal(*req.Params, &params); err != nil {
		return nil, err
	}

	virtualTextDocument, err := ParseVirtualTextDocument(params.TextDocument.URI)
	if err != nil {
		return nil, err
	}

	if virtualTextDocument.TableID != "" {
		marks, err := h.project.GetTableInfo(ctx, virtualTextDocument.ProjectID, virtualTextDocument.DatasetID, virtualTextDocument.TableID)
		if err != nil {
			return nil, err
		}

		return VirtualTextDocument{Contents: marks}, nil
	}

	return nil, nil
}
