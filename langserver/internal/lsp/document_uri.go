package lsp

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

type DocumentURI string

func (d *DocumentURI) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}

	d = toPtr(DocumentURI(s))
	return nil
}

func NewDocumentURI(uri string) DocumentURI {
	return DocumentURI("file://" + uri)
}

func (d DocumentURI) ToURI() string {
	return string(d)[len("file://"):]
}

func (d DocumentURI) IsVirtualTextDocument() bool {
	return strings.HasPrefix(string(d), "bqls://")
}

func (d DocumentURI) VirtualTextDocumentInfo() (VirtualTextDocumentInfo, error) {
	suffix, ok := strings.CutPrefix(string(d), "bqls://")
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

type VirtualTextDocumentInfo struct {
	ProjectID string
	DatasetID string
	TableID   string
	JobID     string
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

func toPtr[T any](t T) *T {
	return &t
}
