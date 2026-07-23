package langserver

import (
	"context"

	"github.com/kitagry/bqls/langserver/internal/lsp"
)

func (h *Handler) handleTextDocumentDidOpen(ctx context.Context, params lsp.DidOpenTextDocumentParams) (struct{}, error) {
	h.updateDocument(params.TextDocument.URI, params.TextDocument.Text, params.TextDocument.Version)

	return struct{}{}, nil
}

func (h *Handler) handleTextDocumentDidChange(ctx context.Context, params lsp.DidChangeTextDocumentParams) (struct{}, error) {
	h.updateDocument(params.TextDocument.URI, params.ContentChanges[0].Text, params.TextDocument.Version)

	return struct{}{}, nil
}

func (h *Handler) handleTextDocumentDidClose(ctx context.Context, params lsp.DidCloseTextDocumentParams) (struct{}, error) {
	h.project.DeleteFile(params.TextDocument.URI)

	return struct{}{}, nil
}

func (h *Handler) handleTextDocumentDidSave(ctx context.Context, params lsp.DidSaveTextDocumentParams) (struct{}, error) {
	h.diagnosticRequest <- params.TextDocument.URI
	h.dryrunRequest <- params.TextDocument.URI

	return struct{}{}, nil
}

func (h *Handler) updateDocument(uri lsp.DocumentURI, text string, version int) {
	h.project.UpdateFile(uri, text, version)
	h.diagnosticRequest <- uri
}
