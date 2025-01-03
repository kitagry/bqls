package source

import (
	"context"

	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/kitagry/bqls/langserver/internal/source/completion"
)

func (p *Project) Complete(ctx context.Context, uri lsp.DocumentURI, position lsp.Position) ([]completion.CompletionItem, error) {
	sql := p.cache.Get(uri)
	parsedFile := p.analyzer.ParseFile(uri, sql.RawText)

	completor := completion.New(p.logger, p.analyzer, p.bqClient)
	return completor.Complete(ctx, parsedFile, position)
}
