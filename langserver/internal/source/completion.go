package source

import (
	"context"
	"fmt"
	"strings"

	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/kitagry/bqls/langserver/internal/source/completion"
)

func (p *Project) Complete(ctx context.Context, uri lsp.DocumentURI, position lsp.Position) ([]completion.CompletionItem, error) {
	sql := p.cache.Get(uri)
	parsedFile := p.analyzer.ParseFile(uri, sql.RawText)
	defer parsedFile.Close()

	completor := completion.New(p.logger, p.analyzer, p.bqClient)
	return completor.Complete(ctx, parsedFile, position)
}

func (p *Project) ResolveCompletionItem(ctx context.Context, item lsp.CompletionItem) (lsp.CompletionItem, error) {
	if item.Kind == lsp.CIKModule {
		separatedItem := strings.Split(item.Documentation.Value, ".")
		if len(separatedItem) == 2 {
			metadata, err := p.bqClient.GetDatasetMetadata(ctx, separatedItem[0], separatedItem[1])
			if err != nil {
				return item, err
			}

			item.Documentation = lsp.MarkupContent{
				Kind: lsp.MKMarkdown,
				Value: fmt.Sprintf(`%s

%s`, metadata.FullID, metadata.Description),
			}
		} else if len(separatedItem) == 3 {
			metadata, err := p.bqClient.GetTableMetadata(ctx, separatedItem[0], separatedItem[1], separatedItem[2])
			if err != nil {
				return item, err
			}

			item.Documentation = lsp.MarkupContent{
				Kind: lsp.MKMarkdown,
				Value: fmt.Sprintf(`%s

%s`, metadata.FullID, metadata.Description),
			}
		}
	}
	return item, nil
}
