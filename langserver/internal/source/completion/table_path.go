package completion

import (
	"context"
	"errors"
	"fmt"
	"strings"

	googlesql "github.com/goccy/go-googlesql"
	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/kitagry/bqls/langserver/internal/source/file"
)

type tablePathParams struct {
	ProjectID string
	DatasetID string
	TableID   string
}

func (c *completor) completeTablePath(ctx context.Context, parsedFile file.ParsedFile, position lsp.Position) ([]CompletionItem, error) {
	termOffset := parsedFile.TermOffset(position)

	// cursor is on table name
	tablePathNode, ok := file.SearchAstNode[*googlesql.ASTTablePathExpression](parsedFile.Node, termOffset)
	if !ok {
		tablePath, ok := findTablePathEndingWithDot(parsedFile, position, termOffset)
		if !ok {
			return nil, nil
		}
		return c.completeTablePathFromPath(ctx, tablePath)
	}
	loc, _ := tablePathNode.GetParseLocationRange()
	// The cursor right after the closing backquote is outside of the table path.
	// An unquoted path has no closing quote, so the cursor at its end is still on it.
	if file.ParseLocEnd(loc) == termOffset && termOffset > 0 && parsedFile.FixedSrc[termOffset-1] == '`' {
		return nil, nil
	}

	tablePath, ok := file.CreateTableNameFromTablePathExpressionNode(tablePathNode)
	if !ok {
		return nil, nil
	}

	return c.completeTablePathFromPath(ctx, tablePath)
}

// findTablePathEndingWithDot finds an unquoted table path that ends with a dot,
// such as `FROM dataset.`. The fix-up for `alias.` rewrites it to `FROM true`,
// so no table path node is left in the AST.
func findTablePathEndingWithDot(parsedFile file.ParsedFile, position lsp.Position, termOffset int) (string, bool) {
	const replaced = "true"

	tablePath := parsedFile.FindIncompleteColumnName(position)
	if !strings.HasSuffix(tablePath, ".") {
		return "", false
	}

	if termOffset > len(parsedFile.FixedSrc) || !strings.HasSuffix(parsedFile.FixedSrc[:termOffset], replaced) {
		return "", false
	}
	words := strings.Fields(parsedFile.FixedSrc[:termOffset-len(replaced)])
	if len(words) == 0 {
		return "", false
	}
	lastWord := words[len(words)-1]
	if !strings.EqualFold(lastWord, "FROM") && !strings.EqualFold(lastWord, "JOIN") {
		return "", false
	}

	return tablePath, true
}

func (c *completor) completeTablePathFromPath(ctx context.Context, tablePath string) ([]CompletionItem, error) {
	splittedTablePath := strings.Split(tablePath, ".")

	// for `project.dataset.table` completion
	params := tablePathParams{}
	if len(splittedTablePath) >= 1 {
		params.ProjectID = splittedTablePath[0]
	}
	if len(splittedTablePath) >= 2 {
		params.DatasetID = splittedTablePath[1]
	}
	if len(splittedTablePath) >= 3 {
		params.TableID = splittedTablePath[2]
	}

	// for `dataset.table` completion
	defaultParams := tablePathParams{
		ProjectID: c.bqClient.GetDefaultProject(),
	}
	if len(splittedTablePath) >= 1 {
		defaultParams.DatasetID = splittedTablePath[0]
	}
	if len(splittedTablePath) >= 2 {
		defaultParams.TableID = splittedTablePath[1]
	}

	switch len(splittedTablePath) {
	case 0, 1:
		result1, err1 := c.completeProjectForTablePath(ctx, params)
		result2, err2 := c.completeDatasetForTablePath(ctx, defaultParams)
		if len(result1) == 0 && len(result2) == 0 {
			return nil, errors.Join(err1, err2)
		}
		return append(result1, result2...), nil
	case 2:
		result1, err1 := c.completeDatasetForTablePath(ctx, params)
		result2, err2 := c.completeTableForTablePath(ctx, defaultParams)
		if len(result1) == 0 && len(result2) == 0 {
			return nil, errors.Join(err1, err2)
		}
		return append(result1, result2...), nil
	case 3:
		return c.completeTableForTablePath(ctx, params)
	}

	return nil, nil
}

func (c *completor) completeProjectForTablePath(ctx context.Context, param tablePathParams) ([]CompletionItem, error) {
	projects, err := c.bqClient.ListProjects(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to ListProjects: %w", err)
	}

	result := make([]CompletionItem, 0)
	for _, p := range projects {
		if !strings.HasPrefix(p.ProjectId, param.ProjectID) {
			continue
		}

		result = append(result, CompletionItem{
			Kind:    lsp.CIKModule,
			NewText: p.ProjectId,
			Detail:  "PROJECT",
			Documentation: lsp.MarkupContent{
				Kind:  lsp.MKPlainText,
				Value: p.Name,
			},
			TypedPrefix: param.ProjectID,
		})
	}

	return result, nil
}

func (c *completor) completeDatasetForTablePath(ctx context.Context, param tablePathParams) ([]CompletionItem, error) {
	datasets, err := c.bqClient.ListDatasets(ctx, param.ProjectID)
	if err != nil {
		return nil, fmt.Errorf("failed to ListDatasets: %w", err)
	}

	result := make([]CompletionItem, 0)
	for _, d := range datasets {
		if !strings.HasPrefix(d.DatasetID, param.DatasetID) {
			continue
		}

		result = append(result, CompletionItem{
			Kind:    lsp.CIKModule,
			NewText: d.DatasetID,
			Detail:  "DATASET",
			Documentation: lsp.MarkupContent{
				Kind:  lsp.MKPlainText,
				Value: fmt.Sprintf("%s.%s", d.ProjectID, d.DatasetID),
			},
			TypedPrefix: param.DatasetID,
		})
	}

	return result, nil
}

func (c *completor) completeTableForTablePath(ctx context.Context, param tablePathParams) ([]CompletionItem, error) {
	tables, err := c.bqClient.ListTables(ctx, param.ProjectID, param.DatasetID)
	if err != nil {
		return nil, fmt.Errorf("failed to bqClient.ListTables: %w", err)
	}

	result := make([]CompletionItem, 0)
	for _, t := range tables {
		if !strings.HasPrefix(t.TableID, param.TableID) {
			continue
		}

		result = append(result, CompletionItem{
			Kind:    lsp.CIKModule,
			NewText: t.TableID,
			Detail:  "TABLE",
			Documentation: lsp.MarkupContent{
				Kind:  lsp.MKPlainText,
				Value: fmt.Sprintf("%s.%s.%s", t.ProjectID, t.DatasetID, t.TableID),
			},
			TypedPrefix: param.TableID,
		})
	}

	return result, nil
}
