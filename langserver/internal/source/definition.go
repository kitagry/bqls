package source

import (
	"context"
	"strings"

	googlesql "github.com/goccy/go-googlesql"
	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/kitagry/bqls/langserver/internal/source/file"
)

func (p *Project) LookupIdent(ctx context.Context, uri lsp.DocumentURI, position lsp.Position) ([]lsp.Location, error) {
	sql := p.cache.Get(uri)
	parsedFile := p.analyzer.ParseFile(uri, sql.RawText)
	defer parsedFile.Close()

	termOffset := parsedFile.TermOffset(position)

	tablePathExpression, ok := file.SearchAstNode[*googlesql.ASTTablePathExpression](parsedFile.Node, termOffset)
	if ok {
		// NOTE: ignore ResolvedScanNode because it is not resolved
		output, ok := parsedFile.FindTargetAnalyzeOutput(termOffset)
		var tablePathRastNode googlesql.ResolvedScanNode
		if ok {
			tablePathRastNode, _ = file.SearchResolvedAstNode[googlesql.ResolvedScanNode](output, termOffset)
		}
		return p.lookupTablePathExpressionNode(ctx, uri, parsedFile, tablePathExpression, tablePathRastNode)
	}

	if locs := p.listupTargetVariableDeclarationEntries(parsedFile, uri, position); len(locs) > 0 {
		return locs, nil
	}

	if locs := p.listupTargetCreateFunctionEntries(parsedFile, uri, position); len(locs) > 0 {
		return locs, nil
	}

	return nil, nil
}

func (p *Project) listupTargetCreateFunctionEntries(parsedFile file.ParsedFile, uri lsp.DocumentURI, position lsp.Position) []lsp.Location {
	rawOffset := rawByteOffset(parsedFile.Src, position)
	identName := wordAtByteOffset(parsedFile.Src, rawOffset)
	if identName == "" {
		return nil
	}

	createFunctions := file.ListAstNode[*googlesql.ASTCreateFunctionStatement](parsedFile.Node)
	for _, fn := range createFunctions {
		decl, err := fn.FunctionDeclaration()
		if err != nil || decl == nil {
			continue
		}
		pathExpr, err := decl.Name()
		if err != nil || pathExpr == nil {
			continue
		}
		names, err := pathExpr.ToIdentifierVector()
		if err != nil || len(names) == 0 {
			continue
		}
		if !strings.EqualFold(names[len(names)-1], identName) {
			continue
		}
		loc, err := pathExpr.GetParseLocationRange()
		if err != nil || loc == nil {
			continue
		}
		r, ok := parsedFile.ToLspRange(loc)
		if !ok {
			continue
		}
		return []lsp.Location{{URI: uri, Range: r}}
	}
	return nil
}

func (p *Project) listupTargetVariableDeclarationEntries(parsedFile file.ParsedFile, uri lsp.DocumentURI, position lsp.Position) []lsp.Location {
	// Use raw source to extract the word at cursor because fixes may have replaced variable
	// references with literal values in the fixedSrc used for AST parsing.
	rawOffset := rawByteOffset(parsedFile.Src, position)
	identName := wordAtByteOffset(parsedFile.Src, rawOffset)
	if identName == "" {
		return nil
	}

	declarations := file.ListAstNode[*googlesql.ASTVariableDeclaration](parsedFile.Node)
	for _, decl := range declarations {
		varList, err := decl.VariableList()
		if err != nil || varList == nil {
			continue
		}
		n, _ := varList.NumChildren()
		for i := int32(0); i < n; i++ {
			declIdent, err := varList.IdentifierList(i)
			if err != nil {
				continue
			}
			declName, err := declIdent.GetAsString()
			if err != nil || declName != identName {
				continue
			}
			loc, err := declIdent.GetParseLocationRange()
			if err != nil || loc == nil {
				continue
			}
			r, ok := parsedFile.ToLspRange(loc)
			if !ok {
				continue
			}
			return []lsp.Location{{URI: uri, Range: r}}
		}
	}
	return nil
}

func rawByteOffset(src string, pos lsp.Position) int {
	lines := strings.Split(src, "\n")
	offset := 0
	for i := 0; i < pos.Line && i < len(lines); i++ {
		offset += len(lines[i]) + 1
	}
	return offset + pos.Character
}

func wordAtByteOffset(src string, offset int) string {
	if offset < 0 || offset > len(src) {
		return ""
	}
	start := offset
	for start > 0 && isIdentByte(src[start-1]) {
		start--
	}
	end := offset
	for end < len(src) && isIdentByte(src[end]) {
		end++
	}
	return src[start:end]
}

func isIdentByte(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_'
}

func (p *Project) lookupTablePathExpressionNode(ctx context.Context, uri lsp.DocumentURI, parsedFile file.ParsedFile, tablePathExpression *googlesql.ASTTablePathExpression, tablePathRastNode googlesql.ResolvedScanNode) ([]lsp.Location, error) {
	p.logger.Println("lookupTablePathExpressionNode", tablePathExpression, tablePathRastNode)
	pathExpr, err := tablePathExpression.PathExpr()
	if err != nil || pathExpr == nil {
		return nil, nil
	}

	tableNames, err := pathExpr.ToIdentifierVector()
	if err != nil {
		return nil, nil
	}
	tableName := strings.Join(tableNames, ".")

	// Search table name in table path expression
	result := p.listupTargetTableExpressionEntries(ctx, parsedFile, uri, tableName, tablePathRastNode)

	// Search table name in with clause
	result = append(result, p.listupTargetWithClauseEntries(parsedFile, uri, tableName)...)

	return result, nil
}

func (p *Project) listupTargetTableExpressionEntries(ctx context.Context, parsedFile file.ParsedFile, uri lsp.DocumentURI, tableName string, scanNode googlesql.ResolvedScanNode) []lsp.Location {
	tableScanNode, ok := scanNode.(*googlesql.ResolvedTableScan)
	if !ok {
		return nil
	}

	table, err := tableScanNode.Table()
	if err != nil || table == nil {
		return nil
	}
	tableFullName, err := table.Name()
	if err != nil {
		return nil
	}

	metadata, err := p.analyzer.GetTableMetadataFromPath(ctx, tableFullName)
	if err != nil {
		return nil
	}

	projectID, datasetID, tableID, ok := extractTableIDsFromMedatada(metadata)
	if !ok {
		return nil
	}

	return []lsp.Location{
		{
			URI: lsp.NewTableVirtualTextDocumentURI(projectID, datasetID, tableID),
			Range: lsp.Range{
				Start: lsp.Position{
					Line:      0,
					Character: 0,
				},
				End: lsp.Position{
					Line:      0,
					Character: 0,
				},
			},
		},
	}
}

func (p *Project) listupTargetWithClauseEntries(parsedFile file.ParsedFile, uri lsp.DocumentURI, tableName string) []lsp.Location {
	withClauseEntries := file.ListAstNode[*googlesql.ASTWithClauseEntry](parsedFile.Node)
	result := make([]lsp.Location, 0, len(withClauseEntries))
	for _, entry := range withClauseEntries {
		aliasedQuery, err := entry.AliasedQuery()
		if err != nil || aliasedQuery == nil {
			continue
		}
		alias, err := aliasedQuery.Alias()
		if err != nil || alias == nil {
			continue
		}
		name, err := alias.GetAsString()
		if err != nil || name != tableName {
			continue
		}
		loc, err := alias.GetParseLocationRange()
		if err != nil || loc == nil {
			continue
		}
		r, ok := parsedFile.ToLspRange(loc)
		if !ok {
			continue
		}
		result = append(result, lsp.Location{
			URI:   uri,
			Range: r,
		})
	}
	return result
}
