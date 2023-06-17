package source

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	bq "cloud.google.com/go/bigquery"
	"github.com/goccy/go-zetasql"
	ast "github.com/goccy/go-zetasql/resolved_ast"
	"github.com/goccy/go-zetasql/types"
	"github.com/kitagry/bqls/langserver/internal/bigquery"
	"github.com/kitagry/bqls/langserver/internal/cache"
	"github.com/kitagry/bqls/langserver/internal/lsp"
)

type Project struct {
	rootPath string
	cache    *cache.GlobalCache
	bqClient bigquery.Client
	catalog  types.Catalog
}

type File struct {
	RawText string
	Version int
}

func NewProject(ctx context.Context, rootPath string) (*Project, error) {
	cache := cache.NewGlobalCache()

	bqClient, err := bigquery.New(ctx, true)
	if err != nil {
		return nil, fmt.Errorf("failed to create bigquery client: %w", err)
	}

	catalog := NewCatalog(bqClient)

	return &Project{
		rootPath: rootPath,
		cache:    cache,
		bqClient: bqClient,
		catalog:  catalog,
	}, nil
}

func NewProjectWithBQClient(rootPath string, bqClient bigquery.Client) *Project {
	cache := cache.NewGlobalCache()
	catalog := NewCatalog(bqClient)
	return &Project{
		rootPath: rootPath,
		cache:    cache,
		bqClient: bqClient,
		catalog:  catalog,
	}
}

func (p *Project) Close() error {
	return p.bqClient.Close()
}

func (p *Project) UpdateFile(path string, text string, version int) error {
	p.cache.Put(path, text)

	return nil
}

func (p *Project) GetFile(path string) (string, bool) {
	sql := p.cache.Get(path)
	if sql == nil {
		return "", false
	}
	return sql.RawText, true
}

func (p *Project) DeleteFile(path string) {
	p.cache.Delete(path)
}

func (p *Project) GetErrors(path string) map[string][]Error {
	sql := p.cache.Get(path)
	if sql == nil {
		return nil
	}

	if len(sql.Errors) > 0 {
		errs := make([]Error, len(sql.Errors))
		for i, e := range sql.Errors {
			errs[i] = parseZetaSQLError(e)
		}
		return map[string][]Error{path: errs}
	}

	_, err := p.analyzeStatement(sql)
	if err != nil {
		return map[string][]Error{path: {parseZetaSQLError(err)}}
	}

	return map[string][]Error{path: nil}
}

func (p *Project) analyzeStatement(sql *cache.SQL) ([]*zetasql.AnalyzerOutput, error) {
	langOpt := zetasql.NewLanguageOptions()
	langOpt.SetNameResolutionMode(zetasql.NameResolutionDefault)
	langOpt.SetProductMode(types.ProductInternal)
	langOpt.SetEnabledLanguageFeatures([]zetasql.LanguageFeature{
		zetasql.FeatureAnalyticFunctions,
		zetasql.FeatureNamedArguments,
		zetasql.FeatureNumericType,
		zetasql.FeatureBignumericType,
		zetasql.FeatureV13DecimalAlias,
		zetasql.FeatureCreateTableNotNull,
		zetasql.FeatureParameterizedTypes,
		zetasql.FeatureTablesample,
		zetasql.FeatureTimestampNanos,
		zetasql.FeatureV11HavingInAggregate,
		zetasql.FeatureV11NullHandlingModifierInAggregate,
		zetasql.FeatureV11NullHandlingModifierInAnalytic,
		zetasql.FeatureV11OrderByCollate,
		zetasql.FeatureV11SelectStarExceptReplace,
		zetasql.FeatureV12SafeFunctionCall,
		zetasql.FeatureJsonType,
		zetasql.FeatureJsonArrayFunctions,
		zetasql.FeatureJsonStrictNumberParsing,
		zetasql.FeatureV13IsDistinct,
		zetasql.FeatureV13FormatInCast,
		zetasql.FeatureV13DateArithmetics,
		zetasql.FeatureV11OrderByInAggregate,
		zetasql.FeatureV11LimitInAggregate,
		zetasql.FeatureV13DateTimeConstructors,
		zetasql.FeatureV13ExtendedDateTimeSignatures,
		zetasql.FeatureV12CivilTime,
		zetasql.FeatureV12WeekWithWeekday,
		zetasql.FeatureIntervalType,
		zetasql.FeatureGroupByRollup,
		zetasql.FeatureV13NullsFirstLastInOrderBy,
		zetasql.FeatureV13Qualify,
		zetasql.FeatureV13AllowDashesInTableName,
		zetasql.FeatureGeography,
		zetasql.FeatureV13ExtendedGeographyParsers,
		zetasql.FeatureTemplateFunctions,
		zetasql.FeatureV11WithOnSubquery,
	})
	langOpt.SetSupportedStatementKinds([]ast.Kind{
		ast.BeginStmt,
		ast.CommitStmt,
		ast.MergeStmt,
		ast.QueryStmt,
		ast.InsertStmt,
		ast.UpdateStmt,
		ast.DeleteStmt,
		ast.DropStmt,
		ast.TruncateStmt,
		ast.CreateTableStmt,
		ast.CreateTableAsSelectStmt,
		ast.CreateProcedureStmt,
		ast.CreateFunctionStmt,
		ast.CreateTableFunctionStmt,
		ast.CreateViewStmt,
	})
	opts := zetasql.NewAnalyzerOptions()
	opts.SetLanguage(langOpt)
	opts.SetAllowUndeclaredParameters(true)
	opts.SetErrorMessageMode(zetasql.ErrorMessageOneLine)
	opts.SetParseLocationRecordType(zetasql.ParseLocationRecordCodeSearch)

	var results []*zetasql.AnalyzerOutput
	for _, stmt := range sql.GetStatementNodes() {
		output, err := zetasql.AnalyzeStatementFromParserAST(sql.RawText, stmt, p.catalog, opts)
		if err != nil {
			return nil, err
		}
		results = append(results, output)
	}
	return results, nil
}

func (p *Project) getTableMetadataFromPath(ctx context.Context, path string) (*bq.TableMetadata, error) {
	splitNode := strings.Split(path, ".")
	switch len(splitNode) {
	case 3:
		return p.bqClient.GetTableMetadata(ctx, splitNode[0], splitNode[1], splitNode[2])
	case 2:
		return p.bqClient.GetTableMetadata(ctx, p.bqClient.GetDefaultProject(), splitNode[0], splitNode[1])
	default:
		return nil, fmt.Errorf("invalid path: %s", path)
	}
}

type Error struct {
	Msg      string
	Position lsp.Position
}

func parseZetaSQLError(err error) Error {
	errStr := err.Error()
	if !strings.Contains(errStr, "[at ") {
		return Error{Msg: errStr}
	}

	// extract position information like "... [at 1:28]"
	positionInd := strings.Index(errStr, "[at ")
	location := errStr[positionInd+4 : len(errStr)-1]
	locationSep := strings.Split(location, ":")
	line, _ := strconv.Atoi(locationSep[0])
	col, _ := strconv.Atoi(locationSep[1])
	pos := lsp.Position{Line: line - 1, Character: col - 1}

	// Trim position information
	errStr = strings.TrimSpace(errStr[:positionInd])
	return Error{Msg: errStr, Position: pos}
}
