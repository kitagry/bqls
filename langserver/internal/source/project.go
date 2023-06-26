package source

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"unicode"

	bq "cloud.google.com/go/bigquery"
	"github.com/goccy/go-zetasql"
	"github.com/goccy/go-zetasql/ast"
	rast "github.com/goccy/go-zetasql/resolved_ast"
	"github.com/goccy/go-zetasql/types"
	"github.com/kitagry/bqls/langserver/internal/bigquery"
	"github.com/kitagry/bqls/langserver/internal/cache"
	"github.com/sirupsen/logrus"
)

type Project struct {
	rootPath string
	logger   *logrus.Logger
	cache    *cache.GlobalCache
	bqClient bigquery.Client
	catalog  types.Catalog
}

type File struct {
	RawText string
	Version int
}

func NewProject(ctx context.Context, rootPath string, logger *logrus.Logger) (*Project, error) {
	cache := cache.NewGlobalCache()

	bqClient, err := bigquery.New(ctx, true)
	if err != nil {
		return nil, fmt.Errorf("failed to create bigquery client: %w", err)
	}

	catalog := NewCatalog(bqClient)

	return &Project{
		rootPath: rootPath,
		logger:   logger,
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

	parsedFile := p.ParseFile(path, sql.RawText)
	if len(parsedFile.Errors) > 0 {
		return map[string][]Error{path: parsedFile.Errors}
	}

	return map[string][]Error{path: nil}
}

func (p *Project) analyzeStatement(rawText string, stmt ast.StatementNode) (*zetasql.AnalyzerOutput, error) {
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
	langOpt.SetSupportedStatementKinds([]rast.Kind{
		rast.BeginStmt,
		rast.CommitStmt,
		rast.MergeStmt,
		rast.QueryStmt,
		rast.InsertStmt,
		rast.UpdateStmt,
		rast.DeleteStmt,
		rast.DropStmt,
		rast.TruncateStmt,
		rast.CreateTableStmt,
		rast.CreateTableAsSelectStmt,
		rast.CreateProcedureStmt,
		rast.CreateFunctionStmt,
		rast.CreateTableFunctionStmt,
		rast.CreateViewStmt,
	})
	opts := zetasql.NewAnalyzerOptions()
	opts.SetLanguage(langOpt)
	opts.SetAllowUndeclaredParameters(true)
	opts.SetErrorMessageMode(zetasql.ErrorMessageOneLine)
	opts.SetParseLocationRecordType(zetasql.ParseLocationRecordCodeSearch)
	return zetasql.AnalyzeStatementFromParserAST(rawText, stmt, p.catalog, opts)
}

func (p *Project) getTableMetadataFromPath(ctx context.Context, path string) (*bq.TableMetadata, error) {
	splitNode := strings.Split(path, ".")

	// validate id
	for _, id := range splitNode {
		if id == "" {
			return nil, fmt.Errorf("invalid path: %s", path)
		}
	}

	switch len(splitNode) {
	case 3:
		return p.bqClient.GetTableMetadata(ctx, splitNode[0], splitNode[1], splitNode[2])
	case 2:
		return p.bqClient.GetTableMetadata(ctx, p.bqClient.GetDefaultProject(), splitNode[0], splitNode[1])
	default:
		return nil, fmt.Errorf("invalid path: %s", path)
	}
}

func (p *Project) listLatestSuffixTables(ctx context.Context, projectID, datasetID string) ([]*bq.Table, error) {
	tables, err := p.bqClient.ListTables(ctx, projectID, datasetID)
	if err != nil {
		return nil, fmt.Errorf("failed to ListTables: %w", err)
	}

	type tableWithSuffix struct {
		suffix int
		table  *bq.Table
	}

	maxSuffixTables := make(map[string]tableWithSuffix)
	for _, table := range tables {
		var filterdIntStr string
		t := strings.TrimRightFunc(table.TableID, func(r rune) bool {
			if !unicode.IsDigit(r) {
				return false
			}
			filterdIntStr = string(r) + filterdIntStr
			return true
		})
		suffix, err := strconv.Atoi(filterdIntStr)
		if err != nil {
			maxSuffixTables[table.TableID] = tableWithSuffix{
				suffix: 0,
				table:  table,
			}
			continue
		}
		maxTable, ok := maxSuffixTables[t]
		if !ok {
			maxSuffixTables[t] = tableWithSuffix{
				suffix: suffix,
				table:  table,
			}
			continue
		}

		if maxTable.suffix < suffix {
			maxSuffixTables[t] = tableWithSuffix{
				suffix: suffix,
				table:  table,
			}
		}
	}

	filteredTables := make([]*bq.Table, 0)
	for _, t := range maxSuffixTables {
		filteredTables = append(filteredTables, t.table)
	}

	sort.Slice(filteredTables, func(i, j int) bool {
		return filteredTables[i].TableID < filteredTables[j].TableID
	})

	return filteredTables, nil
}
