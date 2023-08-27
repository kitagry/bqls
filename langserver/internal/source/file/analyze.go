package file

import (
	"context"
	"fmt"
	"strings"

	bq "cloud.google.com/go/bigquery"
	"github.com/goccy/go-zetasql"
	"github.com/goccy/go-zetasql/ast"
	rast "github.com/goccy/go-zetasql/resolved_ast"
	"github.com/goccy/go-zetasql/types"
	"github.com/kitagry/bqls/langserver/internal/bigquery"
)

type Analyzer struct {
	bqClient bigquery.Client
	catalog  types.Catalog
}

func NewAnalyzer(bqClient bigquery.Client) *Analyzer {
	catalog := NewCatalog(bqClient)

	return &Analyzer{
		bqClient: bqClient,
		catalog:  catalog,
	}
}

func (a *Analyzer) AnalyzeStatement(rawText string, stmt ast.StatementNode) (*zetasql.AnalyzerOutput, error) {
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
		zetasql.FeatureV13Pivot,
		zetasql.FeatureV13Unpivot,
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
	return zetasql.AnalyzeStatementFromParserAST(rawText, stmt, a.catalog, opts)
}

func (p *Analyzer) ParseFile(uri string, src string) ParsedFile {
	fixedSrc, errs, fixOffsets := fixDot(src)

	var node ast.ScriptNode
	rnode := make([]*zetasql.AnalyzerOutput, 0)
	for _retry := 0; _retry < 10; _retry++ {
		var err error
		var fo []FixOffset
		node, err = zetasql.ParseScript(fixedSrc, zetasql.NewParserOptions(), zetasql.ErrorMessageOneLine)
		if err != nil {
			pErr := parseZetaSQLError(err)
			if strings.Contains(pErr.Msg, "SELECT list must not be empty") {
				fixedSrc, pErr, fo = fixSelectListMustNotBeEmptyStatement(fixedSrc, pErr)
			}
			if strings.Contains(pErr.Msg, "Unexpected end of script") {
				fixedSrc, pErr, fo = fixUnexpectedEndOfScript(fixedSrc, pErr)
			}
			errs = append(errs, pErr)
			if len(fo) > 0 {
				// retry
				fixOffsets = append(fixOffsets, fo...)
				continue
			}
		}

		stmts := make([]ast.StatementNode, 0)
		ast.Walk(node, func(n ast.Node) error {
			if n == nil {
				return nil
			}
			if n.IsStatement() {
				stmts = append(stmts, n)
			}
			return nil
		})

		for _, s := range stmts {
			output, err := p.AnalyzeStatement(fixedSrc, s)
			if err == nil {
				rnode = append(rnode, output)
				continue
			}

			pErr := parseZetaSQLError(err)

			isUnrecognizedNameError := strings.Contains(pErr.Msg, "Unrecognized name: ")
			isNotExistInStructError := strings.Contains(pErr.Msg, "does not exist in STRUCT")
			isNotFoundInsideError := strings.Contains(pErr.Msg, "not found inside")
			isTableNotFoundError := strings.Contains(pErr.Msg, "Table not found: ")

			// add information to Error
			switch {
			case isUnrecognizedNameError:
				pErr = addInformationToUnrecognizedNameError(fixedSrc, pErr)
			case isNotExistInStructError:
				pErr = addInformationToNotExistInStructError(fixedSrc, pErr)
			case isNotFoundInsideError:
				pErr = addInformationToNotFoundInsideTableError(fixedSrc, pErr)
			case isTableNotFoundError:
				ind := strings.Index(pErr.Msg, "Table not found: ")
				table := strings.TrimSpace(pErr.Msg[ind+len("Table not found: "):])
				pErr.TermLength = len(table)
				pErr.IncompleteColumnName = table
			}
			errs = append(errs, pErr)

			// fix src
			if isUnrecognizedNameError || isNotExistInStructError || isNotFoundInsideError {
				errTermOffset := positionToByteOffset(fixedSrc, pErr.Position)
				if _, ok := SearchAstNode[*ast.SelectListNode](node, errTermOffset); ok {
					fixedSrc, fo = fixErrorToLiteral(fixedSrc, pErr)
				}
				if _, ok := SearchAstNode[*ast.WhereClauseNode](node, errTermOffset); ok {
					fixedSrc, fo = fixErrorForWhereStatement(fixedSrc, s, pErr)
				}
				if _, ok := SearchAstNode[*ast.GroupByNode](node, errTermOffset); ok {
					fixedSrc, fo = fixErrorToLiteral(fixedSrc, pErr)
				}
				if _, ok := SearchAstNode[*ast.OrderByNode](node, errTermOffset); ok {
					fixedSrc, fo = fixErrorToLiteral(fixedSrc, pErr)
				}
				if _, ok := SearchAstNode[*ast.OnClauseNode](node, errTermOffset); ok {
					fixedSrc, fo = fixErrorForWhereStatement(fixedSrc, s, pErr)
				}
			}

			if len(fo) > 0 {
				fixOffsets = append(fixOffsets, fo...)
				goto retry
			}
		}
		break
	retry:
	}

	return ParsedFile{
		URI:        uri,
		Src:        src,
		Node:       node,
		RNode:      rnode,
		FixOffsets: fixOffsets,
		Errors:     errs,
	}
}

func (a *Analyzer) GetTableMetadataFromPath(ctx context.Context, path string) (*bq.TableMetadata, error) {
	splitNode := strings.Split(path, ".")

	// validate id
	for _, id := range splitNode {
		if id == "" {
			return nil, fmt.Errorf("invalid path: %s", path)
		}
	}

	switch len(splitNode) {
	case 3:
		return a.bqClient.GetTableMetadata(ctx, splitNode[0], splitNode[1], splitNode[2])
	case 2:
		return a.bqClient.GetTableMetadata(ctx, a.bqClient.GetDefaultProject(), splitNode[0], splitNode[1])
	default:
		return nil, fmt.Errorf("invalid path: %s", path)
	}
}
