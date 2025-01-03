package file

import (
	"context"
	"fmt"
	"strings"

	bq "cloud.google.com/go/bigquery"
	"github.com/goccy/go-zetasql"
	"github.com/goccy/go-zetasql/ast"
	"github.com/goccy/go-zetasql/types"
	"github.com/kitagry/bqls/langserver/internal/bigquery"
	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/kitagry/bqls/langserver/internal/source/helper"
	"github.com/sirupsen/logrus"
)

type Analyzer struct {
	logger   *logrus.Logger
	bqClient bigquery.Client
	catalog  *Catalog
}

func NewAnalyzer(logger *logrus.Logger, bqClient bigquery.Client) *Analyzer {
	catalog := NewCatalog(bqClient)

	return &Analyzer{
		logger:   logger,
		bqClient: bqClient,
		catalog:  catalog,
	}
}

func (a *Analyzer) langOpt() (*zetasql.LanguageOptions, error) {
	langOpt := zetasql.NewLanguageOptions()
	langOpt.SetNameResolutionMode(zetasql.NameResolutionDefault)
	langOpt.SetProductMode(types.ProductExternal)
	langOpt.EnableMaximumLanguageFeatures()
	langOpt.EnableLanguageFeature(zetasql.FeatureV13AllowDashesInTableName)
	langOpt.EnableLanguageFeature(zetasql.FeatureV13Qualify)
	langOpt.EnableLanguageFeature(zetasql.FeatureV13ScriptLabel)
	langOpt.EnableLanguageFeature(zetasql.FeatureAnalyticFunctions)
	langOpt.EnableLanguageFeature(zetasql.FeatureV13AllowDashesInTableName)
	langOpt.SetSupportsAllStatementKinds()
	langOpt.EnableAllReservableKeywords(true)
	err := langOpt.EnableReservableKeyword("QUALIFY", true)
	if err != nil {
		return nil, err
	}
	return langOpt, nil
}

func (a *Analyzer) AnalyzeStatement(rawText string, stmt ast.StatementNode, catalog types.Catalog) (*zetasql.AnalyzerOutput, error) {
	langOpt, err := a.langOpt()
	if err != nil {
		return nil, err
	}
	opts := zetasql.NewAnalyzerOptions()
	opts.SetLanguage(langOpt)
	opts.SetAllowUndeclaredParameters(true)
	opts.SetErrorMessageMode(zetasql.ErrorMessageOneLine)
	opts.SetParseLocationRecordType(zetasql.ParseLocationRecordCodeSearch)
	return zetasql.AnalyzeStatementFromParserAST(rawText, stmt, catalog, opts)
}

func (a *Analyzer) parseScript(src string) (ast.ScriptNode, error) {
	langOpt, err := a.langOpt()
	if err != nil {
		return nil, err
	}
	opts := zetasql.NewParserOptions()
	opts.SetLanguageOptions(langOpt)
	return zetasql.ParseScript(src, opts, zetasql.ErrorMessageOneLine)
}

func (a *Analyzer) ParseFile(uri lsp.DocumentURI, src string) ParsedFile {
	fixedSrc, errs, fixOffsets := fixDot(src)

	var node ast.ScriptNode
	rnode := make([]*zetasql.AnalyzerOutput, 0)
	for _retry := 0; _retry < 10; _retry++ {
		var err error
		var fo []FixOffset
		node, err = a.parseScript(fixedSrc)
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

		catalog := a.catalog.Clone()
		declarationMap := make(map[string]string)
		for _, s := range stmts {
			if s.Kind() == ast.VariableDeclaration {
				node := s.(*ast.VariableDeclarationNode)
				dummyValue, err := getDummyValueForDeclarationNode(node)
				if err != nil {
					a.logger.Debug("failed to get default value for declaration", err)
				}
				list := node.VariableList().IdentifierList()
				for _, l := range list {
					declarationMap[l.Name()] = dummyValue
				}
				continue
			}

			if s.Kind() == ast.CreateFunctionStatement {
				node := s.(*ast.CreateFunctionStatementNode)
				newFunc, err := a.createFunctionTypes(node, fixedSrc)
				if err != nil {
					errs = append(errs, *err)
					continue
				}

				name := ""
				for _, n := range node.FunctionDeclaration().Name().Names() {
					name += n.Name()
				}
				catalog.AddFunctionWithName(name, newFunc)
			}

			output, err := a.AnalyzeStatement(fixedSrc, s, catalog)
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

			// fix src
			skipError := false
			if isUnrecognizedNameError || isNotExistInStructError || isNotFoundInsideError {
				errTermOffset := positionToByteOffset(fixedSrc, pErr.Position)
				if defaultVal, ok := declarationMap[pErr.IncompleteColumnName]; isUnrecognizedNameError && ok {
					fixedSrc, fo = fixDeclarationError(fixedSrc, pErr, defaultVal)
					if len(fo) > 0 {
						skipError = true
					}
				} else if _, ok := SearchAstNode[*ast.SelectListNode](node, errTermOffset); ok {
					fixedSrc, fo = fixErrorToLiteral(fixedSrc, pErr)
				} else if _, ok := SearchAstNode[*ast.WhereClauseNode](node, errTermOffset); ok {
					fixedSrc, fo = fixErrorForWhereStatement(fixedSrc, s, pErr)
				} else if _, ok := SearchAstNode[*ast.GroupByNode](node, errTermOffset); ok {
					fixedSrc, fo = fixErrorToLiteral(fixedSrc, pErr)
				} else if _, ok := SearchAstNode[*ast.OrderByNode](node, errTermOffset); ok {
					fixedSrc, fo = fixErrorToLiteral(fixedSrc, pErr)
				} else if _, ok := SearchAstNode[*ast.OnClauseNode](node, errTermOffset); ok {
					fixedSrc, fo = fixErrorForWhereStatement(fixedSrc, s, pErr)
				}
			}

			if !skipError {
				errs = append(errs, pErr)
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

func (p *Analyzer) createFunctionTypes(node *ast.CreateFunctionStatementNode, sourceFile string) (*types.Function, *Error) {
	argTypes := []*types.FunctionArgumentType{}
	for _, parameter := range node.FunctionDeclaration().Parameters().ParameterEntries() {
		typ, err := getTypeFromTypeNode(parameter.Type())
		if err != nil {
			p.logger.Debug("failed to get type from parameter ", err)
			return nil, nil
		}
		opt := types.NewFunctionArgumentTypeOptions(types.RequiredArgumentCardinality)
		opt.SetArgumentName(parameter.Name().Name())
		args := types.NewFunctionArgumentType(typ, opt)
		argTypes = append(argTypes, args)
	}

	returnType := node.ReturnType()
	var typ types.Type
	if returnType != nil {
		var err error
		typ, err = getTypeFromTypeNode(node.ReturnType())
		if err != nil {
			p.logger.Debug("failed to get type from return type ", err)
			return nil, nil
		}
	} else {
		err := Error{
			Msg:      "Currently, bqls does not support function without return type.",
			Severity: lsp.Warning,
		}
		loc := node.ParseLocationRange()
		if loc != nil {
			err.Position = helper.IndexToPosition(sourceFile, loc.Start().ByteOffset())
			err.TermLength = loc.End().ByteOffset() - loc.Start().ByteOffset()
		}
		return nil, &err
	}
	opt := types.NewFunctionArgumentTypeOptions(types.RequiredArgumentCardinality)
	retType := types.NewFunctionArgumentType(typ, opt)

	sig := types.NewFunctionSignature(retType, argTypes)

	name := ""
	for _, n := range node.FunctionDeclaration().Name().Names() {
		name += n.Name()
	}
	newFunc := types.NewFunction([]string{name}, "", types.ScalarMode, []*types.FunctionSignature{sig})
	return newFunc, nil
}

func getDummyValueForDeclarationNode(node *ast.VariableDeclarationNode) (string, error) {
	switch n := node.Type().(type) {
	case *ast.ArrayTypeNode:
		return "[]", nil
	case *ast.SimpleTypeNode:
		if pen, ok := n.Child(0).(*ast.PathExpressionNode); ok {
			if in, ok := pen.Child(0).(*ast.IdentifierNode); ok {
				return getDummyValueForDeclarationIdentifierName(in.Name())
			}
		}

		if node.DefaultValue() != nil {
			return getDummyValueForDefaultValueNode(node.DefaultValue())
		}
		return "", fmt.Errorf("failed to load default value")
	default:
		// If declare statement doen't have the explicit type, node.Type() is nil
		// e.g. `DECLARE x DEFAULT 1;`
		if node.DefaultValue() != nil {
			return getDummyValueForDefaultValueNode(node.DefaultValue())
		}

		if n != nil {
			return "", fmt.Errorf("not implemented: %s", n.Kind())
		}
		return "", fmt.Errorf("don't support declaration `%s`", zetasql.Unparse(node))
	}
}

func getDummyValueForDeclarationIdentifierName(name string) (string, error) {
	switch name {
	case "BOOL":
		return "TRUE", nil
	case "INT64":
		return "1", nil
	case "FLOAT64":
		return "1.0", nil
	case "STRING":
		return "''", nil
	case "BYTES":
		return "b''", nil
	case "DATE":
		return "DATE('1970-01-01')", nil
	case "DATETIME":
		return "DATETIME('1970-01-01 00:00:00')", nil
	case "TIME":
		return "TIME('00:00:00')", nil
	case "TIMESTAMP":
		return "TIMESTAMP('1970-01-01 00:00:00')", nil
	default:
		return "", fmt.Errorf("not implemented: %s", name)
	}
}

func getDummyValueForDefaultValueNode(node ast.ExpressionNode) (string, error) {
	switch node.(type) {
	case *ast.NullLiteralNode:
		return "NULL", nil
	case *ast.BooleanLiteralNode:
		return "TRUE", nil
	case *ast.IntLiteralNode:
		return "0", nil
	case *ast.FloatLiteralNode:
		return "0.0", nil
	case *ast.StringLiteralNode:
		return "''", nil
	case *ast.DateOrTimeLiteralNode:
		return "DATE('1970-01-01')", nil
	default:
		return "", fmt.Errorf("not implemented: %T", node)
	}
}

func getTypeFromTypeNode(node ast.TypeNode) (types.Type, error) {
	if stn, ok := node.(*ast.SimpleTypeNode); ok {
		names := stn.TypeName().Names()
		typeName := ""
		for _, n := range names {
			typeName += n.Name()
		}

		switch typeName {
		case "INT64":
			return types.Int64Type(), nil
		case "FLOAT64":
			return types.FloatType(), nil
		case "BOOL":
			return types.BoolType(), nil
		case "STRING":
			return types.StringType(), nil
		case "BYTES":
			return types.BytesType(), nil
		case "DATE":
			return types.DateType(), nil
		case "DATETIME":
			return types.DatetimeType(), nil
		case "TIME":
			return types.TimeType(), nil
		case "TIMESTAMP":
			return types.TimestampType(), nil
		case "NUMERIC":
			return types.NumericType(), nil
		case "BIGNUMERIC":
			return types.BigNumericType(), nil
		case "GEOGRAPHY":
			return types.GeographyType(), nil
		case "INTERVAL":
			return types.IntervalType(), nil
		case "JSON":
			return types.JsonType(), nil
		default:
			return nil, fmt.Errorf("not implemented: %s", typeName)
		}
	}
	return nil, fmt.Errorf("not implemented: %T", node)
}
