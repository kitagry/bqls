package file

import (
	"context"
	"fmt"
	"strings"

	bq "cloud.google.com/go/bigquery"
	googlesql "github.com/goccy/go-googlesql"
	"github.com/kitagry/bqls/langserver/internal/bigquery"
	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/kitagry/bqls/langserver/internal/source/helper"
	"github.com/sirupsen/logrus"
	tssql "github.com/kitagry/tree-sitter-bigquery/bindings/go"
	ts "github.com/tree-sitter/go-tree-sitter"
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

func newLanguageOptions() (*googlesql.LanguageOptions, error) {
	langOpt, err := googlesql.NewLanguageOptions()
	if err != nil {
		return nil, err
	}
	if err := langOpt.SetNameResolutionMode(googlesql.NameResolutionModeNameResolutionDefault); err != nil {
		return nil, err
	}
	if err := langOpt.SetProductMode(googlesql.ProductModeProductExternal); err != nil {
		return nil, err
	}
	if err := langOpt.EnableMaximumLanguageFeatures(); err != nil {
		return nil, err
	}
	if err := langOpt.EnableLanguageFeature(googlesql.LanguageFeatureFeatureV13AllowDashesInTableName); err != nil {
		return nil, err
	}
	if err := langOpt.EnableLanguageFeature(googlesql.LanguageFeatureFeatureV13Qualify); err != nil {
		return nil, err
	}
	if err := langOpt.EnableLanguageFeature(googlesql.LanguageFeatureFeatureV13ScriptLabel); err != nil {
		return nil, err
	}
	if err := langOpt.EnableLanguageFeature(googlesql.LanguageFeatureFeatureAnalyticFunctions); err != nil {
		return nil, err
	}
	if err := langOpt.SetSupportsAllStatementKinds(); err != nil {
		return nil, err
	}
	if err := langOpt.EnableAllReservableKeywords(true); err != nil {
		return nil, err
	}
	if err := langOpt.EnableReservableKeyword("QUALIFY", true); err != nil {
		return nil, err
	}
	return langOpt, nil
}

func (a *Analyzer) newAnalyzerOptions(langOpt *googlesql.LanguageOptions) (*googlesql.AnalyzerOptions, error) {
	opts, err := googlesql.NewAnalyzerOptions2()
	if err != nil {
		return nil, err
	}
	if err := opts.SetLanguage(langOpt); err != nil {
		return nil, err
	}
	if err := opts.SetAllowUndeclaredParameters(true); err != nil {
		return nil, err
	}
	if err := opts.SetErrorMessageMode(googlesql.ErrorMessageModeErrorMessageOneLine); err != nil {
		return nil, err
	}
	if err := opts.SetParseLocationRecordType(googlesql.ParseLocationRecordTypeParseLocationRecordCodeSearch); err != nil {
		return nil, err
	}
	return opts, nil
}

func (a *Analyzer) AnalyzeStatement(rawText string, stmt googlesql.ASTStatementNode, catalog *Catalog) (*googlesql.AnalyzerOutput, error) {
	langOpt := catalog.langOpts
	opts, err := a.newAnalyzerOptions(langOpt)
	if err != nil {
		return nil, err
	}
	return googlesql.AnalyzeStatementFromParserAST(stmt, opts, rawText, catalog.CatalogNode(), catalog.TypeFactory())
}

func (a *Analyzer) parseScript(src string) (*googlesql.ASTScript, error) {
	// Create a fresh LanguageOptions for parsing to avoid WASM object aliasing
	// issues when the same pointer is used for both parser and analyzer options.
	langOpt, err := newLanguageOptions()
	if err != nil {
		return nil, err
	}
	parserOpts, err := googlesql.NewParserOptions()
	if err != nil {
		return nil, err
	}
	if err := parserOpts.SetLanguageOptions(langOpt); err != nil {
		return nil, err
	}
	errMsgOpts := &googlesql.ErrorMessageOptions{
		Mode:      googlesql.ErrorMessageModeErrorMessageOneLine,
		Stability: googlesql.ErrorMessageStabilityProduction,
	}
	out, err := googlesql.ParseScript(src, parserOpts, errMsgOpts)
	if err != nil {
		return nil, err
	}
	return out.Script()
}

func (a *Analyzer) ParseFile(uri lsp.DocumentURI, src string) ParsedFile {
	fixedSrc, errs, fixOffsets := fixDot(src)

	var node *googlesql.ASTScript
	var rnode []*googlesql.AnalyzerOutput
	for _retry := 0; _retry < 10; _retry++ {
		rnode = make([]*googlesql.AnalyzerOutput, 0)
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
				if len(fo) == 0 {
					// No keyword-based fix worked; try inserting SELECT 1 (handles WITH-clause-only SQL).
					fixedSrc, pErr, fo = fixOnlyWithClauseSyntaxError(fixedSrc, pErr)
				}
			}
			if strings.Contains(pErr.Msg, `Syntax error: Expected "(" or "," or keyword SELECT but got end of script`) {
				fixedSrc, pErr, fo = fixOnlyWithClauseSyntaxError(fixedSrc, pErr)
			}
			errs = append(errs, pErr)
			if len(fo) > 0 {
				fixOffsets = append(fixOffsets, fo...)
				continue
			}
		}

		if node == nil {
			break
		}

		catalog := a.catalog.Clone()

		// Pre-load tables referenced in the SQL from BigQuery
		catalog.PreloadTablesFromAST(node)

		stmts := collectStatements(node)
		declarationMap := make(map[string]string)

		for _, s := range stmts {
			kind, _ := s.NodeKind()
			if kind == googlesql.ASTNodeKindAstVariableDeclaration {
				declNode := s.(*googlesql.ASTVariableDeclaration)
				dummyValue, err := getDummyValueForDeclarationNode(declNode)
				if err != nil {
					a.logger.Debug("failed to get default value for declaration", err)
				}
				varList, _ := declNode.VariableList()
				if varList != nil {
					n, _ := varList.NumChildren()
					for i := int32(0); i < n; i++ {
						ident, err := varList.IdentifierList(i)
						if err != nil {
							continue
						}
						name, _ := ident.GetAsString()
						declarationMap[name] = dummyValue
					}
				}
				continue
			}

			if kind == googlesql.ASTNodeKindAstCreateFunctionStatement {
				fnNode := s.(*googlesql.ASTCreateFunctionStatement)
				newFunc, err := a.createFunctionTypes(fnNode, fixedSrc, catalog)
				if err != nil {
					errs = append(errs, *err)
					continue
				}
				if newFunc != nil {
					decl, _ := fnNode.FunctionDeclaration()
					if decl != nil {
						pathExpr, _ := decl.Name()
						if pathExpr != nil {
							names, _ := pathExpr.ToIdentifierVector()
							name := strings.Join(names, "")
							catalog.AddFunctionWithName(name, newFunc)
						}
					}
				}
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
				} else if _, ok := SearchAstNode[*googlesql.ASTSelectList](node, errTermOffset); ok {
					fixedSrc, fo = fixErrorToLiteral(fixedSrc, pErr)
				} else if _, ok := SearchAstNode[*googlesql.ASTWhereClause](node, errTermOffset); ok {
					fixedSrc, fo = fixErrorForWhereStatement(fixedSrc, s, pErr)
				} else if _, ok := SearchAstNode[*googlesql.ASTGroupBy](node, errTermOffset); ok {
					fixedSrc, fo = fixErrorToLiteral(fixedSrc, pErr)
				} else if _, ok := SearchAstNode[*googlesql.ASTOrderBy](node, errTermOffset); ok {
					fixedSrc, fo = fixErrorToLiteral(fixedSrc, pErr)
				} else if _, ok := SearchAstNode[*googlesql.ASTOnClause](node, errTermOffset); ok {
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

	parser := ts.NewParser()
	defer parser.Close()
	parser.SetLanguage(ts.NewLanguage(tssql.Language()))

	tree := parser.Parse([]byte(fixedSrc), nil)

	return ParsedFile{
		URI:        uri,
		Src:        src,
		Node:       node,
		RNode:      rnode,
		TsTree:     tree,
		FixOffsets: fixOffsets,
		Errors:     errs,
	}
}

// collectStatements walks the ASTScript and collects all statement nodes.
func collectStatements(node *googlesql.ASTScript) []googlesql.ASTStatementNode {
	stmts := make([]googlesql.ASTStatementNode, 0)
	Walk(node, func(n googlesql.ASTNode) error { //nolint
		if n == nil {
			return nil
		}
		isStmt, _ := n.IsStatement()
		if isStmt {
			if sn, ok := n.(googlesql.ASTStatementNode); ok {
				stmts = append(stmts, sn)
			}
		}
		return nil
	})
	return stmts
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

func (a *Analyzer) createFunctionTypes(node *googlesql.ASTCreateFunctionStatement, sourceFile string, catalog *Catalog) (*googlesql.Function, *Error) {
	decl, err := node.FunctionDeclaration()
	if err != nil || decl == nil {
		return nil, nil
	}
	params, err := decl.Parameters()
	if err != nil || params == nil {
		return nil, nil
	}

	argTypes := []*googlesql.FunctionArgumentType{}
	numParams, _ := params.NumChildren()
	for i := int32(0); i < numParams; i++ {
		param, err := params.ParameterEntries(i)
		if err != nil {
			continue
		}
		typeNode, err := param.Type()
		if err != nil {
			continue
		}
		typ, err := getTypeFromTypeNode(typeNode, catalog.TypeFactory())
		if err != nil {
			a.logger.Debug("failed to get type from parameter ", err)
			return nil, nil
		}
		opts, err := googlesql.NewFunctionArgumentTypeOptions()
		if err != nil {
			return nil, nil
		}
		arg, err := googlesql.NewFunctionArgumentType(typ, opts, 1)
		if err != nil {
			return nil, nil
		}
		argTypes = append(argTypes, arg)
	}

	returnTypeNode, err := node.ReturnType()
	if err != nil || returnTypeNode == nil {
		errResult := Error{
			Msg:      "Currently, bqls does not support function without return type.",
			Severity: lsp.Warning,
		}
		loc, _ := node.GetParseLocationRange()
		if loc != nil {
			start := parseLocStart(loc)
			end := parseLocEnd(loc)
			if start >= 0 {
				errResult.Position = helper.IndexToPosition(sourceFile, start)
				if end > start {
					errResult.TermLength = end - start
				}
			}
		}
		return nil, &errResult
	}

	typ, err := getTypeFromTypeNode(returnTypeNode, catalog.TypeFactory())
	if err != nil {
		a.logger.Debug("failed to get type from return type ", err)
		return nil, nil
	}

	retOpts, err := googlesql.NewFunctionArgumentTypeOptions()
	if err != nil {
		return nil, nil
	}
	retType, err := googlesql.NewFunctionArgumentType(typ, retOpts, 1)
	if err != nil {
		return nil, nil
	}

	sig, err := googlesql.NewFunctionSignature3(retType, argTypes, 0)
	if err != nil {
		return nil, nil
	}

	pathExpr, _ := decl.Name()
	name := ""
	if pathExpr != nil {
		names, _ := pathExpr.ToIdentifierVector()
		name = strings.Join(names, "")
	}

	newFunc, err := googlesql.NewFunction2([]string{name}, "", googlesql.FunctionEnums_ModeScalar, []*googlesql.FunctionSignature{sig})
	if err != nil {
		return nil, nil
	}
	return newFunc, nil
}

func getDummyValueForDeclarationNode(node *googlesql.ASTVariableDeclaration) (string, error) {
	typeNode, err := node.Type()
	if err != nil {
		return "", err
	}
	if typeNode == nil {
		// DECLARE x DEFAULT expr; - no explicit type
		defaultVal, _ := node.DefaultValue()
		if defaultVal != nil {
			return getDummyValueForDefaultValueNode(defaultVal)
		}
		return "", fmt.Errorf("don't support declaration without type or default")
	}

	switch typeNode.(type) {
	case *googlesql.ASTArrayType:
		return "[]", nil
	case *googlesql.ASTSimpleType:
		stn := typeNode.(*googlesql.ASTSimpleType)
		pathExpr, _ := stn.TypeName()
		if pathExpr != nil {
			names, _ := pathExpr.ToIdentifierVector()
			if len(names) > 0 {
				val, err := getDummyValueForDeclarationIdentifierName(names[0])
				if err == nil {
					return val, nil
				}
			}
		}
		defaultVal, _ := node.DefaultValue()
		if defaultVal != nil {
			return getDummyValueForDefaultValueNode(defaultVal)
		}
		return "", fmt.Errorf("failed to load default value")
	default:
		defaultVal, _ := node.DefaultValue()
		if defaultVal != nil {
			return getDummyValueForDefaultValueNode(defaultVal)
		}
		return "", fmt.Errorf("not implemented: %T", typeNode)
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

func getDummyValueForDefaultValueNode(node googlesql.ASTExpressionNode) (string, error) {
	switch node.(type) {
	case *googlesql.ASTNullLiteral:
		return "NULL", nil
	case *googlesql.ASTBooleanLiteral:
		return "TRUE", nil
	case *googlesql.ASTIntLiteral:
		return "0", nil
	case *googlesql.ASTFloatLiteral:
		return "0.0", nil
	case *googlesql.ASTStringLiteral:
		return "''", nil
	case *googlesql.ASTDateOrTimeLiteral:
		return "DATE('1970-01-01')", nil
	default:
		return "", fmt.Errorf("not implemented: %T", node)
	}
}

func getTypeFromTypeNode(node googlesql.ASTTypeNode, tf *googlesql.TypeFactory) (googlesql.Googlesql_TypeNode, error) {
	if stn, ok := node.(*googlesql.ASTSimpleType); ok {
		pathExpr, err := stn.TypeName()
		if err != nil || pathExpr == nil {
			return nil, fmt.Errorf("failed to get type name")
		}
		names, err := pathExpr.ToIdentifierVector()
		if err != nil || len(names) == 0 {
			return nil, fmt.Errorf("failed to get type name identifiers")
		}
		typeName := strings.Join(names, "")

		switch typeName {
		case "INT64":
			return tf.GetInt64()
		case "FLOAT64":
			return tf.GetDouble()
		case "BOOL":
			return tf.GetBool()
		case "STRING":
			return tf.GetString()
		case "BYTES":
			return tf.GetBytes()
		case "DATE":
			return tf.GetDate()
		case "DATETIME":
			return tf.GetDatetime()
		case "TIME":
			return tf.GetTime()
		case "TIMESTAMP":
			return tf.GetTimestamp()
		case "NUMERIC":
			return tf.GetNumeric()
		case "BIGNUMERIC":
			return tf.GetBignumeric()
		case "GEOGRAPHY":
			return tf.GetGeography()
		case "INTERVAL":
			return tf.GetInterval()
		case "JSON":
			return tf.GetJson()
		default:
			return nil, fmt.Errorf("not implemented: %s", typeName)
		}
	}
	return nil, fmt.Errorf("not implemented: %T", node)
}
