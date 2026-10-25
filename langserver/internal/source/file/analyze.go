package file

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"

	bq "cloud.google.com/go/bigquery"
	googlesql "github.com/goccy/go-googlesql"
	"github.com/kitagry/bqls/langserver/internal/bigquery"
	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/kitagry/bqls/langserver/internal/source/bqparser"
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

// unsupportedPipeFeatures are GoogleSQL pipe features that are not listed in
// BigQuery's pipe syntax reference, so queries using them must be reported as errors.
// https://cloud.google.com/bigquery/docs/reference/standard-sql/pipe-syntax
var unsupportedPipeFeatures = []googlesql.LanguageFeature{
	googlesql.LanguageFeatureFeaturePipeStaticDescribe,
	googlesql.LanguageFeatureFeaturePipeAssert,
	googlesql.LanguageFeatureFeaturePipeLog,
	googlesql.LanguageFeatureFeaturePipeIf,
	googlesql.LanguageFeatureFeaturePipeFork,
	googlesql.LanguageFeatureFeaturePipeTee,
	googlesql.LanguageFeatureFeaturePipeExportData,
	googlesql.LanguageFeatureFeaturePipeCreateTable,
	googlesql.LanguageFeatureFeaturePipeInsert,
	googlesql.LanguageFeatureFeaturePipeRecursiveUnion,
	googlesql.LanguageFeatureFeaturePipeDescribe,
	googlesql.LanguageFeatureFeaturePipeCallInputTable,
	googlesql.LanguageFeatureFeaturePipeAggregateWithDifferentialPrivacy,
}

// unsupportedFeatures are non-pipe GoogleSQL features that BigQuery's GoogleSQL
// reference does not include, so queries using them must be reported as errors.
// https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax
var unsupportedFeatures = []googlesql.LanguageFeature{
	// https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax
	googlesql.LanguageFeatureFeatureLateralJoin,
	googlesql.LanguageFeatureFeatureForUpdate,
	googlesql.LanguageFeatureFeatureWithRecursiveDepthModifier,
	googlesql.LanguageFeatureFeatureOrderByCollate,
	googlesql.LanguageFeatureFeatureStratifiedReservoirTablesample,
	googlesql.LanguageFeatureFeatureMultiwayUnnest,
	googlesql.LanguageFeatureFeatureUnnestAndFlattenArrays,
	googlesql.LanguageFeatureFeatureMultiGroupingSets,
	// https://cloud.google.com/bigquery/docs/table-sampling#limitations
	googlesql.LanguageFeatureFeatureTablesampleFromTableValuedFunctions,

	// https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax
	googlesql.LanguageFeatureFeatureDmlReturning,
	googlesql.LanguageFeatureFeatureInsertOnConflictClause,
	googlesql.LanguageFeatureFeatureArrayElementsWithSet,
	googlesql.LanguageFeatureFeatureJsonSubfieldsWithSet,
	googlesql.LanguageFeatureFeatureNestedUpdateDeleteWithOffset,
	googlesql.LanguageFeatureFeatureCorrelatedRefsInNestedDml,

	// https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions
	googlesql.LanguageFeatureFeatureInlineLambdaArgument,
	googlesql.LanguageFeatureFeatureSafeFunctionCallWithLambdaArgs,
	// https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#create_function_statement
	googlesql.LanguageFeatureFeatureFunctionArgumentsWithDefaults,
	// https://cloud.google.com/bigquery/docs/reference/standard-sql/collation-concepts#limitations
	googlesql.LanguageFeatureFeatureTypeAnnotationsOnSqlFunctionArguments,

	// https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types
	googlesql.LanguageFeatureFeatureUuidType,
	googlesql.LanguageFeatureFeatureTokenizedSearch,
	googlesql.LanguageFeatureFeatureArrayEquality,
	googlesql.LanguageFeatureFeatureArrayOrdering,
	googlesql.LanguageFeatureFeatureJsonTypeComparison,
	googlesql.LanguageFeatureFeatureGroupByGraphPath,

	// BigQuery has no PROTO type.
	googlesql.LanguageFeatureFeatureReplaceFields,
	googlesql.LanguageFeatureFeatureReplaceFieldsAllowMultiOneof,
	googlesql.LanguageFeatureFeatureFilterFields,
	googlesql.LanguageFeatureFeatureBracedProtoConstructors,
	googlesql.LanguageFeatureFeatureExtractFromProto,
	googlesql.LanguageFeatureFeatureExtractOneofCase,
	googlesql.LanguageFeatureFeatureProtoMaps,
	googlesql.LanguageFeatureFeatureProtoExtensionsWithNew,
	googlesql.LanguageFeatureFeatureProtoExtensionsWithSet,
	googlesql.LanguageFeatureFeatureProtoDefaultIfNull,
	googlesql.LanguageFeatureFeatureEnumValueDescriptorProto,
	googlesql.LanguageFeatureFeatureFromAndToProtoInterval,
	googlesql.LanguageFeatureFeatureIgnoreProto3UseDefaults,

	// https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language
	googlesql.LanguageFeatureFeatureCheckConstraint,
	// The legacy syntax makes GRANT TO mandatory in CREATE ROW ACCESS POLICY, but it is optional in BigQuery.
	googlesql.LanguageFeatureFeatureAllowLegacyRowAccessPolicySyntax,
	googlesql.LanguageFeatureFeatureExperimentalModules,

	// https://cloud.google.com/bigquery/docs/reference/standard-sql/graph-patterns
	googlesql.LanguageFeatureFeatureSqlGraphPathSearchPrefixPathCount,
	// https://cloud.google.com/bigquery/docs/reference/standard-sql/graph-query-statements
	googlesql.LanguageFeatureFeatureSqlGraphSetOperationPropagationMode,
	// https://cloud.google.com/bigquery/docs/reference/standard-sql/graph-schema-statements
	googlesql.LanguageFeatureFeatureSqlGraphDynamicLabelPropertiesInDdl,
	googlesql.LanguageFeatureFeatureSqlGraphDynamicLabelExtensionInDdl,
	googlesql.LanguageFeatureFeatureSqlGraphDynamicElementType,
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
	for _, f := range slices.Concat(unsupportedPipeFeatures, unsupportedFeatures) {
		if err := langOpt.DisableLanguageFeature(f); err != nil {
			return nil, err
		}
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

func (a *Analyzer) newAnalyzerOptions(langOpt *googlesql.LanguageOptions, tf *googlesql.TypeFactory) (*googlesql.AnalyzerOptions, error) {
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
	if err := addBigQuerySystemVariables(opts, tf); err != nil {
		return nil, err
	}
	return opts, nil
}

func (a *Analyzer) AnalyzeStatement(rawText string, stmt googlesql.ASTStatementNode, catalog *Catalog) (*googlesql.AnalyzerOutput, error) {
	langOpt := catalog.langOpts
	opts, err := a.newAnalyzerOptions(langOpt, catalog.TypeFactory())
	if err != nil {
		return nil, err
	}
	// GoogleSQL defaults unconstrained parameters to INT64 and checks the
	// snapshot expression before inferring its required TIMESTAMP type.
	// Supply that constraint for bare named parameters, without changing SQL
	// or suppressing errors for literals and other expressions.
	parameters := make(map[string]bool)
	if err := Walk(stmt, func(node googlesql.ASTNode) error {
		snapshot, ok := node.(*googlesql.ASTForSystemTime)
		if !ok {
			return nil
		}
		expr, err := snapshot.Expression()
		if err != nil {
			return err
		}
		parameter, ok := expr.(*googlesql.ASTParameterExpr)
		if !ok {
			return nil
		}
		identifier, err := parameter.Name()
		if err != nil || identifier == nil {
			return err
		}
		name, err := identifier.GetAsString()
		if err != nil {
			return err
		}
		// Match GoogleSQL's ASCII-only parameter name normalization. Quoted
		// non-ASCII names differing in case remain distinct parameters.
		name = strings.Map(func(r rune) rune {
			if r >= 'A' && r <= 'Z' {
				return r + ('a' - 'A')
			}
			return r
		}, name)
		if parameters[name] {
			return nil
		}
		timestamp, err := catalog.TypeFactory().GetTimestamp()
		if err != nil {
			return err
		}
		if err := opts.AddQueryParameter(name, timestamp); err != nil {
			return err
		}
		parameters[name] = true
		return nil
	}); err != nil {
		return nil, err
	}
	return googlesql.AnalyzeStatementFromParserAST(stmt, opts, rawText, a.scopeCatalog(rawText, stmt, catalog), catalog.TypeFactory())
}

// analyzeScriptStatement analyzes the expressions and the query of a script
// control statement or a SET statement, which the resolver cannot analyze as
// a statement. The statements in its body are analyzed on their own.
func (a *Analyzer) analyzeScriptStatement(rawText string, stmt googlesql.ASTStatementNode, catalog *Catalog) error {
	exprs, err := scriptStatementExpressions(stmt)
	if err != nil {
		return err
	}
	var query googlesql.ASTNode
	if loop, ok := stmt.(*googlesql.ASTForInStatement); ok {
		q, err := loop.Query()
		if err != nil {
			return err
		}
		if q != nil {
			query = q
		}
	}
	if len(exprs) == 0 && query == nil {
		return nil
	}

	opts, err := a.newAnalyzerOptions(catalog.langOpts, catalog.TypeFactory())
	if err != nil {
		return err
	}
	// The scope of stmt excludes the names stmt itself brings into scope, so a
	// FOR...IN query cannot refer to its own loop variable.
	scope := a.scopeCatalog(rawText, stmt, catalog)
	for _, e := range exprs {
		masked, ok := maskOutsideNode(rawText, e.expr)
		if !ok {
			continue
		}
		out, err := googlesql.AnalyzeExpression(masked, opts, scope, catalog.TypeFactory())
		if err != nil {
			return err
		}
		if !e.condition {
			continue
		}
		coercible, err := isCoercibleToBool(out)
		if err != nil {
			return err
		}
		if !coercible {
			loc, _ := e.expr.GetParseLocationRange()
			start, end := parseLocStart(loc), parseLocEnd(loc)
			return fmt.Errorf("Cannot coerce expression %s to type BOOL %s", rawText[start:end], zetaSQLLocation(rawText, start))
		}
	}
	if caseStmt, ok := stmt.(*googlesql.ASTCaseStatement); ok {
		if err := a.checkCaseStatementValues(rawText, caseStmt, exprs, opts, scope, catalog); err != nil {
			return err
		}
	}
	if query != nil {
		masked, ok := maskOutsideNode(rawText, query)
		if !ok {
			return nil
		}
		if _, err := googlesql.AnalyzeStatement(masked, opts, scope, catalog.TypeFactory()); err != nil {
			return err
		}
	}
	return nil
}

// isCoercibleToBool reports whether the analyzed expression can be used where
// BigQuery requires BOOL: a BOOL expression or an untyped NULL.
func isCoercibleToBool(out *googlesql.AnalyzerOutput) (bool, error) {
	expr, err := out.ResolvedExpr()
	if err != nil || expr == nil {
		return true, err
	}
	typ, err := expr.Type()
	if err != nil || typ == nil {
		return true, err
	}
	isBool, err := typ.IsBool()
	if err != nil || isBool {
		return true, err
	}
	literal, ok := expr.(*googlesql.ResolvedLiteral)
	if !ok {
		return false, nil
	}
	explicit, err := literal.HasExplicitType()
	if err != nil || explicit {
		return false, err
	}
	value, err := literal.Value()
	if err != nil || value == nil {
		return false, err
	}
	return value.IsNull()
}

// checkCaseStatementValues reports an error if the WHEN values of a CASE
// statement with an operand cannot be compared with it. exprs are the
// expressions of stmt, which have already been analyzed on their own.
func (a *Analyzer) checkCaseStatementValues(rawText string, stmt *googlesql.ASTCaseStatement, exprs []scriptExpression, opts *googlesql.AnalyzerOptions, scope googlesql.CatalogNode, catalog *Catalog) error {
	operand, err := stmt.Expression()
	if err != nil || operand == nil || len(exprs) < 2 {
		return err
	}
	// Let GoogleSQL find a common supertype by resolving the equivalent CASE
	// expression. Each operand is followed by a line break so that a trailing
	// line comment cannot swallow the parenthesis.
	var b strings.Builder
	for i, e := range exprs {
		loc, err := e.expr.GetParseLocationRange()
		if err != nil {
			return err
		}
		start, end := parseLocStart(loc), parseLocEnd(loc)
		if start < 0 || end < start || end > len(rawText) {
			return nil
		}
		text := rawText[start:end]
		if i == 0 {
			fmt.Fprintf(&b, "CASE (%s\n)", text)
			continue
		}
		fmt.Fprintf(&b, " WHEN (%s\n) THEN NULL", text)
	}
	b.WriteString(" END")
	if _, err := googlesql.AnalyzeExpression(b.String(), opts, scope, catalog.TypeFactory()); err != nil {
		loc, _ := stmt.GetParseLocationRange()
		return fmt.Errorf("CASE...WHEN statement values have incompatible types %s", zetaSQLLocation(rawText, parseLocStart(loc)))
	}
	return nil
}

// scriptExpression is an expression that a script statement evaluates.
type scriptExpression struct {
	expr googlesql.ASTExpressionNode
	// condition reports whether BigQuery requires expr to be BOOL.
	condition bool
}

// scriptStatementExpressions returns the expressions that stmt itself
// evaluates, in source order, excluding those in the statements of its body.
func scriptStatementExpressions(stmt googlesql.ASTStatementNode) ([]scriptExpression, error) {
	var exprs []scriptExpression
	adder := func(condition bool) func(googlesql.ASTExpressionNode, error) error {
		return func(expr googlesql.ASTExpressionNode, err error) error {
			if err != nil {
				return err
			}
			if expr != nil {
				exprs = append(exprs, scriptExpression{expr: expr, condition: condition})
			}
			return nil
		}
	}
	add, addCondition := adder(false), adder(true)
	switch n := stmt.(type) {
	case *googlesql.ASTSingleAssignment:
		return exprs, add(n.Expression())
	case *googlesql.ASTAssignmentFromStruct:
		return exprs, add(n.StructExpression())
	case *googlesql.ASTParameterAssignment:
		return exprs, add(n.Expression())
	case *googlesql.ASTIfStatement:
		if err := addCondition(n.Condition()); err != nil {
			return nil, err
		}
		clauses, err := n.ElseifClauses()
		if err != nil || clauses == nil {
			return exprs, err
		}
		num, err := clauses.NumChildren()
		if err != nil {
			return nil, err
		}
		for i := int32(0); i < num; i++ {
			clause, err := clauses.ElseifClauses(i)
			if err != nil {
				return nil, err
			}
			if err := addCondition(clause.Condition()); err != nil {
				return nil, err
			}
		}
		return exprs, nil
	case *googlesql.ASTWhileStatement:
		// WHILE without a condition is LOOP.
		return exprs, addCondition(n.Condition())
	case *googlesql.ASTRepeatStatement:
		until, err := n.UntilClause()
		if err != nil || until == nil {
			return exprs, err
		}
		return exprs, addCondition(until.Condition())
	case *googlesql.ASTCaseStatement:
		operand, err := n.Expression()
		if err := add(operand, err); err != nil {
			return nil, err
		}
		// Without an operand, CASE...WHEN takes conditions like IF.
		addWhen := addCondition
		if operand != nil {
			addWhen = add
		}
		clauses, err := n.WhenThenClauses()
		if err != nil || clauses == nil {
			return exprs, err
		}
		num, err := clauses.NumChildren()
		if err != nil {
			return nil, err
		}
		for i := int32(0); i < num; i++ {
			clause, err := clauses.WhenThenClauses(i)
			if err != nil {
				return nil, err
			}
			if err := addWhen(clause.Condition()); err != nil {
				return nil, err
			}
		}
		return exprs, nil
	case *googlesql.ASTRaiseStatement:
		return exprs, add(n.Message())
	}
	return nil, nil
}

// maskOutsideNode masks rawText outside node, reporting false if node's
// location does not lie within rawText.
func maskOutsideNode(rawText string, node googlesql.ASTNode) (string, bool) {
	loc, err := node.GetParseLocationRange()
	if err != nil {
		return "", false
	}
	start, end := parseLocStart(loc), parseLocEnd(loc)
	if start < 0 || end < start || end > len(rawText) {
		return "", false
	}
	return maskOutside(rawText, start, end), true
}

// maskOutside blanks everything except rawText[start:end]. Each character
// becomes one space and line breaks and tabs are kept, so the masked text has
// the same code-point/tab column layout as rawText in GoogleSQL's eyes and
// reported columns can be converted to byte offsets against rawText.
func maskOutside(rawText string, start, end int) string {
	var b strings.Builder
	b.Grow(len(rawText))
	for i, r := range rawText {
		if (i >= start && i < end) || r == '\n' || r == '\r' || r == '\t' {
			b.WriteRune(r)
			continue
		}
		b.WriteByte(' ')
	}
	return b.String()
}

func (a *Analyzer) parseScript(src string) (*googlesql.ASTScript, error) {
	return ParseScript(src)
}

// ParseScript parses src as a script without analyzing it, so that the AST
// keeps the text as written (declared variables are substituted in ParsedFile.Node).
func ParseScript(src string) (*googlesql.ASTScript, error) {
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
	// Errors are reported against src, but the analysis sees fixedSrc. Each error is
	// converted when it is recorded, because later fix-ups move the text it points at.
	edits := editsOf(fixOffsets)

	var node *googlesql.ASTScript
	var statements []StatementResult
	var catalog *Catalog
	// Statements are re-analyzed on every retry, so only errors whose fix-up was applied
	// (they no longer reproduce in the fixed source) survive a pass; the rest come from the final pass.
	var fixedErrs, passErrs []statementError
	// Every pass analyzes against a fresh catalog, so lookups are shared across passes
	// to fetch each table at most once.
	client := newMemoClient(a.bqClient)
	maxRetries := minRetries
	budgeted := false
	for retry := 0; retry < maxRetries; retry++ {
		// The last pass applies no fix-up, so that it analyzes every statement instead
		// of stopping at the first one it would fix.
		lastPass := retry == maxRetries-1
		statements = make([]StatementResult, 0)
		var err error
		var fo []FixOffset
		passSrc := fixedSrc
		node, err = a.parseScript(fixedSrc)
		if err != nil {
			pErr := parseZetaSQLError(fixedSrc, err)
			if strings.Contains(pErr.Msg, "SELECT list must not be empty") {
				fixedSrc, pErr, fo = fixSelectListMustNotBeEmptyStatement(fixedSrc, pErr)
			}
			if strings.Contains(pErr.Msg, "Unexpected end of script") {
				fixedSrc, pErr, fo = fixUnexpectedEndOfScript(fixedSrc, pErr)
				if len(fo) == 0 {
					fixedSrc, fo = fixIncompletePipeOperator(fixedSrc, pErr)
				}
				if len(fo) == 0 {
					// No keyword-based fix worked; try inserting SELECT 1 (handles WITH-clause-only SQL).
					fixedSrc, pErr, fo = fixOnlyWithClauseSyntaxError(fixedSrc, pErr)
				}
			} else if strings.HasSuffix(pErr.Msg, "but got end of script") {
				// e.g. ORDER without BY, or a set operation without its query.
				fixedSrc, pErr, fo = fixUnexpectedEndOfScript(fixedSrc, pErr)
			}
			if len(fo) == 0 && strings.Contains(pErr.Msg, `Syntax error: Expected "(" or "," or keyword SELECT but got end of script`) {
				fixedSrc, pErr, fo = fixOnlyWithClauseSyntaxError(fixedSrc, pErr)
			}
			if len(fo) == 0 {
				fixedSrc, fo = fixIncompletePipeOperator(fixedSrc, pErr)
			}
			if len(fo) == 0 && strings.Contains(pErr.Msg, "A pipe operator cannot follow the WITH clause") {
				fixedSrc, fo = fixPipeOperatorAfterWithClause(fixedSrc, pErr)
			}
			srcErr := errorToSrc(src, passSrc, edits, pErr)
			// An error where one is already reported is left by a fix-up that deleted
			// what the user has not finished, such as "(SELECT 1 UNION ALL" becoming
			// "(SELECT 1" without its closing parenthesis.
			reported := slices.ContainsFunc(errs, func(e Error) bool { return e.Position == srcErr.Position })
			if !reported && !insideInsertedText(passSrc, edits, positionToByteOffset(passSrc, pErr.Position)) {
				errs = append(errs, srcErr)
			}
			if len(fo) > 0 {
				fixOffsets = append(fixOffsets, fo...)
				edits = append(edits, editsOf(fo)...)
				continue
			}
		}

		if node == nil {
			break
		}
		// Reset only once this pass re-analyzes statements; a pass that stops at a parse error
		// must not drop the previous pass's errors.
		passErrs = nil

		catalog = a.catalog.cloneWithClient(client)

		// Pre-load tables referenced in the SQL from BigQuery
		if err := catalog.PreloadTablesFromAST(node); err != nil {
			// Debug, because this runs on every edit and the cause also
			// appears in the "Table not found" diagnostic.
			a.logger.Debugf("failed to load tables: %v", err)
		}
		if err := catalog.PreloadProceduresFromAST(node); err != nil {
			a.logger.Debugf("failed to load procedures: %v", err)
		}

		stmts := collectStatements(node)
		declarationMap := make(map[string]string)
		if !budgeted {
			// A substitution fixes one reference to a declared variable per pass (#467).
			maxRetries = min(minRetries+countDeclaredVariableReferences(node), maxRetriesLimit)
			lastPass = retry == maxRetries-1
			budgeted = true
		}

		for i, s := range stmts {
			kind, _ := s.NodeKind()
			if kind == googlesql.ASTNodeKindAstVariableDeclaration {
				declNode := s.(*googlesql.ASTVariableDeclaration)
				dummyValue, err := a.getDummyValueForDeclarationNode(fixedSrc, declNode, catalog)
				if err != nil {
					a.logger.Debug("failed to get default value for declaration", err)
					continue
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
						declarationMap[strings.ToLower(name)] = dummyValue
					}
				}
				continue
			}

			if kind == googlesql.ASTNodeKindAstCreateFunctionStatement {
				fnNode := s.(*googlesql.ASTCreateFunctionStatement)
				newFunc, err := a.createFunctionTypes(fnNode, fixedSrc, catalog)
				if err != nil {
					srcErr := errorToSrc(src, fixedSrc, edits, *err)
					passErrs = append(passErrs, statementError{index: i, err: srcErr})
					statements = append(statements, StatementResult{Stmt: s, Err: &srcErr})
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

			var output *googlesql.AnalyzerOutput
			var err error
			if isScriptControlStatement(kind) {
				err = a.analyzeScriptStatement(fixedSrc, s, catalog)
			} else {
				output, err = a.AnalyzeStatement(fixedSrc, s, catalog)
			}
			if err == nil {
				if output != nil {
					a.registerCreatedObject(output, catalog)
				}
				statements = append(statements, StatementResult{Stmt: s, Output: output})
				continue
			}

			errSrc := fixedSrc
			pErr := parseZetaSQLError(fixedSrc, err)

			isUnrecognizedNameError := strings.Contains(pErr.Msg, "Unrecognized name: ")
			isNotExistInStructError := strings.Contains(pErr.Msg, "does not exist in STRUCT")
			isNotFoundInsideError := strings.Contains(pErr.Msg, "not found inside")
			isTableNotFoundError := strings.Contains(pErr.Msg, "Table not found: ")
			isProcedureNotFoundError := strings.Contains(pErr.Msg, "Procedure not found: ")
			isEmptyPipeAggregateError := strings.Contains(pErr.Msg, "Pipe AGGREGATE cannot have both an empty aggregate list and an empty GROUP BY")
			isNotBoolClauseError := strings.Contains(pErr.Msg, "clause should return type BOOL")
			isLogicalOperandError := logicalOperandErrorRegex.MatchString(pErr.Msg)
			isNotBoolConditionError := notBoolConditionErrorRegex.MatchString(pErr.Msg)
			isPipeColumnNotFoundError := strings.HasPrefix(pErr.Msg, "Column name in pipe ") && strings.Contains(pErr.Msg, " not found in input table: ")

			// add information to Error
			switch {
			case isUnrecognizedNameError:
				pErr = addInformationToUnrecognizedNameError(fixedSrc, pErr)
			case isNotExistInStructError:
				pErr = addInformationToNotExistInStructError(fixedSrc, s, pErr)
			case isNotFoundInsideError:
				pErr = addInformationToNotFoundInsideTableError(fixedSrc, pErr)
			case isTableNotFoundError:
				ind := strings.Index(pErr.Msg, "Table not found: ")
				table := strings.TrimSpace(pErr.Msg[ind+len("Table not found: "):])
				pErr.TermLength = len(table)
				pErr.IncompleteColumnName = table
				if loadErr := catalog.TableLoadError(table); loadErr != nil {
					pErr.Msg += fmt.Sprintf(" (%v)", loadErr)
				}
			case isProcedureNotFoundError:
				ind := strings.Index(pErr.Msg, "Procedure not found: ")
				procedure := strings.TrimSpace(pErr.Msg[ind+len("Procedure not found: "):])
				if loadErr := catalog.ProcedureLoadError(procedure); loadErr != nil {
					pErr.Msg += fmt.Sprintf(" (%v)", loadErr)
				}
			case isNotBoolClauseError:
				pErr = addInformationToNotBoolClauseError(fixedSrc, s, pErr)
			case isLogicalOperandError:
				pErr = addInformationToLogicalOperandError(fixedSrc, s, pErr)
			case isNotBoolConditionError:
				pErr = addInformationToNotBoolConditionError(pErr)
			case isPipeColumnNotFoundError:
				pErr = addInformationToPipeColumnNotFoundError(fixedSrc, s, pErr)
			}

			// fix src
			srcBeforeFix := fixedSrc
			skipError := false
			if isUnrecognizedNameError || isNotExistInStructError || isNotFoundInsideError {
				errTermOffset := positionToByteOffset(fixedSrc, pErr.Position)
				if defaultVal, ok := declarationMap[strings.ToLower(pErr.IncompleteColumnName)]; isUnrecognizedNameError && ok {
					fixedSrc, fo = fixDeclarationError(fixedSrc, pErr, defaultVal)
					if len(fo) > 0 {
						skipError = true
					}
				} else if _, ok := SearchAstNode[*googlesql.ASTSelectList](node, errTermOffset); ok {
					fixedSrc, fo = fixErrorToLiteral(fixedSrc, pErr)
				} else if _, ok := SearchAstNode[*googlesql.ASTWhereClause](node, errTermOffset); ok {
					fixedSrc, fo = fixErrorForWhereStatement(fixedSrc, s, pErr)
				} else if _, ok := SearchAstNode[*googlesql.ASTGroupBy](node, errTermOffset); ok {
					if _, ok := SearchAstNode[*googlesql.ASTPipeAggregate](node, errTermOffset); ok {
						// Pipe AGGREGATE rejects both GROUP BY ordinals and literals.
						fixedSrc, fo = fixErrorToExpression(fixedSrc, pErr, "1+1")
					} else {
						fixedSrc, fo = fixErrorToLiteral(fixedSrc, pErr)
					}
				} else if _, ok := SearchAstNode[*googlesql.ASTOrderBy](node, errTermOffset); ok {
					fixedSrc, fo = fixErrorToLiteral(fixedSrc, pErr)
				} else if _, ok := SearchAstNode[*googlesql.ASTOnClause](node, errTermOffset); ok {
					fixedSrc, fo = fixErrorForWhereStatement(fixedSrc, s, pErr)
				}
			}

			if isPipeColumnNotFoundError {
				fixedSrc, fo = fixPipeColumnNotFound(fixedSrc, s, pErr)
			}

			if isEmptyPipeAggregateError {
				fixedSrc, fo = fixIncompletePipeOperator(fixedSrc, pErr)
			}

			// A partially typed name that already matches a non-BOOL column is not "Unrecognized".
			if (isNotBoolClauseError || isLogicalOperandError) && pErr.IncompleteColumnName != "" {
				fixedSrc, fo = fixNotBoolClauseError(fixedSrc, pErr)
			}

			if lastPass && len(fo) > 0 {
				fixedSrc, fo = srcBeforeFix, nil
				if skipError {
					// The error is not the user's, but the rest of the statement is left unchecked.
					skipError = false
					pErr = Error{
						Msg:      fmt.Sprintf("bqls could not finish analyzing this statement: reached the limit of %d fix-ups", maxRetries),
						Position: pErr.Position,
						Severity: lsp.Warning,
					}
				}
			}
			srcErr := errorToSrc(src, errSrc, edits, pErr)
			if isNotBoolConditionError {
				srcErr = notBoolConditionErrorInSrc(src, srcErr)
			}
			if insideInsertedText(errSrc, edits, positionToByteOffset(errSrc, pErr.Position)) {
				skipError = true
			}
			if !skipError {
				statements = append(statements, StatementResult{Stmt: s, Err: &srcErr})
			}
			if len(fo) > 0 {
				if !skipError {
					fixedErrs = append(fixedErrs, statementError{index: i, err: srcErr})
				}
				fixOffsets = append(fixOffsets, fo...)
				edits = append(edits, editsOf(fo)...)
				goto retry
			}
			if !skipError {
				passErrs = append(passErrs, statementError{index: i, err: srcErr})
			}
		}
		break
	retry:
	}

	// Ordering by statement keeps errors in source order even though fixed errors were found in earlier passes.
	stmtErrs := append(fixedErrs, passErrs...)
	sort.SliceStable(stmtErrs, func(i, j int) bool { return stmtErrs[i].index < stmtErrs[j].index })
	for _, e := range stmtErrs {
		errs = append(errs, e.err)
	}

	// Assignments are not statement outputs, so they never enter RNode.
	rnode := make([]*googlesql.AnalyzerOutput, 0, len(statements))
	for _, s := range statements {
		if s.Output != nil {
			rnode = append(rnode, s.Output)
		}
	}

	return ParsedFile{
		URI:        uri,
		Src:        src,
		FixedSrc:   fixedSrc,
		Node:       node,
		RNode:      rnode,
		Statements: statements,
		ParseTree:  bqparser.Parse(fixedSrc),
		FixOffsets: fixOffsets,
		edits:      edits,
		Errors:     errs,
		catalog:    catalog,
	}
}

const (
	minRetries = 10
	// maxRetriesLimit bounds the time spent on a file, as every pass re-analyzes every statement.
	maxRetriesLimit = 100
)

// countDeclaredVariableReferences counts the identifiers named after a declared variable,
// including the declarations themselves.
func countDeclaredVariableReferences(node *googlesql.ASTScript) int {
	declared := make(map[string]bool)
	Walk(node, func(n googlesql.ASTNode) error { //nolint
		decl, ok := n.(*googlesql.ASTVariableDeclaration)
		if !ok {
			return nil
		}
		varList, _ := decl.VariableList()
		if varList == nil {
			return nil
		}
		num, _ := varList.NumChildren()
		for i := int32(0); i < num; i++ {
			ident, err := varList.IdentifierList(i)
			if err != nil {
				continue
			}
			name, _ := ident.GetAsString()
			declared[strings.ToLower(name)] = true
		}
		return nil
	})
	if len(declared) == 0 {
		return 0
	}
	count := 0
	Walk(node, func(n googlesql.ASTNode) error { //nolint
		ident, ok := n.(*googlesql.ASTIdentifier)
		if !ok {
			return nil
		}
		name, _ := ident.GetAsString()
		if declared[strings.ToLower(name)] {
			count++
		}
		return nil
	})
	return count
}

func editsOf(fixOffsets []FixOffset) []sourceEdit {
	var edits []sourceEdit
	for _, fo := range fixOffsets {
		edits = append(edits, fo.edits...)
	}
	return edits
}

type statementError struct {
	index int
	err   Error
}

// collectStatements walks the ASTScript and collects all statement nodes.
func collectStatements(node *googlesql.ASTScript) []googlesql.ASTStatementNode {
	stmts := make([]googlesql.ASTStatementNode, 0)
	Walk(node, func(n googlesql.ASTNode) error { //nolint
		if n == nil {
			return nil
		}
		isStmt, _ := n.IsStatement()
		if isStmt && !isNestedDMLStatement(n) {
			if sn, ok := n.(googlesql.ASTStatementNode); ok {
				stmts = append(stmts, sn)
			}
		}
		return nil
	})
	return stmts
}

// isNestedDMLStatement reports whether n is a DML inside UPDATE ... SET (...).
// It targets an array column of the outer table, so it is part of the outer
// statement rather than a statement of its own.
func isNestedDMLStatement(n googlesql.ASTNode) bool {
	parent, err := n.Parent()
	if err != nil {
		return false
	}
	_, ok := parent.(*googlesql.ASTUpdateItem)
	return ok
}

// isScriptControlStatement reports whether the statement is a procedural
// script statement. The GoogleSQL resolver rejects these with "Statement not
// supported", although BigQuery accepts them.
func isScriptControlStatement(kind googlesql.ASTNodeKind) bool {
	if isAssignmentStatement(kind) {
		return true
	}
	switch kind {
	case googlesql.ASTNodeKindAstIfStatement,
		googlesql.ASTNodeKindAstCaseStatement,
		googlesql.ASTNodeKindAstBeginEndBlock,
		googlesql.ASTNodeKindAstWhileStatement,
		googlesql.ASTNodeKindAstRepeatStatement,
		googlesql.ASTNodeKindAstForInStatement,
		googlesql.ASTNodeKindAstBreakStatement,
		googlesql.ASTNodeKindAstContinueStatement,
		googlesql.ASTNodeKindAstReturnStatement,
		googlesql.ASTNodeKindAstRaiseStatement:
		return true
	}
	return false
}

// isAssignmentStatement reports whether the statement is a SET of script
// variables or query parameters. SET @@system_variable is excluded because
// the resolver supports it.
func isAssignmentStatement(kind googlesql.ASTNodeKind) bool {
	switch kind {
	case googlesql.ASTNodeKindAstSingleAssignment,
		googlesql.ASTNodeKindAstAssignmentFromStruct,
		googlesql.ASTNodeKindAstParameterAssignment:
		return true
	}
	return false
}

func (a *Analyzer) GetTableMetadataFromPath(ctx context.Context, path string) (*bq.TableMetadata, error) {
	splitNode := strings.Split(path, ".")

	// validate id
	for _, id := range splitNode {
		if id == "" {
			return nil, fmt.Errorf("invalid path: %s", path)
		}
	}

	ref, err := parseTablePath(splitNode, a.bqClient.GetDefaultProject)
	if err != nil {
		return nil, fmt.Errorf("invalid path: %s", path)
	}
	return fetchTableMetadata(ctx, a.bqClient, ref)
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

// getDummyValueForDeclarationNode returns a typed expression that stands in
// for references to the declared variables.
func (a *Analyzer) getDummyValueForDeclarationNode(src string, node *googlesql.ASTVariableDeclaration, catalog *Catalog) (string, error) {
	typeNode, err := node.Type()
	if err != nil {
		return "", err
	}
	if typeNode != nil {
		typeName, err := declaredTypeName(typeNode)
		if err != nil {
			return "", err
		}
		return getDummyValueForTypeName(typeName), nil
	}

	defaultVal, err := node.DefaultValue()
	if err != nil {
		return "", err
	}
	if defaultVal == nil {
		return "", fmt.Errorf("don't support declaration without type or default")
	}
	loc, err := defaultVal.GetParseLocationRange()
	if err != nil {
		return "", err
	}
	start, end := parseLocStart(loc), parseLocEnd(loc)
	if start < 0 || end < start || end > len(src) {
		return "", fmt.Errorf("invalid default value location")
	}
	typeName, err := a.analyzeExpressionTypeName(maskOutside(src, start, end), catalog)
	if err != nil {
		// The default may reference other declared variables, which cannot be
		// resolved on their own. Substituting the expression itself lets the
		// retry loop replace those references in turn.
		return "(" + src[start:end] + ")", nil
	}
	return getDummyValueForTypeName(typeName), nil
}

func (a *Analyzer) analyzeExpressionTypeName(sql string, catalog *Catalog) (string, error) {
	opts, err := a.newAnalyzerOptions(catalog.langOpts, catalog.TypeFactory())
	if err != nil {
		return "", err
	}
	out, err := googlesql.AnalyzeExpression(sql, opts, catalog.node(), catalog.TypeFactory())
	if err != nil {
		return "", err
	}
	expr, err := out.ResolvedExpr()
	if err != nil {
		return "", err
	}
	if expr == nil {
		return "", fmt.Errorf("failed to get resolved expression")
	}
	typ, err := expr.Type()
	if err != nil {
		return "", err
	}
	if typ == nil {
		return "", fmt.Errorf("failed to get expression type")
	}
	named, ok := typ.(interface {
		TypeName(mode googlesql.ProductMode) (string, error)
	})
	if !ok {
		return "", fmt.Errorf("not implemented: %T", typ)
	}
	return named.TypeName(googlesql.ProductModeProductExternal)
}

// declaredTypeName renders a declared type with BigQuery-only aliases
// normalized, since GoogleSQL rejects them (e.g. CAST(NULL AS INTEGER)).
func declaredTypeName(node googlesql.ASTTypeNode) (string, error) {
	switch n := node.(type) {
	case *googlesql.ASTSimpleType:
		pathExpr, err := n.TypeName()
		if err != nil || pathExpr == nil {
			return "", fmt.Errorf("failed to get type name")
		}
		names, err := pathExpr.ToIdentifierVector()
		if err != nil || len(names) == 0 {
			return "", fmt.Errorf("failed to get type name identifiers")
		}
		return canonicalTypeName(strings.Join(names, ".")), nil
	case *googlesql.ASTArrayType:
		elem, err := n.ElementType()
		if err != nil || elem == nil {
			return "", fmt.Errorf("failed to get array element type")
		}
		elemName, err := declaredTypeName(elem)
		if err != nil {
			return "", err
		}
		return "ARRAY<" + elemName + ">", nil
	case *googlesql.ASTRangeType:
		elem, err := n.ElementType()
		if err != nil || elem == nil {
			return "", fmt.Errorf("failed to get range element type")
		}
		elemName, err := declaredTypeName(elem)
		if err != nil {
			return "", err
		}
		return "RANGE<" + elemName + ">", nil
	case *googlesql.ASTStructType:
		numChildren, err := n.NumChildren()
		if err != nil {
			return "", err
		}
		var fields []string
		for i := int32(0); i < numChildren; i++ {
			field, err := n.StructFields(i)
			if err != nil || field == nil {
				break
			}
			fieldType, err := field.Type()
			if err != nil || fieldType == nil {
				return "", fmt.Errorf("failed to get struct field type")
			}
			fieldTypeName, err := declaredTypeName(fieldType)
			if err != nil {
				return "", err
			}
			ident, _ := field.Name()
			if ident == nil {
				fields = append(fields, fieldTypeName)
				continue
			}
			name, err := ident.GetAsString()
			if err != nil {
				return "", err
			}
			fields = append(fields, "`"+name+"` "+fieldTypeName)
		}
		return "STRUCT<" + strings.Join(fields, ", ") + ">", nil
	default:
		return "", fmt.Errorf("not implemented: %T", node)
	}
}

func canonicalTypeName(name string) string {
	// Type names are case-insensitive in BigQuery.
	name = strings.ToUpper(name)
	switch name {
	case "INT", "SMALLINT", "INTEGER", "BIGINT", "TINYINT", "BYTEINT":
		return "INT64"
	case "BOOLEAN":
		return "BOOL"
	case "DECIMAL":
		return "NUMERIC"
	case "BIGDECIMAL":
		return "BIGNUMERIC"
	default:
		return name
	}
}

func getDummyValueForTypeName(typeName string) string {
	switch typeName {
	case "BOOL":
		return "TRUE"
	case "INT64":
		return "1"
	case "FLOAT64":
		return "1.0"
	case "NUMERIC":
		return "NUMERIC '1'"
	case "BIGNUMERIC":
		return "BIGNUMERIC '1'"
	case "STRING":
		return "''"
	case "BYTES":
		return "b''"
	case "DATE":
		return "DATE('1970-01-01')"
	case "DATETIME":
		return "DATETIME('1970-01-01 00:00:00')"
	case "TIME":
		return "TIME('00:00:00')"
	case "TIMESTAMP":
		return "TIMESTAMP('1970-01-01 00:00:00')"
	case "INTERVAL":
		return "INTERVAL 0 DAY"
	case "JSON":
		return "JSON 'null'"
	case "GEOGRAPHY":
		return "ST_GEOGPOINT(0, 0)"
	}

	// Whitespace inside the dummy breaks the error recovery that extracts the
	// incomplete column name, e.g. for an unknown struct field `s.b`.
	compact := strings.ReplaceAll(typeName, " ", "/**/")
	if strings.HasPrefix(typeName, "ARRAY<") {
		return compact + "[]"
	}
	return "CAST(NULL/**/AS/**/" + compact + ")"
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
