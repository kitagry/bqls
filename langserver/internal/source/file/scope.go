package file

import (
	"fmt"

	"github.com/goccy/go-googlesql"
)

// scopeCatalog returns a catalog in which the names that the statements
// enclosing stmt bring into scope resolve, on top of the base catalog: the
// variables of FOR...IN loops and the parameters of CREATE PROCEDURE. Each
// name is a NULL constant of its type, so it is visible only to statements
// inside the enclosing statement's body.
func (a *Analyzer) scopeCatalog(rawText string, stmt googlesql.ASTNode, catalog *Catalog) googlesql.CatalogNode {
	var scopes []googlesql.ASTNode
	for n := stmt; n != nil; {
		parent, err := n.Parent()
		if err != nil || parent == nil {
			break
		}
		switch parent.(type) {
		case *googlesql.ASTForInStatement, *googlesql.ASTCreateProcedureStatement:
			scopes = append(scopes, parent)
		}
		n = parent
	}

	// Outer scopes come first so that an inner loop's query can refer to outer
	// names, and an inner name shadows an outer one of the same name because
	// MultiCatalog resolves names from the front.
	current := catalog.node()
	for i := len(scopes) - 1; i >= 0; i-- {
		var names []string
		var types []googlesql.Googlesql_TypeNode
		var err error
		switch s := scopes[i].(type) {
		case *googlesql.ASTForInStatement:
			names, types, err = a.loopVariable(rawText, s, current, catalog)
		case *googlesql.ASTCreateProcedureStatement:
			names, types, err = a.procedureParameters(rawText, s, current, catalog)
		}
		var scoped googlesql.CatalogNode
		if err == nil {
			scoped, err = withConstants(names, types, current, catalog)
		}
		if err != nil {
			a.logger.Debugf("failed to register names in scope of %T: %v", scopes[i], err)
			continue
		}
		current = scoped
	}
	return current
}

// withConstants layers NULL constants of the given names and types over current.
func withConstants(names []string, types []googlesql.Googlesql_TypeNode, current googlesql.CatalogNode, catalog *Catalog) (googlesql.CatalogNode, error) {
	scope, err := googlesql.NewSimpleCatalog("", catalog.TypeFactory())
	if err != nil {
		return nil, err
	}
	for i, name := range names {
		value, err := googlesql.NewValueNull(types[i])
		if err != nil {
			return nil, err
		}
		constant, err := googlesql.NewSimpleConstantCreate([]string{name}, value)
		if err != nil {
			return nil, err
		}
		if err := scope.AddOwnedConstant(constant); err != nil {
			return nil, err
		}
	}
	return googlesql.NewMultiCatalogCreate("", []googlesql.CatalogNode{scope, current})
}

func (a *Analyzer) loopVariable(rawText string, loop *googlesql.ASTForInStatement, current googlesql.CatalogNode, catalog *Catalog) ([]string, []googlesql.Googlesql_TypeNode, error) {
	variable, err := loop.Variable()
	if err != nil {
		return nil, nil, err
	}
	name, err := variable.GetAsString()
	if err != nil {
		return nil, nil, err
	}
	typ, err := a.loopVariableType(rawText, loop, current, catalog)
	if err != nil {
		return nil, nil, err
	}
	return []string{name}, []googlesql.Googlesql_TypeNode{typ}, nil
}

// procedureParameters resolves the procedure's signature, so that parameters
// of any declarable type, e.g. ARRAY<STRUCT<...>>, keep their type.
func (a *Analyzer) procedureParameters(rawText string, proc *googlesql.ASTCreateProcedureStatement, current googlesql.CatalogNode, catalog *Catalog) ([]string, []googlesql.Googlesql_TypeNode, error) {
	opts, err := a.newAnalyzerOptions(catalog.langOpts, catalog.TypeFactory())
	if err != nil {
		return nil, nil, err
	}
	output, err := googlesql.AnalyzeStatementFromParserAST(proc, opts, rawText, current, catalog.TypeFactory())
	if err != nil {
		return nil, nil, err
	}
	resolved, err := output.ResolvedStatement()
	if err != nil {
		return nil, nil, err
	}
	createStmt, ok := resolved.(*googlesql.ResolvedCreateProcedureStmt)
	if !ok {
		return nil, nil, fmt.Errorf("CREATE PROCEDURE resolved to %T", resolved)
	}
	names, err := createStmt.ArgumentNameList()
	if err != nil {
		return nil, nil, err
	}
	signature, err := createStmt.Signature()
	if err != nil {
		return nil, nil, err
	}
	args, err := signature.Arguments()
	if err != nil {
		return nil, nil, err
	}
	if len(args) != len(names) {
		return nil, nil, fmt.Errorf("procedure has %d argument names for %d arguments", len(names), len(args))
	}
	types := make([]googlesql.Googlesql_TypeNode, len(args))
	for i, arg := range args {
		types[i], err = arg.Type()
		if err != nil {
			return nil, nil, err
		}
	}
	return names, types, nil
}

// loopVariableType analyzes the loop's query alone and returns a STRUCT type
// of its output columns.
func (a *Analyzer) loopVariableType(rawText string, loop *googlesql.ASTForInStatement, current googlesql.CatalogNode, catalog *Catalog) (googlesql.Googlesql_TypeNode, error) {
	query, err := loop.Query()
	if err != nil {
		return nil, err
	}
	masked, ok := maskOutsideNode(rawText, query)
	if !ok {
		return nil, fmt.Errorf("invalid FOR...IN query location")
	}

	opts, err := a.newAnalyzerOptions(catalog.langOpts, catalog.TypeFactory())
	if err != nil {
		return nil, err
	}
	output, err := googlesql.AnalyzeStatement(masked, opts, current, catalog.TypeFactory())
	if err != nil {
		return nil, err
	}
	resolved, err := output.ResolvedStatement()
	if err != nil {
		return nil, err
	}
	queryStmt, ok := resolved.(*googlesql.ResolvedQueryStmt)
	if !ok {
		return nil, fmt.Errorf("FOR...IN query resolved to %T", resolved)
	}
	columns, err := queryStmt.OutputColumnList()
	if err != nil {
		return nil, err
	}
	fields := make([]*googlesql.StructField, len(columns))
	for i, column := range columns {
		name, err := column.Name()
		if err != nil {
			return nil, err
		}
		col, err := column.Column()
		if err != nil {
			return nil, err
		}
		typ, err := col.Type()
		if err != nil {
			return nil, err
		}
		fields[i] = &googlesql.StructField{Name: name, Type_: typ}
	}
	return catalog.TypeFactory().MakeStructType2(fields)
}
