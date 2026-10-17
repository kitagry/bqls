package file

import (
	"fmt"

	"github.com/goccy/go-googlesql"
)

// loopVariableCatalog returns a catalog in which the variables of the
// FOR...IN loops enclosing stmt resolve, on top of the base catalog.
// Each loop variable is a NULL constant typed as a STRUCT of its query's
// output columns, so it is visible only to statements inside the loop body.
func (a *Analyzer) loopVariableCatalog(rawText string, stmt googlesql.ASTNode, catalog *Catalog) googlesql.CatalogNode {
	var loops []*googlesql.ASTForInStatement
	for n := stmt; n != nil; {
		parent, err := n.Parent()
		if err != nil || parent == nil {
			break
		}
		if loop, ok := parent.(*googlesql.ASTForInStatement); ok {
			loops = append(loops, loop)
		}
		n = parent
	}

	// Outer loops come first so that an inner loop's query can refer to outer
	// loop variables, and an inner variable shadows an outer one of the same
	// name because MultiCatalog resolves names from the front.
	var current googlesql.CatalogNode = catalog.CatalogNode()
	for i := len(loops) - 1; i >= 0; i-- {
		scoped, err := a.withLoopVariable(rawText, loops[i], current, catalog)
		if err != nil {
			a.logger.Debugf("failed to register FOR...IN loop variable: %v", err)
			continue
		}
		current = scoped
	}
	return current
}

func (a *Analyzer) withLoopVariable(rawText string, loop *googlesql.ASTForInStatement, current googlesql.CatalogNode, catalog *Catalog) (googlesql.CatalogNode, error) {
	variable, err := loop.Variable()
	if err != nil {
		return nil, err
	}
	name, err := variable.GetAsString()
	if err != nil {
		return nil, err
	}
	typ, err := a.loopVariableType(rawText, loop, current, catalog)
	if err != nil {
		return nil, err
	}
	value, err := googlesql.NewValueNull(typ)
	if err != nil {
		return nil, err
	}
	constant, err := googlesql.NewSimpleConstantCreate([]string{name}, value)
	if err != nil {
		return nil, err
	}
	scope, err := googlesql.NewSimpleCatalog("", catalog.TypeFactory())
	if err != nil {
		return nil, err
	}
	if err := scope.AddOwnedConstant(constant); err != nil {
		return nil, err
	}
	return googlesql.NewMultiCatalogCreate("", []googlesql.CatalogNode{scope, current})
}

// loopVariableType analyzes the loop's query alone and returns a STRUCT type
// of its output columns.
func (a *Analyzer) loopVariableType(rawText string, loop *googlesql.ASTForInStatement, current googlesql.CatalogNode, catalog *Catalog) (googlesql.Googlesql_TypeNode, error) {
	query, err := loop.Query()
	if err != nil {
		return nil, err
	}
	loc, err := query.GetParseLocationRange()
	if err != nil {
		return nil, err
	}
	start, end := parseLocStart(loc), parseLocEnd(loc)
	if start < 0 || end < start || end > len(rawText) {
		return nil, fmt.Errorf("invalid FOR...IN query location: [%d, %d)", start, end)
	}

	opts, err := a.newAnalyzerOptions(catalog.langOpts, catalog.TypeFactory())
	if err != nil {
		return nil, err
	}
	output, err := googlesql.AnalyzeStatement(maskOutside(rawText, start, end), opts, current, catalog.TypeFactory())
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
