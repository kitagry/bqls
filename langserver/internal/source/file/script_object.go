package file

import (
	"github.com/goccy/go-googlesql"
)

// registerCreatedObject makes the table or the procedure that a CREATE
// statement creates resolvable by the later statements of the script, as
// BigQuery does for temporary tables and objects that do not exist yet.
func (a *Analyzer) registerCreatedObject(output *googlesql.AnalyzerOutput, catalog *Catalog) {
	resolved, err := output.ResolvedStatement()
	if err != nil {
		return
	}
	switch s := resolved.(type) {
	case *googlesql.ResolvedCreateTableStmt:
		err = a.addCreatedTable(s.ResolvedCreateTableStmtBase, catalog)
	case *googlesql.ResolvedCreateTableAsSelectStmt:
		err = a.addCreatedTable(s.ResolvedCreateTableStmtBase, catalog)
	case *googlesql.ResolvedCreateProcedureStmt:
		err = a.addCreatedProcedure(s, catalog)
	default:
		return
	}
	if err != nil {
		a.logger.Debugf("failed to register created object: %v", err)
	}
}

func (a *Analyzer) addCreatedTable(stmt *googlesql.ResolvedCreateTableStmtBase, catalog *Catalog) error {
	namePath, err := stmt.NamePath()
	if err != nil {
		return err
	}
	mode, err := stmt.CreateMode()
	if err != nil {
		return err
	}
	if mode == googlesql.ResolvedCreateStatementEnums_CreateModeCreateIfNotExists && catalog.hasTable(namePath) {
		return nil
	}
	columns, err := stmt.ColumnDefinitionList()
	if err != nil {
		return err
	}
	names := make([]string, len(columns))
	types := make([]googlesql.Googlesql_TypeNode, len(columns))
	for i, column := range columns {
		if names[i], err = column.Name(); err != nil {
			return err
		}
		if types[i], err = column.Type(); err != nil {
			return err
		}
	}
	return catalog.addScriptTable(namePath, names, types)
}

func (a *Analyzer) addCreatedProcedure(stmt *googlesql.ResolvedCreateProcedureStmt, catalog *Catalog) error {
	namePath, err := stmt.NamePath()
	if err != nil {
		return err
	}
	mode, err := stmt.CreateMode()
	if err != nil {
		return err
	}
	if mode == googlesql.ResolvedCreateStatementEnums_CreateModeCreateIfNotExists && catalog.hasProcedure(namePath) {
		return nil
	}
	signature, err := stmt.Signature()
	if err != nil {
		return err
	}
	return catalog.addScriptProcedure(namePath, signature)
}
