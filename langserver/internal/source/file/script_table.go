package file

import (
	"github.com/goccy/go-googlesql"
)

// registerCreatedTable makes the table that a CREATE TABLE statement creates
// resolvable by the later statements of the script, as BigQuery does for
// temporary tables and tables that do not exist yet.
func (a *Analyzer) registerCreatedTable(output *googlesql.AnalyzerOutput, catalog *Catalog) {
	resolved, err := output.ResolvedStatement()
	if err != nil {
		return
	}
	var stmt *googlesql.ResolvedCreateTableStmtBase
	switch s := resolved.(type) {
	case *googlesql.ResolvedCreateTableStmt:
		stmt = s.ResolvedCreateTableStmtBase
	case *googlesql.ResolvedCreateTableAsSelectStmt:
		stmt = s.ResolvedCreateTableStmtBase
	default:
		return
	}
	if err := a.addCreatedTable(stmt, catalog); err != nil {
		a.logger.Debugf("failed to register created table: %v", err)
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
