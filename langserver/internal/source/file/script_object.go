package file

import (
	"errors"
	"strings"

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

// setErroneousTable records in tables, by the catalog's key, whether the table that a CREATE
// TABLE statement creates has come from a statement with errors or fix-ups, since a later
// clean CREATE OR REPLACE gives the table the user's columns again.
func setErroneousTable(tables map[string]string, catalog *Catalog, stmt googlesql.ASTNode, erroneous bool) {
	create, ok := stmt.(*googlesql.ASTCreateTableStatement)
	if !ok {
		return
	}
	path, err := create.Name()
	if err != nil || path == nil {
		return
	}
	name, ok := joinPathExpression(path)
	if !ok {
		return
	}
	key := catalog.scriptTableKey(name)
	if erroneous {
		tables[key] = name
	} else {
		delete(tables, key)
	}
}

var errTableFound = errors.New("table found")

// readsTable returns the first of tables that stmt reads from, other than through a CTE of
// the same name.
func readsTable(stmt googlesql.ASTNode, tables map[string]string, catalog *Catalog) (string, bool) {
	if len(tables) == 0 {
		return "", false
	}
	ctes := make(map[string]bool)
	Walk(stmt, func(n googlesql.ASTNode) error { //nolint
		if q, ok := n.(*googlesql.ASTAliasedQuery); ok {
			if alias, err := q.Alias(); err == nil && alias != nil {
				if name, err := alias.GetAsString(); err == nil {
					ctes[strings.ToLower(name)] = true
				}
			}
		}
		return nil
	})
	var used string
	Walk(stmt, func(n googlesql.ASTNode) error { //nolint
		table, ok := n.(*googlesql.ASTTablePathExpression)
		if !ok {
			return nil
		}
		path, err := table.PathExpr()
		if err != nil || path == nil {
			return nil
		}
		name, ok := joinPathExpression(path)
		if !ok || ctes[strings.ToLower(name)] {
			return nil
		}
		if created, ok := tables[catalog.scriptTableKey(name)]; ok {
			used = created
			return errTableFound
		}
		return nil
	})
	return used, used != ""
}
