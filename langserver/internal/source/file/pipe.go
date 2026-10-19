package file

import (
	"strings"

	googlesql "github.com/goccy/go-googlesql"
)

// PipeColumnIdentifier finds the identifier at termOffset that names a column
// by itself in a pipe operator: a column of SET, DROP or RENAME, or an alias.
// It reports whether the name refers to the input of the operator, as the
// columns of SET, DROP and the old names of RENAME do, or to its output.
func PipeColumnIdentifier(node googlesql.ASTNode, termOffset int) (ident *googlesql.ASTIdentifier, op googlesql.ASTPipeOperatorNode, isInput bool, ok bool) {
	ident, ok = SearchAstNode[*googlesql.ASTIdentifier](node, termOffset)
	if !ok {
		return nil, nil, false, false
	}
	parent, err := ident.Parent()
	if err != nil || parent == nil {
		return nil, nil, false, false
	}
	switch n := parent.(type) {
	case *googlesql.ASTPipeSetItem:
		isInput = true
	case *googlesql.ASTIdentifierList:
		grandParent, err := n.Parent()
		if err != nil {
			return nil, nil, false, false
		}
		if _, ok := grandParent.(*googlesql.ASTPipeDrop); !ok {
			return nil, nil, false, false
		}
		isInput = true
	case *googlesql.ASTPipeRenameItem:
		oldName, err := n.OldName()
		if err != nil || oldName == nil {
			return nil, nil, false, false
		}
		isInput = locStart(oldName) == locStart(ident)
	case *googlesql.ASTAlias:
		// Only column aliases name a column; table aliases such as
		// `|> AS t` do not.
		grandParent, err := n.Parent()
		if err != nil {
			return nil, nil, false, false
		}
		switch grandParent.(type) {
		case *googlesql.ASTSelectColumn, *googlesql.ASTGroupingItem:
		default:
			return nil, nil, false, false
		}
	default:
		return nil, nil, false, false
	}
	op, ok = enclosingPipeOperator(parent)
	if !ok {
		return nil, nil, false, false
	}
	return ident, op, isInput, true
}

func enclosingPipeOperator(n googlesql.ASTNode) (googlesql.ASTPipeOperatorNode, bool) {
	for n != nil {
		// GoogleSQL makes RENAME items pipe operators of their own.
		if _, ok := n.(*googlesql.ASTPipeRenameItem); !ok {
			if op, ok := n.(googlesql.ASTPipeOperatorNode); ok {
				return op, true
			}
		}
		if _, ok := n.(*googlesql.ASTQuery); ok {
			return nil, false
		}
		parent, err := n.Parent()
		if err != nil {
			return nil, false
		}
		n = parent
	}
	return nil, false
}

// PipeColumn returns the column that name refers to in the input of op, or in
// its output if isInput is false. The resolved tree keeps no names of the
// columns between pipe operators, so the query up to op is analyzed on its
// own and the name is looked up in its output columns.
func (a *Analyzer) PipeColumn(p ParsedFile, op googlesql.ASTPipeOperatorNode, name string, isInput bool) (*googlesql.ResolvedColumn, bool) {
	if p.catalog == nil {
		return nil, false
	}
	parent, err := op.Parent()
	if err != nil {
		return nil, false
	}
	query, ok := parent.(*googlesql.ASTQuery)
	if !ok {
		return nil, false
	}
	start, end := locStart(query), locEnd(op)
	if isInput {
		end = locStart(op)
	}
	if start < 0 || end < start || end > len(p.FixedSrc) {
		return nil, false
	}

	// Masking keeps the offsets, and the statements before the query, such as
	// DECLARE, are already registered in the catalog.
	src := maskOutside(p.FixedSrc, start, end)
	script, err := a.parseScript(src)
	if err != nil {
		return nil, false
	}
	stmts := collectStatements(script)
	if len(stmts) != 1 {
		return nil, false
	}
	output, err := a.AnalyzeStatement(src, stmts[0], p.catalog)
	if err != nil {
		a.logger.Debugf("failed to analyze the query before the pipe operator: %v", err)
		return nil, false
	}
	stmt, err := output.ResolvedStatement()
	if err != nil {
		return nil, false
	}
	queryStmt, ok := stmt.(*googlesql.ResolvedQueryStmt)
	if !ok {
		return nil, false
	}
	outputColumns, err := queryStmt.OutputColumnList()
	if err != nil {
		return nil, false
	}

	var found *googlesql.ResolvedColumn
	for _, c := range outputColumns {
		n, _ := c.Name()
		if !strings.EqualFold(n, name) {
			continue
		}
		// An ambiguous name refers to no column.
		if found != nil {
			return nil, false
		}
		found, _ = c.Column()
	}
	return found, found != nil
}
