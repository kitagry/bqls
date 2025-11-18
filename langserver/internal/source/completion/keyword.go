package completion

import (
	"context"

	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/kitagry/bqls/langserver/internal/source/file"
	ts "github.com/tree-sitter/go-tree-sitter"
)

func (c *completor) completeKeyword(ctx context.Context, parsedFile file.ParsedFile, position lsp.Position) []CompletionItem {
	if parsedFile.TsTree == nil {
		return []CompletionItem{}
	}

	rootNode := parsedFile.TsTree.RootNode()

	// Check if the program node has no children (empty file)
	if rootNode.ChildCount() == 0 {
		return completeFromEmptyProgram()
	}

	// Get the node at the cursor position
	return completeFromCursorPosition(rootNode, parsedFile, position, false)
}

// completeFromEmptyProgram handles keyword completion for an empty file
func completeFromEmptyProgram() []CompletionItem {
	return append(createSelectKeywordCompletionItem(""), createWithKeywordCompletionItem("")...)
}

// completeFromCursorPosition handles keyword completion based on cursor position
// insideCTE indicates if we're currently inside a CTE definition
func completeFromCursorPosition(rootNode *ts.Node, parsedFile file.ParsedFile, position lsp.Position, insideCTE bool) []CompletionItem {
	offset := parsedFile.TermOffset(position)

	// Find the select_statement node that contains the cursor
	var selectStmt *ts.Node

	// Check if the cursor is inside a CTE node (only if not already inside one)
	if !insideCTE {
		cteNode, cteStmtNode := findCTENodeContainingPosition(rootNode, uint(offset))
		if cteNode != nil && cteStmtNode != nil {
			// Recursively handle completion inside the CTE statement
			return completeFromCursorPosition(cteStmtNode, parsedFile, position, true)
		}
	}

	// Find the select_statement containing the cursor position
	selectStmt = findSelectStatementAtPosition(rootNode, uint(offset), insideCTE)
	if selectStmt == nil {
		// No select statement found - suggest starting keywords
		return completeFromEmptyProgram()
	}

	// Find the last clause before the cursor position
	lastClause := findLastClauseBeforeCursor(selectStmt, uint(offset))

	// Get clauses that appear after the cursor (we should not suggest these)
	clausesAfter := getClausesAfterCursor(selectStmt, uint(offset))

	// Check if there's a join_expression in the from_clause
	hasJoinWithoutOn := hasJoinExpressionWithoutOn(selectStmt)

	// Determine what to suggest based on the last clause before cursor
	result := []CompletionItem{}

	switch lastClause {
	case "offset_clause":
		// After OFFSET, nothing to suggest
		return result

	case "limit_clause":
		// After LIMIT, suggest OFFSET (unless OFFSET already exists after cursor)
		if !clausesAfter["offset_clause"] && !hasClause(selectStmt, "offset_clause") {
			result = append(result, createOffsetKeywordCompletionItem("")...)
		}
		return result

	case "order_by_clause":
		// After ORDER BY, suggest ASC/DESC and LIMIT
		orderByNode := findClause(selectStmt, "order_by_clause")
		hasAscOrDesc := hasAscOrDescInOrderBy(orderByNode)

		if !hasAscOrDesc {
			result = append(result, createAscDescKeywordCompletionItems("")...)
		}
		if !clausesAfter["limit_clause"] {
			result = append(result, createLimitKeywordCompletionItem("")...)
		}
		return result

	case "having_clause":
		// After HAVING, suggest ORDER BY and LIMIT
		if !clausesAfter["order_by_clause"] {
			result = append(result, createOrderByKeywordCompletionItem("")...)
		}
		if !clausesAfter["limit_clause"] {
			result = append(result, createLimitKeywordCompletionItem("")...)
		}
		return result

	case "group_by_clause":
		// After GROUP BY, suggest HAVING, ORDER BY, and LIMIT
		if !clausesAfter["having_clause"] {
			result = append(result, createHavingKeywordCompletionItem("")...)
		}
		if !clausesAfter["order_by_clause"] {
			result = append(result, createOrderByKeywordCompletionItem("")...)
		}
		if !clausesAfter["limit_clause"] {
			result = append(result, createLimitKeywordCompletionItem("")...)
		}
		return result

	case "where_clause":
		// After WHERE, suggest GROUP BY, ORDER BY, and LIMIT
		if !clausesAfter["group_by_clause"] {
			result = append(result, createGroupByKeywordCompletionItem("")...)
		}
		if !clausesAfter["order_by_clause"] {
			result = append(result, createOrderByKeywordCompletionItem("")...)
		}
		if !clausesAfter["limit_clause"] {
			result = append(result, createLimitKeywordCompletionItem("")...)
		}
		return result

	case "from_clause":
		// After FROM, check for JOIN without ON
		if hasJoinWithoutOn {
			return createOnKeywordCompletionItem("")
		}

		// Otherwise suggest JOIN, WHERE, GROUP BY, ORDER BY, LIMIT
		result = append(result, createJoinKeywordCompletionItems("")...)
		if !clausesAfter["where_clause"] {
			result = append(result, createWhereKeywordCompletionItem("")...)
		}
		if !clausesAfter["group_by_clause"] {
			result = append(result, createGroupByKeywordCompletionItem("")...)
		}
		if !clausesAfter["order_by_clause"] {
			result = append(result, createOrderByKeywordCompletionItem("")...)
		}
		if !clausesAfter["limit_clause"] {
			result = append(result, createLimitKeywordCompletionItem("")...)
		}
		return result

	case "select_clause":
		// After SELECT, suggest FROM
		return createFromKeywordCompletionItem("")

	default:
		// No clause found before cursor - suggest SELECT and WITH
		return completeFromEmptyProgram()
	}
}

// hasClause checks if the select statement has a specific clause
func hasClause(selectStmt *ts.Node, clauseKind string) bool {
	if selectStmt == nil {
		return false
	}

	for i := uint(0); i < selectStmt.NamedChildCount(); i++ {
		child := selectStmt.NamedChild(i)
		if child != nil && child.Kind() == clauseKind {
			return true
		}
	}

	return false
}

// findClause finds a specific clause in the select statement
func findClause(selectStmt *ts.Node, clauseKind string) *ts.Node {
	if selectStmt == nil {
		return nil
	}

	for i := uint(0); i < selectStmt.NamedChildCount(); i++ {
		child := selectStmt.NamedChild(i)
		if child != nil && child.Kind() == clauseKind {
			return child
		}
	}

	return nil
}

// findLastClauseBeforeCursor finds the last clause that ends before the cursor position
func findLastClauseBeforeCursor(selectStmt *ts.Node, cursorOffset uint) string {
	if selectStmt == nil {
		return ""
	}

	clauseOrder := []string{
		"select_clause",
		"from_clause",
		"where_clause",
		"group_by_clause",
		"having_clause",
		"order_by_clause",
		"limit_clause",
		"offset_clause",
	}

	lastClause := ""
	for _, clauseKind := range clauseOrder {
		clause := findClause(selectStmt, clauseKind)
		if clause != nil && clause.EndByte() <= cursorOffset {
			lastClause = clauseKind
		}
	}

	return lastClause
}

// getClausesAfterCursor returns a set of clause kinds that appear after the cursor position
func getClausesAfterCursor(selectStmt *ts.Node, cursorOffset uint) map[string]bool {
	if selectStmt == nil {
		return map[string]bool{}
	}

	clausesAfter := map[string]bool{}
	clauseKinds := []string{
		"where_clause",
		"group_by_clause",
		"having_clause",
		"order_by_clause",
		"limit_clause",
		"offset_clause",
	}

	for _, clauseKind := range clauseKinds {
		clause := findClause(selectStmt, clauseKind)
		if clause != nil && clause.StartByte() > cursorOffset {
			clausesAfter[clauseKind] = true
		}
	}

	return clausesAfter
}

// findJoinExpression recursively finds a join_expression node
func findJoinExpression(node *ts.Node) *ts.Node {
	if node == nil {
		return nil
	}

	if node.Kind() == "join_expression" {
		return node
	}

	for i := uint(0); i < node.NamedChildCount(); i++ {
		if result := findJoinExpression(node.NamedChild(i)); result != nil {
			return result
		}
	}

	return nil
}

// hasJoinExpressionWithoutOn checks if there's a join_expression that doesn't have an ON clause
// Based on the S-expression: (join_expression left: (backtick_identifier) right: (backtick_identifier))
// A complete join would have more structure including the ON condition
func hasJoinExpressionWithoutOn(selectStmt *ts.Node) bool {
	fromClause := findClause(selectStmt, "from_clause")
	if fromClause == nil {
		return false
	}

	joinExpr := findJoinExpression(fromClause)
	if joinExpr == nil {
		return false
	}

	// Check if the join expression has an ON clause
	// In the new parser, a complete join has child nodes for the condition
	// An incomplete join (without ON) will have fewer children
	// We need to check if there are only "left" and "right" without the join condition

	// Look for a field named "condition" or check the number of children
	for i := uint(0); i < joinExpr.NamedChildCount(); i++ {
		child := joinExpr.NamedChild(i)
		if child != nil {
			fieldName := joinExpr.FieldNameForChild(uint32(i))
			if fieldName == "condition" || fieldName == "on" {
				return false // Has ON clause
			}
		}
	}

	// If we only have "left" and "right" (2 children), it's a join without ON
	return joinExpr.NamedChildCount() >= 2
}

// hasAscOrDescInOrderBy checks if an order_by_clause contains ASC or DESC keywords
func hasAscOrDescInOrderBy(orderByNode *ts.Node) bool {
	if orderByNode == nil {
		return false
	}

	// Look for order_item nodes and check if they have asc/desc specifiers
	return hasAscOrDescRecursive(orderByNode)
}

// hasAscOrDescRecursive recursively checks for ASC or DESC nodes
func hasAscOrDescRecursive(node *ts.Node) bool {
	if node == nil {
		return false
	}

	// Check for ASC or DESC node kinds - update these based on actual grammar
	kind := node.Kind()
	if kind == "asc" || kind == "desc" || kind == "ASC" || kind == "DESC" {
		return true
	}

	for i := uint(0); i < node.NamedChildCount(); i++ {
		if hasAscOrDescRecursive(node.NamedChild(i)) {
			return true
		}
	}

	return false
}

// findSelectStatementAtPosition finds the select_statement node containing the given position
// If insideCTE is true, skip the WITH clause when searching
func findSelectStatementAtPosition(node *ts.Node, offset uint, insideCTE bool) *ts.Node {
	if node == nil {
		return nil
	}

	// If this is a select_statement, check if offset is within or just after it
	// We need to be lenient because the cursor might be right after the last token
	if node.Kind() == "select_statement" {
		// Allow cursor to be at or just after the end of the statement
		if offset >= node.StartByte() && offset <= node.EndByte()+10 {
			return node
		}
	}

	// Recursively search children
	for i := uint(0); i < node.NamedChildCount(); i++ {
		child := node.NamedChild(i)

		// Skip with_clause if we're already inside a CTE
		if insideCTE && child != nil && child.Kind() == "with_clause" {
			continue
		}

		if result := findSelectStatementAtPosition(child, offset, insideCTE); result != nil {
			return result
		}
	}

	// If we didn't find anything and this node has select_statement children,
	// return the last one (assuming cursor is after the query)
	if !insideCTE {
		for i := int(node.NamedChildCount()) - 1; i >= 0; i-- {
			child := node.NamedChild(uint(i))
			if child != nil && child.Kind() == "select_statement" {
				// Check if offset is close to this statement
				if offset >= node.StartByte() {
					return child
				}
			}
		}
	}

	return nil
}

func createSelectKeywordCompletionItem(typedPrefix string) []CompletionItem {
	return []CompletionItem{
		{
			Kind:    lsp.CIKKeyword,
			NewText: "SELECT ",
			Documentation: lsp.MarkupContent{
				Kind:  lsp.MKPlainText,
				Value: "The SELECT statement is used to query data from a table.",
			},
			TypedPrefix: typedPrefix,
		},
	}
}

func createWithKeywordCompletionItem(typedPrefix string) []CompletionItem {
	return []CompletionItem{
		{
			Kind:        lsp.CIKKeyword,
			NewText:     "WITH ",
			SnippetText: "WITH ${1:name} AS (${2:query})",
			Documentation: lsp.MarkupContent{
				Kind:  lsp.MKPlainText,
				Value: "The WITH statement is used to create tempolary named subquery.",
			},
			TypedPrefix: typedPrefix,
		},
	}
}

func createFromKeywordCompletionItem(typedPrefix string) []CompletionItem {
	return []CompletionItem{
		{
			Kind:    lsp.CIKKeyword,
			NewText: "FROM ",
			Documentation: lsp.MarkupContent{
				Kind:  lsp.MKPlainText,
				Value: "The FROM clause specifies the table to query data from.",
			},
			TypedPrefix: typedPrefix,
		},
	}
}

func createWhereKeywordCompletionItem(typedPrefix string) []CompletionItem {
	return []CompletionItem{
		{
			Kind:    lsp.CIKKeyword,
			NewText: "WHERE ",
			Documentation: lsp.MarkupContent{
				Kind:  lsp.MKPlainText,
				Value: "The WHERE clause is used to filter records.",
			},
			TypedPrefix: typedPrefix,
		},
	}
}

func createGroupByKeywordCompletionItem(typedPrefix string) []CompletionItem {
	return []CompletionItem{
		{
			Kind:    lsp.CIKKeyword,
			NewText: "GROUP BY ",
			Documentation: lsp.MarkupContent{
				Kind:  lsp.MKPlainText,
				Value: "The GROUP BY clause groups rows that have the same values.",
			},
			TypedPrefix: typedPrefix,
		},
	}
}

func createOrderByKeywordCompletionItem(typedPrefix string) []CompletionItem {
	return []CompletionItem{
		{
			Kind:    lsp.CIKKeyword,
			NewText: "ORDER BY ",
			Documentation: lsp.MarkupContent{
				Kind:  lsp.MKPlainText,
				Value: "The ORDER BY clause is used to sort the result set.",
			},
			TypedPrefix: typedPrefix,
		},
	}
}

func createLimitKeywordCompletionItem(typedPrefix string) []CompletionItem {
	return []CompletionItem{
		{
			Kind:    lsp.CIKKeyword,
			NewText: "LIMIT ",
			Documentation: lsp.MarkupContent{
				Kind:  lsp.MKPlainText,
				Value: "The LIMIT clause is used to limit the number of rows returned.",
			},
			TypedPrefix: typedPrefix,
		},
	}
}

func createHavingKeywordCompletionItem(typedPrefix string) []CompletionItem {
	return []CompletionItem{
		{
			Kind:    lsp.CIKKeyword,
			NewText: "HAVING ",
			Documentation: lsp.MarkupContent{
				Kind:  lsp.MKPlainText,
				Value: "The HAVING clause is used to filter groups based on aggregate functions.",
			},
			TypedPrefix: typedPrefix,
		},
	}
}

func createAscDescKeywordCompletionItems(typedPrefix string) []CompletionItem {
	return []CompletionItem{
		{
			Kind:    lsp.CIKKeyword,
			NewText: "ASC",
			Documentation: lsp.MarkupContent{
				Kind:  lsp.MKPlainText,
				Value: "Sort in ascending order (default).",
			},
			TypedPrefix: typedPrefix,
		},
		{
			Kind:    lsp.CIKKeyword,
			NewText: "DESC",
			Documentation: lsp.MarkupContent{
				Kind:  lsp.MKPlainText,
				Value: "Sort in descending order.",
			},
			TypedPrefix: typedPrefix,
		},
	}
}

func createOffsetKeywordCompletionItem(typedPrefix string) []CompletionItem {
	return []CompletionItem{
		{
			Kind:    lsp.CIKKeyword,
			NewText: "OFFSET ",
			Documentation: lsp.MarkupContent{
				Kind:  lsp.MKPlainText,
				Value: "The OFFSET clause is used to skip a specified number of rows.",
			},
			TypedPrefix: typedPrefix,
		},
	}
}

func createJoinKeywordCompletionItems(typedPrefix string) []CompletionItem {
	return []CompletionItem{
		{
			Kind:        lsp.CIKKeyword,
			NewText:     "JOIN ",
			SnippetText: "JOIN ${1:table} ON ${2:condition}",
			Documentation: lsp.MarkupContent{
				Kind:  lsp.MKPlainText,
				Value: "INNER JOIN - returns rows when there is a match in both tables.",
			},
			TypedPrefix: typedPrefix,
		},
		{
			Kind:        lsp.CIKKeyword,
			NewText:     "LEFT JOIN ",
			SnippetText: "LEFT JOIN ${1:table} ON ${2:condition}",
			Documentation: lsp.MarkupContent{
				Kind:  lsp.MKPlainText,
				Value: "LEFT JOIN - returns all rows from the left table, and matched rows from the right table.",
			},
			TypedPrefix: typedPrefix,
		},
		{
			Kind:        lsp.CIKKeyword,
			NewText:     "RIGHT JOIN ",
			SnippetText: "RIGHT JOIN ${1:table} ON ${2:condition}",
			Documentation: lsp.MarkupContent{
				Kind:  lsp.MKPlainText,
				Value: "RIGHT JOIN - returns all rows from the right table, and matched rows from the left table.",
			},
			TypedPrefix: typedPrefix,
		},
		{
			Kind:        lsp.CIKKeyword,
			NewText:     "FULL OUTER JOIN ",
			SnippetText: "FULL OUTER JOIN ${1:table} ON ${2:condition}",
			Documentation: lsp.MarkupContent{
				Kind:  lsp.MKPlainText,
				Value: "FULL OUTER JOIN - returns all rows when there is a match in either table.",
			},
			TypedPrefix: typedPrefix,
		},
		{
			Kind:        lsp.CIKKeyword,
			NewText:     "CROSS JOIN ",
			SnippetText: "CROSS JOIN ${1:table}",
			Documentation: lsp.MarkupContent{
				Kind:  lsp.MKPlainText,
				Value: "CROSS JOIN - returns the Cartesian product of both tables.",
			},
			TypedPrefix: typedPrefix,
		},
	}
}

func createOnKeywordCompletionItem(typedPrefix string) []CompletionItem {
	return []CompletionItem{
		{
			Kind:    lsp.CIKKeyword,
			NewText: "ON ",
			Documentation: lsp.MarkupContent{
				Kind:  lsp.MKPlainText,
				Value: "The ON clause specifies the join condition between tables.",
			},
			TypedPrefix: typedPrefix,
		},
	}
}

// findCTENodeContainingPosition finds a CTE node that contains the given position
// Returns the CTE node and the select_statement node inside it, or nil if not found
func findCTENodeContainingPosition(node *ts.Node, offset uint) (*ts.Node, *ts.Node) {
	if node == nil {
		return nil, nil
	}

	// Check if this node is a CTE node
	if node.Kind() == "cte" {
		// Check if the offset is within the CTE node's range
		// This includes the entire "AS (...)" part
		if offset >= node.StartByte() && offset <= node.EndByte() {
			// Find the select_statement node inside this CTE (the query definition)
			for i := uint(0); i < node.NamedChildCount(); i++ {
				child := node.NamedChild(i)
				if child.Kind() == "select_statement" {
					return node, child
				}
			}
		}
	}

	// Recursively search in children
	for i := uint(0); i < node.NamedChildCount(); i++ {
		if cteNode, stmtNode := findCTENodeContainingPosition(node.NamedChild(i), offset); cteNode != nil {
			return cteNode, stmtNode
		}
	}

	return nil, nil
}
