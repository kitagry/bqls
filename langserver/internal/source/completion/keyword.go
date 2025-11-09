package completion

import (
	"context"
	"strings"

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

	// Check if the program has only one ERROR node (incomplete input)
	if rootNode.ChildCount() == 1 {
		if items := completeFromSingleErrorNode(rootNode, parsedFile.Src); items != nil {
			return items
		}
	}

	// Check if the last child is an ERROR node (statement followed by incomplete input)
	if rootNode.ChildCount() >= 2 {
		if items := completeFromMultipleNodes(rootNode, parsedFile.Src); items != nil {
			return items
		}
	}

	// Get the node at the cursor position
	return completeFromCursorPosition(rootNode, parsedFile, position)
}

// completeFromEmptyProgram handles keyword completion for an empty file
func completeFromEmptyProgram() []CompletionItem {
	return createSelectKeywordCompletionItem("")
}

// completeFromSingleErrorNode handles keyword completion when the program has only one ERROR node
func completeFromSingleErrorNode(rootNode *ts.Node, src string) []CompletionItem {
	childNode := rootNode.Child(0)
	if childNode.Kind() != "ERROR" {
		return nil
	}

	// Check if the ERROR node contains FROM keyword
	if hasKeywordFrom(childNode) {
		// Extract typed prefix if the user has started typing
		typedPrefix := extractTypedPrefixFromError(childNode, src)

		// Check if we also have OFFSET keyword - this is the end of the query
		if hasKeywordOffset(childNode, src) {
			return []CompletionItem{}
		}

		// Check if we also have LIMIT keyword
		if hasKeywordLimit(childNode, src) {
			// We have LIMIT, so suggest OFFSET only
			// Don't use typed prefix from ERROR node as it's likely a number, not a keyword prefix
			return createOffsetKeywordCompletionItem("")
		}

		// Check if we also have ORDER BY keyword
		if hasKeywordOrderBy(childNode, src) {
			// Check if we also have ASC or DESC
			if hasKeywordAsc(childNode, src) || hasKeywordDesc(childNode, src) {
				// We have ORDER BY with ASC/DESC, so suggest LIMIT only
				return createLimitKeywordCompletionItem("")
			}
			// We have ORDER BY but no ASC/DESC, so suggest ASC, DESC, and LIMIT
			// Don't use typed prefix from ERROR node as it's likely a column name, not a keyword prefix
			result := []CompletionItem{}
			result = append(result, createAscDescKeywordCompletionItems("")...)
			result = append(result, createLimitKeywordCompletionItem("")...)
			return result
		}

		// Check if we also have HAVING keyword
		if hasKeywordHaving(childNode, src) {
			// We have HAVING but no ORDER BY, so suggest ORDER BY and LIMIT
			// Don't use typed prefix from ERROR node as it's likely a column name, not a keyword prefix
			result := []CompletionItem{}
			result = append(result, createOrderByKeywordCompletionItem("")...)
			result = append(result, createLimitKeywordCompletionItem("")...)
			return result
		}

		// Check if we also have GROUP BY keyword
		if hasKeywordGroupBy(childNode, src) {
			// We have GROUP BY but no HAVING, so suggest HAVING, ORDER BY and LIMIT
			// Don't use typed prefix from ERROR node as it's likely a column name, not a keyword prefix
			result := []CompletionItem{}
			result = append(result, createHavingKeywordCompletionItem("")...)
			result = append(result, createOrderByKeywordCompletionItem("")...)
			result = append(result, createLimitKeywordCompletionItem("")...)
			return result
		}

		// Check if we also have WHERE keyword
		if hasKeywordWhere(childNode, src) {
			// We have WHERE but no GROUP BY, so suggest GROUP BY, ORDER BY, and LIMIT
			// Don't use typed prefix from ERROR node as it's likely a column name, not a keyword prefix
			result := []CompletionItem{}
			result = append(result, createGroupByKeywordCompletionItem("")...)
			result = append(result, createOrderByKeywordCompletionItem("")...)
			result = append(result, createLimitKeywordCompletionItem("")...)
			return result
		}

		// We have FROM but no WHERE, GROUP BY, or ORDER BY, so suggest WHERE, GROUP BY, ORDER BY, and LIMIT
		result := []CompletionItem{}
		result = append(result, createWhereKeywordCompletionItem(typedPrefix)...)
		result = append(result, createGroupByKeywordCompletionItem(typedPrefix)...)
		result = append(result, createOrderByKeywordCompletionItem(typedPrefix)...)
		result = append(result, createLimitKeywordCompletionItem(typedPrefix)...)
		return result
	}

	// Check if the ERROR node contains a SELECT keyword
	if hasKeywordSelect(childNode) {
		// We have SELECT, so suggest FROM
		return createFromKeywordCompletionItem("")
	}

	// No SELECT keyword found, suggest SELECT
	typedPrefix := childNode.Utf8Text([]byte(src))
	return createSelectKeywordCompletionItem(typedPrefix)
}

// completeFromMultipleNodes handles keyword completion when there are multiple nodes
func completeFromMultipleNodes(rootNode *ts.Node, src string) []CompletionItem {
	lastChild := rootNode.Child(rootNode.ChildCount() - 1)
	if lastChild.Kind() != "ERROR" {
		return nil
	}

	firstChild := rootNode.Child(0)

	// Check if we have a statement with FROM keyword before the ERROR
	if hasKeywordFrom(firstChild) {
		// Extract typed prefix from the ERROR node
		typedPrefix := extractTypedPrefixFromError(lastChild, src)

		// Check if we also have OFFSET keyword - this is the end of the query
		if hasKeywordOffset(firstChild, src) || hasKeywordOffset(lastChild, src) {
			return []CompletionItem{}
		}

		// Check if we also have LIMIT keyword
		if hasKeywordLimit(firstChild, src) || hasKeywordLimit(lastChild, src) {
			// We have LIMIT, so suggest OFFSET only
			// Don't use typed prefix from ERROR node as it's likely a number, not a keyword prefix
			return createOffsetKeywordCompletionItem("")
		}

		// Check if we also have ORDER BY keyword
		if hasKeywordOrderBy(firstChild, src) || hasKeywordOrderBy(lastChild, src) {
			// Check if we also have ASC or DESC
			if hasKeywordAsc(firstChild, src) || hasKeywordDesc(firstChild, src) || hasKeywordAsc(lastChild, src) || hasKeywordDesc(lastChild, src) {
				// We have ORDER BY with ASC/DESC, so suggest LIMIT only
				return createLimitKeywordCompletionItem("")
			}
			// We have ORDER BY but no ASC/DESC, so suggest ASC, DESC, and LIMIT
			// Don't use typed prefix from ERROR node as it's likely a column name, not a keyword prefix
			result := []CompletionItem{}
			result = append(result, createAscDescKeywordCompletionItems("")...)
			result = append(result, createLimitKeywordCompletionItem("")...)
			return result
		}

		// Check if we also have HAVING keyword
		if hasKeywordHaving(firstChild, src) || hasKeywordHaving(lastChild, src) {
			// We have HAVING but no ORDER BY, so suggest ORDER BY and LIMIT
			// Don't use typed prefix from ERROR node as it's likely a column name, not a keyword prefix
			result := []CompletionItem{}
			result = append(result, createOrderByKeywordCompletionItem("")...)
			result = append(result, createLimitKeywordCompletionItem("")...)
			return result
		}

		// Check if we also have GROUP BY keyword
		if hasKeywordGroupBy(firstChild, src) || hasKeywordGroupBy(lastChild, src) {
			// We have GROUP BY but no HAVING, so suggest HAVING, ORDER BY and LIMIT
			// Don't use typed prefix from ERROR node as it's likely a column name, not a keyword prefix
			result := []CompletionItem{}
			result = append(result, createHavingKeywordCompletionItem("")...)
			result = append(result, createOrderByKeywordCompletionItem("")...)
			result = append(result, createLimitKeywordCompletionItem("")...)
			return result
		}

		// Check if we also have WHERE keyword
		if hasKeywordWhere(firstChild, src) || hasKeywordWhere(lastChild, src) {
			// We have WHERE but no GROUP BY, so suggest GROUP BY, ORDER BY, and LIMIT
			// Don't use typed prefix from ERROR node as it's likely a column name, not a keyword prefix
			result := []CompletionItem{}
			result = append(result, createGroupByKeywordCompletionItem("")...)
			result = append(result, createOrderByKeywordCompletionItem("")...)
			result = append(result, createLimitKeywordCompletionItem("")...)
			return result
		}

		// We have FROM but no WHERE, GROUP BY, or ORDER BY, so suggest WHERE, GROUP BY, ORDER BY, and LIMIT
		result := []CompletionItem{}
		result = append(result, createWhereKeywordCompletionItem(typedPrefix)...)
		result = append(result, createGroupByKeywordCompletionItem(typedPrefix)...)
		result = append(result, createOrderByKeywordCompletionItem(typedPrefix)...)
		result = append(result, createLimitKeywordCompletionItem(typedPrefix)...)
		return result
	}

	return nil
}

// completeFromCursorPosition handles keyword completion based on cursor position
func completeFromCursorPosition(rootNode *ts.Node, parsedFile file.ParsedFile, position lsp.Position) []CompletionItem {
	offset := parsedFile.TermOffset(position)
	node := rootNode.NamedDescendantForByteRange(uint(offset), uint(offset))
	if node == nil {
		return []CompletionItem{}
	}

	// Check if we are after an OFFSET keyword - this is the end of the query
	if hasKeywordOffset(node, parsedFile.Src) {
		return []CompletionItem{}
	}

	// Check if we are after a LIMIT keyword by searching descendants
	if hasKeywordLimit(node, parsedFile.Src) {
		return createOffsetKeywordCompletionItem("")
	}

	// Check if we are after an ORDER BY keyword by searching descendants
	if hasKeywordOrderBy(node, parsedFile.Src) {
		// If we already have ASC or DESC, only suggest LIMIT
		if hasKeywordAsc(node, parsedFile.Src) || hasKeywordDesc(node, parsedFile.Src) {
			return createLimitKeywordCompletionItem("")
		}
		// Otherwise, suggest ASC, DESC, and LIMIT
		result := []CompletionItem{}
		result = append(result, createAscDescKeywordCompletionItems("")...)
		result = append(result, createLimitKeywordCompletionItem("")...)
		return result
	}

	// Check if we are after a HAVING keyword by searching descendants
	if hasKeywordHaving(node, parsedFile.Src) {
		result := []CompletionItem{}
		result = append(result, createOrderByKeywordCompletionItem("")...)
		result = append(result, createLimitKeywordCompletionItem("")...)
		return result
	}

	// Check if we are after a GROUP BY keyword by searching descendants
	if hasKeywordGroupBy(node, parsedFile.Src) {
		result := []CompletionItem{}
		result = append(result, createHavingKeywordCompletionItem("")...)
		result = append(result, createOrderByKeywordCompletionItem("")...)
		result = append(result, createLimitKeywordCompletionItem("")...)
		return result
	}

	// Check if we are after a WHERE keyword by searching descendants
	if hasKeywordWhere(node, parsedFile.Src) {
		result := []CompletionItem{}
		result = append(result, createGroupByKeywordCompletionItem("")...)
		result = append(result, createOrderByKeywordCompletionItem("")...)
		result = append(result, createLimitKeywordCompletionItem("")...)
		return result
	}

	// Check if we are after a FROM keyword by searching descendants
	if hasKeywordFrom(node) {
		result := []CompletionItem{}
		result = append(result, createWhereKeywordCompletionItem("")...)
		result = append(result, createGroupByKeywordCompletionItem("")...)
		result = append(result, createOrderByKeywordCompletionItem("")...)
		result = append(result, createLimitKeywordCompletionItem("")...)
		return result
	}

	// Check if we are after a SELECT keyword by searching descendants
	if hasKeywordSelect(node) {
		return createFromKeywordCompletionItem("")
	}

	return []CompletionItem{}
}

// hasKeywordSelect recursively checks if the node or its descendants contain a keyword_select node
func hasKeywordSelect(node *ts.Node) bool {
	if node == nil {
		return false
	}

	if node.Kind() == "keyword_select" {
		return true
	}

	for i := uint(0); i < node.NamedChildCount(); i++ {
		if hasKeywordSelect(node.NamedChild(i)) {
			return true
		}
	}

	return false
}

// hasKeywordFrom recursively checks if the node or its descendants contain a keyword_from node
func hasKeywordFrom(node *ts.Node) bool {
	if node == nil {
		return false
	}

	if node.Kind() == "keyword_from" {
		return true
	}

	for i := uint(0); i < node.NamedChildCount(); i++ {
		if hasKeywordFrom(node.NamedChild(i)) {
			return true
		}
	}

	return false
}

// hasKeywordWhere recursively checks if the node or its descendants contain a keyword_where node
// or if the node text contains "WHERE"
func hasKeywordWhere(node *ts.Node, src string) bool {
	if node == nil {
		return false
	}

	if node.Kind() == "keyword_where" {
		return true
	}

	// Check if ERROR node contains WHERE keyword
	if node.Kind() == "ERROR" {
		text := node.Utf8Text([]byte(src))
		if strings.Contains(text, "WHERE") {
			return true
		}
	}

	// Also check child nodes (not just named children)
	for i := uint(0); i < node.ChildCount(); i++ {
		child := node.Child(i)
		if child != nil && child.Kind() == "keyword_where" {
			return true
		}
	}

	for i := uint(0); i < node.NamedChildCount(); i++ {
		if hasKeywordWhere(node.NamedChild(i), src) {
			return true
		}
	}

	return false
}

// hasKeywordGroupBy recursively checks if the node or its descendants contain a GROUP BY keyword
// or if the node text contains "GROUP BY"
func hasKeywordGroupBy(node *ts.Node, src string) bool {
	if node == nil {
		return false
	}

	// Check if ERROR node contains GROUP BY keyword
	if node.Kind() == "ERROR" {
		text := node.Utf8Text([]byte(src))
		if strings.Contains(text, "GROUP BY") {
			return true
		}
	}

	// Check for group_by node (if it exists in the grammar)
	if node.Kind() == "group_by" || node.Kind() == "keyword_group" {
		return true
	}

	for i := uint(0); i < node.NamedChildCount(); i++ {
		if hasKeywordGroupBy(node.NamedChild(i), src) {
			return true
		}
	}

	return false
}

// hasKeywordOrderBy recursively checks if the node or its descendants contain an ORDER BY keyword
// or if the node text contains "ORDER BY"
func hasKeywordOrderBy(node *ts.Node, src string) bool {
	if node == nil {
		return false
	}

	// Check if ERROR node contains ORDER BY keyword
	if node.Kind() == "ERROR" {
		text := node.Utf8Text([]byte(src))
		if strings.Contains(text, "ORDER BY") {
			return true
		}
	}

	// Check for order_by node (if it exists in the grammar)
	if node.Kind() == "order_by" || node.Kind() == "keyword_order" {
		return true
	}

	for i := uint(0); i < node.NamedChildCount(); i++ {
		if hasKeywordOrderBy(node.NamedChild(i), src) {
			return true
		}
	}

	return false
}

// hasKeywordHaving recursively checks if the node or its descendants contain a HAVING keyword
// or if the node text contains "HAVING"
func hasKeywordHaving(node *ts.Node, src string) bool {
	if node == nil {
		return false
	}

	// Check if ERROR node contains HAVING keyword
	if node.Kind() == "ERROR" {
		text := node.Utf8Text([]byte(src))
		if strings.Contains(text, "HAVING") {
			return true
		}
	}

	// Check for having node (if it exists in the grammar)
	if node.Kind() == "having" || node.Kind() == "keyword_having" {
		return true
	}

	for i := uint(0); i < node.NamedChildCount(); i++ {
		if hasKeywordHaving(node.NamedChild(i), src) {
			return true
		}
	}

	return false
}

// hasKeywordAsc recursively checks if the node or its descendants contain an ASC keyword
func hasKeywordAsc(node *ts.Node, src string) bool {
	if node == nil {
		return false
	}

	// Check if ERROR node contains ASC keyword
	if node.Kind() == "ERROR" {
		text := node.Utf8Text([]byte(src))
		if strings.Contains(text, "ASC") {
			return true
		}
	}

	// Check for asc node (if it exists in the grammar)
	if node.Kind() == "asc" || node.Kind() == "keyword_asc" {
		return true
	}

	for i := uint(0); i < node.NamedChildCount(); i++ {
		if hasKeywordAsc(node.NamedChild(i), src) {
			return true
		}
	}

	return false
}

// hasKeywordDesc recursively checks if the node or its descendants contain a DESC keyword
func hasKeywordDesc(node *ts.Node, src string) bool {
	if node == nil {
		return false
	}

	// Check if ERROR node contains DESC keyword
	if node.Kind() == "ERROR" {
		text := node.Utf8Text([]byte(src))
		if strings.Contains(text, "DESC") {
			return true
		}
	}

	// Check for desc node (if it exists in the grammar)
	if node.Kind() == "desc" || node.Kind() == "keyword_desc" {
		return true
	}

	for i := uint(0); i < node.NamedChildCount(); i++ {
		if hasKeywordDesc(node.NamedChild(i), src) {
			return true
		}
	}

	return false
}

// hasKeywordLimit recursively checks if the node or its descendants contain a LIMIT keyword
// or if the node text contains "LIMIT"
func hasKeywordLimit(node *ts.Node, src string) bool {
	if node == nil {
		return false
	}

	// Check if ERROR node contains LIMIT keyword
	if node.Kind() == "ERROR" {
		text := node.Utf8Text([]byte(src))
		if strings.Contains(text, "LIMIT") {
			return true
		}
	}

	// Check for limit node (if it exists in the grammar)
	if node.Kind() == "limit" || node.Kind() == "keyword_limit" {
		return true
	}

	for i := uint(0); i < node.NamedChildCount(); i++ {
		if hasKeywordLimit(node.NamedChild(i), src) {
			return true
		}
	}

	return false
}

// hasKeywordOffset recursively checks if the node or its descendants contain an OFFSET keyword
// or if the node text contains "OFFSET"
func hasKeywordOffset(node *ts.Node, src string) bool {
	if node == nil {
		return false
	}

	// Check if ERROR node contains OFFSET keyword
	if node.Kind() == "ERROR" {
		text := node.Utf8Text([]byte(src))
		if strings.Contains(text, "OFFSET") {
			return true
		}
	}

	// Check for offset node (if it exists in the grammar)
	if node.Kind() == "offset" || node.Kind() == "keyword_offset" {
		return true
	}

	for i := uint(0); i < node.NamedChildCount(); i++ {
		if hasKeywordOffset(node.NamedChild(i), src) {
			return true
		}
	}

	return false
}

// extractTypedPrefixFromError extracts the last word from an ERROR node
// This is used to get the typed prefix when the user has partially typed a keyword
func extractTypedPrefixFromError(node *ts.Node, src string) string {
	if node == nil || node.Kind() != "ERROR" {
		return ""
	}

	// Get the full text of the ERROR node
	errorText := node.Utf8Text([]byte(src))

	// Find the last word (letters only) in the error text
	var lastWord string
	for i := len(errorText) - 1; i >= 0; i-- {
		c := errorText[i]
		if (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') {
			// Continue collecting letters
			lastWord = string(c) + lastWord
		} else if lastWord != "" {
			// We've found a non-letter after collecting some letters
			break
		}
	}

	return lastWord
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
