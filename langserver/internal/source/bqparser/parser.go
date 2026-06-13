package bqparser

import "strings"

// Node represents an AST node for BigQuery SQL.
type Node struct {
	kind      string
	startByte uint
	endByte   uint
	children  []*Node
}

func (n *Node) Kind() string                      { return n.kind }
func (n *Node) StartByte() uint                   { return n.startByte }
func (n *Node) EndByte() uint                     { return n.endByte }
func (n *Node) ChildCount() uint                  { return uint(len(n.children)) }
func (n *Node) NamedChildCount() uint             { return uint(len(n.children)) }
func (n *Node) FieldNameForChild(_ uint32) string { return "" }

func (n *Node) NamedChild(i uint) *Node {
	if i >= uint(len(n.children)) {
		return nil
	}
	return n.children[i]
}

// Parse parses a BigQuery SQL string and returns the root "program" node.
func Parse(sql string) *Node {
	if sql == "" {
		return &Node{kind: "program"}
	}
	p := &parser{src: sql, tokens: tokenize(sql)}
	return p.parseProgram()
}

// --- tokenizer ---

type tokKind int

const (
	tokWord   tokKind = iota
	tokLParen         // (
	tokRParen         // )
	tokComma          // ,
	tokEOF
)

type token struct {
	kind  tokKind
	text  string
	start uint
	end   uint
}

func tokenize(sql string) []token {
	var tokens []token
	i := 0
	n := len(sql)

	for i < n {
		if isSpace(sql[i]) {
			i++
			continue
		}

		// line comment
		if i+1 < n && sql[i] == '-' && sql[i+1] == '-' {
			for i < n && sql[i] != '\n' {
				i++
			}
			continue
		}

		// block comment
		if i+1 < n && sql[i] == '/' && sql[i+1] == '*' {
			i += 2
			for i+1 < n && !(sql[i] == '*' && sql[i+1] == '/') {
				i++
			}
			i += 2
			continue
		}

		// quoted strings / backtick identifiers — treat as opaque words
		if sql[i] == '\'' || sql[i] == '"' || sql[i] == '`' {
			start := i
			quote := sql[i]
			i++
			for i < n && sql[i] != quote {
				if sql[i] == '\\' {
					i++
				}
				i++
			}
			if i < n {
				i++
			}
			tokens = append(tokens, token{kind: tokWord, text: sql[start:i], start: uint(start), end: uint(i)})
			continue
		}

		switch sql[i] {
		case '(':
			tokens = append(tokens, token{kind: tokLParen, text: "(", start: uint(i), end: uint(i + 1)})
			i++
		case ')':
			tokens = append(tokens, token{kind: tokRParen, text: ")", start: uint(i), end: uint(i + 1)})
			i++
		case ',':
			tokens = append(tokens, token{kind: tokComma, text: ",", start: uint(i), end: uint(i + 1)})
			i++
		default:
			start := i
			for i < n && !isSpace(sql[i]) && sql[i] != '(' && sql[i] != ')' && sql[i] != ',' && sql[i] != '\'' && sql[i] != '"' && sql[i] != '`' {
				i++
			}
			if i > start {
				tokens = append(tokens, token{kind: tokWord, text: sql[start:i], start: uint(start), end: uint(i)})
			}
		}
	}

	return tokens
}

func isSpace(b byte) bool {
	return b == ' ' || b == '\t' || b == '\n' || b == '\r'
}

// --- parser ---

type parser struct {
	src    string
	tokens []token
	pos    int
}

func (p *parser) peek() token {
	if p.pos >= len(p.tokens) {
		return token{kind: tokEOF}
	}
	return p.tokens[p.pos]
}

func (p *parser) peekN(n int) token {
	if p.pos+n >= len(p.tokens) {
		return token{kind: tokEOF}
	}
	return p.tokens[p.pos+n]
}

func (p *parser) consume() token {
	t := p.peek()
	if p.pos < len(p.tokens) {
		p.pos++
	}
	return t
}

func (p *parser) parseProgram() *Node {
	root := &Node{kind: "program", startByte: 0, endByte: uint(len(p.src))}
	for p.peek().kind != tokEOF {
		stmt := p.parseTopLevelStatement()
		if stmt != nil {
			root.children = append(root.children, stmt)
		} else {
			p.consume()
		}
	}
	return root
}

func (p *parser) parseTopLevelStatement() *Node {
	switch strings.ToUpper(p.peek().text) {
	case "WITH":
		return p.parseCTEStatement()
	case "SELECT":
		return p.parseSelectStatement()
	}
	return nil
}

// parseCTEStatement parses WITH ... SELECT ...
func (p *parser) parseCTEStatement() *Node {
	startByte := p.peek().start
	p.consume() // consume WITH

	withClause := &Node{kind: "with_clause", startByte: startByte}

	for {
		cte := p.parseCTEDefinition()
		if cte == nil {
			break
		}
		withClause.children = append(withClause.children, cte)
		if p.peek().kind == tokComma {
			p.consume()
		} else {
			break
		}
	}

	withClause.endByte = p.peek().start

	mainSelect := p.parseSelectStatement()
	if mainSelect == nil {
		return &Node{kind: "select_statement", startByte: startByte, endByte: withClause.endByte, children: []*Node{withClause}}
	}

	stmt := &Node{
		kind:      "select_statement",
		startByte: startByte,
		endByte:   mainSelect.endByte,
	}
	stmt.children = append(stmt.children, withClause)
	stmt.children = append(stmt.children, mainSelect.children...)
	return stmt
}

// parseCTEDefinition parses one "name AS (query)" block.
func (p *parser) parseCTEDefinition() *Node {
	nameTok := p.peek()
	if nameTok.kind == tokEOF || nameTok.kind == tokLParen || nameTok.kind == tokRParen {
		return nil
	}
	if strings.ToUpper(nameTok.text) == "SELECT" {
		return nil
	}

	cteStart := nameTok.start
	p.consume() // name

	if strings.ToUpper(p.peek().text) != "AS" {
		return nil
	}
	p.consume() // AS

	if p.peek().kind != tokLParen {
		return nil
	}
	p.consume() // (

	innerSelect := p.parseSelectStatement()

	var cteEnd uint
	if p.peek().kind == tokRParen {
		cteEnd = p.peek().end
		p.consume() // )
	} else if innerSelect != nil {
		cteEnd = innerSelect.endByte
	} else {
		cteEnd = uint(len(p.src))
	}

	cte := &Node{kind: "cte", startByte: cteStart, endByte: cteEnd}
	if innerSelect != nil {
		cte.children = append(cte.children, innerSelect)
	}
	return cte
}

// clauseKeyword returns (clauseKind, tokensToConsume) for clause-starting keywords.
func clauseKeyword(t, next token) (string, int) {
	switch strings.ToUpper(t.text) {
	case "SELECT":
		return "select_clause", 1
	case "FROM":
		return "from_clause", 1
	case "WHERE":
		return "where_clause", 1
	case "GROUP":
		if strings.ToUpper(next.text) == "BY" {
			return "group_by_clause", 2
		}
	case "HAVING":
		return "having_clause", 1
	case "ORDER":
		if strings.ToUpper(next.text) == "BY" {
			return "order_by_clause", 2
		}
	case "LIMIT":
		return "limit_clause", 1
	case "OFFSET":
		return "offset_clause", 1
	case "QUALIFY":
		return "qualify_clause", 1
	}
	return "", 0
}

// parseSelectStatement parses from the current SELECT keyword until EOF or an unmatched ')'.
func (p *parser) parseSelectStatement() *Node {
	startTok := p.peek()
	if startTok.kind == tokEOF {
		return nil
	}

	stmt := &Node{kind: "select_statement", startByte: startTok.start}
	var currentClause *Node
	depth := 0

	flushClause := func(end uint) {
		if currentClause != nil {
			currentClause.endByte = trimEnd(p.src, end)
			stmt.children = append(stmt.children, currentClause)
			currentClause = nil
		}
	}

	for {
		t := p.peek()
		if t.kind == tokEOF {
			break
		}
		// unmatched ')' signals end of this select (CTE body or subquery)
		if t.kind == tokRParen && depth == 0 {
			break
		}
		if t.kind == tokLParen {
			depth++
			p.consume()
			continue
		}
		if t.kind == tokRParen {
			depth--
			p.consume()
			continue
		}

		if depth == 0 && t.kind == tokWord {
			if kind, consumed := clauseKeyword(t, p.peekN(1)); kind != "" {
				flushClause(t.start)
				clauseStart := t.start
				for i := 0; i < consumed; i++ {
					p.consume()
				}
				currentClause = &Node{kind: kind, startByte: clauseStart}
				continue
			}
		}

		p.consume()
	}

	endByte := p.peek().start
	if p.peek().kind == tokEOF {
		endByte = uint(len(p.src))
	}
	flushClause(endByte)
	stmt.endByte = trimEnd(p.src, endByte)

	if len(stmt.children) == 0 {
		return nil
	}
	return stmt
}

// FromClauseHasJoinWithoutOn reports whether fromClause contains a JOIN keyword
// not matched by an ON condition, by scanning tokens at depth 0.
func FromClauseHasJoinWithoutOn(fromClause *Node, src string) bool {
	if fromClause == nil {
		return false
	}
	text := src[fromClause.StartByte():fromClause.EndByte()]
	tokens := tokenize(text)
	depth := 0
	joinCount := 0
	onCount := 0
	for _, t := range tokens {
		switch t.kind {
		case tokLParen:
			depth++
		case tokRParen:
			depth--
		case tokWord:
			if depth == 0 {
				switch strings.ToUpper(t.text) {
				case "JOIN":
					joinCount++
				case "ON":
					onCount++
				}
			}
		}
	}
	return joinCount > onCount
}

func trimEnd(src string, end uint) uint {
	for end > 0 && isSpace(src[end-1]) {
		end--
	}
	return end
}
