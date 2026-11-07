package lint

import (
	"regexp"
	"slices"
	"strings"
	"time"

	googlesql "github.com/goccy/go-googlesql"
	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/kitagry/bqls/langserver/internal/source/file"
)

var DateBoundOnDatetime = &Analyzer{
	Name:           "date-bound-on-datetime",
	Doc:            "reports DATETIME / TIMESTAMP values whose upper bound or equality is written as a date, which stands for midnight",
	DefaultEnabled: true,
	Run:            runDateBoundOnDatetime,
}

// dateBoundKind is how a date bound misses the rest of its day.
type dateBoundKind int

const (
	// boundUpper is an inclusive upper bound (x <= date), which leaves out
	// the rest of the day.
	boundUpper dateBoundKind = iota
	// boundAfter is an exclusive lower bound (x > date), which keeps it.
	boundAfter
	// boundEqual matches midnight only.
	boundEqual
	// boundNotEqual leaves out midnight only.
	boundNotEqual
)

func dateBoundOnDatetimeMessage(typ string, kind dateBoundKind) string {
	msg := "this " + typ + " is compared with a date, which stands for midnight at the start of that day, so "
	switch kind {
	case boundUpper:
		return msg + "the rest of that day is left out; compare with < the next day instead"
	case boundAfter:
		return msg + "the rest of that day is kept; compare with >= the next day instead"
	case boundNotEqual:
		return msg + "only midnight is left out; compare the date part instead, e.g. DATE(x) != ..."
	}
	return msg + "only midnight matches; compare the date part instead, e.g. DATE(x) = ..."
}

// dateBoundComparisons maps each comparison to the kind of bound its second
// and its first argument would be. The first argument of `date >= x` is an
// upper bound of x, for example. A zero entry is a correct bound.
var dateBoundComparisons = map[string][2]*dateBoundKind{
	"$less_or_equal":        {ptr(boundUpper), nil},
	"$greater_or_equal":     {nil, ptr(boundUpper)},
	"$greater":              {ptr(boundAfter), nil},
	"$less":                 {nil, ptr(boundAfter)},
	"$equal":                {ptr(boundEqual), ptr(boundEqual)},
	"$is_not_distinct_from": {ptr(boundEqual), ptr(boundEqual)},
	"$not_equal":            {ptr(boundNotEqual), ptr(boundNotEqual)},
	"$is_distinct_from":     {ptr(boundNotEqual), ptr(boundNotEqual)},
}

func ptr[T any](v T) *T { return &v }

// fixedOperators is the operator that makes the next day the bound, for a
// bound on the right and on the left.
var fixedOperators = map[dateBoundKind][2]string{
	boundUpper: {"<", ">"},
	boundAfter: {">=", "<="},
}

var (
	dateKeyword = regexp.MustCompile(`(?i)^DATE\s*`)
	dateOnly    = regexp.MustCompile(`^\d{4}-\d{1,2}-\d{1,2}$`)
)

func runDateBoundOnDatetime(pass *Pass) {
	binaries := make(map[offsets]*googlesql.ASTBinaryExpression)
	for _, b := range file.ListAstNode[*googlesql.ASTBinaryExpression](pass.File.Node) {
		binaries[astOffsets(b)] = b
	}
	for _, output := range pass.File.RNode {
		for _, call := range file.ListResolvedAstNode[*googlesql.ResolvedFunctionCall](output) {
			name, ok := functionName(call)
			if !ok {
				continue
			}
			args, err := call.ArgumentList()
			if err != nil {
				continue
			}
			loc := resolvedOffsets(call)
			if loc.start < 0 {
				continue
			}
			if name == "$between" && len(args) == 3 {
				if typ, ok := dateBounded(args[0]); ok {
					if _, ok := dateBound(pass, args[2]); ok {
						reportDateBound(pass, loc, typ, boundUpper, nil)
					}
				}
				continue
			}
			listKind := boundEqual
			if name == "$not" && len(args) == 1 {
				// `x NOT IN (...)` resolves to $not of an $in that has no
				// location of its own; `NOT (x IN (...))` is reported on the $in.
				in, ok := args[0].(*googlesql.ResolvedFunctionCall)
				if !ok || resolvedOffsets(in).start >= 0 {
					continue
				}
				if n, ok := functionName(in); !ok || n != "$in" {
					continue
				}
				if args, err = in.ArgumentList(); err != nil {
					continue
				}
				name, listKind = "$in", boundNotEqual
			}
			if name == "$in" && len(args) >= 2 {
				if typ, ok := dateBounded(args[0]); ok && slices.ContainsFunc(args[1:], func(e googlesql.ResolvedExprNode) bool {
					_, ok := dateBound(pass, e)
					return ok
				}) {
					reportDateBound(pass, loc, typ, listKind, nil)
				}
				continue
			}
			kinds, ok := dateBoundComparisons[name]
			if !ok || len(args) != 2 {
				continue
			}
			for side, bound := range []int{1, 0} {
				kind := kinds[side]
				if kind == nil {
					continue
				}
				typ, ok := dateBounded(args[1-bound])
				if !ok {
					continue
				}
				lit, ok := dateBound(pass, args[bound])
				if !ok {
					continue
				}
				var fix *SuggestedFix
				if lit != nil {
					fix = nextDayFix(pass, binaries[loc], *kind, side, *lit)
				}
				reportDateBound(pass, loc, typ, *kind, fix)
				break
			}
		}
	}
}

// dateBounded returns the type of a DATETIME or TIMESTAMP value that is
// not a literal and can fall after midnight.
func dateBounded(expr googlesql.ResolvedExprNode) (string, bool) {
	if _, ok := expr.(*googlesql.ResolvedLiteral); ok {
		return "", false
	}
	typ, ok := exprTypeName(expr)
	return typ, ok && (typ == "DATETIME" || typ == "TIMESTAMP") && !dayAligned(expr) && !isPartitionTime(expr)
}

// isPartitionTime reports whether expr is the _PARTITIONTIME pseudo column,
// which is midnight on the usual daily partitioned tables, so comparing it
// with a date is the documented way to filter it. bqls does not tell the
// rarer hourly partitioning apart.
func isPartitionTime(expr googlesql.ResolvedExprNode) bool {
	ref, ok := expr.(*googlesql.ResolvedColumnRef)
	if !ok {
		return false
	}
	col, err := ref.Column()
	if err != nil || col == nil {
		return false
	}
	name, err := col.Name()
	return err == nil && strings.EqualFold(name, "_PARTITIONTIME")
}

// dayAligned reports whether expr is always midnight, a DATE converted to
// DATETIME or TIMESTAMP or a value truncated to a day or longer, which a date
// bounds exactly.
func dayAligned(expr googlesql.ResolvedExprNode) bool {
	call, ok := expr.(*googlesql.ResolvedFunctionCall)
	if !ok {
		return false
	}
	name, ok := functionName(call)
	if !ok {
		return false
	}
	args, err := call.ArgumentList()
	if err != nil {
		return false
	}
	switch name {
	case "datetime", "timestamp":
		// With a time zone, TIMESTAMP(date, zone) is midnight there, which a
		// date read in the default time zone does not match.
		if len(args) != 1 {
			return false
		}
		typ, ok := exprTypeName(args[0])
		return ok && typ == "DATE"
	case "datetime_trunc", "timestamp_trunc":
		if len(args) != 2 {
			return false
		}
		part, ok := literalString(args[1])
		return ok && !subDayParts[part]
	}
	return false
}

// dateBoundLiteral is a date written as a literal, which a fix can move to
// the next day.
type dateBoundLiteral struct {
	at offsets
	// prefix is the literal up to the date, and suffix the closing quote.
	prefix, date, suffix string
}

// dateBound reports whether expr is a date compared as a DATETIME or
// TIMESTAMP, and returns it when it is a literal.
func dateBound(pass *Pass, expr googlesql.ResolvedExprNode) (*dateBoundLiteral, bool) {
	if inner, ok := implicitCastOperand(expr); ok {
		from, ok := exprTypeName(inner)
		return nil, ok && from == "DATE"
	}
	lit, ok := expr.(*googlesql.ResolvedLiteral)
	if !ok {
		return nil, false
	}
	at := resolvedOffsets(lit)
	if !inBounds(pass.File.FixedSrc, at) {
		return nil, false
	}
	text := pass.File.FixedSrc[at.start:at.end]
	explicit, err := lit.HasExplicitType()
	if err != nil {
		return nil, false
	}
	// A DATE literal folded into a DATETIME keeps its explicit type, while
	// a string literal converted to one does not.
	quoted := text
	if explicit {
		m := dateKeyword.FindString(text)
		if m == "" {
			return nil, false
		}
		quoted = text[len(m):]
	}
	date, ok := stringLiteralContent(quoted)
	if !ok || !dateOnly.MatchString(date) {
		return nil, false
	}
	i := len(text) - len(quoted) + strings.Index(quoted, date)
	return &dateBoundLiteral{at: at, prefix: text[:i], date: date, suffix: text[i+len(date):]}, true
}

// nextDayFix moves the bound to the next day and makes the comparison
// exclusive (or inclusive for x > date), rewriting only the operator and the
// literal so that the other operand keeps its text.
func nextDayFix(pass *Pass, b *googlesql.ASTBinaryExpression, kind dateBoundKind, side int, lit dateBoundLiteral) *SuggestedFix {
	ops, ok := fixedOperators[kind]
	if !ok || b == nil {
		return nil
	}
	d, err := time.Parse("2006-1-2", lit.date)
	if err != nil {
		return nil
	}
	next := d.AddDate(0, 0, 1)
	// 9999-12-31 is the last day a DATE, DATETIME or TIMESTAMP can hold.
	if next.Year() > 9999 {
		return nil
	}
	lhs, err := b.Lhs()
	rhs, err2 := b.Rhs()
	if err != nil || err2 != nil || isNilNode(lhs) || isNilNode(rhs) {
		return nil
	}
	src := pass.File.FixedSrc
	between := offsets{astOffsets(lhs).end, astOffsets(rhs).start}
	if !inBounds(src, between) || containsComment(src[between.start:between.end]) {
		return nil
	}
	// The operands' locations leave out their parentheses, so only the
	// operator itself is replaced.
	text := src[between.start:between.end]
	opStart := strings.IndexAny(text, "<>")
	if opStart < 0 {
		return nil
	}
	opEnd := opStart + len(text[opStart:]) - len(strings.TrimLeft(text[opStart:], "<>="))
	opRange, ok := pass.File.OffsetsToLspRange(between.start+opStart, between.start+opEnd)
	if !ok {
		return nil
	}
	litRange, ok := pass.File.OffsetsToLspRange(lit.at.start, lit.at.end)
	if !ok {
		return nil
	}
	nextLit := lit.prefix + next.Format("2006-01-02") + lit.suffix
	op := ops[side]
	return &SuggestedFix{
		Title: "Compare with " + op + " " + nextLit,
		Edits: []lsp.TextEdit{
			{Range: opRange, NewText: op},
			{Range: litRange, NewText: nextLit},
		},
		ChangesResult: true,
	}
}

func reportDateBound(pass *Pass, loc offsets, typ string, kind dateBoundKind, fix *SuggestedFix) {
	r, ok := pass.File.OffsetsToLspRange(loc.start, loc.end)
	if !ok {
		return
	}
	d := Diagnostic{Range: r, Message: dateBoundOnDatetimeMessage(typ, kind), Severity: SeverityWarning}
	if fix != nil {
		d.SuggestedFixes = []SuggestedFix{*fix}
	}
	pass.Report(d)
}
