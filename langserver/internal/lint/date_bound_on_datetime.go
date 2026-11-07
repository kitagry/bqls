package lint

import (
	"regexp"
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
)

func dateBoundOnDatetimeMessage(typ string, kind dateBoundKind) string {
	msg := "this " + typ + " is compared with a date, which stands for midnight at the start of that day, so "
	switch kind {
	case boundUpper:
		return msg + "the rest of that day is left out; compare with < the next day instead"
	case boundAfter:
		return msg + "the rest of that day is kept; compare with >= the next day instead"
	}
	return msg + "only midnight matches; compare the date part instead, e.g. DATE(x) = ..."
}

// dateBoundComparisons maps each comparison to the kind of bound its second
// and its first argument would be. The first argument of `date >= x` is an
// upper bound of x, for example. A zero entry is a correct bound.
var dateBoundComparisons = map[string][2]*dateBoundKind{
	"$less_or_equal":    {ptr(boundUpper), nil},
	"$greater_or_equal": {nil, ptr(boundUpper)},
	"$greater":          {ptr(boundAfter), nil},
	"$less":             {nil, ptr(boundAfter)},
	"$equal":            {ptr(boundEqual), ptr(boundEqual)},
	"$not_equal":        {ptr(boundEqual), ptr(boundEqual)},
}

func ptr[T any](v T) *T { return &v }

// fixedOperators is the operator that makes the next day the bound, for a
// bound on the right and on the left.
var fixedOperators = map[dateBoundKind][2]string{
	boundUpper: {"<", ">"},
	boundAfter: {">=", "<="},
}

var (
	dateLiteral       = regexp.MustCompile(`(?i)^DATE\s*(['"])(\d{4}-\d{1,2}-\d{1,2})(['"])$`)
	dateStringLiteral = regexp.MustCompile(`^(['"])(\d{4}-\d{1,2}-\d{1,2})(['"])$`)
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
// not a literal.
func dateBounded(expr googlesql.ResolvedExprNode) (string, bool) {
	if _, ok := expr.(*googlesql.ResolvedLiteral); ok {
		return "", false
	}
	typ, ok := exprTypeName(expr)
	return typ, ok && (typ == "DATETIME" || typ == "TIMESTAMP")
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
	re := dateStringLiteral
	if explicit {
		re = dateLiteral
	}
	m := re.FindStringSubmatch(text)
	if m == nil || m[1] != m[3] {
		return nil, false
	}
	i := strings.LastIndex(text, m[2])
	return &dateBoundLiteral{at: at, prefix: text[:i], date: m[2], suffix: m[3]}, true
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
	lhs, err := b.Lhs()
	rhs, err2 := b.Rhs()
	if err != nil || err2 != nil || isNilNode(lhs) || isNilNode(rhs) {
		return nil
	}
	between := offsets{astOffsets(lhs).end, astOffsets(rhs).start}
	if between.start < 0 || between.end < between.start || containsComment(pass.File.FixedSrc[between.start:between.end]) {
		return nil
	}
	opRange, ok := pass.File.OffsetsToLspRange(between.start, between.end)
	if !ok {
		return nil
	}
	litRange, ok := pass.File.OffsetsToLspRange(lit.at.start, lit.at.end)
	if !ok {
		return nil
	}
	next := lit.prefix + d.AddDate(0, 0, 1).Format("2006-01-02") + lit.suffix
	op := ops[side]
	return &SuggestedFix{
		Title: "Compare with " + op + " " + next,
		Edits: []lsp.TextEdit{
			{Range: opRange, NewText: " " + op + " "},
			{Range: litRange, NewText: next},
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
