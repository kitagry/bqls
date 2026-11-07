package lint

import (
	"maps"
	"slices"
	"strings"
	"time"

	googlesql "github.com/goccy/go-googlesql"
	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/kitagry/bqls/langserver/internal/source/file"
)

var TimestampDateWithoutZone = &Analyzer{
	Name:           "timestamp-date-without-zone",
	Doc:            "reports DATE / EXTRACT / TIMESTAMP_TRUNC and friends of a TIMESTAMP without a time zone, which work in UTC",
	DefaultEnabled: false,
	NewOptions:     func() any { return &TimestampDateWithoutZoneOptions{} },
	Run:            runTimestampDateWithoutZone,
}

// TimestampDateWithoutZoneOptions takes the same time_zone as
// timestamp-string-literal, offered as a second quick fix.
type TimestampDateWithoutZoneOptions struct {
	TimestampStringLiteralOptions
}

func timestampDateWithoutZoneMessage(fn string) string {
	return fn + " of a TIMESTAMP without a time zone works in UTC, so dates, hours and day boundaries follow UTC, not local time; pass a time zone (e.g. 'Asia/Tokyo'), or 'UTC' if UTC is intended"
}

// zonelessTimestampFunction describes a call that takes an optional time zone
// after its TIMESTAMP argument.
type zonelessTimestampFunction struct {
	name string
	// ts is the position of the TIMESTAMP argument, and args the number of
	// arguments without a time zone.
	ts, args int
	// precise lists the date parts that every time zone at a whole-hour
	// offset agrees on with UTC, and preciseFractional those that zones at
	// other offsets (e.g. Asia/Kolkata, +05:30) agree on.
	precise, preciseFractional map[string]bool
}

var (
	subMinuteParts = map[string]bool{"NANOSECOND": true, "MICROSECOND": true, "MILLISECOND": true, "SECOND": true}
	subHourParts   = withPart(subMinuteParts, "MINUTE")
	subDayParts    = withPart(subHourParts, "HOUR")
)

func withPart(parts map[string]bool, part string) map[string]bool {
	m := maps.Clone(parts)
	m[part] = true
	return m
}

var zonelessTimestampFunctions = map[string]zonelessTimestampFunction{
	"date":              {name: "DATE", ts: 0, args: 1},
	"datetime":          {name: "DATETIME", ts: 0, args: 1},
	"time":              {name: "TIME", ts: 0, args: 1},
	"format_timestamp":  {name: "FORMAT_TIMESTAMP", ts: 1, args: 2},
	"timestamp_trunc":   {name: "TIMESTAMP_TRUNC", ts: 0, args: 2, precise: subDayParts, preciseFractional: subHourParts},
	"$extract":          {name: "EXTRACT", ts: 0, args: 2, precise: subHourParts, preciseFractional: subMinuteParts},
	"$extract_date":     {name: "EXTRACT", ts: 0, args: 1},
	"$extract_datetime": {name: "EXTRACT", ts: 0, args: 1},
	"$extract_time":     {name: "EXTRACT", ts: 0, args: 1},
}

func runTimestampDateWithoutZone(pass *Pass) {
	if setsTimeZone(pass) {
		return
	}
	tz := pass.Options.(*TimestampDateWithoutZoneOptions).TimeZone
	zones := fixZones(tz)
	fractional := !wholeHourZone(tz)
	// A resolved function call is located at its name and EXTRACT at the
	// whole expression, so the whole call comes from the parse tree.
	calls := make(map[offsets]googlesql.ASTNode)
	for _, c := range file.ListAstNode[*googlesql.ASTFunctionCall](pass.File.Node) {
		if fn, err := c.Function(); err == nil && fn != nil {
			calls[astOffsets(fn)] = c
		}
	}
	for _, e := range file.ListAstNode[*googlesql.ASTExtractExpression](pass.File.Node) {
		calls[astOffsets(e)] = e
	}
	var groupBys []offsets
	for _, g := range file.ListAstNode[*googlesql.ASTGroupBy](pass.File.Node) {
		groupBys = append(groupBys, astOffsets(g))
	}
	for _, output := range pass.File.RNode {
		for _, call := range file.ListResolvedAstNode[*googlesql.ResolvedFunctionCall](output) {
			name, ok := functionName(call)
			if !ok {
				continue
			}
			fn, ok := zonelessTimestampFunctions[name]
			if !ok || !zonelessTimestampCall(call, fn, fractional) {
				continue
			}
			astCall, ok := calls[resolvedOffsets(call)]
			if !ok {
				continue
			}
			_, extract := astCall.(*googlesql.ASTExtractExpression)
			reportTimestampDateWithoutZone(pass, astCall, fn.name, extract, zones, groupBys)
		}
	}
}

// fractional is whether the configured time zone is at an offset that is not
// a whole number of hours, which disagrees with UTC on more date parts.
func zonelessTimestampCall(call *googlesql.ResolvedFunctionCall, fn zonelessTimestampFunction, fractional bool) bool {
	args, err := call.ArgumentList()
	if err != nil || len(args) != fn.args {
		return false
	}
	if typ, ok := exprTypeName(args[fn.ts]); !ok || typ != "TIMESTAMP" {
		return false
	}
	precise := fn.precise
	if fractional {
		precise = fn.preciseFractional
	}
	if precise == nil {
		return true
	}
	// The date part follows the TIMESTAMP argument.
	part, ok := literalString(args[fn.ts+1])
	return ok && !precise[part]
}

// wholeHourZone reports whether tz (a validated time_zone option, or none)
// is at an offset from UTC that is a whole number of hours all year.
func wholeHourZone(tz string) bool {
	if tz == "" {
		return true
	}
	if timeZoneOffset.MatchString(tz) {
		_, minutes, _ := strings.Cut(tz, ":")
		return minutes == "" || minutes == "00"
	}
	loc, err := time.LoadLocation(tz)
	if err != nil {
		return true
	}
	year := time.Now().Year()
	// January and July cover both sides of daylight saving time.
	for _, month := range []time.Month{time.January, time.July} {
		if _, offset := time.Date(year, month, 1, 0, 0, 0, 0, loc).Zone(); offset%3600 != 0 {
			return false
		}
	}
	return true
}

func reportTimestampDateWithoutZone(pass *Pass, call googlesql.ASTNode, fn string, extract bool, zones []string, groupBys []offsets) {
	o := astOffsets(call)
	src := pass.File.FixedSrc
	if !inBounds(src, o) || o.end == o.start || src[o.end-1] != ')' {
		return
	}
	r, ok := pass.File.OffsetsToLspRange(o.start, o.end)
	if !ok {
		return
	}
	// The zone goes right before the closing parenthesis.
	at, ok := pass.File.OffsetsToLspRange(o.end-1, o.end-1)
	if !ok {
		return
	}
	d := Diagnostic{
		Range:    r,
		Message:  timestampDateWithoutZoneMessage(fn),
		Severity: SeverityWarning,
	}
	// A SELECT list, HAVING or ORDER BY expression equal to a GROUP BY
	// expression is resolved as a reference to it, so adding the zone to the
	// GROUP BY one alone would leave the others ungrouped.
	if slices.ContainsFunc(groupBys, func(g offsets) bool { return g.start <= o.start && o.end <= g.end }) {
		pass.Report(d)
		return
	}
	d.SuggestedFixes = make([]SuggestedFix, len(zones))
	for i, zone := range zones {
		text := ", '" + zone + "'"
		if extract {
			text = " AT TIME ZONE '" + zone + "'"
		}
		d.SuggestedFixes[i] = SuggestedFix{
			Title: "Add the time zone '" + zone + "'",
			Edits: []lsp.TextEdit{{Range: at, NewText: text}},
			// UTC keeps the result; any other zone changes it.
			ChangesResult: i > 0,
		}
	}
	pass.Report(d)
}
