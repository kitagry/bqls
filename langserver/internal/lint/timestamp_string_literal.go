package lint

import (
	"fmt"
	"regexp"
	"strings"
	"time"
	// Validating time_zone must not depend on the zone database of the host.
	_ "time/tzdata"

	googlesql "github.com/goccy/go-googlesql"
	"github.com/kitagry/bqls/langserver/internal/lsp"
	"github.com/kitagry/bqls/langserver/internal/source/file"
)

var TimestampStringLiteral = &Analyzer{
	Name:           "timestamp-string-literal",
	Doc:            "reports a string literal without a time zone implicitly converted to TIMESTAMP in a comparison",
	DefaultEnabled: false,
	NewOptions:     func() any { return &TimestampStringLiteralOptions{} },
	Run:            runTimestampStringLiteral,
}

type TimestampStringLiteralOptions struct {
	// TimeZone is the zone the literals are usually meant in, offered as a
	// second quick fix.
	TimeZone string `json:"time_zone"`
}

var timeZoneOffset = regexp.MustCompile(`^[+-]\d{1,2}(?::\d{2})?$`)

func (o *TimestampStringLiteralOptions) Validate() error {
	if o.TimeZone == "" || timeZoneOffset.MatchString(o.TimeZone) {
		return nil
	}
	// "Local" is the host's zone in Go, not a name BigQuery knows.
	if _, err := time.LoadLocation(o.TimeZone); err != nil || o.TimeZone == "Local" {
		return fmt.Errorf("time_zone must be a time zone name (e.g. Asia/Tokyo) or an offset (e.g. +09:00), got %q", o.TimeZone)
	}
	return nil
}

var timestampComparisonFunctions = map[string]bool{
	"$equal":                true,
	"$not_equal":            true,
	"$less":                 true,
	"$less_or_equal":        true,
	"$greater":              true,
	"$greater_or_equal":     true,
	"$between":              true,
	"$not_between":          true,
	"$in":                   true,
	"$not_in":               true,
	"$is_distinct_from":     true,
	"$is_not_distinct_from": true,
}

// timestampWithoutZone matches the canonical TIMESTAMP literal formats that
// carry no time zone. Anything else (an offset, `Z`, a zone name, or a format
// this does not know) is left alone, since the literal already parsed as a
// TIMESTAMP and the extra text can only be a zone.
var timestampWithoutZone = regexp.MustCompile(`^\d{4}-\d{1,2}-\d{1,2}(?:[ Tt]\d{1,2}:\d{1,2}(?::\d{1,2}(?:\.\d+)?)?)?$`)

func runTimestampStringLiteral(pass *Pass) {
	if setsTimeZone(pass) {
		return
	}
	zones := fixZones(pass.Options.(*TimestampStringLiteralOptions).TimeZone)
	for _, output := range pass.File.RNode {
		for _, call := range file.ListResolvedAstNode[*googlesql.ResolvedFunctionCall](output) {
			name, ok := functionName(call)
			if !ok || !timestampComparisonFunctions[name] {
				continue
			}
			args, err := call.ArgumentList()
			if err != nil {
				continue
			}
			for _, arg := range args {
				checkTimestampStringLiteral(pass, arg, zones)
			}
		}
	}
}

// fixZones lists the time zones offered as fixes: UTC, which keeps the
// result, then the configured zone, if any.
func fixZones(tz string) []string {
	zones := []string{"UTC"}
	if tz != "" && !strings.EqualFold(tz, "UTC") {
		zones = append(zones, tz)
	}
	return zones
}

// setsTimeZone reports whether the script sets @@time_zone, which BigQuery
// uses instead of UTC to interpret these literals.
func setsTimeZone(pass *Pass) bool {
	for _, assign := range file.ListAstNode[*googlesql.ASTSystemVariableAssignment](pass.File.Node) {
		v, err := assign.SystemVariable()
		if err != nil || v == nil {
			continue
		}
		path, err := v.Path()
		if err != nil || path == nil {
			continue
		}
		o := astOffsets(path)
		if o.start < 0 || o.end > len(pass.File.FixedSrc) {
			continue
		}
		if strings.EqualFold(strings.Trim(pass.File.FixedSrc[o.start:o.end], "`"), "time_zone") {
			return true
		}
	}
	return false
}

// zones lists the time zones offered as fixes. The first is UTC, which the
// literal is read in today, so applying it keeps the result.
func checkTimestampStringLiteral(pass *Pass, expr googlesql.ResolvedExprNode, zones []string) {
	lit, ok := expr.(*googlesql.ResolvedLiteral)
	if !ok {
		return
	}
	typ, err := lit.Type()
	if err != nil || typ == nil {
		return
	}
	if isTimestamp, err := typ.IsTimestamp(); err != nil || !isTimestamp {
		return
	}
	loc, err := lit.GetParseLocationRangeOrNULL()
	if err != nil || loc == nil {
		return
	}
	// Only a bare string literal is implicitly converted: TIMESTAMP '...' and
	// CAST('...' AS TIMESTAMP) state the type on purpose, and declared
	// variables are analyzed as substituted literals.
	src, ok := pass.File.ExtractSQL(loc)
	if !ok {
		return
	}
	content, ok := stringLiteralContent(src)
	if !ok || !timestampWithoutZone.MatchString(strings.TrimSpace(content)) {
		return
	}
	r, ok := pass.File.ToLspRange(loc)
	if !ok {
		return
	}
	fixes := make([]SuggestedFix, len(zones))
	for i, zone := range zones {
		text := fmt.Sprintf("TIMESTAMP(%s, '%s')", src, zone)
		fixes[i] = SuggestedFix{
			Title: "Replace with " + text,
			Edits: []lsp.TextEdit{{Range: r, NewText: text}},
		}
	}
	pass.Report(Diagnostic{
		Range:          r,
		Message:        fmt.Sprintf("The string literal %s has no time zone, so it is converted to TIMESTAMP in the default time zone (UTC), not local time. Add an offset or a time zone name (e.g. '2024-01-01 00:00:00+09', TIMESTAMP('2024-01-01', 'Asia/Tokyo')), or write TIMESTAMP '...' if UTC is intended", src),
		Severity:       SeverityWarning,
		SuggestedFixes: fixes,
	})
}

// stringLiteralContent returns the text between the quotes of a (raw) string
// literal.
func stringLiteralContent(src string) (string, bool) {
	s := strings.TrimPrefix(strings.TrimPrefix(src, "r"), "R")
	for _, q := range []string{`'''`, `"""`, `'`, `"`} {
		if len(s) >= 2*len(q) && strings.HasPrefix(s, q) && strings.HasSuffix(s, q) {
			return s[len(q) : len(s)-len(q)], true
		}
	}
	return "", false
}
