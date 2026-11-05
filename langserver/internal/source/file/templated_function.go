package file

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	googlesql "github.com/goccy/go-googlesql"
)

// templatedFunction is a SQL function with ANY TYPE parameters and without
// RETURNS, whose return type depends on the argument types of each call.
//
// GoogleSQL resolves such a function with TemplatedSQLFunction, which
// go-googlesql cannot construct. Instead, the function is registered with a
// fallback signature that takes ARG_TYPE_ARBITRARY for the ANY TYPE
// parameters and returns ARG_TYPE_ARBITRARY. A call matching the fallback
// fails with an error naming the argument types of the call, which lets
// analyzeWithTemplatedFunctions resolve the body with those types, add a
// signature with them and analyze again.
type templatedFunction struct {
	name string
	// params have a nil type for the ANY TYPE parameters.
	params []functionArgColumn
	body   string
	fn     *googlesql.Function
	// fallback is kept last, because GoogleSQL picks the first of the
	// signatures matching a call equally well.
	fallback    *googlesql.FunctionSignature
	specialized []*googlesql.FunctionSignature
	// known is keyed by the argument types of the specialized signatures,
	// and resolving by those whose body is being resolved, which detects
	// a recursive call.
	known     map[string]bool
	resolving map[string]bool
}

// maxTemplatedSpecializations bounds the number of analyses of a statement,
// each of which resolves one more combination of argument types.
const maxTemplatedSpecializations = 64

const nonConcreteResultPrefix = "Non-concrete result signature for non-templated function: "

var errorLocationRe = regexp.MustCompile(`\s*\[at \d+:\d+\]$`)

// analyzeWithTemplatedFunctions runs analyze until every call of a templated
// function in it is resolved with the argument types of the call.
func analyzeWithTemplatedFunctions[T any](a *Analyzer, catalog *Catalog, analyze func() (T, error)) (T, error) {
	var out T
	var err error
	for range maxTemplatedSpecializations {
		out, err = analyze()
		if err == nil {
			return out, nil
		}
		tmpl, typeNames, ok := catalog.templatedCall(err)
		if !ok || tmpl.known[strings.Join(typeNames, ", ")] {
			return out, err
		}
		if serr := a.specializeTemplatedFunction(tmpl, typeNames, catalog); serr != nil {
			return analyzeFailedTemplatedCall(tmpl, serr, analyze)
		}
	}
	return out, err
}

// templatedCall returns the templated function and the argument types of the
// call that err reports as matching the fallback signature.
func (c *Catalog) templatedCall(err error) (*templatedFunction, []string, bool) {
	name, typeNames, ok := parseNonConcreteResultError(err.Error())
	if !ok {
		return nil, nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	tmpl, ok := c.templatedFunctions[strings.ToLower(name)]
	if !ok || len(typeNames) != len(tmpl.params) {
		return nil, nil, false
	}
	return tmpl, typeNames, true
}

// parseNonConcreteResultError parses an error such as
// "Non-concrete result signature for non-templated function: :F (INT64, STRUCT<a STRING>) -> ANY TYPE".
func parseNonConcreteResultError(msg string) (string, []string, bool) {
	rest, ok := strings.CutPrefix(msg, nonConcreteResultPrefix)
	if !ok {
		return "", nil, false
	}
	open := strings.Index(rest, " (")
	end := strings.LastIndex(rest, ") -> ")
	if open < 0 || end < open {
		return "", nil, false
	}
	// The name is prefixed by its empty function group.
	name := strings.TrimPrefix(rest[:open], ":")
	return name, splitTopLevel(rest[open+2 : end]), true
}

// splitTopLevel splits a comma separated list of types, ignoring the commas
// inside STRUCT<...> and quoted field names.
func splitTopLevel(s string) []string {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	var parts []string
	depth, quoted, start := 0, false, 0
	for i, r := range s {
		switch {
		case r == '`':
			quoted = !quoted
		case quoted:
		case r == '<' || r == '(':
			depth++
		case r == '>' || r == ')':
			depth--
		case r == ',' && depth == 0:
			parts = append(parts, strings.TrimSpace(s[start:i]))
			start = i + 1
		}
	}
	return append(parts, strings.TrimSpace(s[start:]))
}

// specializeTemplatedFunction adds a signature of tmpl for the given argument
// types, with the type of its body resolved with them.
func (a *Analyzer) specializeTemplatedFunction(tmpl *templatedFunction, typeNames []string, catalog *Catalog) error {
	key := strings.Join(typeNames, ", ")
	if tmpl.resolving[key] {
		return fmt.Errorf("Function %s calls itself", tmpl.name)
	}
	if tmpl.resolving == nil {
		tmpl.resolving = make(map[string]bool)
	}
	tmpl.resolving[key] = true
	defer delete(tmpl.resolving, key)

	types, err := a.analyzeTypeNames(typeNames, catalog)
	if err != nil {
		return err
	}
	args := make([]functionArgColumn, len(tmpl.params))
	argTypes := make([]*googlesql.FunctionArgumentType, len(tmpl.params))
	for i, p := range tmpl.params {
		args[i] = functionArgColumn{name: p.name, typ: types[i]}
		argTypes[i], err = newFixedArgumentType(types[i])
		if err != nil {
			return err
		}
	}
	typ, err := a.inferFunctionReturnType(tmpl.body, args, catalog)
	if err != nil {
		return err
	}
	retType, err := newFixedArgumentType(typ)
	if err != nil {
		return err
	}
	sig, err := googlesql.NewFunctionSignature3(retType, argTypes, 0)
	if err != nil {
		return err
	}
	if err := tmpl.fn.ResetSignatures(append(append(tmpl.specialized, sig), tmpl.fallback)); err != nil {
		return err
	}
	tmpl.specialized = append(tmpl.specialized, sig)
	if tmpl.known == nil {
		tmpl.known = make(map[string]bool)
	}
	tmpl.known[key] = true
	return nil
}

// analyzeTypeNames resolves the type names in the error messages of
// GoogleSQL, which name FLOAT64 DOUBLE as the internal product mode does.
func (a *Analyzer) analyzeTypeNames(typeNames []string, catalog *Catalog) ([]googlesql.Googlesql_TypeNode, error) {
	langOpt, err := newLanguageOptions()
	if err != nil {
		return nil, err
	}
	if err := langOpt.SetProductMode(googlesql.ProductModeProductInternal); err != nil {
		return nil, err
	}
	opts, err := a.newAnalyzerOptions(langOpt, catalog.TypeFactory())
	if err != nil {
		return nil, err
	}
	types := make([]googlesql.Googlesql_TypeNode, len(typeNames))
	for i, name := range typeNames {
		types[i], err = googlesql.AnalyzeType(name, opts, catalog.node(), catalog.TypeFactory())
		if err != nil {
			return nil, err
		}
	}
	return types, nil
}

// analyzeFailedTemplatedCall reports bodyErr at the call of tmpl whose body
// fails with its argument types. The fallback error has no location, so the
// call is analyzed again without the fallback signature, which then fails
// at the call.
func analyzeFailedTemplatedCall[T any](tmpl *templatedFunction, bodyErr error, analyze func() (T, error)) (T, error) {
	if err := tmpl.fn.ResetSignatures(tmpl.specialized); err != nil {
		var zero T
		return zero, err
	}
	out, err := analyze()
	if rerr := tmpl.fn.ResetSignatures(append(tmpl.specialized, tmpl.fallback)); rerr != nil {
		return out, errors.Join(err, rerr)
	}
	if err == nil {
		// Another signature of the function accepts the arguments by
		// coercing them.
		return out, nil
	}
	msg := err.Error()
	if !isCallOfFunction(msg, tmpl.name) {
		return out, err
	}
	inner := errorLocationRe.ReplaceAllString(bodyErr.Error(), "")
	loc := errorLocationRe.FindString(msg)
	return out, fmt.Errorf("Analysis of function %s failed: %s%s", tmpl.name, inner, loc)
}

// isCallOfFunction reports whether msg is the error GoogleSQL reports when no
// signature of the function name matches a call.
func isCallOfFunction(msg, name string) bool {
	lower := strings.ToLower(msg)
	name = strings.ToLower(name)
	return strings.HasPrefix(lower, "no matching signature for function :"+name) ||
		strings.HasPrefix(lower, "no matching signature for function "+name) ||
		strings.HasPrefix(lower, "function not found: :"+name) ||
		strings.HasPrefix(lower, "function not found: "+name)
}
