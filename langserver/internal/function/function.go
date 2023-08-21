package function

import "strings"

//go:generate go run gen_builtin_function.go

type BuiltInFunction struct {
	Name        string
	Method      string
	Description string
	ExampleSQLs []string
	URL         string
}

var nameToFunction = make(map[string]BuiltInFunction, len(BuiltInFunctions))

func init() {
	for _, f := range BuiltInFunctions {
		nameToFunction[strings.ToLower(f.Name)] = f
	}
}

func FindBuiltInFunction(name string) (BuiltInFunction, bool) {
	f, ok := nameToFunction[strings.ToLower(name)]
	return f, ok
}
