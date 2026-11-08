package lint

// ColumnFlows exposes Pass.columnFlows so tests can check that it is shared.
func ColumnFlows(pass *Pass) []any {
	flows := pass.columnFlows()
	result := make([]any, len(flows))
	for i, f := range flows {
		if f != nil {
			result[i] = f
		}
	}
	return result
}
