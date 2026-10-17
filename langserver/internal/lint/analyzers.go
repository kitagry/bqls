package lint

// Analyzers is every rule bqls ships; Config decides which of them run.
var Analyzers = []*Analyzer{
	NullComparison,
	InNull,
	UnusedCTE,
	PartitionFilter,
}
