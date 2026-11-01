package lint

// Analyzers is every rule bqls ships; Config decides which of them run.
var Analyzers = []*Analyzer{
	NullComparison,
	InNull,
	UnusedCTE,
	UnusedDeclare,
	PartitionFilter,
	PartitionFilterNotPrunable,
	OrderByInSubquery,
	UnusedJoin,
	WildcardTableSuffix,
	CrossJoin,
	BytesProcessedThreshold,
	SelectStar,
}
