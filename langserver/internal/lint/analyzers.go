package lint

// Analyzers is every rule bqls ships; Config decides which of them run.
var Analyzers = []*Analyzer{
	NullComparison,
	InNull,
	NotInNullable,
	UnusedCTE,
	UnusedDeclare,
	UnusedTempFunction,
	UnusedTempFunctionArgument,
	PartitionFilter,
	PartitionFilterNotPrunable,
	OrderByInSubquery,
	UnusedJoin,
	WildcardTableSuffix,
	WildcardTableSuffixNotConstant,
	CrossJoin,
	UnionColumnOrder,
	BytesProcessedThreshold,
	SelectStar,
	LeftJoinAsInner,
	VariableColumnShadow,
}
