package cache

type SQL struct {
	RawText string
}

func NewSQL(rawText string) *SQL {
	return &SQL{RawText: rawText}
}
