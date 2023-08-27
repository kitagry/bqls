package bigquery

import (
	"sort"
	"strconv"
	"strings"
	"unicode"

	"cloud.google.com/go/bigquery"
)

func extractLatestSuffixTables(tables []*bigquery.Table) []*bigquery.Table {
	type tableWithSuffix struct {
		suffix int
		table  *bigquery.Table
	}

	maxSuffixTables := make(map[string]tableWithSuffix)
	for _, table := range tables {
		var filterdIntStr string
		t := strings.TrimRightFunc(table.TableID, func(r rune) bool {
			if !unicode.IsDigit(r) {
				return false
			}
			filterdIntStr = string(r) + filterdIntStr
			return true
		})
		suffix, err := strconv.Atoi(filterdIntStr)
		if err != nil {
			maxSuffixTables[table.TableID] = tableWithSuffix{
				suffix: 0,
				table:  table,
			}
			continue
		}
		maxTable, ok := maxSuffixTables[t]
		if !ok {
			maxSuffixTables[t] = tableWithSuffix{
				suffix: suffix,
				table:  table,
			}
			continue
		}

		if maxTable.suffix < suffix {
			maxSuffixTables[t] = tableWithSuffix{
				suffix: suffix,
				table:  table,
			}
		}
	}

	filteredTables := make([]*bigquery.Table, 0)
	for _, t := range maxSuffixTables {
		filteredTables = append(filteredTables, t.table)
	}

	sort.Slice(filteredTables, func(i, j int) bool {
		return filteredTables[i].TableID < filteredTables[j].TableID
	})

	return filteredTables
}
