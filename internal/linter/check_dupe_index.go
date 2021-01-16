package linter

import (
	"fmt"
	"regexp"

	"github.com/skeema/tengo"
)

func init() {
	RegisterRule(Rule{
		CheckerFunc:     TableChecker(dupeIndexChecker),
		Name:            "dupe-index",
		Description:     "Flag redundant secondary indexes",
		DefaultSeverity: SeverityWarning,
	})
}

func dupeIndexChecker(table *tengo.Table, createStatement string, _ *tengo.Schema, _ Options) []Note {
	makeNote := func(dupeIndexName, betterIndexName string, equivalent bool) Note {
		re := regexp.MustCompile(fmt.Sprintf("(?i)(key|index)\\s+`?%s(?:`|\\s)", dupeIndexName))
		var reason string
		if equivalent {
			reason = fmt.Sprintf("Indexes %s and %s of table %s are functionally identical.\nOne of them should be dropped.", dupeIndexName, betterIndexName, table.Name)
		} else {
			reason = fmt.Sprintf("Index %s of table %s is redundant to larger index %s.\nConsider dropping index %s.", dupeIndexName, table.Name, betterIndexName, dupeIndexName)
		}
		message := fmt.Sprintf("%s Redundant indexes waste disk space, and harm write performance.", reason)
		return Note{
			LineOffset: FindFirstLineOffset(re, createStatement),
			Summary:    "Redundant index detected",
			Message:    message,
		}
	}
	results := make([]Note, 0)
	for i, idx := range table.SecondaryIndexes {
		if idx.RedundantTo(table.PrimaryKey) {
			results = append(results, makeNote(idx.Name, "PRIMARY", false))
			continue // max one note for each idx
		}
		for j, other := range table.SecondaryIndexes {
			if i != j && idx.RedundantTo(other) {
				equivalent := idx.Equivalent(other)
				if !equivalent || i > j { // avoid 2 annotations for an equivalent pair
					results = append(results, makeNote(idx.Name, other.Name, equivalent))
				}
				break // max one note for each idx
			}
		}
	}
	return results
}
