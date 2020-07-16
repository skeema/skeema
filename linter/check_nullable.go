package linter

import (
	"fmt"

	"github.com/skeema/tengo"
)

// This linter rule is intentionally undocumented. It flags all columns lacking
// NOT NULL clauses. This may be excessively noisy for many users.

func init() {
	RegisterRule(Rule{
		CheckerFunc: TableChecker(nullableChecker),
		Name:        "nullable",
	})
}

func nullableChecker(table *tengo.Table, createStatement string, _ *tengo.Schema, _ Options) []Note {
	results := make([]Note, 0)
	for _, col := range table.Columns {
		if col.Nullable {
			message := fmt.Sprintf(
				"Column %s of table %s permits NULL values. To prevent this, please add a NOT NULL clause to the column definition.",
				col.Name, table.Name,
			)
			results = append(results, Note{
				LineOffset: FindColumnLineOffset(col, createStatement),
				Summary:    "Column permits NULLs",
				Message:    message,
			})
		}
	}
	return results
}
