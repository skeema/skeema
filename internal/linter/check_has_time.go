package linter

import (
	"fmt"
	"strings"

	"github.com/skeema/skeema/internal/tengo"
)

func init() {
	RegisterRule(Rule{
		CheckerFunc:     TableChecker(hasTimeChecker),
		Name:            "has-time",
		Description:     "Flag columns using TIMESTAMP, DATETIME, or TIME data types",
		DefaultSeverity: SeverityIgnore,
	})
}

func hasTimeChecker(table *tengo.Table, createStatement string, _ *tengo.Schema, _ Options) []Note {
	results := make([]Note, 0)
	for _, col := range table.Columns {
		if strings.Contains(col.TypeInDB, "time") {
			message := fmt.Sprintf(
				"Column %s of table %s is using type %s. Temporal data types can be problematic when dealing with timezone conversions, daylight savings time transitions, and leap seconds. Some companies prefer to store time-related values using unsigned ints or unsigned bigints for this reason.",
				col.Name, table.Name, col.TypeInDB,
			)
			results = append(results, Note{
				LineOffset: FindColumnLineOffset(col, createStatement),
				Summary:    "Column using temporal type",
				Message:    message,
			})
		}
	}
	return results
}
