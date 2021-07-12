package linter

import (
	"fmt"
	"strings"

	"github.com/skeema/tengo"
)

func init() {
	RegisterRule(Rule{
		CheckerFunc:     TableChecker(hasEnumChecker),
		Name:            "has-enum",
		Description:     "Flag columns using ENUM data types",
		DefaultSeverity: SeverityIgnore,
	})
}

func hasEnumChecker(table *tengo.Table, createStatement string, _ *tengo.Schema, _ Options) []Note {
	results := make([]Note, 0)
	for _, col := range table.Columns {
		if strings.Contains(col.TypeInDB, "enum") {
			message := fmt.Sprintf(
				"Column %s of table %s is using type %s. ENUM is not a recommended type.",
				col.Name, table.Name, col.TypeInDB,
			)
			results = append(results, Note{
				LineOffset: FindColumnLineOffset(col, createStatement),
				Summary:    "Column using enum type",
				Message:    message,
			})
		}
	}
	return results
}
