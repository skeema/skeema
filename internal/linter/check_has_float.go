package linter

import (
	"fmt"
	"strings"

	"github.com/skeema/skeema/internal/tengo"
)

func init() {
	RegisterRule(Rule{
		CheckerFunc:     TableChecker(hasFloatChecker),
		Name:            "has-float",
		Description:     "Flag columns using FLOAT or DOUBLE data types",
		DefaultSeverity: SeverityIgnore,
	})
}

func hasFloatChecker(table *tengo.Table, createStatement string, _ *tengo.Schema, _ *Options) []Note {
	results := make([]Note, 0)
	for _, col := range table.Columns {
		if strings.HasPrefix(col.TypeInDB, "float") || strings.HasPrefix(col.TypeInDB, "double") {
			message := fmt.Sprintf(
				"Column %s of table %s is using type %s. Floating-point types can only store approximate values. For use-cases requiring exact precision, such as monetary data, use the decimal type instead.",
				col.Name, table.Name, col.TypeInDB,
			)
			results = append(results, Note{
				LineOffset: FindColumnLineOffset(col, createStatement),
				Summary:    "Column using floating point type",
				Message:    message,
			})
		}
	}
	return results
}
