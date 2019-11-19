package linter

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/skeema/tengo"
)

func init() {
	RegisterRule(Rule{
		CheckerFunc:     TableChecker(hasFloatChecker),
		Name:            "has-float",
		Description:     "Flag columns using FLOAT or DOUBLE data types",
		DefaultSeverity: SeverityIgnore,
	})
}

func hasFloatChecker(table *tengo.Table, createStatement string, _ *tengo.Schema, _ Options) []Note {
	results := make([]Note, 0)
	for _, col := range table.Columns {
		if strings.Contains(col.TypeInDB, "float") || strings.Contains(col.TypeInDB, "double") {
			re := regexp.MustCompile(fmt.Sprintf(`\b%s\b`, regexp.QuoteMeta(col.Name)))
			message := fmt.Sprintf(
				"Column %s of table %s is using type %s. Floating-point types can only store approximate values. For use-cases requiring exact precision, such as monetary data, use the decimal type instead.",
				col.Name, table.Name, col.TypeInDB,
			)
			results = append(results, Note{
				LineOffset: FindFirstLineOffset(re, createStatement),
				Summary:    "Column using floating point type",
				Message:    message,
			})
		}
	}
	return results
}
