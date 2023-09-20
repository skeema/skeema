package linter

import (
	"fmt"
	"strings"

	"github.com/skeema/skeema/internal/tengo"
)

func init() {
	RegisterRule(Rule{
		CheckerFunc:     TableChecker(hasEnumChecker),
		Name:            "has-enum",
		Description:     "Flag columns using ENUM or SET data types",
		DefaultSeverity: SeverityIgnore,
	})
}

func hasEnumChecker(table *tengo.Table, createStatement string, _ *tengo.Schema, _ *Options) []Note {
	results := make([]Note, 0)
	for _, col := range table.Columns {
		if strings.HasPrefix(col.TypeInDB, "enum") || strings.HasPrefix(col.TypeInDB, "set") {
			// col.TypeInDB includes the full list of allowed enum/set values, which may be overly long
			typeWithoutValues := "enum"
			if strings.HasPrefix(col.TypeInDB, "set") {
				typeWithoutValues = "set"
			}
			message := fmt.Sprintf(
				"Column %s of %s is using type %s. This data type can cause operational difficulties due to lack of flexibility, and may be prone to subtle errors.",
				col.Name, table.ObjectKey(), typeWithoutValues,
			)
			results = append(results, Note{
				LineOffset: FindColumnLineOffset(col, createStatement),
				Summary:    fmt.Sprintf("Column using %s type", typeWithoutValues),
				Message:    message,
			})
		}
	}
	return results
}
