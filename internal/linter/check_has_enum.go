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
		if strings.HasPrefix(col.Type.Base, "enum") || strings.HasPrefix(col.Type.Base, "set") {
			message := fmt.Sprintf(
				"Column %s of %s is using type %s. If the enumerated value list requires future adjustments, this can become an operational burden. Application-side queries must be kept closely in sync with any changes.",
				col.Name, table.ObjectKey(), col.Type.Base,
			)
			results = append(results, Note{
				LineOffset: FindColumnLineOffset(col, createStatement),
				Summary:    fmt.Sprintf("Column using %s type", col.Type.Base),
				Message:    message,
			})
		}
	}
	return results
}
