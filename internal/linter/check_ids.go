package linter

import (
	"fmt"
	"strings"

	"github.com/skeema/skeema/internal/tengo"
)

// This linter rule is intentionally undocumented. It flags columns whose names
// are "id" or end in "_id" (case insensitive) unless they are bigint unsigned.
// This may be excessively noisy for many users.

func init() {
	RegisterRule(Rule{
		CheckerFunc: TableChecker(idsChecker),
		Name:        "ids",
	})
}

func idsChecker(table *tengo.Table, createStatement string, _ *tengo.Schema, _ *Options) []Note {
	results := make([]Note, 0)
	for _, col := range table.Columns {
		lowerColName := strings.ToLower(col.Name)
		if lowerColName != "id" && !strings.HasSuffix(lowerColName, "_id") {
			continue
		}

		// Check for bigint unsigned in a way that ignores display width, without
		// needing a regex
		if strings.HasPrefix(col.TypeInDB, "bigint") && strings.HasSuffix(col.TypeInDB, " unsigned") {
			continue
		}

		message := fmt.Sprintf(
			"Column %s of %s is using data type %s. If this column is intended to store an integer ID, please use data type bigint unsigned instead.",
			col.Name, table.ObjectKey(), col.TypeInDB,
		)
		results = append(results, Note{
			LineOffset: FindColumnLineOffset(col, createStatement),
			Summary:    "Wrong data type for ID column",
			Message:    message,
		})
	}
	return results
}
