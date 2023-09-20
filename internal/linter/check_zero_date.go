package linter

import (
	"fmt"
	"strings"

	"github.com/skeema/skeema/internal/tengo"
)

func init() {
	RegisterRule(Rule{
		CheckerFunc:     TableChecker(zeroDateChecker),
		Name:            "zero-date",
		Description:     "Flag DATE, DATETIME, and TIMESTAMP columns that have zero-date default values",
		DefaultSeverity: SeverityWarning,
	})
}

func zeroDateChecker(table *tengo.Table, createStatement string, _ *tengo.Schema, _ *Options) []Note {
	results := make([]Note, 0)
	for _, col := range table.Columns {
		if strings.HasPrefix(col.TypeInDB, "timestamp") || strings.HasPrefix(col.TypeInDB, "date") {
			var summary, subject string
			if strings.HasPrefix(col.Default, "'0000-00-00") {
				summary = "Default value is zero date"
				subject = "Zero dates"
			} else if strings.HasPrefix(col.Default, "'0000-") || strings.Contains(col.Default, "-00") {
				summary = "Default value contains zero in date"
				subject = "Dates with zero year, month, or day"
			}
			if summary != "" {
				// Depending on the flavor and/or use of explicit_defaults_for_timestamp,
				// timestamp columns must explicitly be declared NULL to permit DEFAULT NULL
				var recoNullable string
				if strings.HasPrefix(col.TypeInDB, "timestamp") {
					recoNullable = "NULL "
				}
				results = append(results, Note{
					LineOffset: FindColumnLineOffset(col, createStatement),
					Summary:    summary,
					Message:    fmt.Sprintf("Column %s of %s has a default value of %s. %s prevent use of strict sql_mode, which provides important safety checks. Consider making the column %sDEFAULT NULL instead.", col.Name, table.ObjectKey(), col.Default, subject, recoNullable),
				})
			}
		}
	}
	return results
}
