package linter

import (
	"fmt"

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

func hasTimeChecker(table *tengo.Table, createStatement string, _ *tengo.Schema, opts *Options) []Note {
	results := make([]Note, 0)
	onlyWarning := (opts.RuleSeverity["has-time"] == SeverityWarning)
	oldTimestampDefaults := !opts.flavor.MinMySQL(8) && !opts.flavor.MinMariaDB(10, 10)
	var alreadySeenTimestamp bool
	for _, col := range table.Columns {
		var message string
		if col.Type.Base == "timestamp" {
			prefix := fmt.Sprintf("Column %s of %s is using type timestamp. ", col.Name, table.ObjectKey())
			suffix := "timestamps have automatic timezone conversion behavior, between the time_zone session variable and UTC."
			if opts.flavor.IsMySQL() {
				message = prefix + "This column type cannot store values beyond January 2038, which is problematic for software with long-term support requirements. It should not be used for storing arbitrary future dates, especially from user input.\nAlso note that " + suffix
			} else if opts.flavor.MinMariaDB(11, 5) { // MariaDB 11.5+ fixes the Y2K38 problem with timestamp (assuming 64bit platforms)
				message = prefix + "Note that " + suffix
			} else { /* opts.flavor.IsMariaDB() && !opts.flavor.MinMariaDB(11, 5) */
				message = prefix + "Prior to MariaDB 11.5, this column type cannot store values beyond January 2038. It should not be used for storing arbitrary future dates, especially from user input.\nAlso note that " + suffix
			}
			if oldTimestampDefaults && !alreadySeenTimestamp && !col.Nullable {
				when := "MySQL 8"
				if opts.flavor.IsMariaDB() {
					when = "MariaDB 10.10+"
				}
				message += "\nFinally, the automatic DEFAULT / ON UPDATE timestamp behavior depends on the explicit_defaults_for_timestamp system variable, which will flip from default OFF to default ON if you upgrade to " + when + "."
			}
			alreadySeenTimestamp = true
		} else if col.Type.Base == "datetime" || col.Type.Base == "time" {
			message = fmt.Sprintf(
				"Column %s of %s is using type %s. Please note this data type does not include timezone information, and does not perform automatic timezone conversions on storage or retrieval.",
				col.Name, table.ObjectKey(), col.Type.Base,
			)
			if onlyWarning {
				message += " Consider strictly using UTC in all contexts to prevent issues with timezone conversions and daylight savings time transitions."
			}
		} else {
			continue
		}
		if !onlyWarning {
			message += "\nTo avoid these issues, consider storing temporal data using unsigned ints or unsigned bigints."
		}
		results = append(results, Note{
			LineOffset: FindColumnLineOffset(col, createStatement),
			Summary:    "Column using temporal type",
			Message:    message,
		})
	}
	return results
}
