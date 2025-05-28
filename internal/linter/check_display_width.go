package linter

import (
	"fmt"

	"github.com/skeema/skeema/internal/tengo"
)

func init() {
	RegisterRule(Rule{
		CheckerFunc:     TableChecker(displayWidthChecker),
		Name:            "display-width",
		Description:     "Only allow default display width for int types",
		DefaultSeverity: SeverityWarning,
		Deprecation:     "This option will be removed in Skeema v2. For more information, visit https://www.skeema.io/blog/skeema-v2-roadmap",
	})
}

// Default display widths for signed int types
var signedDefaultWidths = map[string]uint16{
	"tinyint":   4,  // unsigned is 3
	"smallint":  6,  // unsigned is 5
	"mediumint": 9,  // unsigned is 8
	"int":       11, // unsigned is 10
	"bigint":    20, // unsigned also 20
}

func displayWidthChecker(table *tengo.Table, createStatement string, _ *tengo.Schema, _ *Options) []Note {
	results := make([]Note, 0)
	for _, col := range table.Columns {
		if col.Type.Size == 0 || !col.Type.Integer() { // non-int, or no display width due to MySQL 8.0.19+
			continue
		}
		if col.Type.Zerofill {
			continue // non-default display width may be intentional with zerofill
		}
		if col.Type.Base == "tinyint" && col.Type.Size == 1 {
			continue // allow tinyint(1) since bool is an alias for this
		}
		defaultWidth := signedDefaultWidths[col.Type.Base]
		var suffix string
		if col.Type.Unsigned {
			suffix = " unsigned"
			if col.Type.Base != "bigint" {
				defaultWidth--
			}
		}
		if col.Type.Size != defaultWidth {
			message := fmt.Sprintf(
				"Column %s of %s is using display width %d, but the default for %s%s is %d.\nInteger display widths do not control what range of values may be stored in a column. Typically they have no effect whatsoever. If in doubt, omit the width entirely, or use the default of %s(%d)%s.",
				col.Name, table.ObjectKey(), col.Type.Size,
				col.Type.Base, suffix, defaultWidth,
				col.Type.Base, defaultWidth, suffix,
			)
			results = append(results, Note{
				LineOffset: FindColumnLineOffset(col, createStatement),
				Summary:    "Non-default display width detected",
				Message:    message,
			})
		}
	}
	return results
}
