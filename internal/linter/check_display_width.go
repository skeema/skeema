package linter

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/skeema/tengo"
)

func init() {
	RegisterRule(Rule{
		CheckerFunc:     TableChecker(displayWidthChecker),
		Name:            "display-width",
		Description:     "Only allow default display width for int types",
		DefaultSeverity: SeverityWarning,
	})
}

// Regular expression for parsing out parts of an int type:
// [1] is the base type
// [2] is the display width (digits only, no parens)
// [3] is " unsigned" or ""
// [4] is " zerofill" or ""
var reDisplayWidth = regexp.MustCompile(`^(tinyint|smallint|mediumint|int|bigint)\((\d+)\)( unsigned)?( zerofill)?`)

// Default display widths for signed int types
var signedDefaultWidths = map[string]int{
	"tinyint":   4,  // unsigned is 3
	"smallint":  6,  // unsigned is 5
	"mediumint": 9,  // unsigned is 8
	"int":       11, // unsigned is 10
	"bigint":    20, // unsigned also 20
}

func displayWidthChecker(table *tengo.Table, createStatement string, _ *tengo.Schema, _ Options) []Note {
	results := make([]Note, 0)
	for _, col := range table.Columns {
		if !strings.Contains(col.TypeInDB, "int(") {
			continue
		}
		matches := reDisplayWidth.FindStringSubmatch(col.TypeInDB)
		rawType, displayWidth := matches[1], matches[2]
		unsigned, zerofill := (matches[3] != ""), (matches[4] != "")
		if zerofill {
			continue // non-default display width may be intentional with zerofill
		}
		if rawType == "tinyint" && displayWidth == "1" {
			continue // allow tinyint(1) since bool is an alias for this
		}
		defaultWidthInt := signedDefaultWidths[rawType]
		if unsigned && rawType != "bigint" {
			defaultWidthInt--
		}
		defaultWidth := strconv.Itoa(defaultWidthInt)
		if displayWidth != defaultWidth {
			message := fmt.Sprintf(
				"Column %s of table %s is using display width %s, but the default for %s%s is %s.\nInteger display widths do not control what range of values may be stored in a column. Typically they have no effect whatsoever. If in doubt, omit the width entirely, or use the default of %s(%s)%s.",
				col.Name, table.Name, displayWidth,
				rawType, matches[3], defaultWidth,
				rawType, defaultWidth, matches[3],
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
