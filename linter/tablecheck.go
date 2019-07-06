package linter

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/skeema/mybase"
	"github.com/skeema/tengo"
)

// TableChecker is a function that looks for problems in a table.
type TableChecker func(table *tengo.Table, createStatement string, schema *tengo.Schema, opts Options) []Note

// CheckObject provides arg conversion in order for TableChecker functions to
// satisfy the ObjectChecker interface.
func (tc TableChecker) CheckObject(object interface{}, createStatement string, schema *tengo.Schema, opts Options) []Note {
	if table, ok := object.(*tengo.Table); ok {
		return tc(table, createStatement, schema, opts)
	}
	return nil
}

func init() {
	RegisterRules([]Rule{
		{
			CheckerFunc:     TableChecker(pkChecker),
			Name:            "pk",
			Description:     "Require tables to have a primary key",
			DefaultSeverity: SeverityWarning,
		},
		{
			CheckerFunc:     TableChecker(charsetChecker),
			Name:            "charset",
			Description:     "Only allow character sets listed in --allow-charset",
			DefaultSeverity: SeverityWarning,
			RelatedOption:   mybase.StringOption("allow-charset", 0, "latin1,utf8mb4", "List of allowed character sets for --lint-charset"),
		},
		{
			CheckerFunc:     TableChecker(engineChecker),
			Name:            "engine",
			Description:     "Only allow storage engines listed in --allow-engine",
			DefaultSeverity: SeverityWarning,
			RelatedOption:   mybase.StringOption("allow-engine", 0, "innodb", "List of allowed storage engines for --lint-engine"),
		},
	})
}

func pkChecker(table *tengo.Table, _ string, _ *tengo.Schema, _ Options) []Note {
	if table.PrimaryKey != nil {
		return nil
	}
	var advice string
	if table.Engine == "InnoDB" && table.ClusteredIndexKey() == nil {
		advice = " Lack of a PRIMARY KEY hurts performance, and prevents use of third-party tools such as pt-online-schema-change."
	}
	message := fmt.Sprintf("Table %s does not define a PRIMARY KEY.%s", table.Name, advice)
	return OneNote(0, "No primary key", message)
}

func charsetChecker(table *tengo.Table, createStatement string, _ *tengo.Schema, opts Options) []Note {
	makeMessage := func(column *tengo.Column) string {
		var subject, charSet, using, allowedList, moreInfo string
		if column == nil {
			subject = fmt.Sprintf("Table %s", table.Name)
			charSet = table.CharSet
			using = "default character set"
		} else {
			subject = fmt.Sprintf("Column %s of table %s", column.Name, table.Name)
			charSet = column.CharSet
			using = "character set"
		}
		if len(opts.AllowedCharSets) == 1 {
			allowedList = fmt.Sprintf(" Only the %s character set is permitted.", opts.AllowedCharSets[0])
		} else if len(opts.AllowedCharSets) > 1 && len(opts.AllowedCharSets) <= 5 {
			allowedList = fmt.Sprintf(" The following character sets are permitted: %s.", strings.Join(opts.AllowedCharSets, ", "))
		}
		if charSet == "utf8" && isAllowed("utf8mb4", opts.AllowedCharSets) {
			moreInfo = "\nTo permit storage of all valid UTF-8 characters, use the utf8mb4 character set instead of the legacy utf8 character set."
		} else if charSet == "binary" {
			moreInfo = "\nUsing equivalent binary column types (e.g. BINARY, VARBINARY, BLOB) is preferred for readability."
		}
		return fmt.Sprintf("%s is using %s %s, which is not listed in option allow-charset.%s%s", subject, using, charSet, allowedList, moreInfo)
	}

	// Check the table's default charset. If it fails, return a single
	// Note without checking individual columns, as we don't want a bunch
	// of redundant messages for columns using the table default charset.
	if !isAllowed(table.CharSet, opts.AllowedCharSets) {
		re := regexp.MustCompile(fmt.Sprintf(`(?i)(default)?\s*(character\s+set|charset|collate)\s*=?\s*(%s|%s)`, table.CharSet, table.Collation))
		return OneNote(findLastLineOffset(re, createStatement), "Character set not permitted", makeMessage(nil))
	}

	// Now check individual columns
	var results []Note
	for _, col := range table.Columns {
		if col.CharSet != "" && !isAllowed(col.CharSet, opts.AllowedCharSets) {
			re := regexp.MustCompile(fmt.Sprintf(`(?i)(character\s+set|charset|collate)\s*(%s|%s)`, col.CharSet, col.Collation))
			results = append(results, Note{
				LineOffset: findFirstLineOffset(re, createStatement),
				Summary:    "Character set not permitted",
				Message:    makeMessage(col),
			})
		}
	}
	return results
}

func engineChecker(table *tengo.Table, createStatement string, _ *tengo.Schema, opts Options) []Note {
	if isAllowed(table.Engine, opts.AllowedEngines) {
		return nil
	}

	re := regexp.MustCompile(fmt.Sprintf(`(?i)ENGINE\s*=?\s*%s`, table.Engine))
	message := fmt.Sprintf("Table %s is using storage engine %s, which is not listed in option allow-engine.", table.Name, table.Engine)
	if len(opts.AllowedEngines) == 1 {
		message = fmt.Sprintf("%s Only the %s storage engine is permitted.", message, opts.AllowedEngines[0])
	} else if len(opts.AllowedEngines) > 1 && len(opts.AllowedEngines) <= 5 {
		message = fmt.Sprintf("%s The following storage engines are permitted: %s.", message, strings.Join(opts.AllowedEngines, ", "))
	}
	return OneNote(findFirstLineOffset(re, createStatement), "Storage engine not permitted", message)
}
