package linter

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/skeema/tengo"
)

func init() {
	rule := Rule{
		CheckerFunc:     TableChecker(charsetChecker),
		Name:            "charset",
		Description:     "Only allow character sets listed in --allow-charset",
		DefaultSeverity: SeverityWarning,
	}
	rule.RelatedListOption(
		"allow-charset",
		"latin1,utf8mb4",
		"List of allowed character sets for --lint-charset",
		true, // must specify at least 1 allowed charset if --lint-charset is "warning" or "error"
	)
	RegisterRule(rule)
}

func charsetChecker(table *tengo.Table, createStatement string, _ *tengo.Schema, opts Options) []Note {
	// If utf8mb3 is on the allow-list, ensure its alias utf8 is as well, and vice
	// versa. This is intended to handle MySQL 8.0.24+ and MariaDB 10.6+ which have
	// started to change how these aliases work.
	allowUTF8 := opts.IsAllowed("charset", "utf8")
	allowUTF8mb3 := opts.IsAllowed("charset", "utf8mb3")
	if (allowUTF8 && !allowUTF8mb3) || (allowUTF8mb3 && !allowUTF8) {
		allowList := opts.AllowList("charset")
		if !allowUTF8 {
			allowList = append(allowList, "utf8")
		} else {
			allowList = append(allowList, "utf8mb3")
		}
		opts.RuleConfig["charset"] = allowList
	}

	// Check the table's default charset. If it fails, return a single
	// Note without checking individual columns, as we don't want a bunch
	// of redundant messages for columns using the table default charset.
	if !opts.IsAllowed("charset", table.CharSet) {
		re := regexp.MustCompile(fmt.Sprintf(`(?i)(default)?\s*(character\s+set|charset|collate)\s*=?\s*(%s|%s)`, table.CharSet, table.Collation))
		note := Note{
			LineOffset: FindLastLineOffset(re, createStatement),
			Summary:    "Character set not permitted",
			Message:    makeCharsetMessage(table, nil, opts),
		}
		return []Note{note}
	}

	// Now check individual columns
	var results []Note
	for _, col := range table.Columns {
		if col.CharSet != "" && !opts.IsAllowed("charset", col.CharSet) {
			results = append(results, Note{
				LineOffset: FindColumnLineOffset(col, createStatement),
				Summary:    "Character set not permitted",
				Message:    makeCharsetMessage(table, col, opts),
			})
		}
	}
	return results
}

func makeCharsetMessage(table *tengo.Table, column *tengo.Column, opts Options) string {
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
	allowedCharSets := opts.AllowList("charset")
	if len(allowedCharSets) == 1 {
		allowedList = fmt.Sprintf(" Only the %s character set is listed in option allow-charset.", allowedCharSets[0])
	} else {
		allowedList = fmt.Sprintf(" The following character sets are listed in option allow-charset: %s.", strings.Join(allowedCharSets, ", "))
	}
	if (charSet == "utf8" || charSet == "utf8mb3") && opts.IsAllowed("charset", "utf8mb4") {
		moreInfo = fmt.Sprintf("\nTo permit storage of all valid four-byte UTF-8 characters, use the utf8mb4 character set instead of the legacy three-byte %s character set.", charSet)
	} else if charSet == "binary" {
		moreInfo = "\nUsing equivalent binary column types (e.g. BINARY, VARBINARY, BLOB) is preferred for readability."
	}
	return fmt.Sprintf("%s is using %s %s, which is not configured to be permitted.%s%s", subject, using, charSet, allowedList, moreInfo)
}
