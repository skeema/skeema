package linter

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/skeema/skeema/internal/tengo"
)

func init() {
	rule := Rule{
		CheckerFunc:     TableBinaryChecker(compressionChecker),
		Name:            "compression",
		Description:     "Only allow compression settings listed in --allow-compression",
		DefaultSeverity: SeverityWarning,
	}
	rule.RelatedListOption(
		"allow-compression",
		"none,4kb,8kb",
		"List of allowed compression settings for --lint-compression",
		true, // must specify at least 1 allowed compression setting if --lint-compression is "warning" or "error"
	)
	RegisterRule(rule)
}

var (
	reAnyCompression  = regexp.MustCompile(`(?i)KEY_BLOCK_SIZE|COMPRESSION|page_compressed|ROW_FORMAT`)
	reKeyBlockSize    = regexp.MustCompile(`KEY_BLOCK_SIZE=(\d+)`)
	rePageCompression = regexp.MustCompile("(?i)COMPRESSION='(\\w+)'|`?page_compressed`?='?(\\w+)'?")
	reBlockSizeEnum   = regexp.MustCompile(`^(1|2|4|8|16)kb$`)
)

func compressionChecker(table *tengo.Table, createStatement string, _ *tengo.Schema, opts *Options) *Note {
	// Only InnoDB tables are checked by this linter rule at this time.
	if table.Engine != "InnoDB" {
		return nil
	}

	mode, clause := tableCompressionMode(table)
	if opts.IsAllowed("compression", mode) {
		return nil
	}

	note := &Note{
		LineOffset: FindLastLineOffset(reAnyCompression, createStatement),
		Summary:    "Table compression setting not permitted",
		Message:    makeCompressionMessage(table, mode, clause, opts),
	}

	// If table is not compressed, but uncompressed tables are not permitted due
	// to "none" being intentionally excluded from allow-compression
	if mode == "none" {
		note.Summary = "Table is not compressed"
	}

	// If the table is not compressed OR if the position of the compression clause
	// could not be located, set the note's line offset to be the last line of the
	// CREATE statement.
	if mode == "none" || note.LineOffset == 0 {
		note.LineOffset = strings.Count(createStatement, "\n")
	}

	return note
}

// tableCompressionMode is a helper function to determine which compression mode
// enum value a table is using. This method bases its output entirely on
// table.CreateOptions, which is typically correct in the vast majority of
// cases, but does not handle a few rare edge cases involving MySQL global
// variables:
//   - Users who override innodb_page_size and also omit key_block_size in
//     compressed table definitions. In this case this method will return "8kb"
//     instead of the correct value of innodb_page_size/2.
//   - Users with innodb_file_per_table=OFF and/or innodb_file_format=Antelope
//     but still have compression settings applied in table definitions. In this
//     case this method will return a value based on the table definition, even
//     instead of the correct value of "none" (since these settings prevent
//     compression options from having any effect).
func tableCompressionMode(table *tengo.Table) (mode string, clause string) {
	if table.RowFormat() == "COMPRESSED" {
		matches := reKeyBlockSize.FindStringSubmatch(table.CreateOptions)
		if matches == nil {
			return "8kb", "ROW_FORMAT=COMPRESSED" // see explanation in function doc above
		}
		return matches[1] + "kb", matches[0]
	} else if matches := rePageCompression.FindStringSubmatch(table.CreateOptions); matches != nil {
		value := strings.ToLower(matches[1])
		if value == "" && matches[2] != "" {
			value = strings.ToLower(matches[2])
		}
		if value != "0" && value != "off" && value != "none" {
			return "page", matches[0]
		}
	}
	return "none", ""
}

// makeCompressionMessage is a helper function to translate allow-compression
// enum strings into a human-friendly message including the list of
// corresponding allowed CREATE option clauses.
func makeCompressionMessage(table *tengo.Table, mode, clause string, opts *Options) string {
	allowed := opts.AllowList("compression")
	var clauses []string
	for _, value := range allowed {
		value = strings.ToLower(value)
		if value == "page" {
			if opts.flavor.IsMariaDB() {
				clauses = append(clauses, "PAGE_COMPRESSED=1")
			} else {
				clauses = append(clauses, "COMPRESSION='zlib'")
			}
		} else if matches := reBlockSizeEnum.FindStringSubmatch(value); matches != nil {
			clauses = append(clauses, fmt.Sprintf("KEY_BLOCK_SIZE=%s", matches[1]))
		} else if value != "none" {
			if mode == "none" {
				return fmt.Sprintf("%s is not compressed, but option allow-compression is misconfigured to include unknown value %q. Please refer to Skeema's options reference manual to fix the configuration of this linter rule.", table.ObjectKey(), value)
			}
			return fmt.Sprintf("%s is using compression clause %s, but option allow-compression is misconfigured to include unknown value %q. Please refer to Skeema's options reference manual to fix the configuration of this linter rule.", table.ObjectKey(), clause, value)
		}
	}
	clausesString := strings.Join(clauses, ", ")

	// Table isn't compressed, but allow-compression *requires* compression
	if mode == "none" {
		if len(clauses) == 1 {
			return fmt.Sprintf("%s is not compressed, but option allow-compression is configured to only permit compressed tables. Please use compression clause %s.", table.ObjectKey(), clauses[0])
		}
		return fmt.Sprintf("%s is not compressed, but option allow-compression is not configured to allow uncompressed tables. Please use one of these compression clauses: %s", table.ObjectKey(), clausesString)
	}

	// Table is compressed, but allow-compression prohibits ANY compression
	if len(allowed) == 1 && strings.EqualFold(allowed[0], "none") {
		return fmt.Sprintf("%s is using compression clause %s, but option allow-compression is configured to prohibit use of compression.", table.ObjectKey(), clause)
	}

	// Table is compressed, but allow-compression allows some OTHER clause
	prefix := fmt.Sprintf("%s is using compression clause %s, but option allow-compression is not configured to permit this. ", table.ObjectKey(), clause)
	middle := "Please use"
	if opts.IsAllowed("compression", "none") {
		middle = "Please either leave the table uncompressed, or use"
	}
	if len(clauses) == 1 {
		return fmt.Sprintf("%s%s compression clause %s.", prefix, middle, clauses[0])
	}
	return fmt.Sprintf("%s%s one of these compression clauses: %s", prefix, middle, clausesString)
}
