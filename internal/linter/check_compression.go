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

var reAnyCompression = regexp.MustCompile(`(?i)KEY_BLOCK_SIZE|COMPRESSION|page_compressed|ROW_FORMAT=COMPRESSED`)

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
// enum value an InnoDB table is using. This method bases its output entirely on
// the table's CreateOptions (which are parsed from SHOW CREATE TABLE), and this
// is typically correct in the vast majority of cases, but may be wrong in some
// rare edge cases involving MySQL global variables:
//   - Users who override innodb_page_size and also omit key_block_size in
//     compressed table definitions. In this case this method will return "8kb"
//     instead of the correct value of innodb_page_size/2.
//   - Users with innodb_file_per_table=OFF and/or innodb_file_format=Antelope
//     but still have compression settings applied in table definitions. In this
//     case this method will return a value based on the table definition, even
//     instead of the correct value of "none" (since these settings prevent
//     compression options from having any effect).
func tableCompressionMode(table *tengo.Table) (mode string, clause string) {
	opts := table.OptionMap()
	normalizedValue := func(input string) string {
		if len(input) > 1 && input[0] == '\'' && input[len(input)-1] == '\'' {
			return strings.ToLower(input[1 : len(input)-1])
		}
		return strings.ToLower(input)
	}
	if value, ok := opts["KEY_BLOCK_SIZE"]; ok {
		return value + "kb", "KEY_BLOCK_SIZE=" + value
	} else if rowFormat, _ := opts["ROW_FORMAT"]; rowFormat == "COMPRESSED" {
		return "8kb", "ROW_FORMAT=COMPRESSED" // see explanation in function doc above
	} else if mysqlPageComp, ok := opts["COMPRESSION"]; ok && normalizedValue(mysqlPageComp) != "none" {
		return "page", "COMPRESSION=" + mysqlPageComp
	} else if mariaPageComp, ok := opts["PAGE_COMPRESSED"]; ok {
		if nv := normalizedValue(mariaPageComp); nv != "0" && nv != "off" && nv != "false" && nv != "no" {
			return "page", "PAGE_COMPRESSED=" + mariaPageComp
		}
	}
	return "none", ""
}

// makeCompressionMessage is a helper function to translate allow-compression
// enum strings into a human-friendly message including the list of
// corresponding allowed CREATE option clauses.
func makeCompressionMessage(table *tengo.Table, mode, clause string, opts *Options) string {
	// Obtain allow-compression option slice, and convert to table create options SQL clauses
	allowed := opts.AllowList("compression")
	var clauses []string
	for _, value := range allowed {
		switch value = strings.ToLower(value); value {
		case "none":
			// Intentionally empty case, don't add to clauses
		case "page":
			if opts.flavor.IsMariaDB() {
				clauses = append(clauses, "PAGE_COMPRESSED=1")
			} else {
				clauses = append(clauses, "COMPRESSION='zlib'")
			}
		case "1kb", "2kb", "4kb", "8kb", "16kb":
			clauses = append(clauses, "KEY_BLOCK_SIZE="+strings.TrimSuffix(value, "kb"))
		default: // invalid allow-compression value
			if mode == "none" {
				return fmt.Sprintf("%s is not compressed, but option allow-compression is misconfigured to include unknown value %q. Please refer to Skeema's options reference manual to fix the configuration of this linter rule.", table.ObjectKey(), value)
			}
			return fmt.Sprintf("%s is using compression clause %s, but option allow-compression is misconfigured to include unknown value %q. Please refer to Skeema's options reference manual to fix the configuration of this linter rule.", table.ObjectKey(), clause, value)
		}
	}
	clausesString := strings.Join(clauses, ", ")

	// Table isn't compressed, but allow-compression *requires* compression. (This
	// entire method only gets called if the table's mode is not allowed.)
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
		return prefix + middle + " compression clause " + clauses[0] + "."
	}
	return prefix + middle + " one of these compression clauses: " + clausesString
}
