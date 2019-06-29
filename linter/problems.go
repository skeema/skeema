package linter

import (
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/skeema/skeema/fs"
	"github.com/skeema/tengo"
)

// A Detector function analyzes a schema for a particular problem, returning
// annotations for cases of the problem found.
type Detector func(*tengo.Schema, *fs.LogicalSchema, Options) []*Annotation

var problems map[string]Detector

// RegisterProblem adds a new named problem, along with its detector function.
func RegisterProblem(name string, fn Detector) {
	problems[name] = fn
}

func init() {
	problems = map[string]Detector{
		"no-pk":                   noPKDetector,
		"bad-charset":             badCharsetDetector,
		"bad-engine":              badEngineDetector,
		"non-unique-fk-ref":       nonUniqueForeignKeyReferenceDetector,
		"duplicate-fk":            duplicateForeignKeyDetector,
		"fk-missing-parent-table": parentTableMissingDetector,
	}
}

func noPKDetector(schema *tengo.Schema, logicalSchema *fs.LogicalSchema, _ Options) []*Annotation {
	results := make([]*Annotation, 0)
	for _, table := range schema.Tables {
		if table.PrimaryKey == nil {
			key := tengo.ObjectKey{Type: tengo.ObjectTypeTable, Name: table.Name}
			message := fmt.Sprintf("Table %s does not define a PRIMARY KEY.", table.Name)
			if table.Engine == "InnoDB" && table.ClusteredIndexKey() == nil {
				message += " Lack of an explicit PRIMARY KEY hurts performance, and prevents use of third-party tools such as pt-online-schema-change."
			}
			results = append(results, &Annotation{
				Statement: logicalSchema.Creates[key],
				Summary:   "No primary key",
				Message:   message,
			})
		}
	}
	return results
}

func badCharsetDetector(schema *tengo.Schema, logicalSchema *fs.LogicalSchema, opts Options) []*Annotation {
	results := make([]*Annotation, 0)
	makeMessage := func(table *tengo.Table, column *tengo.Column) string {
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

	for _, table := range schema.Tables {
		// Check the table's default charset
		if !isAllowed(table.CharSet, opts.AllowedCharSets) {
			key := tengo.ObjectKey{Type: tengo.ObjectTypeTable, Name: table.Name}
			stmt := logicalSchema.Creates[key]
			re := regexp.MustCompile(fmt.Sprintf(`(?i)(default)?\s*(character\s+set|charset|collate)\s*=?\s*(%s|%s)`, table.CharSet, table.Collation))
			results = append(results, &Annotation{
				Statement:  logicalSchema.Creates[key],
				LineOffset: findLastLineOffset(re, stmt.Text),
				Summary:    "Character set not permitted",
				Message:    makeMessage(table, nil),
			})
			continue // if a table's default charset isn't allowed, don't generate col-level annotations too
		}

		// If default charset was ok, now check individual columns
		for _, col := range table.Columns {
			if col.CharSet != "" && !isAllowed(col.CharSet, opts.AllowedCharSets) {
				key := tengo.ObjectKey{Type: tengo.ObjectTypeTable, Name: table.Name}
				stmt := logicalSchema.Creates[key]
				re := regexp.MustCompile(fmt.Sprintf(`(?i)(character\s+set|charset|collate)\s*(%s|%s)`, col.CharSet, col.Collation))
				results = append(results, &Annotation{
					Statement:  logicalSchema.Creates[key],
					LineOffset: findFirstLineOffset(re, stmt.Text),
					Summary:    "Character set not permitted",
					Message:    makeMessage(table, col),
				})
			}
		}
	}
	return results
}

func badEngineDetector(schema *tengo.Schema, logicalSchema *fs.LogicalSchema, opts Options) []*Annotation {
	results := make([]*Annotation, 0)
	for _, table := range schema.Tables {
		if !isAllowed(table.Engine, opts.AllowedEngines) {
			key := tengo.ObjectKey{Type: tengo.ObjectTypeTable, Name: table.Name}
			stmt := logicalSchema.Creates[key]
			re := regexp.MustCompile(fmt.Sprintf(`(?i)ENGINE\s*=?\s*%s`, table.Engine))
			message := fmt.Sprintf("Table %s is using storage engine %s, which is not listed in option allow-engine.", table.Name, table.Engine)
			if len(opts.AllowedEngines) == 1 {
				message = fmt.Sprintf("%s Only the %s storage engine is permitted.", message, opts.AllowedEngines[0])
			} else if len(opts.AllowedEngines) > 1 && len(opts.AllowedEngines) <= 5 {
				message = fmt.Sprintf("%s The following storage engines are permitted: %s.", message, strings.Join(opts.AllowedEngines, ", "))
			}
			results = append(results, &Annotation{
				Statement:  stmt,
				LineOffset: findFirstLineOffset(re, stmt.Text),
				Summary:    "Storage engine not permitted",
				Message:    message,
			})
		}
	}

	return results
}

func problemExists(name string) bool {
	_, ok := problems[strings.ToLower(name)]
	return ok
}

func allProblemNames() []string {
	result := make([]string, 0, len(problems))
	for name := range problems {
		result = append(result, name)
	}
	sort.Strings(result)
	return result
}

// isAllowed performs a case-insensitive search for value in allowed, returning
// true if found.
func isAllowed(value string, allowed []string) bool {
	value = strings.ToLower(value)
	for _, allowedValue := range allowed {
		if value == strings.ToLower(allowedValue) {
			return true
		}
	}
	return false
}

// findFirstLineOffset returns the line offset (i.e. line number starting at 0)
// for the first match of re within createStatement. If no match occurs, 0 is
// returned. This may happen often due to createStatement being arbitrarily
// formatted.
func findFirstLineOffset(re *regexp.Regexp, createStatement string) int {
	loc := re.FindStringIndex(createStatement)
	if loc == nil {
		return 0
	}
	// Count how many newlines occur in createStatement before the match
	return strings.Count(createStatement[0:loc[0]], "\n")
}

// findLastLineOffset returns the line offset (i.e. line number starting at 0)
// for the last match of re within createStatement. If no match occurs, 0 is
// returned. This may happen often due to createStatement being arbitrarily
// formatted.
func findLastLineOffset(re *regexp.Regexp, createStatement string) int {
	locs := re.FindAllStringIndex(createStatement, -1)
	if locs == nil {
		return 0
	}
	lastLoc := locs[len(locs)-1]
	return strings.Count(createStatement[0:lastLoc[0]], "\n")
}
