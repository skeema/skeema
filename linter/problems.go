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
		"no-pk":       noPKDetector,
		"bad-charset": badCharsetDetector,
		"bad-engine":  badEngineDetector,
	}
}

func noPKDetector(schema *tengo.Schema, logicalSchema *fs.LogicalSchema, _ Options) []*Annotation {
	results := make([]*Annotation, 0)
	for _, table := range schema.Tables {
		if table.PrimaryKey == nil {
			key := tengo.ObjectKey{Type: tengo.ObjectTypeTable, Name: table.Name}
			results = append(results, &Annotation{
				Statement: logicalSchema.Creates[key],
				Summary:   "No primary key",
				Message:   fmt.Sprintf("Table %s does not define a PRIMARY KEY", table.Name),
			})
		}
	}
	return results
}

func badCharsetDetector(schema *tengo.Schema, logicalSchema *fs.LogicalSchema, opts Options) []*Annotation {
	results := make([]*Annotation, 0)
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
				Message:    fmt.Sprintf("Table %s is using default character set %s, which is not listed in option allow-charset", table.Name, table.CharSet),
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
					Message:    fmt.Sprintf("Column %s of table %s is using character set %s, which is not listed in option allow-charset", col.Name, table.Name, table.CharSet),
				})
				break // stop after the first disallowed charset col per table
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
			results = append(results, &Annotation{
				Statement:  stmt,
				LineOffset: findFirstLineOffset(re, stmt.Text),
				Summary:    "Storage engine not permitted",
				Message:    fmt.Sprintf("Table %s is using storage engine %s, which is not listed in option allow-engine", table.Name, table.Engine),
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
