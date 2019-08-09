// Package linter handles logic around linting schemas and returning results.
package linter

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/skeema/mybase"
	"github.com/skeema/skeema/workspace"
	"github.com/skeema/tengo"
)

// CheckSchema runs all registered lint rules on objects in a workspace.Schema.
// (This function does not operate directly on a tengo.Schema alone, because the
// original fs.LogicalSchema is also needed, in order to generate annotations
// corresponding to SQL statements / files / line numbers.)
func CheckSchema(wsSchema *workspace.Schema, opts Options) *Result {
	result := &Result{}
	tables := wsSchema.TablesByName()
	procs := wsSchema.ProceduresByName()
	funcs := wsSchema.FunctionsByName()

	for key, stmt := range wsSchema.LogicalSchema.Creates {
		if opts.shouldIgnore(key) {
			continue
		}
		var object interface{}
		var ok bool
		switch key.Type {
		case tengo.ObjectTypeTable:
			object, ok = tables[key.Name]
		case tengo.ObjectTypeProc:
			object, ok = procs[key.Name]
		case tengo.ObjectTypeFunc:
			object, ok = funcs[key.Name]
		}
		if !ok { // happens normally if the create SQL errored
			continue
		}
		for ruleName, severity := range opts.RuleSeverity {
			if severity == SeverityIgnore {
				continue
			}
			r := rulesByName[ruleName]
			output := r.CheckerFunc.CheckObject(object, stmt.Text, wsSchema.Schema, opts)
			for _, lo := range output {
				result.Annotate(stmt, severity, ruleName, lo)
			}
		}
	}
	return result
}

// ObjectChecker values may be used to check for problems in database objects.
type ObjectChecker interface {
	CheckObject(object interface{}, createStatement string, schema *tengo.Schema, opts Options) []Note
}

// Rule combines an ObjectChecker with a string name and corresponding
// option-related handling.
type Rule struct {
	CheckerFunc     ObjectChecker
	Name            string
	Description     string
	DefaultSeverity Severity
	RelatedOption   *mybase.Option // for rules that have supplemental options, e.g. list of allowed values
}

func (r *Rule) optionName() string {
	return fmt.Sprintf("lint-%s", r.Name)
}

func (r *Rule) optionDescription() string {
	return fmt.Sprintf("%s (valid values: \"ignore\", \"warning\", \"error\")", r.Description)
}

var rulesByName = map[string]*Rule{}

// RegisterRules indexes one or more Rules by name in a package-level registry.
// Registered rules are automatically converted to Options in config.go's
// AddCommandOptions, and are automatically tested by integration tests.
func RegisterRules(rules []Rule) {
	for i := range rules {
		rulesByName[rules[i].Name] = &rules[i]
	}
}

// isAllowed performs a case-insensitive search for value in allowed, returning
// true if found. Useful as a helper function for Rules that have a RelatedOption
// specifying a list of allowed values.
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
// This is useful for ObjectCheckers when populating Note.LineOffset.
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
// This is useful for ObjectCheckers when populating Note.LineOffset.
func findLastLineOffset(re *regexp.Regexp, createStatement string) int {
	locs := re.FindAllStringIndex(createStatement, -1)
	if locs == nil {
		return 0
	}
	lastLoc := locs[len(locs)-1]
	return strings.Count(createStatement[0:lastLoc[0]], "\n")
}
