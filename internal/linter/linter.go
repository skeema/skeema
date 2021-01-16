// Package linter handles logic around linting schemas and returning results.
package linter

import (
	"fmt"

	"github.com/skeema/mybase"
	"github.com/skeema/skeema/internal/workspace"
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

// TableChecker is a function that looks for problems in a table. It can return
// any number of notes per table.
type TableChecker func(table *tengo.Table, createStatement string, schema *tengo.Schema, opts Options) []Note

// CheckObject provides arg conversion in order for TableChecker functions to
// satisfy the ObjectChecker interface.
func (tc TableChecker) CheckObject(object interface{}, createStatement string, schema *tengo.Schema, opts Options) []Note {
	if table, ok := object.(*tengo.Table); ok {
		return tc(table, createStatement, schema, opts)
	}
	return nil
}

// TableBinaryChecker is like a TableChecker that returns at most a single Note
// per table.
type TableBinaryChecker func(table *tengo.Table, createStatement string, schema *tengo.Schema, opts Options) *Note

// CheckObject provides arg and return conversion in order for
// TableBinaryChecker functions to satisfy the ObjectChecker interface.
func (tbc TableBinaryChecker) CheckObject(object interface{}, createStatement string, schema *tengo.Schema, opts Options) []Note {
	if table, ok := object.(*tengo.Table); ok {
		if note := tbc(table, createStatement, schema, opts); note != nil {
			return []Note{*note}
		}
	}
	return nil
}

// RoutineChecker is a function that looks for problems in a stored procedure
// or function. Routine checks are always strictly binary; in other words, for
// each routine, either a single note is found (non-nil return), or no note is
// found (nil return).
type RoutineChecker func(routine *tengo.Routine, createStatement string, schema *tengo.Schema, opts Options) *Note

// CheckObject provides arg conversion in order for RoutineChecker functions to
// satisfy the ObjectChecker interface.
func (rc RoutineChecker) CheckObject(object interface{}, createStatement string, schema *tengo.Schema, opts Options) []Note {
	if routine, ok := object.(*tengo.Routine); ok {
		if note := rc(routine, createStatement, schema, opts); note != nil {
			return []Note{*note}
		}
	}
	return nil
}

// RuleConfigFunc is a function that performs supplemental configuration for
// a Rule. The function can return any arbitrary value. If the return value
// isn't an error or an untyped nil, it will be indexed in Config.
type RuleConfigFunc func(*mybase.Config) interface{}

// Rule combines an ObjectChecker with a string name and corresponding
// option-related handling.
type Rule struct {
	CheckerFunc     ObjectChecker
	Name            string
	Description     string
	DefaultSeverity Severity
	RelatedOption   *mybase.Option // for rules that have supplemental options, e.g. list of allowed values
	ConfigFunc      RuleConfigFunc
}

// RelatedListOption populates RelatedOption and ConfigFunc by creating a
// supplemental option which configures a list of allowed values. The supplied
// name, defaultValue, and description are used in the supplemental option. If
// required is true, the user may not set the option to an empty list unless the
// corresponding rule has been set to be ignored.
// For examples of use, see several of the table checkers.
// This method panics if called on a Rule that already has a RelatedOption or
// ConfigFunc, since this is indicative of programmer error.
func (r *Rule) RelatedListOption(name, defaultValue, description string, required bool) {
	if r.RelatedOption != nil || r.ConfigFunc != nil {
		panic("Cannot call RelatedListOption on a rule that already has a RelatedOption or ConfigFunc")
	}
	r.RelatedOption = mybase.StringOption(name, 0, defaultValue, description)
	fn := func(config *mybase.Config) interface{} {
		values := config.GetSlice(name, ',', true)
		if required && len(values) == 0 {
			return fmt.Errorf(
				"With option %s=%s, corresponding option %s must be non-empty",
				r.optionName(), config.Get(r.optionName()), name,
			)
		}
		return values
	}
	r.ConfigFunc = RuleConfigFunc(fn)
}

func (r *Rule) optionName() string {
	return fmt.Sprintf("lint-%s", r.Name)
}

func (r *Rule) optionDescription() string {
	if r.hidden() {
		return "hidden/internal linter rule"
	}
	return fmt.Sprintf("%s (valid values: \"ignore\", \"warning\", \"error\")", r.Description)
}

func (r *Rule) hidden() bool {
	return (r.Description == "")
}

var rulesByName = map[string]*Rule{}

// RegisterRule indexes a single Rule by name in a package-level registry.
// Registered rules are automatically converted to Options in config.go's
// AddCommandOptions, and are automatically tested by integration tests.
func RegisterRule(rule Rule) {
	if rule.Description == "" || rule.DefaultSeverity == Severity("") {
		rule.DefaultSeverity = SeverityIgnore
	}
	rulesByName[rule.Name] = &rule
}
