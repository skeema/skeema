package linter

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/skeema/mybase"
	"github.com/skeema/tengo"
)

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

func init() {
	RegisterRules([]Rule{
		{
			CheckerFunc:     RoutineChecker(hasRoutinesChecker),
			Name:            "has-routine",
			Description:     "Flag any use of stored procs or funcs; intended for environments that restrict their presence",
			DefaultSeverity: SeverityIgnore,
		},
		{
			CheckerFunc:     RoutineChecker(definerChecker),
			Name:            "definer",
			Description:     "Only allow routine definers listed in --allow-definer",
			DefaultSeverity: SeverityError,
			RelatedOption:   mybase.StringOption("allow-definer", 0, "%@%", "List of allowed routine definers for --lint-definer"),
		},
	})
}

func hasRoutinesChecker(routine *tengo.Routine, _ string, _ *tengo.Schema, _ Options) *Note {
	return &Note{
		Summary: "Routine present",
		Message: fmt.Sprintf("%s %s found. Some environments restrict use of stored procedures and functions for reasons of scalability or operational complexity.", routine.Type, routine.Name),
	}
}

func definerChecker(routine *tengo.Routine, createStatement string, _ *tengo.Schema, opts Options) *Note {
	for _, re := range opts.AllowedDefinersMatch {
		if re.MatchString(routine.Definer) {
			return nil
		}
	}
	reOffset := regexp.MustCompile("(?i)definer")
	message := fmt.Sprintf(
		"%s %s is using definer %s, which is not configured to be permitted. The following definers are listed in option allow-definer: %s.",
		routine.Type, routine.Name, routine.Definer,
		strings.Join(opts.AllowedDefiners, ", "),
	)
	return &Note{
		LineOffset: findFirstLineOffset(reOffset, createStatement),
		Summary:    "Definer not permitted",
		Message:    message,
	}
}
