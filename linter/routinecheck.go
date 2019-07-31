package linter

import (
	"fmt"

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
			CheckerFunc:     RoutineChecker(hasRoutines),
			Name:            "has-routine",
			Description:     "Flag use of stored procs or funcs; useful in environments that restrict their presence",
			DefaultSeverity: SeverityIgnore,
		},
	})
}

func hasRoutines(routine *tengo.Routine, _ string, _ *tengo.Schema, _ Options) *Note {
	return &Note{
		Summary: "Routine present",
		Message: fmt.Sprintf("%s %s found. Some environments restrict use of stored procedures and functions for reasons of scalability or operational complexity.", routine.Type, routine.Name),
	}
}
