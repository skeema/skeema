package linter

import (
	//"fmt"
	//"regexp"

	//	"github.com/skeema/mybase"
	"github.com/skeema/tengo"
)

func init() {
}

// RoutineChecker is a function that looks for problems in a stored procedure
// or function.
type RoutineChecker func(routine *tengo.Routine, createStatement string, schema *tengo.Schema, opts Options) []Note

// CheckObject provides arg conversion in order for RoutineChecker functions to
// satisfy the ObjectChecker interface.
func (rc RoutineChecker) CheckObject(object interface{}, createStatement string, schema *tengo.Schema, opts Options) []Note {
	if routine, ok := object.(*tengo.Routine); ok {
		return rc(routine, createStatement, schema, opts)
	}
	return nil
}
