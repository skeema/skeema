package linter

import (
	"strings"

	"github.com/skeema/skeema/internal/tengo"
)

func init() {
	RegisterRule(Rule{
		CheckerFunc:     RoutineChecker(hasRoutinesChecker),
		Name:            "has-routine",
		Description:     "Flag any use of stored procs or funcs; intended for environments that restrict their presence",
		DefaultSeverity: SeverityIgnore,
	})
}

func hasRoutinesChecker(routine *tengo.Routine, _ string, _ *tengo.Schema, _ *Options) *Note {
	keyString := routine.ObjectKey().String()
	noun := strings.ToUpper(keyString[0:1]) + keyString[1:]
	return &Note{
		Summary: "Routine present",
		Message: noun + " found. Some environments restrict use of stored procedures and functions for reasons of scalability or operational complexity.",
	}
}
