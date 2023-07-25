package linter

import (
	"fmt"
	"regexp"

	"github.com/skeema/skeema/internal/tengo"
)

func init() {
	RegisterRule(Rule{
		CheckerFunc:     TableBinaryChecker(hasForeignKeysChecker),
		Name:            "has-fk",
		Description:     "Flag any use of foreign keys; intended for environments that restrict their presence",
		DefaultSeverity: SeverityIgnore,
	})
}

var reHasFK = regexp.MustCompile(`(?i)foreign key`)

func hasForeignKeysChecker(table *tengo.Table, createStatement string, _ *tengo.Schema, _ *Options) *Note {
	if len(table.ForeignKeys) == 0 {
		return nil
	}
	var plural string
	if len(table.ForeignKeys) > 1 {
		plural = "s"
	}
	message := fmt.Sprintf(
		"Table %s has %d foreign key%s. Foreign keys may harm write performance, and can be problematic for online schema change tools. They are also ineffective in sharded environments.",
		table.Name, len(table.ForeignKeys), plural,
	)
	return &Note{
		LineOffset: FindFirstLineOffset(reHasFK, createStatement),
		Summary:    "Table has foreign keys",
		Message:    message,
	}
}
