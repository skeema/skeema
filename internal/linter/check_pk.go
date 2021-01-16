package linter

import (
	"fmt"

	"github.com/skeema/tengo"
)

func init() {
	RegisterRule(Rule{
		CheckerFunc:     TableBinaryChecker(pkChecker),
		Name:            "pk",
		Description:     "Flag tables that lack a primary key",
		DefaultSeverity: SeverityWarning,
	})
}

func pkChecker(table *tengo.Table, _ string, _ *tengo.Schema, _ Options) *Note {
	if table.PrimaryKey != nil {
		return nil
	}
	var advice string
	if table.Engine == "InnoDB" && table.ClusteredIndexKey() == nil {
		advice = " Lack of a PRIMARY KEY hurts performance, and prevents use of third-party tools such as pt-online-schema-change."
	}
	message := fmt.Sprintf("Table %s does not define a PRIMARY KEY.%s", table.Name, advice)
	return &Note{
		LineOffset: 0,
		Summary:    "No primary key",
		Message:    message,
	}
}
