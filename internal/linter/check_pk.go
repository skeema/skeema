package linter

import (
	"github.com/skeema/skeema/internal/tengo"
)

func init() {
	RegisterRule(Rule{
		CheckerFunc:     TableBinaryChecker(pkChecker),
		Name:            "pk",
		Description:     "Flag tables that lack a primary key",
		DefaultSeverity: SeverityWarning,
	})
}

func pkChecker(table *tengo.Table, _ string, _ *tengo.Schema, _ *Options) *Note {
	if table.PrimaryKey != nil {
		return nil
	}
	var advice string
	if table.Engine == "InnoDB" && table.ClusteredIndexKey() == nil {
		advice = " Lack of a PRIMARY KEY hurts performance, and prevents use of third-party tools such as pt-online-schema-change."
		for _, idx := range table.SecondaryIndexes {
			if idx.Unique {
				advice += " (Although this table does have a UNIQUE index, it cannot serve as the clustered index key either, since that requires use of only NOT NULL columns.)"
				break
			}
		}
	}
	return &Note{
		LineOffset: 0,
		Summary:    "No primary key",
		Message:    table.ObjectKey().String() + " does not define a PRIMARY KEY." + advice,
	}
}
