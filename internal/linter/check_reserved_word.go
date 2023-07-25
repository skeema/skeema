package linter

import (
	"fmt"
	"strings"

	"github.com/skeema/skeema/internal/tengo"
)

func init() {
	RegisterRule(Rule{
		CheckerFunc:     GenericChecker(reservedWordChecker),
		Name:            "reserved-word",
		Description:     "Flag names of tables, columns, or routines that used reserved words",
		DefaultSeverity: SeverityWarning,
	})
}

func reservedWordChecker(object tengo.DefKeyer, createStatement string, schema *tengo.Schema, opts *Options) (notes []Note) {
	// For all object types, we check the object name
	key := object.ObjectKey()
	if tengo.IsVendorReservedWord(key.Name, opts.Flavor.Vendor) {
		notes = append(notes, Note{
			LineOffset: 0,
			Summary:    string(key.Type) + " name matches reserved word",
			Message:    makeReservedWordMessage(key.Name, opts.Flavor),
		})
	}

	// For tables, we also check all column names in the table
	if table, ok := object.(*tengo.Table); ok {
		reservedWords := tengo.VendorReservedWordMap(opts.Flavor.Vendor)
		for _, col := range table.Columns {
			if reservedWords[strings.ToLower(col.Name)] {
				notes = append(notes, Note{
					LineOffset: FindColumnLineOffset(col, createStatement),
					Summary:    "column name matches reserved word",
					Message:    makeReservedWordMessage(col.Name, opts.Flavor),
				})
			}
		}
	}
	return notes
}

func makeReservedWordMessage(word string, flavor tengo.Flavor) string {
	what := "MySQL"
	if flavor.IsMariaDB() {
		what = "MariaDB"
	}
	when := "a later version"
	why := "This name will become problematic if you upgrade your database version, since names matching reserved words must be backtick-wrapped in SQL queries."
	if tengo.IsReservedWord(word, flavor) {
		when = "your version"
		why = "This name may be problematic, since it must be backtick-wrapped in SQL queries."
	}
	return fmt.Sprintf("%s is a reserved word in %s of %s.\n%s", tengo.EscapeIdentifier(word), when, what, why)
}
