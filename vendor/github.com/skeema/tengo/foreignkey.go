package tengo

import (
	"fmt"
	"strings"
)

// ForeignKey represents a single foreign key constraint in a table. Note that
// the "referenced" side of the FK is tracked as strings, rather than *Schema,
// *Table, *[]Column to avoid potentially having to introspect multiple schemas
// in a particular order. Also, the referenced side is not gauranteed to exist,
// especially if foreign_key_checks=0 has been used at any point in the past.
type ForeignKey struct {
	Name                  string
	Columns               []*Column
	ReferencedSchemaName  string // will be empty string if same schema
	ReferencedTableName   string
	ReferencedColumnNames []string // slice length always identical to len(Columns)
	UpdateRule            string
	DeleteRule            string
}

// Definition returns this ForeignKey's definition clause, for use as part of a DDL
// statement.
func (fk *ForeignKey) Definition(flavor Flavor) string {
	colParts := make([]string, len(fk.Columns))
	for n, col := range fk.Columns {
		colParts[n] = EscapeIdentifier(col.Name)
	}
	childCols := strings.Join(colParts, ", ")

	referencedTable := EscapeIdentifier(fk.ReferencedTableName)
	if fk.ReferencedSchemaName != "" {
		referencedTable = fmt.Sprintf("%s.%s", EscapeIdentifier(fk.ReferencedSchemaName), referencedTable)
	}

	for n, col := range fk.ReferencedColumnNames {
		colParts[n] = EscapeIdentifier(col)
	}
	parentCols := strings.Join(colParts, ", ")

	// MySQL does not output ON DELETE RESTRICT or ON UPDATE RESTRICT in its table
	// create syntax. Ditto for the completely-equivalent ON DELETE NO ACTION or ON
	// UPDATE NO ACTION in MySQL 8+, but other flavors still display them.
	var deleteRule, updateRule string
	if fk.DeleteRule != "RESTRICT" && (fk.DeleteRule != "NO ACTION" || !flavor.HasDataDictionary()) {
		deleteRule = fmt.Sprintf(" ON DELETE %s", fk.DeleteRule)
	}
	if fk.UpdateRule != "RESTRICT" && (fk.UpdateRule != "NO ACTION" || !flavor.HasDataDictionary()) {
		updateRule = fmt.Sprintf(" ON UPDATE %s", fk.UpdateRule)
	}

	return fmt.Sprintf("CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s)%s%s", EscapeIdentifier(fk.Name), childCols, referencedTable, parentCols, deleteRule, updateRule)
}

// Equals returns true if two ForeignKeys are identical, false otherwise.
func (fk *ForeignKey) Equals(other *ForeignKey) bool {
	if fk == nil || other == nil {
		return fk == other // only equal if BOTH are nil
	}
	return fk.Name == other.Name && fk.Equivalent(other)
}

// Equivalent returns true if two ForeignKeys are functionally equivalent,
// regardless of whether or not they have the same names.
func (fk *ForeignKey) Equivalent(other *ForeignKey) bool {
	if fk == nil || other == nil {
		return fk == other // only equivalent if BOTH are nil
	}

	if fk.ReferencedSchemaName != other.ReferencedSchemaName || fk.ReferencedTableName != other.ReferencedTableName {
		return false
	}
	if fk.UpdateRule != other.UpdateRule || fk.DeleteRule != other.DeleteRule {
		return false
	}
	if len(fk.Columns) != len(other.Columns) {
		return false
	}
	for n := range fk.Columns {
		if fk.Columns[n].Name != other.Columns[n].Name || fk.ReferencedColumnNames[n] != other.ReferencedColumnNames[n] {
			return false
		}
	}
	return true
}
