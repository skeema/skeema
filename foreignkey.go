package tengo

import (
	"fmt"
	"strings"
)

// ForeignKey represents a single foreign key constraint in a table.
type ForeignKey struct {
	Name                 string
	Column               *Column
	ReferencedSchemaName string
	ReferencedTableName  string
	ReferencedColumnName string
	UpdateRule           string
	DeleteRule           string
}

// Definition returns this ForeignKey's definition clause, for use as part of a DDL
// statement.
func (fk *ForeignKey) Definition() string {
	if fk == nil {
		return ""
	}

	// If the referenced schema == "", this means that the foreign key constraint does not reference a column from another database/schema
	// We only include it in the definition if it is not ""
	referencedIdentifierName := ""
	if fk.ReferencedSchemaName != "" {
		referencedIdentifierName = fmt.Sprintf("%s.%s",
			EscapeIdentifier(fk.ReferencedSchemaName),
			EscapeIdentifier(fk.ReferencedTableName))
	} else {
		referencedIdentifierName = fmt.Sprintf("%s",
			EscapeIdentifier(fk.ReferencedTableName))
	}

	// MySQL does not output ON DELETE RESTRICT or ON UPDATE RESTRICT in its table create syntax.
	// Therefore we need to omit these clauses as well if the UpdateRule or DeleteRule == "RESTRICT"
	deleteRule := ""
	if fk.DeleteRule != "RESTRICT" {
		deleteRule = fmt.Sprintf(" ON DELETE %s", fk.DeleteRule)
	}

	updateRule := ""
	if fk.UpdateRule != "RESTRICT" {
		updateRule = fmt.Sprintf(" ON UPDATE %s", fk.UpdateRule)
	}

	def := fmt.Sprintf("CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s)%s%s",
		EscapeIdentifier(fk.Name),
		EscapeIdentifier(fk.Column.Name),
		referencedIdentifierName,
		EscapeIdentifier(fk.ReferencedColumnName),
		deleteRule,
		updateRule)

	// Trim the tailing spaces which may be brought about due to the use of RESTRICT, which would render some extra spaces at the end.
	return strings.Trim(def, " ")
}

// Equals returns true if two ForeignKeys are identical, false otherwise.
func (fk *ForeignKey) Equals(other *ForeignKey) bool {
	// shortcut if both nil pointers, or both pointing to same underlying struct
	if fk == other {
		return true
	}
	// if one is nil, but we already know the two aren't equal, then we know the other is non-nil
	if fk == nil || other == nil {
		return false
	}
	if fk.Name != other.Name {
		return false
	}
	if !fk.Column.Equals(other.Column) {
		return false
	}
	if fk.ReferencedSchemaName != other.ReferencedSchemaName {
		return false
	}
	if fk.ReferencedTableName != other.ReferencedTableName {
		return false
	}
	if fk.ReferencedColumnName != other.ReferencedColumnName {
		return false
	}
	if fk.UpdateRule != other.UpdateRule {
		return false
	}
	if fk.DeleteRule != other.DeleteRule {
		return false
	}
	return true
}
