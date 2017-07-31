package tengo

import (
	"fmt"
	"strings"
)

// Constraint represents a single Constraint in a table.
type Constraint struct {
	Name                 string
	Column               *Column
	ReferencedSchemaName string
	ReferencedTableName  string
	ReferencedColumnName string
	UpdateRule           string
	DeleteRule           string
}

// Definition returns this Constraint's definition clause, for use as part of a DDL
// statement.
func (cst *Constraint) Definition() string {
	if cst == nil {
		return ""
	}

	// If the referenced schema == "", this means that the foreign key constraint does not reference a column from another database/schema
	// We only include it in the definition if it is not ""
	referencedIdentifierName := ""
	if cst.ReferencedSchemaName != "" {
		referencedIdentifierName = fmt.Sprintf("%s.%s",
			EscapeIdentifier(cst.ReferencedSchemaName),
			EscapeIdentifier(cst.ReferencedTableName))
	} else {
		referencedIdentifierName = fmt.Sprintf("%s",
			EscapeIdentifier(cst.ReferencedTableName))
	}

	//MySQL does not output ON DELETE RESTRICT or ON UPDATE RESTRICT in its table create syntax.
	//Therefore we need to omit these clauses as well if the UpdateRule or DeleteRule == "RESTRICT"
	deleteRule := ""
	if cst.DeleteRule != "RESTRICT" {
		deleteRule = fmt.Sprintf("ON DELETE %s", cst.DeleteRule)
	}

	updateRule := ""
	if cst.UpdateRule != "RESTRICT" {
		updateRule = fmt.Sprintf("ON UPDATE %s", cst.UpdateRule)
	}

	def := fmt.Sprintf("CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s) %s %s",
		EscapeIdentifier(cst.Name),
		EscapeIdentifier(cst.Column.Name),
		referencedIdentifierName,
		EscapeIdentifier(cst.ReferencedColumnName),
		deleteRule,
		updateRule)

	//Trim the tailing spaces which may be brought about due to the use of RESTRICT, which would render some extra spaces at the end.
	return strings.Trim(def, " ")
}

// Equals returns true if two Constraints are identical, false otherwise.
func (cst *Constraint) Equals(other *Constraint) bool {
	// shortcut if both nil pointers, or both pointing to same underlying struct
	if cst == other {
		return true
	}
	// if one is nil, but we already know the two aren't equal, then we know the other is non-nil
	if cst == nil || other == nil {
		return false
	}
	if cst.Name != other.Name {
		return false
	}
	if !cst.Column.Equals(other.Column) {
		return false
	}
	if cst.ReferencedSchemaName != other.ReferencedSchemaName {
		return false
	}
	if cst.ReferencedTableName != other.ReferencedTableName {
		return false
	}
	if cst.ReferencedColumnName != other.ReferencedColumnName {
		return false
	}
	if cst.UpdateRule != other.UpdateRule {
		return false
	}
	if cst.DeleteRule != other.DeleteRule {
		return false
	}
	return true
}
