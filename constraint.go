package tengo

import (
	"fmt"
)

// Constraint represents a single Constraint in a table.
type Constraint struct {
	Name                 string
	ColumnName           string
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
	return fmt.Sprintf("CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s) ON DELETE %s ON UPDATE %s",
		EscapeIdentifier(cst.Name),
		EscapeIdentifier(cst.ColumnName),
		EscapeIdentifier(cst.ReferencedTableName),
		EscapeIdentifier(cst.ReferencedColumnName),
		cst.DeleteRule,
		cst.UpdateRule)
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
	if cst.ColumnName != other.ColumnName {
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
