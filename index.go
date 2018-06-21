package tengo

import (
	"errors"
	"fmt"
	"strings"
)

// Index represents a single index (primary key, unique secondary index, or non-
// unique secondard index) in a table.
type Index struct {
	Name       string
	Columns    []*Column
	SubParts   []uint16
	PrimaryKey bool
	Unique     bool
	Comment    string
}

// Definition returns this index's definition clause, for use as part of a DDL
// statement.
func (idx *Index) Definition() string {
	colParts := make([]string, len(idx.Columns))
	for n := range idx.Columns {
		if idx.SubParts[n] > 0 {
			colParts[n] = fmt.Sprintf("%s(%d)", EscapeIdentifier(idx.Columns[n].Name), idx.SubParts[n])
		} else {
			colParts[n] = fmt.Sprintf("%s", EscapeIdentifier(idx.Columns[n].Name))
		}
	}
	var typeAndName, comment string
	if idx.PrimaryKey {
		if !idx.Unique {
			panic(errors.New("Index is primary key, but isn't marked as unique"))
		}
		typeAndName = "PRIMARY KEY"
	} else if idx.Unique {
		typeAndName = fmt.Sprintf("UNIQUE KEY %s", EscapeIdentifier(idx.Name))
	} else {
		typeAndName = fmt.Sprintf("KEY %s", EscapeIdentifier(idx.Name))
	}
	if idx.Comment != "" {
		comment = fmt.Sprintf(" COMMENT '%s'", EscapeValueForCreateTable(idx.Comment))
	}

	return fmt.Sprintf("%s (%s)%s", typeAndName, strings.Join(colParts, ","), comment)
}

// Equals returns true if two indexes are identical, false otherwise.
func (idx *Index) Equals(other *Index) bool {
	// shortcut if both nil pointers, or both pointing to same underlying struct
	if idx == other {
		return true
	}
	// if one is nil, but we already know the two aren't equal, then we know the other is non-nil
	if idx == nil || other == nil {
		return false
	}
	if idx.Name != other.Name || idx.Comment != other.Comment {
		return false
	}
	if idx.PrimaryKey != other.PrimaryKey || idx.Unique != other.Unique {
		return false
	}
	if len(idx.Columns) != len(other.Columns) {
		return false
	}
	for n, col := range idx.Columns {
		if col.Name != other.Columns[n].Name || idx.SubParts[n] != other.SubParts[n] {
			return false
		}
	}
	return true
}
