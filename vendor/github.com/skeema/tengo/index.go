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
	Type       string
}

// Definition returns this index's definition clause, for use as part of a DDL
// statement.
func (idx *Index) Definition(_ Flavor) string {
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
	} else if idx.Type != "BTREE" && idx.Type != "" {
		typeAndName = fmt.Sprintf("%s KEY %s", idx.Type, EscapeIdentifier(idx.Name))
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
	if idx == nil || other == nil {
		return idx == other // only equal if BOTH are nil
	}
	return idx.Name == other.Name && idx.Comment == other.Comment && idx.Equivalent(other)
}

// Equivalent returns true if two Indexes are functionally equivalent,
// regardless of whether or not they have the same names or comments.
func (idx *Index) Equivalent(other *Index) bool {
	if idx == nil || other == nil {
		return idx == other // only equivalent if BOTH are nil
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

// RedundantTo returns true if idx is equivalent to, or a strict subset of,
// other. Both idx and other should be indexes of the same table.
// Uniqueness and sub-parts are accounted for in the logic; for example, a
// unique index is not considered redundant with a non-unique index having
// the same or more cols. A primary key is never redundant, although another
// unique index may be redundant to the primary key.
func (idx *Index) RedundantTo(other *Index) bool {
	if idx == nil || other == nil {
		return false
	}
	if idx.PrimaryKey || (idx.Unique && !other.Unique) {
		return false
	}
	if len(idx.Columns) > len(other.Columns) {
		return false // can't be redundant to an index with fewer cols
	}
	for n, col := range idx.Columns {
		if col.Name != other.Columns[n].Name {
			return false
		}
		if (idx.SubParts[n] == 0 && other.SubParts[n] > 0) || (other.SubParts[n] > 0 && idx.SubParts[n] > other.SubParts[n]) {
			return false
		}
	}
	return true
}
