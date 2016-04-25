package tengo

import (
	"errors"
	"fmt"
	"strings"
)

type Index struct {
	Name       string
	Columns    []*Column
	SubParts   []uint16
	PrimaryKey bool
	Unique     bool
}

func (idx *Index) Definition() string {
	if idx == nil {
		return ""
	}
	colParts := make([]string, len(idx.Columns))
	for n := range idx.Columns {
		if idx.SubParts[n] > 0 {
			colParts[n] = fmt.Sprintf("%s(%d)", EscapeIdentifier(idx.Columns[n].Name), idx.SubParts[n])
		} else {
			colParts[n] = fmt.Sprintf("%s", EscapeIdentifier(idx.Columns[n].Name))
		}
	}
	var typeAndName string
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

	return fmt.Sprintf("%s (%s)", typeAndName, strings.Join(colParts, ","))
}

func (idx *Index) Equals(other *Index) bool {
	// shortcut if both nil pointers, or both pointing to same underlying struct
	if idx == other {
		return true
	}
	// if one is nil, but we already know the two aren't equal, then we know the other is non-nil
	if idx == nil || other == nil {
		return false
	}
	if idx.Name != other.Name {
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
