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
	Parts      []IndexPart
	PrimaryKey bool
	Unique     bool
	Invisible  bool
	Comment    string
	Type       string
}

// IndexPart represents an individual indexed column or expression. Each index
// has one or more IndexPart values.
type IndexPart struct {
	ColumnName   string // name of column, or empty if expression
	Expression   string // expression value (MySQL 8+), or empty if column
	PrefixLength uint16 // nonzero if only a prefix of column is indexed
	Descending   bool   // if true, collation is descending (MySQL 8+)
}

// Definition returns this index's definition clause, for use as part of a DDL
// statement.
func (idx *Index) Definition(flavor Flavor) string {
	parts := make([]string, len(idx.Parts))
	for n := range idx.Parts {
		parts[n] = idx.Parts[n].Definition(flavor)
	}
	var typeAndName, comment, invis string
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
	if idx.Invisible {
		invis = " /*!80000 INVISIBLE */"
	}
	return fmt.Sprintf("%s (%s)%s%s", typeAndName, strings.Join(parts, ","), comment, invis)
}

// Equals returns true if two indexes are completely identical, false otherwise.
func (idx *Index) Equals(other *Index) bool {
	if idx == nil || other == nil {
		return idx == other // only equal if BOTH are nil
	}
	return idx.Invisible == other.Invisible && idx.EqualsIgnoringVisibility(other)
}

// EqualsIgnoringVisibility returns true if two indexes are identical, or only
// differ in visibility.
func (idx *Index) EqualsIgnoringVisibility(other *Index) bool {
	if idx == nil || other == nil {
		return idx == other // only equal if BOTH are nil
	}
	return idx.Name == other.Name && idx.Comment == other.Comment && idx.Equivalent(other)
}

// OnlyVisibilityDiffers returns true if idx and other have different values
// for Invisible, but otherwise are equal.
func (idx *Index) OnlyVisibilityDiffers(other *Index) bool {
	if idx == nil || other == nil {
		return false
	}
	return idx.Invisible != other.Invisible && idx.EqualsIgnoringVisibility(other)
}

// Equivalent returns true if two Indexes are functionally equivalent,
// regardless of whether or not they have the same names, comments, or
// visibility.
func (idx *Index) Equivalent(other *Index) bool {
	if idx == nil || other == nil {
		return idx == other // only equivalent if BOTH are nil
	}
	if idx.PrimaryKey != other.PrimaryKey || idx.Unique != other.Unique || idx.Type != other.Type {
		return false
	}
	if len(idx.Parts) != len(other.Parts) {
		return false
	}
	for n := range idx.Parts {
		if idx.Parts[n] != other.Parts[n] {
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
	if idx.PrimaryKey || (idx.Unique && !other.Unique) || idx.Type != other.Type {
		return false
	}
	if !idx.Invisible && other.Invisible {
		return false // a visible index is never redundant to an invisible one
	}
	if idx.Type == "FULLTEXT" && len(idx.Parts) != len(other.Parts) {
		return false // FT composite indexes don't behave like BTREE in terms of left-right prefixing
	} else if len(idx.Parts) > len(other.Parts) {
		return false // can't be redundant to an index with fewer cols
	}
	for n, part := range idx.Parts {
		if part.ColumnName != other.Parts[n].ColumnName || part.Expression != other.Parts[n].Expression || part.Descending != other.Parts[n].Descending {
			return false
		}
		partPrefix, otherPrefix := part.PrefixLength, other.Parts[n].PrefixLength
		if otherPrefix > 0 && (partPrefix == 0 || partPrefix > otherPrefix) {
			return false
		}
	}
	return true
}

// Definition returns this index part's definition clause.
func (part *IndexPart) Definition(_ Flavor) string {
	var base, prefix, collation string
	if part.ColumnName != "" {
		base = EscapeIdentifier(part.ColumnName)
	} else {
		base = fmt.Sprintf("(%s)", part.Expression)
	}
	if part.PrefixLength > 0 {
		prefix = fmt.Sprintf("(%d)", part.PrefixLength)
	}
	if part.Descending {
		collation = " DESC"
	}
	return fmt.Sprintf("%s%s%s", base, prefix, collation)
}
