package tengo

import (
	"fmt"
	"strings"
)

// Column represents a single column of a table.
type Column struct {
	Name                string `json:"name"`
	TypeInDB            string `json:"type"`
	Nullable            bool   `json:"nullable,omitempty"`
	AutoIncrement       bool   `json:"autoIncrement,omitempty"`
	Default             string `json:"default,omitempty"` // Stored as an expression, i.e. quote-wrapped if string
	OnUpdate            string `json:"onUpdate,omitempty"`
	GenerationExpr      string `json:"generationExpression,omitempty"` // Only populated if generated column
	Virtual             bool   `json:"virtual,omitempty"`
	CharSet             string `json:"charSet,omitempty"`       // Only populated if textual type
	Collation           string `json:"collation,omitempty"`     // Only populated if textual type
	ShowCharSet         bool   `json:"showCharSet,omitempty"`   // Include CHARACTER SET in SHOW CREATE TABLE: always true if different than table default, sometimes true in other cases
	ShowCollation       bool   `json:"showCollation,omitempty"` // Include COLLATE in SHOW CREATE TABLE: logic differs by flavor
	Compression         string `json:"compression,omitempty"`   // Only non-empty if using column compression in Percona Server or MariaDB
	Comment             string `json:"comment,omitempty"`
	Invisible           bool   `json:"invisible,omitempty"` // True if an invisible column (MariaDB 10.3+, MySQL 8.0.23+)
	CheckClause         string `json:"check,omitempty"`     // Only non-empty for MariaDB inline check constraint clause
	SpatialReferenceID  uint32 `json:"srid,omitempty"`      // Can be non-zero only for spatial types in MySQL 8+
	HasSpatialReference bool   `json:"has_srid,omitempty"`  // True if SRID attribute present; disambiguates SRID 0 vs no SRID
}

// Definition returns this column's definition clause, for use as part of a DDL
// statement.
func (c *Column) Definition(flavor Flavor) string {
	var compression, charSet, collation, generated, nullability, srid, visibility,
		autoIncrement, defaultValue, onUpdate, colFormat, comment, check string
	if c.Compression != "" && flavor.IsMariaDB() {
		// MariaDB puts compression modifiers in a different place than Percona Server
		compression = fmt.Sprintf(" /*!100301 %s*/", c.Compression)
	}
	if c.CharSet != "" && c.ShowCharSet {
		charSet = " CHARACTER SET " + c.CharSet
	}
	if c.Collation != "" && c.ShowCollation {
		collation = " COLLATE " + c.Collation
	}
	if c.GenerationExpr != "" {
		genKind := "STORED"
		if c.Virtual {
			genKind = "VIRTUAL"
		}
		generated = fmt.Sprintf(" GENERATED ALWAYS AS (%s) %s", c.GenerationExpr, genKind)
	}
	if !c.Nullable {
		nullability = " NOT NULL"
	} else if strings.HasPrefix(c.TypeInDB, "timestamp") {
		// Oddly the timestamp type always displays nullability
		nullability = " NULL"
	}
	if c.HasSpatialReference && flavor.Min(FlavorMySQL80) {
		// Although MariaDB also attribute syntax for this (REF_SYSTEM_ID), it isn't
		// exposed in SHOW CREATE TABLE, so here we restrict to MySQL only
		srid = fmt.Sprintf(" /*!80003 SRID %d */", c.SpatialReferenceID)
	}
	if c.Invisible {
		if flavor.IsMariaDB() {
			visibility = " INVISIBLE"
		} else {
			visibility = " /*!80023 INVISIBLE */"
		}
	}
	if c.AutoIncrement {
		autoIncrement = " AUTO_INCREMENT"
	}
	if c.Default != "" {
		defaultValue = fmt.Sprintf(" DEFAULT %s", c.Default)
	}
	if c.OnUpdate != "" {
		onUpdate = fmt.Sprintf(" ON UPDATE %s", c.OnUpdate)
	}
	if c.Compression != "" && flavor.HasVariant(VariantPercona) {
		colFormat = fmt.Sprintf(" /*!50633 COLUMN_FORMAT %s */", c.Compression)
	}
	if c.Comment != "" {
		comment = fmt.Sprintf(" COMMENT '%s'", EscapeValueForCreateTable(c.Comment))
	}
	if c.CheckClause != "" {
		check = fmt.Sprintf(" CHECK (%s)", c.CheckClause)
	}

	// Attribute ordering differs slightly between MariaDB and MySQL. Also only
	// Maria has inline checks, and only MySQL exposes SRID in SHOW CREATE TABLE.
	clauses := []string{
		EscapeIdentifier(c.Name), " ", c.TypeInDB, compression, charSet, collation, generated, nullability,
	}
	if flavor.IsMariaDB() {
		clauses = append(clauses, visibility, autoIncrement, defaultValue, onUpdate, colFormat, comment, check)
	} else {
		clauses = append(clauses, autoIncrement, srid, defaultValue, onUpdate, visibility, colFormat, comment)
	}
	return strings.Join(clauses, "")
}

// Equals returns true if two columns are identical, false otherwise.
func (c *Column) Equals(other *Column) bool {
	// shortcut if both nil pointers, or both pointing to same underlying struct
	if c == other {
		return true
	}
	// if one is nil, but we already know the two aren't equal, then we know the other is non-nil
	if c == nil || other == nil {
		return false
	}
	// Just compare the fields, they're all simple non-pointer scalars. This does
	// intentionally treat two columns as different if they only differ in
	// cosmetic / non-functional ways; see Column.Equivalent() below for a looser
	// comparison.
	return *c == *other
}

// Equivalent returns true if two columns are equal, or only differ in cosmetic/
// non-functional ways. Cosmetic differences can come about in MySQL 8 when a
// column was created with CHARACTER SET or COLLATION clauses that are
// unnecessary (equal to table's default); or when comparing a table across
// different versions of MySQL 8 (one which supports int display widths, and
// one that removes them).
// Note that, for the purposes of this method, column comments are NOT
// considered cosmetic. This method returns false if c and other only differ
// by a comment.
func (c *Column) Equivalent(other *Column) bool {
	// If they're equal, they're also equivalent
	if c.Equals(other) {
		return true
	}
	// if one is nil, but we already know the two aren't equal, then we know the other is non-nil
	if c == nil || other == nil {
		return false
	}

	// Examine column types with and without integer display widths. If they
	// differ only in *presence/lack* of int display width, this is cosmetic; any
	// other difference (including *changing* an int display width) is functional.
	selfStrippedType, selfHadDisplayWidth := StripDisplayWidth(c.TypeInDB)
	otherStrippedType, otherHadDisplayWidth := StripDisplayWidth(other.TypeInDB)
	if selfStrippedType != otherStrippedType || (c.TypeInDB != other.TypeInDB && selfHadDisplayWidth && otherHadDisplayWidth) {
		return false
	}
	// If we didn't return early, we know either TypeInDB didn't change at all, or
	// it only differs in a cosmetic manner.

	// Make a copy of c, and make all cosmetic-related fields equal to other's, and
	// then check equality again to determine equivalence.
	selfCopy := *c
	selfCopy.TypeInDB = other.TypeInDB
	selfCopy.ShowCharSet = other.ShowCharSet
	selfCopy.ShowCollation = other.ShowCollation
	if (other.CharSet == "utf8mb3" && c.CharSet == "utf8") || (other.CharSet == "utf8" && c.CharSet == "utf8mb3") {
		selfCopy.CharSet = other.CharSet
	}
	if strings.HasPrefix(other.Collation, "utf8mb3_") {
		selfCopy.Collation = strings.Replace(selfCopy.Collation, "utf8_", "utf8mb3_", 1)
	} else if strings.HasPrefix(other.Collation, "utf8_") {
		selfCopy.Collation = strings.Replace(selfCopy.Collation, "utf8mb3_", "utf8_", 1)
	}
	return selfCopy == *other
}
