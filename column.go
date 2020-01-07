package tengo

import (
	"fmt"
	"strings"
)

// Column represents a single column of a table.
type Column struct {
	Name               string `json:"name"`
	TypeInDB           string `json:"type"`
	Nullable           bool   `json:"nullable,omitempty"`
	AutoIncrement      bool   `json:"autoIncrement,omitempty"`
	Default            string `json:"default,omitempty"` // Stored as an expression, i.e. quote-wrapped if string
	OnUpdate           string `json:"onUpdate,omitempty"`
	GenerationExpr     string `json:"generationExpression,omitempty"` // Only populated if generated column
	Virtual            bool   `json:"virtual,omitempty"`
	CharSet            string `json:"charSet,omitempty"`            // Only populated if textual type
	Collation          string `json:"collation,omitempty"`          // Only populated if textual type
	CollationIsDefault bool   `json:"collationIsDefault,omitempty"` // Only populated if textual type; indicates default for CharSet
	Comment            string `json:"comment,omitempty"`
	Invisible          bool   `json:"invisible,omitempty"` // True if a MariaDB 10.3+ invisible column
}

// Definition returns this column's definition clause, for use as part of a DDL
// statement. A table may optionally be supplied, which simply causes CHARACTER
// SET clause to be omitted if the table and column have the same *collation*
// (mirroring the specific display logic used by SHOW CREATE TABLE)
func (c *Column) Definition(flavor Flavor, table *Table) string {
	var charSet, collation, generated, nullability, visibility, autoIncrement, defaultValue, onUpdate, comment string
	if c.CharSet != "" && (table == nil || c.Collation != table.Collation || c.CharSet != table.CharSet) {
		charSet = fmt.Sprintf(" CHARACTER SET %s", c.CharSet)
	}
	// Any flavor: Collations are displayed if not the default for the charset
	// 8.0 only: Collations are also displayed any time a charset is displayed
	if c.Collation != "" && (!c.CollationIsDefault || (charSet != "" && flavor.HasDataDictionary())) {
		collation = fmt.Sprintf(" COLLATE %s", c.Collation)
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
	if c.Invisible {
		visibility = " INVISIBLE"
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
	if c.Comment != "" {
		comment = fmt.Sprintf(" COMMENT '%s'", EscapeValueForCreateTable(c.Comment))
	}
	clauses := []string{
		EscapeIdentifier(c.Name), " ", c.TypeInDB, charSet, collation, generated, nullability, visibility, autoIncrement, defaultValue, onUpdate, comment,
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
	return *c == *other
}
