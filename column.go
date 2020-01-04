package tengo

import (
	"fmt"
	"strings"
)

// ColumnDefault represents the default value for a column.
type ColumnDefault struct {
	Null   bool   `json:"null,omitempty"`
	Quoted bool   `json:"quoted,omitempty"`
	Value  string `json:"value,omitempty"`
}

// ColumnDefaultNull indicates a column has a default value of NULL.
var ColumnDefaultNull = ColumnDefault{Null: true}

// ColumnDefaultValue is a constructor for creating non-NULL,
// non-CURRENT_TIMESTAMP default values.
func ColumnDefaultValue(value string) ColumnDefault {
	return ColumnDefault{
		Quoted: true,
		Value:  value,
	}
}

// ColumnDefaultExpression is a constructor for creating a default value that
// represents a SQL expression, which won't be wrapped in quotes. Examples
// include "CURRENT_TIMESTAMP", "CURRENT_TIMESTAMP(N)" where N is a digit for
// fractional precision, bit-value literals "b'N'" where N is a value expressed
// in binary, or arbitrary default expressions which some flavors support.
func ColumnDefaultExpression(expression string) ColumnDefault {
	return ColumnDefault{Value: expression}
}

// Clause returns the DEFAULT clause for use in a DDL statement. If non-blank,
// it will be prefixed with a space.
func (cd ColumnDefault) Clause(flavor Flavor, col *Column) string {
	if col.AutoIncrement || col.GenerationExpr != "" {
		return ""
	}
	if !flavor.AllowBlobDefaults() && (strings.HasSuffix(col.TypeInDB, "blob") || strings.HasSuffix(col.TypeInDB, "text")) {
		return ""
	}
	if cd.Null {
		if !col.Nullable {
			return ""
		}
		return " DEFAULT NULL"
	} else if cd.Quoted {
		return fmt.Sprintf(" DEFAULT '%s'", EscapeValueForCreateTable(cd.Value))
	} else {
		return fmt.Sprintf(" DEFAULT %s", cd.Value)
	}
}

// Column represents a single column of a table.
type Column struct {
	Name               string        `json:"name"`
	TypeInDB           string        `json:"type"`
	Nullable           bool          `json:"nullable,omitempty"`
	AutoIncrement      bool          `json:"autoIncrement,omitempty"`
	Default            ColumnDefault `json:"default"`
	OnUpdate           string        `json:"onUpdate,omitempty"`
	GenerationExpr     string        `json:"generationExpression,omitempty"` // Only populated if generated column
	Virtual            bool          `json:"virtual,omitempty"`
	CharSet            string        `json:"charSet,omitempty"`            // Only populated if textual type
	Collation          string        `json:"collation,omitempty"`          // Only populated if textual type
	CollationIsDefault bool          `json:"collationIsDefault,omitempty"` // Only populated if textual type; indicates default for CharSet
	Comment            string        `json:"comment,omitempty"`
}

// Definition returns this column's definition clause, for use as part of a DDL
// statement. A table may optionally be supplied, which simply causes CHARACTER
// SET clause to be omitted if the table and column have the same *collation*
// (mirroring the specific display logic used by SHOW CREATE TABLE)
func (c *Column) Definition(flavor Flavor, table *Table) string {
	var charSet, collation, generated, nullability, autoIncrement, onUpdate, comment string
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
	if c.AutoIncrement {
		autoIncrement = " AUTO_INCREMENT"
	}
	defaultValue := c.Default.Clause(flavor, c)
	if c.OnUpdate != "" {
		onUpdate = fmt.Sprintf(" ON UPDATE %s", c.OnUpdate)
	}
	if c.Comment != "" {
		comment = fmt.Sprintf(" COMMENT '%s'", EscapeValueForCreateTable(c.Comment))
	}
	clauses := []string{
		EscapeIdentifier(c.Name), " ", c.TypeInDB, charSet, collation, generated, nullability, autoIncrement, defaultValue, onUpdate, comment,
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
