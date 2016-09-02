package tengo

import (
	"fmt"
	"strings"
)

type ColumnDefault struct {
	Null   bool
	Quoted bool
	Value  string
}

var ColumnDefaultNull = ColumnDefault{Null: true}
var ColumnDefaultCurrentTimestamp = ColumnDefault{Value: "CURRENT_TIMESTAMP"}

func ColumnDefaultValue(value string) ColumnDefault {
	return ColumnDefault{
		Quoted: true,
		Value:  value,
	}
}

// EscapedValue returns Value escaped in the same manner as SHOW CREATE TABLE
func (cd ColumnDefault) EscapedValue() string {
	value := strings.Replace(cd.Value, "\\", "\\\\", -1)
	value = strings.Replace(value, "\000", "\\0", -1)
	value = strings.Replace(value, "'", "''", -1)
	return value
}

// Clause returns the DEFAULT clause for use in DDL
func (cd ColumnDefault) Clause() string {
	if cd.Null {
		return "DEFAULT NULL"
	} else if cd.Quoted {
		return fmt.Sprintf("DEFAULT '%s'", cd.EscapedValue())
	} else {
		return fmt.Sprintf("DEFAULT %s", cd.Value)
	}
}

type Column struct {
	Name          string
	TypeInDB      string
	Nullable      bool
	AutoIncrement bool
	Default       ColumnDefault
	Extra         string
	//Comment       string
}

func (c Column) Definition() string {
	var nullability, autoIncrement, defaultValue, extraModifiers string
	emitDefault := c.CanHaveDefault()
	if !c.Nullable {
		nullability = " NOT NULL"
		if c.Default.Null {
			emitDefault = false
		}
	} else if emitDefault && c.Default == ColumnDefaultCurrentTimestamp {
		nullability = " NULL"
	}
	if c.AutoIncrement {
		autoIncrement = " AUTO_INCREMENT"
	}
	if emitDefault {
		defaultValue = fmt.Sprintf(" %s", c.Default.Clause())
	}
	if c.Extra != "" {
		extraModifiers = fmt.Sprintf(" %s", c.Extra)
	}
	return fmt.Sprintf("%s %s%s%s%s%s", EscapeIdentifier(c.Name), c.TypeInDB, nullability, autoIncrement, defaultValue, extraModifiers)
}

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

// Returns true if the column is allowed to have a DEFAULT clause
func (c Column) CanHaveDefault() bool {
	if c.AutoIncrement {
		return false
	}
	// MySQL does not permit defaults for these types
	if strings.HasSuffix(c.TypeInDB, "blob") || strings.HasSuffix(c.TypeInDB, "text") {
		return false
	}
	return true
}
