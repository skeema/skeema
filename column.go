package tengo

import (
	"fmt"
	"strings"
)

type Column struct {
	Name          string
	TypeInDB      string
	Nullable      bool
	AutoIncrement bool
	Default       string
	//Comment string
}

func (c Column) Definition() string {
	var notNull, autoIncrement, defaultValue string
	emitDefault := c.CanHaveDefault()
	if !c.Nullable {
		notNull = " NOT NULL"
	} else if c.Default == "" && emitDefault {
		defaultValue = " DEFAULT NULL"
	}
	if c.AutoIncrement {
		autoIncrement = " AUTO_INCREMENT"
	}
	if c.Default != "" && emitDefault {
		// TODO: within the default, needs proper escaping
		// TODO: handle explicit blank string defaults
		defaultValue = fmt.Sprintf(" DEFAULT '%s'", c.Default)
	}
	return fmt.Sprintf("%s %s%s%s%s", EscapeIdentifier(c.Name), c.TypeInDB, notNull, autoIncrement, defaultValue)
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
	return (c.Name == other.Name &&
		c.TypeInDB == other.TypeInDB &&
		c.Nullable == other.Nullable &&
		c.AutoIncrement == other.AutoIncrement)
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
