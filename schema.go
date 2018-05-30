package tengo

import (
	"errors"
	"fmt"
)

// Schema represents a database schema.
type Schema struct {
	Name      string
	CharSet   string
	Collation string
	Tables    []*Table
}

// TablesByName returns a mapping of table names to Table struct values, for
// all tables in the schema.
func (s *Schema) TablesByName() map[string]*Table {
	if s == nil {
		return map[string]*Table{}
	}
	result := make(map[string]*Table, len(s.Tables))
	for _, t := range s.Tables {
		result[t.Name] = t
	}
	return result
}

// HasTable returns true if a table with the given name exists in the schema.
func (s *Schema) HasTable(name string) bool {
	return s != nil && s.Table(name) != nil
}

// Table returns a table by name.
func (s *Schema) Table(name string) *Table {
	if s != nil {
		for _, t := range s.Tables {
			if t.Name == name {
				return t
			}
		}
	}
	return nil
}

// Diff returns the set of differences between this schema and another schema.
func (s *Schema) Diff(other *Schema) *SchemaDiff {
	return NewSchemaDiff(s, other)
}

// DropStatement returns a SQL statement that, if run, would drop this schema.
func (s *Schema) DropStatement() string {
	return fmt.Sprintf("DROP DATABASE %s", EscapeIdentifier(s.Name))
}

// CreateStatement returns a SQL statement that, if run, would create this
// schema.
func (s *Schema) CreateStatement() string {
	var charSet, collate string
	if s.CharSet != "" {
		charSet = fmt.Sprintf(" CHARACTER SET %s", s.CharSet)
	}
	if s.Collation != "" {
		collate = fmt.Sprintf(" COLLATE %s", s.Collation)
	}
	return fmt.Sprintf("CREATE DATABASE %s%s%s", EscapeIdentifier(s.Name), charSet, collate)
}

// AlterStatement returns a SQL statement that, if run, would alter this
// schema's default charset and/or collation to the supplied values.
// If charSet is "" and collation isn't, only the collation will be changed.
// If collation is "" and charSet isn't, the default collation for charSet is
// used automatically.
// If both params are "", or if values equal to the schema's current charSet
// and collation are supplied, an empty string is returned.
func (s *Schema) AlterStatement(charSet, collation string) string {
	var charSetClause, collateClause string
	if s.CharSet != charSet && charSet != "" {
		charSetClause = fmt.Sprintf(" CHARACTER SET %s", charSet)
	}
	if s.Collation != collation && collation != "" {
		collateClause = fmt.Sprintf(" COLLATE %s", collation)
	}
	if charSetClause == "" && collateClause == "" {
		return ""
	}
	return fmt.Sprintf("ALTER DATABASE %s%s%s", EscapeIdentifier(s.Name), charSetClause, collateClause)
}

// OverridesServerCharSet checks if the schema's default character set and
// collation differ from an instance's server-level default character set
// and collation. The first return value will be true if the schema's charset
// differs from the instance's; the second return value will be true if the
// schema's collation differs from the instance's.
func (s *Schema) OverridesServerCharSet(instance *Instance) (overridesCharSet bool, overridesCollation bool, err error) {
	if s == nil {
		return false, false, errors.New("Attempted to check character set and collation on a nil schema")
	}

	serverCharSet, serverCollation, err := instance.DefaultCharSetAndCollation()
	if err != nil {
		return false, false, err
	}
	if serverCharSet != s.CharSet {
		// Different charset also inherently means different collation
		return true, true, nil
	} else if serverCollation != s.Collation {
		return false, true, nil
	}
	return false, false, nil
}
