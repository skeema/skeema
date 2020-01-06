package tengo

import (
	"fmt"
)

// Schema represents a database schema.
type Schema struct {
	Name      string     `json:"databaseName"`
	CharSet   string     `json:"defaultCharSet"`
	Collation string     `json:"defaultCollation"`
	Tables    []*Table   `json:"tables,omitempty"`
	Routines  []*Routine `json:"routines,omitempty"`
}

// TablesByName returns a mapping of table names to Table struct pointers, for
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

// ProceduresByName returns a mapping of stored procedure names to Routine
// struct pointers, for all stored procedures in the schema.
func (s *Schema) ProceduresByName() map[string]*Routine {
	return s.routinesByNameAndType(ObjectTypeProc)
}

// FunctionsByName returns a mapping of function names to Routine struct
// pointers, for all functions in the schema.
func (s *Schema) FunctionsByName() map[string]*Routine {
	return s.routinesByNameAndType(ObjectTypeFunc)
}

func (s *Schema) routinesByNameAndType(ot ObjectType) map[string]*Routine {
	if s == nil {
		return map[string]*Routine{}
	}
	result := make(map[string]*Routine, len(s.Routines))
	for _, r := range s.Routines {
		if r.Type == ot {
			result[r.Name] = r
		}
	}
	return result
}

// ObjectDefinitions returns a mapping of ObjectKey (type+name) to an SQL string
// containing the corresponding CREATE statement, for all supported object types
// in the schema.
// Note that the returned map does NOT include an entry for the schema itself.
func (s *Schema) ObjectDefinitions() map[ObjectKey]string {
	dict := make(map[ObjectKey]string)
	for name, table := range s.TablesByName() {
		key := ObjectKey{Type: ObjectTypeTable, Name: name}
		dict[key] = table.CreateStatement
	}
	for name, procedure := range s.ProceduresByName() {
		key := ObjectKey{Type: ObjectTypeProc, Name: name}
		dict[key] = procedure.CreateStatement
	}
	for name, function := range s.FunctionsByName() {
		key := ObjectKey{Type: ObjectTypeFunc, Name: name}
		dict[key] = function.CreateStatement
	}
	return dict
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
