package tengo

import (
	"fmt"
	"strings"
)

// Routine represents a stored procedure or function.
type Routine struct {
	Name              string
	Type              ObjectType // Will be ObjectTypeProcedure or ObjectTypeFunction
	Body              string     // From information_schema; different char escaping vs CreateStatement
	ParamString       string     // Formatted as per original CREATE
	ReturnDataType    string     // Includes charset/collation when relevant
	Definer           string
	DatabaseCollation string // from creation time
	Comment           string
	Deterministic     bool
	SQLDataAccess     string
	SecurityType      string
	SQLMode           string // sql_mode in effect at creation time
	CreateStatement   string // complete SHOW CREATE obtained from an instance
}

// Definition generates and returns a canonical CREATE PROCEDURE or CREATE
// FUNCTION statement based on the Routine's Go field values.
func (r *Routine) Definition(flavor Flavor) string {
	return fmt.Sprintf("%s%s", r.head(flavor), r.Body)
}

// head returns the portion of a CREATE statement prior to the body.
func (r *Routine) head(_ Flavor) string {
	var definer, returnClause, characteristics string

	atPos := strings.LastIndex(r.Definer, "@")
	if atPos >= 0 {
		definer = fmt.Sprintf("%s@%s", EscapeIdentifier(r.Definer[0:atPos]), EscapeIdentifier(r.Definer[atPos+1:]))
	}
	if r.Type == ObjectTypeFunc {
		returnClause = fmt.Sprintf(" RETURNS %s", r.ReturnDataType)
	}

	clauses := make([]string, 0)
	if r.SQLDataAccess != "CONTAINS SQL" {
		clauses = append(clauses, fmt.Sprintf("    %s\n", r.SQLDataAccess))
	}
	if r.Deterministic {
		clauses = append(clauses, "    DETERMINISTIC\n")
	}
	if r.SecurityType != "DEFINER" {
		clauses = append(clauses, fmt.Sprintf("    SQL SECURITY %s\n", r.SecurityType))
	}
	if r.Comment != "" {
		clauses = append(clauses, fmt.Sprintf("    COMMENT '%s'\n", EscapeValueForCreateTable(r.Comment)))
	}
	characteristics = strings.Join(clauses, "")

	return fmt.Sprintf("CREATE DEFINER=%s %s %s(%s)%s\n%s",
		definer,
		r.Type.Caps(),
		EscapeIdentifier(r.Name),
		r.ParamString,
		returnClause,
		characteristics)
}

// Equals returns true if two routines are identical, false otherwise.
func (r *Routine) Equals(other *Routine) bool {
	// shortcut if both nil pointers, or both pointing to same underlying struct
	if r == other {
		return true
	}
	// if one is nil, but the two pointers aren't equal, then one is non-nil
	if r == nil || other == nil {
		return false
	}

	// All fields are simple scalars, so we can just use equality check once we
	// know neither is nil
	return *r == *other
}

// DropStatement returns a SQL statement that, if run, would drop this routine.
func (r *Routine) DropStatement() string {
	return fmt.Sprintf("DROP %s %s", r.Type.Caps(), EscapeIdentifier(r.Name))
}
