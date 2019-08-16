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

// parseCreateStatement populates Body, ParamString, and ReturnDataType by
// parsing CreateStatement. It is used during introspection of routines in
// situations where the mysql.proc table is unavailable or does not exist.
func (r *Routine) parseCreateStatement(flavor Flavor, schema string) error {
	// Find matching parens around arg list
	argStart := strings.IndexRune(r.CreateStatement, '(')
	var argEnd int
	nestCount := 1
	for pos, r := range r.CreateStatement {
		if nestCount == 0 {
			argEnd = pos
			break
		} else if pos <= argStart {
			continue
		} else if r == '(' {
			nestCount++
		} else if r == ')' {
			nestCount--
		}
	}
	if argStart <= 0 || argEnd <= 0 {
		return fmt.Errorf("Failed to parse SHOW CREATE %s %s.%s: %s", r.Type.Caps(), EscapeIdentifier(schema), EscapeIdentifier(r.Name), r.CreateStatement)
	}
	r.ParamString = r.CreateStatement[argStart+1 : argEnd-1]

	if r.Type == ObjectTypeFunc {
		retStart := argEnd + len(" RETURNS ")
		retEnd := retStart + strings.IndexRune(r.CreateStatement[retStart:], '\n')
		if retEnd <= 0 {
			return fmt.Errorf("Failed to parse SHOW CREATE %s %s.%s: %s", r.Type.Caps(), EscapeIdentifier(schema), EscapeIdentifier(r.Name), r.CreateStatement)
		}
		r.ReturnDataType = r.CreateStatement[retStart:retEnd]
	}

	// Attempt to replace r.Body with one that doesn't have character conversion problems
	if header := r.head(flavor); strings.HasPrefix(r.CreateStatement, header) {
		r.Body = r.CreateStatement[len(header):]
	}
	return nil
}
