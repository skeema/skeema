package tengo

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/VividCortex/mysqlerr"
	"github.com/jmoiron/sqlx"
	"golang.org/x/sync/errgroup"
)

// Routine represents a stored procedure or function.
type Routine struct {
	Name              string     `json:"name"`
	Type              ObjectType `json:"type"`                     // Will be ObjectTypeProcedure or ObjectTypeFunction
	Body              string     `json:"body"`                     // Has correct escaping despite I_S mutilating it
	ParamString       string     `json:"paramString"`              // Formatted as per original CREATE
	ReturnDataType    string     `json:"returnDataType,omitempty"` // Includes charset/collation when relevant
	Definer           string     `json:"definer"`
	DatabaseCollation string     `json:"dbCollation"` // from creation time
	Comment           string     `json:"comment,omitempty"`
	Deterministic     bool       `json:"deterministic,omitempty"`
	SQLDataAccess     string     `json:"sqlDataAccess,omitempty"`
	SecurityType      string     `json:"securityType"`
	SQLMode           string     `json:"sqlMode"`    // sql_mode in effect at creation time
	CreateStatement   string     `json:"showCreate"` // complete SHOW CREATE obtained from an instance
}

// ObjectKey returns a value useful for uniquely refering to a Routine within a
// single Schema, for example as a map key.
func (r *Routine) ObjectKey() ObjectKey {
	if r == nil {
		return ObjectKey{}
	}
	return ObjectKey{
		Type: r.Type,
		Name: r.Name,
	}
}

// Def returns the routine's CREATE statement as a string.
func (r *Routine) Def() string {
	return r.CreateStatement
}

// Definition generates and returns a canonical CREATE PROCEDURE or CREATE
// FUNCTION statement based on the Routine's Go field values.
func (r *Routine) Definition(flavor Flavor) string {
	return fmt.Sprintf("%s%s", r.head(flavor), r.Body)
}

// DefinerClause returns the routine's DEFINER, quoted/escaped in a way
// consistent with SHOW CREATE.
func (r *Routine) DefinerClause() string {
	if atPos := strings.LastIndex(r.Definer, "@"); atPos >= 0 {
		return fmt.Sprintf("DEFINER=%s@%s", EscapeIdentifier(r.Definer[0:atPos]), EscapeIdentifier(r.Definer[atPos+1:]))
	}
	return fmt.Sprintf("DEFINER=%s", r.Definer)
}

// head returns the portion of a CREATE statement prior to the body.
func (r *Routine) head(_ Flavor) string {
	var definer, returnClause, characteristics string

	if r.Definer != "" {
		definer = r.DefinerClause() + " "
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

	return fmt.Sprintf("CREATE %s%s %s(%s)%s\n%s",
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

///// Diff logic ///////////////////////////////////////////////////////////////

// RoutineDiff represents a difference between two routines.
type RoutineDiff struct {
	From        *Routine
	To          *Routine
	ForReplace  bool // if true, routine is being dropped/re-created to replace
	ForMetadata bool // if true, routine is being replaced only to update creation-time metadata
}

// ObjectKey returns a value representing the type and name of the routine being
// diff'ed. The type will be either ObjectTypeFunc or ObjectTypeProc. The name
// will be the From side routine, unless this is a Create, in which case the To
// side routine name is used.
func (rd *RoutineDiff) ObjectKey() ObjectKey {
	if rd != nil && rd.From != nil {
		return rd.From.ObjectKey()
	} else if rd != nil && rd.To != nil {
		return rd.To.ObjectKey()
	}
	return ObjectKey{}
}

// DiffType returns the type of diff operation.
func (rd *RoutineDiff) DiffType() DiffType {
	if rd == nil || (rd.To == nil && rd.From == nil) {
		return DiffTypeNone
	} else if rd.To == nil {
		return DiffTypeDrop
	} else if rd.From == nil {
		return DiffTypeCreate
	}
	return DiffTypeAlter
}

// Statement returns the full DDL statement corresponding to the RoutineDiff. A
// blank string may be returned if the mods indicate the statement should be
// skipped. If the mods indicate the statement should be disallowed, it will
// still be returned as-is, but the error will be non-nil. Be sure not to
// ignore the error value of this method.
func (rd *RoutineDiff) Statement(mods StatementModifiers) (string, error) {
	if rd == nil {
		return "", nil
	}

	// If we're replacing a routine only because its creation-time sql_mode or
	// db collation has changed, only proceed if mods indicate we should. (This
	// type of replacement is effectively opt-in because it is counter-intuitive
	// and obscure.)
	if rd.ForMetadata && !mods.CompareMetadata {
		return "", nil
	}

	var comment string
	mariaReplace := rd.ForReplace && mods.Flavor.IsMariaDB()
	switch rd.DiffType() {
	case DiffTypeCreate:
		if mariaReplace && rd.ForMetadata {
			comment = fmt.Sprintf("# Replacing %s to update metadata\n", rd.ObjectKey())
		}
		stmt := rd.To.CreateStatement
		if mariaReplace {
			stmt = strings.Replace(stmt, "CREATE ", "CREATE OR REPLACE ", 1)
		}
		return comment + stmt, nil
	case DiffTypeDrop:
		// MariaDB 10.1+ can use CREATE OR REPLACE, so omit any replacement-motivated
		// DROP statements
		if mariaReplace {
			return "", nil
		}
		if rd.ForMetadata {
			comment = fmt.Sprintf("# Dropping and re-creating %s to update metadata\n", rd.ObjectKey())
		}
		stmt := comment + rd.From.DropStatement()
		var err error
		if !mods.AllowUnsafe {
			err = &ForbiddenDiffError{
				Reason: fmt.Sprintf("DROP %s not permitted", rd.From.Type.Caps()),
			}
		}
		return stmt, err
	default: // DiffTypeAlter and DiffTypeRename not supported yet
		return "", fmt.Errorf("Unsupported diff type %d", rd.DiffType())
	}
}

// IsCompoundStatement returns true if the diff is a compound CREATE statement,
// requiring special delimiter handling.
func (rd *RoutineDiff) IsCompoundStatement() bool {
	return rd.To != nil && ParseStatementInString(rd.To.CreateStatement).Compound
}

func compareRoutines(from, to *Schema) []*RoutineDiff {
	routineDiffs := compareRoutinesByName(from.ProceduresByName(), to.ProceduresByName())
	routineDiffs = append(routineDiffs, compareRoutinesByName(from.FunctionsByName(), to.FunctionsByName())...)
	return routineDiffs
}

// compareRoutinesByName is a helper function for comparing maps of procs or
// funcs, keyed by name. Both maps should only contain the same type of routine.
// In other words, both fromByName and toByName should only contain procs, or
// both only contain funcs. No validation of this is performed here.
func compareRoutinesByName(fromByName map[string]*Routine, toByName map[string]*Routine) (routineDiffs []*RoutineDiff) {
	for name, fromRoutine := range fromByName {
		toRoutine, stillExists := toByName[name]
		if !stillExists {
			routineDiffs = append(routineDiffs, &RoutineDiff{From: fromRoutine})
		} else if !fromRoutine.Equals(toRoutine) {
			// Determine if only the creation-time metadata (db collation, sql_mode)
			// has changed, and flag the diffs if so. This type of change requires
			// StatementModifiers to execute, since its appearance is counterintuitive
			// (since otherwise it looks like a routine is being dropped and recreated
			// with the exact same statement)
			metadataOnly := fromRoutine.CreateStatement == toRoutine.CreateStatement

			// TODO: Currently this handles all changes to existing routines via DROP-
			// then-ADD, but characteristic-only changes could use ALTER FUNCTION /
			// ALTER PROCEDURE instead.
			routineDiffs = append(routineDiffs,
				&RoutineDiff{From: fromRoutine, ForReplace: true, ForMetadata: metadataOnly},
				&RoutineDiff{To: toRoutine, ForReplace: true, ForMetadata: metadataOnly},
			)
		}
	}
	for name, toRoutine := range toByName {
		if _, alreadyExists := fromByName[name]; !alreadyExists {
			routineDiffs = append(routineDiffs, &RoutineDiff{To: toRoutine})
		}
	}
	return
}

///// Introspection logic //////////////////////////////////////////////////////

func querySchemaRoutines(ctx context.Context, db *sqlx.DB, schema string, flavor Flavor) ([]*Routine, error) {
	// Obtain the routines in the schema
	// We completely exclude routines that the user can call, but not examine --
	// e.g. user has EXECUTE priv but missing other vital privs. In this case
	// routine_definition will be NULL.
	var rawRoutines []struct {
		Name              string         `db:"routine_name"`
		Type              string         `db:"routine_type"`
		Body              sql.NullString `db:"routine_definition"`
		IsDeterministic   string         `db:"is_deterministic"`
		SQLDataAccess     string         `db:"sql_data_access"`
		SecurityType      string         `db:"security_type"`
		SQLMode           string         `db:"sql_mode"`
		Comment           string         `db:"routine_comment"`
		Definer           string         `db:"definer"`
		DatabaseCollation string         `db:"database_collation"`
	}
	// Note on this query: MySQL 8.0 changes information_schema column names to
	// come back from queries in all caps, so we need to explicitly use AS clauses
	// in order to get them back as lowercase and have sqlx Select() work
	query := `
		SELECT SQL_BUFFER_RESULT
		       r.routine_name AS routine_name, UPPER(r.routine_type) AS routine_type,
		       r.routine_definition AS routine_definition,
		       UPPER(r.is_deterministic) AS is_deterministic,
		       UPPER(r.sql_data_access) AS sql_data_access,
		       UPPER(r.security_type) AS security_type,
		       r.sql_mode AS sql_mode, r.routine_comment AS routine_comment,
		       r.definer AS definer, r.database_collation AS database_collation
		FROM   information_schema.routines r
		WHERE  r.routine_schema = ? AND routine_definition IS NOT NULL`
	if err := db.SelectContext(ctx, &rawRoutines, query, schema); err != nil {
		return nil, fmt.Errorf("Error querying information_schema.routines for schema %s: %s", schema, err)
	}
	if len(rawRoutines) == 0 {
		return []*Routine{}, nil
	}
	routines := make([]*Routine, len(rawRoutines))
	dict := make(map[ObjectKey]*Routine, len(rawRoutines))
	for n, rawRoutine := range rawRoutines {
		routines[n] = &Routine{
			Name:              rawRoutine.Name,
			Type:              ObjectType(strings.ToLower(rawRoutine.Type)),
			Body:              rawRoutine.Body.String, // This contains incorrect formatting conversions; overwritten later
			Definer:           rawRoutine.Definer,
			DatabaseCollation: rawRoutine.DatabaseCollation,
			Comment:           rawRoutine.Comment,
			Deterministic:     rawRoutine.IsDeterministic == "YES",
			SQLDataAccess:     rawRoutine.SQLDataAccess,
			SecurityType:      rawRoutine.SecurityType,
			SQLMode:           rawRoutine.SQLMode,
		}
		if routines[n].Type != ObjectTypeProc && routines[n].Type != ObjectTypeFunc {
			return nil, fmt.Errorf("Unsupported routine type %s found in %s.%s", rawRoutine.Type, schema, rawRoutine.Name)
		}
		key := ObjectKey{Type: routines[n].Type, Name: routines[n].Name}
		dict[key] = routines[n]
	}

	// Obtain param string, return type string, and full create statement:
	// We can't rely only on information_schema, since it doesn't have the param
	// string formatted in the same way as the original CREATE, nor does
	// routines.body handle strings/charsets correctly for re-runnable SQL.
	// In flavors without the new data dictionary, we first try querying mysql.proc
	// to bulk-fetch sufficient info to rebuild the CREATE without needing to run
	// a SHOW CREATE per routine.
	// If mysql.proc doesn't exist or that query fails, we then run a SHOW CREATE
	// per routine, using multiple goroutines for performance reasons.
	var alreadyObtained int
	if !flavor.Min(FlavorMySQL80) {
		var rawRoutineMeta []struct {
			Name      string `db:"name"`
			Type      string `db:"type"`
			Body      string `db:"body"`
			ParamList string `db:"param_list"`
			Returns   string `db:"returns"`
		}
		query := `
			SELECT name, type, body, param_list, returns
			FROM   mysql.proc
			WHERE  db = ?`
		// Errors here are non-fatal. No need to even check; slice will be empty which is fine
		db.SelectContext(ctx, &rawRoutineMeta, query, schema)
		for _, meta := range rawRoutineMeta {
			key := ObjectKey{Type: ObjectType(strings.ToLower(meta.Type)), Name: meta.Name}
			if routine, ok := dict[key]; ok {
				routine.ParamString = strings.Replace(meta.ParamList, "\r\n", "\n", -1)
				routine.ReturnDataType = meta.Returns
				routine.Body = strings.Replace(meta.Body, "\r\n", "\n", -1)
				routine.CreateStatement = routine.Definition(flavor)
				alreadyObtained++
			}
		}
	}

	var err error
	if alreadyObtained < len(routines) {
		g, subCtx := errgroup.WithContext(ctx)
		for n := range routines {
			r := routines[n] // avoid issues with goroutines and loop iterator values
			if r.CreateStatement == "" {
				g.Go(func() (err error) {
					r.CreateStatement, err = showCreateRoutine(subCtx, db, r.Name, r.Type)
					if err == nil {
						r.CreateStatement = strings.Replace(r.CreateStatement, "\r\n", "\n", -1)
						err = r.parseCreateStatement(flavor, schema)
					} else {
						err = fmt.Errorf("Error executing SHOW CREATE %s for %s.%s: %s", r.Type.Caps(), EscapeIdentifier(schema), EscapeIdentifier(r.Name), err)
					}
					return err
				})
			}
		}
		err = g.Wait()
	}

	return routines, err
}

func showCreateRoutine(ctx context.Context, db *sqlx.DB, routine string, ot ObjectType) (create string, err error) {
	query := fmt.Sprintf("SHOW CREATE %s %s", ot.Caps(), EscapeIdentifier(routine))
	if ot == ObjectTypeProc {
		var createRows []struct {
			CreateStatement sql.NullString `db:"Create Procedure"`
		}
		err = db.SelectContext(ctx, &createRows, query)
		if (err == nil && len(createRows) != 1) || IsDatabaseError(err, mysqlerr.ER_SP_DOES_NOT_EXIST) {
			err = sql.ErrNoRows
		} else if err == nil {
			create = createRows[0].CreateStatement.String
		}
	} else if ot == ObjectTypeFunc {
		var createRows []struct {
			CreateStatement sql.NullString `db:"Create Function"`
		}
		err = db.SelectContext(ctx, &createRows, query)
		if (err == nil && len(createRows) != 1) || IsDatabaseError(err, mysqlerr.ER_SP_DOES_NOT_EXIST) {
			err = sql.ErrNoRows
		} else if err == nil {
			create = createRows[0].CreateStatement.String
		}
	} else {
		err = fmt.Errorf("Object type %s is not a routine", ot)
	}
	return
}
