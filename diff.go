// Package tengo (Go La Tengo) is a database automation library. In its current
// form, its functionality is focused on MySQL schema introspection and
// diff'ing. Future releases will add more general-purpose automation features.
package tengo

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

// NextAutoIncMode enumerates various ways of handling AUTO_INCREMENT
// discrepancies between two tables.
type NextAutoIncMode int

// Constants for how to handle next-auto-inc values in table diffs. Usually
// these are ignored in diffs entirely, but in some cases they are included.
const (
	NextAutoIncIgnore      NextAutoIncMode = iota // omit auto-inc value changes in diff
	NextAutoIncIfIncreased                        // only include auto-inc value if the "from" side is less than the "to" side
	NextAutoIncIfAlready                          // only include auto-inc value if the "from" side is already greater than 1
	NextAutoIncAlways                             // always include auto-inc value in diff
)

// StatementModifiers are options that may be applied to adjust the DDL emitted
// for a particular table, and/or generate errors if certain clauses are
// present.
type StatementModifiers struct {
	NextAutoInc     NextAutoIncMode // How to handle differences in next-auto-inc values
	AllowUnsafe     bool            // Whether to allow potentially-destructive DDL (drop table, drop column, modify col type, etc)
	LockClause      string          // Include a LOCK=[value] clause in generated ALTER TABLE
	AlgorithmClause string          // Include an ALGORITHM=[value] clause in generated ALTER TABLE
	IgnoreTable     *regexp.Regexp  // Generate blank DDL if table name matches this regexp
}

// TableDiff interface represents a difference between two tables. Structs
// satisfying this interface can generate a DDL Statement prefix, such as ALTER
// TABLE, CREATE TABLE, DROP TABLE, etc.
type TableDiff interface {
	Statement(StatementModifiers) (string, error)
}

// SchemaDiff stores a set of differences between two database schemas.
type SchemaDiff struct {
	FromSchema        *Schema
	ToSchema          *Schema
	SchemaDDL         string      // a single statement affecting the schema itself (CREATE DATABASE, ALTER DATABASE, or DROP DATABASE), or blank string if n/a
	TableDiffs        []TableDiff // a set of statements that, if run, would turn FromSchema into ToSchema
	SameTables        []*Table    // slice of tables that were identical between schemas
	UnsupportedTables []*Table    // slice of tables that changed, but in ways not parsable by this version of tengo. Table is version from ToSchema.
}

// NewSchemaDiff computes the set of differences between two database schemas.
func NewSchemaDiff(from, to *Schema) *SchemaDiff {
	result := &SchemaDiff{
		FromSchema:        from,
		ToSchema:          to,
		TableDiffs:        make([]TableDiff, 0),
		SameTables:        make([]*Table, 0),
		UnsupportedTables: make([]*Table, 0),
	}

	if from == nil && to == nil {
		return result
	} else if from == nil {
		result.SchemaDDL = to.CreateStatement()
	} else if to == nil {
		result.SchemaDDL = from.DropStatement()
	} else {
		result.SchemaDDL = from.AlterStatement(to.CharSet, to.Collation)
	}

	fromTablesByName := from.TablesByName()
	toTablesByName := to.TablesByName()

	if to != nil {
		for n := range to.Tables {
			newTable := to.Tables[n]
			if _, existedBefore := fromTablesByName[newTable.Name]; !existedBefore {
				result.TableDiffs = append(result.TableDiffs, CreateTable{Table: newTable})
			}
		}
	}

	if from != nil {
		for n := range from.Tables {
			origTable := from.Tables[n]
			newTable, stillExists := toTablesByName[origTable.Name]
			if stillExists {
				clauses, supported := origTable.Diff(newTable)
				if !supported {
					result.UnsupportedTables = append(result.UnsupportedTables, newTable)
				} else if len(clauses) > 0 {
					alter := AlterTable{
						Table:   origTable,
						Clauses: clauses,
					}
					result.TableDiffs = append(result.TableDiffs, alter)
				} else {
					result.SameTables = append(result.SameTables, newTable)
				}
			} else {
				result.TableDiffs = append(result.TableDiffs, DropTable{Table: origTable})
			}
		}
	}

	return result
}

// String returns the set of differences between two schemas as a single string.
func (sd *SchemaDiff) String() string {
	diffStatements := make([]string, len(sd.TableDiffs))
	for n, diff := range sd.TableDiffs {
		stmt, _ := diff.Statement(StatementModifiers{})
		diffStatements[n] = fmt.Sprintf("%s;\n", stmt)
	}
	return strings.Join(diffStatements, "")
}

// ForbiddenDiffError can be returned by TableDiff.Statement when the supplied
// statement modifiers do not permit the generated TableDiff to be used in this
// situation.
type ForbiddenDiffError struct {
	Reason    string
	Statement string
}

// Error satisfies the builtin error interface.
func (e *ForbiddenDiffError) Error() string {
	return e.Reason
}

// NewForbiddenDiffError is a constructor for ForbiddenDiffError.
func NewForbiddenDiffError(reason, statement string) error {
	return &ForbiddenDiffError{
		Reason:    reason,
		Statement: statement,
	}
}

///// CreateTable //////////////////////////////////////////////////////////////

// CreateTable represents a new table that only exists in the right-side ("to")
// schema. It satisfies the TableDiff interface.
type CreateTable struct {
	Table *Table
}

// Statement returns a DDL statement containing CREATE TABLE.
func (ct CreateTable) Statement(mods StatementModifiers) (string, error) {
	if mods.IgnoreTable != nil && mods.IgnoreTable.MatchString(ct.Table.Name) {
		return "", nil
	}
	stmt := ct.Table.CreateStatement
	if ct.Table.HasAutoIncrement() && (mods.NextAutoInc == NextAutoIncIgnore || mods.NextAutoInc == NextAutoIncIfAlready) {
		stmt, _ = ParseCreateAutoInc(stmt)
	}
	return stmt, nil
}

///// DropTable ////////////////////////////////////////////////////////////////

// DropTable represents a table that only exists in the left-side ("from")
// schema. It satisfies the TableDiff interface.
type DropTable struct {
	Table *Table
}

// Statement returns a DDL statement containing DROP TABLE. Note that if mods
// forbid running the statement, *it will still be returned as-is* but err will
// be non-nil. It is the caller's responsibility to handle appropriately.
func (dt DropTable) Statement(mods StatementModifiers) (string, error) {
	if mods.IgnoreTable != nil && mods.IgnoreTable.MatchString(dt.Table.Name) {
		return "", nil
	}
	var err error
	stmt := dt.Table.DropStatement()
	if !mods.AllowUnsafe {
		err = NewForbiddenDiffError("DROP TABLE not permitted", stmt)
	}
	return stmt, err
}

///// AlterTable ///////////////////////////////////////////////////////////////

// AlterTable represents a table that exists on both schemas, but with one or
// more differences in table definition. It satisfies the TableDiff interface.
type AlterTable struct {
	Table   *Table
	Clauses []TableAlterClause
}

// Statement returns a DDL statement containing ALTER TABLE. Note that if mods
// forbid running the statement, *it will still be returned as-is* but err will
// be non-nil. It is the caller's responsibility to handle appropriately.
func (at AlterTable) Statement(mods StatementModifiers) (string, error) {
	if mods.IgnoreTable != nil && mods.IgnoreTable.MatchString(at.Table.Name) {
		return "", nil
	}
	clauseStrings := make([]string, 0, len(at.Clauses))
	var err error
	for _, clause := range at.Clauses {
		if clause, ok := clause.(ChangeAutoIncrement); ok {
			if mods.NextAutoInc == NextAutoIncIgnore {
				continue
			} else if mods.NextAutoInc == NextAutoIncIfIncreased && clause.OldNextAutoIncrement >= clause.NewNextAutoIncrement {
				continue
			} else if mods.NextAutoInc == NextAutoIncIfAlready && clause.OldNextAutoIncrement <= 1 {
				continue
			}
		}
		if err == nil && !mods.AllowUnsafe && clause.Unsafe() {
			err = NewForbiddenDiffError("Unsafe or potentially destructive ALTER TABLE not permitted", "")
		}
		clauseStrings = append(clauseStrings, clause.Clause())
	}

	if len(clauseStrings) == 0 {
		return "", err
	}

	if mods.LockClause != "" {
		lockClause := fmt.Sprintf("LOCK=%s", strings.ToUpper(mods.LockClause))
		clauseStrings = append([]string{lockClause}, clauseStrings...)
	}
	if mods.AlgorithmClause != "" {
		algorithmClause := fmt.Sprintf("ALGORITHM=%s", strings.ToUpper(mods.AlgorithmClause))
		clauseStrings = append([]string{algorithmClause}, clauseStrings...)
	}

	stmt := fmt.Sprintf("%s %s", at.Table.AlterStatement(), strings.Join(clauseStrings, ", "))
	if fde, isForbiddenDiff := err.(*ForbiddenDiffError); isForbiddenDiff {
		fde.Statement = stmt
	}
	return stmt, err
}

///// RenameTable //////////////////////////////////////////////////////////////

// RenameTable represents a table that exists on both schemas, but with a
// different name. It satisfies the TableDiff interface.
type RenameTable struct {
	Table   *Table
	NewName string
}

// Statement returns a DDL statement containing RENAME TABLE.
func (rt RenameTable) Statement(mods StatementModifiers) (string, error) {
	return "", errors.New("Rename Table not yet supported")
}
