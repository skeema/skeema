// Package tengo (Go La Tengo) is a database automation library. In its current
// form, its functionality is focused on MySQL schema introspection and
// diff'ing. Future releases will add more general-purpose automation features.
package tengo

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/pmezard/go-difflib/difflib"
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

// SchemaDiff stores a set of differences between two database schemas.
type SchemaDiff struct {
	FromSchema *Schema
	ToSchema   *Schema
	SchemaDDL  string       // a single statement affecting the schema itself (CREATE DATABASE, ALTER DATABASE, or DROP DATABASE), or blank string if n/a
	TableDiffs []*TableDiff // a set of statements that, if run, would turn FromSchema into ToSchema
	SameTables []*Table     // slice of tables that were identical between schemas
}

// NewSchemaDiff computes the set of differences between two database schemas.
func NewSchemaDiff(from, to *Schema) *SchemaDiff {
	result := &SchemaDiff{
		FromSchema: from,
		ToSchema:   to,
		TableDiffs: make([]*TableDiff, 0),
		SameTables: make([]*Table, 0),
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
				result.TableDiffs = append(result.TableDiffs, NewCreateTable(newTable))
			}
		}
	}

	if from != nil {
		for n := range from.Tables {
			origTable := from.Tables[n]
			newTable, stillExists := toTablesByName[origTable.Name]
			if stillExists {
				td := NewAlterTable(origTable, newTable)
				if td != nil {
					result.TableDiffs = append(result.TableDiffs, td)
				} else {
					result.SameTables = append(result.SameTables, newTable)
				}
			} else {
				result.TableDiffs = append(result.TableDiffs, NewDropTable(origTable))
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

// FilteredTableDiffs returns any TableDiffs of the specified type(s).
func (sd *SchemaDiff) FilteredTableDiffs(onlyTypes ...TableDiffType) []*TableDiff {
	result := make([]*TableDiff, 0, len(sd.TableDiffs))
	for _, td := range sd.TableDiffs {
		for _, typ := range onlyTypes {
			if td.Type == typ {
				result = append(result, td)
				break
			}
		}
	}
	return result
}

///// Errors ///////////////////////////////////////////////////////////////////

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

// IsForbiddenDiff returns true if err represents an "unsafe" alteration that
// has not explicitly been permitted by the supplied StatementModifiers.
func IsForbiddenDiff(err error) bool {
	_, ok := err.(*ForbiddenDiffError)
	return ok
}

// UnsupportedDiffError can be returned by TableDiff.Statement if Tengo is
// unable to transform the table due to use of unsupported features.
type UnsupportedDiffError struct {
	Name                string
	ExpectedCreateTable string
	ActualCreateTable   string
}

// Error satisfies the builtin error interface.
func (e *UnsupportedDiffError) Error() string {
	return fmt.Sprintf("Table %s uses unsupported features and cannot be diff'ed", e.Name)
}

// ExtendedError returns a string with more information about why the table is
// not supported.
func (e *UnsupportedDiffError) ExtendedError() string {
	diff := difflib.UnifiedDiff{
		A:        difflib.SplitLines(e.ExpectedCreateTable),
		B:        difflib.SplitLines(e.ActualCreateTable),
		FromFile: "Expected",
		ToFile:   "MySQL-actual",
		Context:  0,
	}
	diffText, err := difflib.GetUnifiedDiffString(diff)
	if err != nil {
		return err.Error()
	}
	return diffText
}

// IsUnsupportedDiff returns true if err represents a table that cannot be
// diff'ed due to use of features not supported by this package.
func IsUnsupportedDiff(err error) bool {
	_, ok := err.(*UnsupportedDiffError)
	return ok
}

///// TableDiff ////////////////////////////////////////////////////////////////

// TableDiffType enumerates possible ways that tables differ.
type TableDiffType int

// Constants representing the types of diffs between tables.
const (
	TableDiffCreate TableDiffType = iota // CREATE TABLE
	TableDiffAlter                       // ALTER TABLE
	TableDiffDrop                        // DROP TABLE
	TableDiffRename                      // RENAME TABLE
)

func (tdt TableDiffType) String() string {
	switch tdt {
	case TableDiffCreate:
		return "CREATE"
	case TableDiffAlter:
		return "ALTER"
	case TableDiffDrop:
		return "DROP"
	default: // TableDiffRename not supported yet
		panic(fmt.Errorf("Unsupported diff type %d", tdt))
	}
}

// TableDiff represents a difference between two tables.
type TableDiff struct {
	Type         TableDiffType
	From         *Table
	To           *Table
	alterClauses []TableAlterClause
	supported    bool
}

// NewCreateTable returns a *TableDiff representing a CREATE TABLE statement,
// i.e. a table that only exists in the "to" side schema in a diff.
func NewCreateTable(table *Table) *TableDiff {
	return &TableDiff{
		Type:      TableDiffCreate,
		To:        table,
		supported: true,
	}
}

// NewAlterTable returns a *TableDiff representing an ALTER TABLE statement,
// i.e. a table that exists in the "from" and "to" side schemas but with one
// or more differences. If the supplied tables are identical, nil will be
// returned instead of a TableDiff.
func NewAlterTable(from, to *Table) *TableDiff {
	clauses, supported := from.Diff(to)
	if supported && len(clauses) == 0 {
		return nil
	}
	return &TableDiff{
		Type:         TableDiffAlter,
		From:         from,
		To:           to,
		alterClauses: clauses,
		supported:    supported,
	}
}

// NewDropTable returns a *TableDiff representing a DROP TABLE statement,
// i.e. a table that only exists in the "from" side schema in a diff.
func NewDropTable(table *Table) *TableDiff {
	return &TableDiff{
		Type:      TableDiffDrop,
		From:      table,
		supported: true,
	}
}

// TypeString returns the type of table diff as a string.
func (td *TableDiff) TypeString() string {
	return td.Type.String()
}

// Statement returns the full DDL statement corresponding to the TableDiff. A
// blank string may be returned if the mods indicate the statement should be
// skipped. If the mods indicate the statement should be disallowed, it will
// still be returned as-is, but the error will be non-nil. Be sure not to
// ignore the error value of this method.
func (td *TableDiff) Statement(mods StatementModifiers) (string, error) {
	if mods.IgnoreTable != nil {
		if (td.From != nil && mods.IgnoreTable.MatchString(td.From.Name)) || (td.To != nil && mods.IgnoreTable.MatchString(td.To.Name)) {
			return "", nil
		}
	}

	var err error
	switch td.Type {
	case TableDiffCreate:
		stmt := td.To.CreateStatement
		if td.To.HasAutoIncrement() && (mods.NextAutoInc == NextAutoIncIgnore || mods.NextAutoInc == NextAutoIncIfAlready) {
			stmt, _ = ParseCreateAutoInc(stmt)
		}
		return stmt, nil
	case TableDiffAlter:
		return td.alterStatement(mods)
	case TableDiffDrop:
		stmt := td.From.DropStatement()
		if !mods.AllowUnsafe {
			err = &ForbiddenDiffError{
				Reason:    "DROP TABLE not permitted",
				Statement: stmt,
			}
		}
		return stmt, err
	default: // TableDiffRename not supported yet
		panic(fmt.Errorf("Unsupported diff type %d", td.Type))
	}
}

// Clauses returns the body of the statement represented by the table diff.
// For DROP statements, this will be an empty string. For CREATE statements,
// it will be everything after "CREATE TABLE [name] ". For ALTER statements,
// it will be everything after "ALTER TABLE [name] ".
func (td *TableDiff) Clauses(mods StatementModifiers) (string, error) {
	stmt, err := td.Statement(mods)
	if stmt == "" {
		return stmt, err
	}
	switch td.Type {
	case TableDiffCreate:
		prefix := fmt.Sprintf("CREATE TABLE %s ", EscapeIdentifier(td.To.Name))
		return strings.Replace(stmt, prefix, "", 1), err
	case TableDiffAlter:
		prefix := fmt.Sprintf("%s ", td.From.AlterStatement())
		return strings.Replace(stmt, prefix, "", 1), err
	case TableDiffDrop:
		return "", err
	default: // TableDiffRename not supported yet
		panic(fmt.Errorf("Unsupported diff type %d", td.Type))
	}
}

func (td *TableDiff) alterStatement(mods StatementModifiers) (string, error) {
	if !td.supported {
		if td.To.UnsupportedDDL {
			return "", &UnsupportedDiffError{
				Name:                td.To.Name,
				ExpectedCreateTable: td.To.GeneratedCreateStatement(),
				ActualCreateTable:   td.To.CreateStatement,
			}
		} else if td.From.UnsupportedDDL {
			return "", &UnsupportedDiffError{
				Name:                td.From.Name,
				ExpectedCreateTable: td.From.GeneratedCreateStatement(),
				ActualCreateTable:   td.From.CreateStatement,
			}
		} else {
			return "", &UnsupportedDiffError{
				Name:                td.From.Name,
				ExpectedCreateTable: td.From.CreateStatement,
				ActualCreateTable:   td.To.CreateStatement,
			}
		}
	}

	clauseStrings := make([]string, 0, len(td.alterClauses))
	var err error
	for _, clause := range td.alterClauses {
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
			err = &ForbiddenDiffError{
				Reason:    "Unsafe or potentially destructive ALTER TABLE not permitted",
				Statement: "",
			}
		}
		clauseStrings = append(clauseStrings, clause.Clause())
	}
	if len(clauseStrings) == 0 {
		return "", nil
	}

	if mods.LockClause != "" {
		lockClause := fmt.Sprintf("LOCK=%s", strings.ToUpper(mods.LockClause))
		clauseStrings = append([]string{lockClause}, clauseStrings...)
	}
	if mods.AlgorithmClause != "" {
		algorithmClause := fmt.Sprintf("ALGORITHM=%s", strings.ToUpper(mods.AlgorithmClause))
		clauseStrings = append([]string{algorithmClause}, clauseStrings...)
	}

	stmt := fmt.Sprintf("%s %s", td.From.AlterStatement(), strings.Join(clauseStrings, ", "))
	if fde, isForbiddenDiff := err.(*ForbiddenDiffError); isForbiddenDiff {
		fde.Statement = stmt
	}
	return stmt, err
}
