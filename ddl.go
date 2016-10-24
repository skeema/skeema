package tengo

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
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

// ParseCreateAutoInc parses a CREATE TABLE statement, formatted in the same
// manner as SHOW CREATE TABLE, and removes the table-level next-auto-increment
// clause if present. The modified CREATE TABLE will be returned, along with
// the next auto-increment value if one was found.
func ParseCreateAutoInc(createStmt string) (string, uint64) {
	reParseCreate := regexp.MustCompile(`[)] ENGINE=\w+ (AUTO_INCREMENT=(\d+) )DEFAULT CHARSET=`)
	matches := reParseCreate.FindStringSubmatch(createStmt)
	if matches == nil {
		return createStmt, 0
	}
	nextAutoInc, _ := strconv.ParseUint(matches[2], 10, 64)
	newStmt := strings.Replace(createStmt, matches[1], "", 1)
	return newStmt, nextAutoInc
}

// StatementModifiers are options that may be applied to adjust the DDL emitted
// for a particular table, and/or generate errors if certain clauses are
// present.
type StatementModifiers struct {
	NextAutoInc     NextAutoIncMode
	AllowDropTable  bool
	AllowDropColumn bool
}

// TableDiff interface represents a difference between two tables. Structs
// satisfying this interface can generate a DDL Statement prefix, such as ALTER
// TABLE, CREATE TABLE, DROP TABLE, etc.
type TableDiff interface {
	Statement(StatementModifiers) (string, error)
}

// TableAlterClause interface represents a specific single-element difference
// between two tables. Structs satisfying this interface can generate an ALTER
// TABLE clause, such as ADD COLUMN, MODIFY COLUMN, ADD INDEX, etc.
type TableAlterClause interface {
	Clause() string
}

// SchemaDiff stores a set of differences between two database schemas.
type SchemaDiff struct {
	FromSchema *Schema
	ToSchema   *Schema
	TableDiffs []TableDiff // a set of statements that, if run, would turn FromSchema into ToSchema
	SameTables []*Table    // slice of tables that were identical between schemas
	// TODO: schema-level default charset and collation changes
}

// NewSchemaDiff computes the set of differences between two database schemas.
func NewSchemaDiff(from, to *Schema) (*SchemaDiff, error) {
	result := &SchemaDiff{
		FromSchema: from,
		ToSchema:   to,
		TableDiffs: make([]TableDiff, 0),
		SameTables: make([]*Table, 0),
	}

	fromTablesByName, fromErr := from.TablesByName()
	toTablesByName, toErr := to.TablesByName()
	if fromErr != nil {
		return nil, fromErr
	} else if toErr != nil {
		return nil, toErr
	}

	toTables, err := to.Tables()
	if err != nil {
		return nil, err
	}
	for n := range toTables {
		newTable := toTables[n]
		if _, existedBefore := fromTablesByName[newTable.Name]; !existedBefore {
			result.TableDiffs = append(result.TableDiffs, CreateTable{Table: newTable})
		}
	}

	fromTables, err := from.Tables()
	if err != nil {
		return nil, err
	}
	for n := range fromTables {
		origTable := fromTables[n]
		newTable, stillExists := toTablesByName[origTable.Name]
		if stillExists {
			clauses := origTable.Diff(newTable)
			if len(clauses) > 0 {
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

	return result, nil
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

type ForbiddenDiffError struct {
	Reason    string
	Statement string
}

func (e *ForbiddenDiffError) Error() string {
	return e.Reason
}

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
	stmt := ct.Table.CreateStatement()
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
	var err error
	stmt := dt.Table.DropStatement()
	if !mods.AllowDropTable {
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
	clauseStrings := make([]string, 0, len(at.Clauses))
	var err error
	for _, clause := range at.Clauses {
		switch clause := clause.(type) {
		case ChangeAutoIncrement:
			if mods.NextAutoInc == NextAutoIncIgnore {
				continue
			} else if mods.NextAutoInc == NextAutoIncIfIncreased && clause.OldNextAutoIncrement >= clause.NewNextAutoIncrement {
				continue
			} else if mods.NextAutoInc == NextAutoIncIfAlready && clause.OldNextAutoIncrement <= 1 {
				continue
			}
		case DropColumn:
			if !mods.AllowDropColumn {
				err = NewForbiddenDiffError("DROP COLUMN not permitted", "")
			}
		}
		clauseStrings = append(clauseStrings, clause.Clause())
	}

	if len(clauseStrings) == 0 {
		return "", err
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

///// AddColumn ////////////////////////////////////////////////////////////////

// AddColumn represents a new column that is present on the right-side ("to")
// schema version of the table, but not the left-side ("from") version. It
// satisfies the TableAlterClause interface.
type AddColumn struct {
	Table         *Table
	Column        *Column
	PositionFirst bool
	PositionAfter *Column
}

// Clause returns an ADD COLUMN clause of an ALTER TABLE statement.
func (ac AddColumn) Clause() string {
	var positionClause string
	if ac.PositionFirst {
		// Positioning variables are mutually exclusive
		if ac.PositionAfter != nil {
			panic(fmt.Errorf("New column %s cannot be both first and after another column", ac.Column.Name))
		}
		positionClause = " FIRST"
	} else if ac.PositionAfter != nil {
		positionClause = fmt.Sprintf(" AFTER %s", EscapeIdentifier(ac.PositionAfter.Name))
	}
	return fmt.Sprintf("ADD COLUMN %s%s", ac.Column.Definition(), positionClause)
}

///// DropColumn ///////////////////////////////////////////////////////////////

// DropColumn represents a column that was present on the left-side ("from")
// schema version of the table, but not the right-side ("to") version. It
// satisfies the TableAlterClause interface.
type DropColumn struct {
	Table  *Table
	Column *Column
}

// Clause returns a DROP COLUMN clause of an ALTER TABLE statement.
func (dc DropColumn) Clause() string {
	return fmt.Sprintf("DROP COLUMN %s", EscapeIdentifier(dc.Column.Name))
}

///// AddIndex /////////////////////////////////////////////////////////////////

// AddIndex represents a new index that is present on the right-side ("to")
// schema version of the table, but not the left-side ("from") version. It
// satisfies the TableAlterClause interface.
type AddIndex struct {
	Table *Table
	Index *Index
}

// Clause returns an ADD INDEX clause of an ALTER TABLE statement.
func (ai AddIndex) Clause() string {
	return fmt.Sprintf("ADD %s", ai.Index.Definition())
}

///// DropIndex ////////////////////////////////////////////////////////////////

// DropIndex represents an index that was present on the left-side ("from")
// schema version of the table, but not the right-side ("to") version. It
// satisfies the TableAlterClause interface.
type DropIndex struct {
	Table *Table
	Index *Index
}

// Clause returns a DROP INDEX clause of an ALTER TABLE statement.
func (di DropIndex) Clause() string {
	if di.Index.PrimaryKey {
		return "DROP PRIMARY KEY"
	}
	return fmt.Sprintf("DROP INDEX %s", EscapeIdentifier(di.Index.Name))
}

///// RenameColumn /////////////////////////////////////////////////////////////

// RenameColumn represents a column that exists in both versions of the table,
// but with a different name. It satisfies the TableAlterClause interface.
type RenameColumn struct {
	Table          *Table
	OriginalColumn *Column
	NewName        string
}

// Clause returns a CHANGE COLUMN clause of an ALTER TABLE statement.
func (rc RenameColumn) Clause() string {
	panic(fmt.Errorf("Rename Column not yet supported"))
}

///// ModifyColumn /////////////////////////////////////////////////////////////
// for changing type, nullable, auto-incr, default, and/or position

// ModifyColumn represents a column that exists in both versions of the table,
// but with a different definition. It satisfies the TableAlterClause interface.
type ModifyColumn struct {
	Table          *Table
	OriginalColumn *Column
	NewColumn      *Column
	PositionFirst  bool
	PositionAfter  *Column
}

// Clause returns a MODIFY COLUMN clause of an ALTER TABLE statement.
func (mc ModifyColumn) Clause() string {
	var positionClause string
	if mc.PositionFirst {
		// Positioning variables are mutually exclusive
		if mc.PositionAfter != nil {
			panic(fmt.Errorf("Modified column %s cannot be both first and after another column", mc.NewColumn.Name))
		}
		positionClause = " FIRST"
	} else if mc.PositionAfter != nil {
		positionClause = fmt.Sprintf(" AFTER %s", EscapeIdentifier(mc.PositionAfter.Name))
	}
	return fmt.Sprintf("MODIFY COLUMN %s%s", mc.NewColumn.Definition(), positionClause)
}

///// ChangeAutoIncrement //////////////////////////////////////////////////////

// ChangeAutoIncrement represents a a difference in next-auto-increment value
// between two versions of a table. It satisfies the TableAlterClause interface.
type ChangeAutoIncrement struct {
	Table                *Table
	OldNextAutoIncrement uint64
	NewNextAutoIncrement uint64
}

// Clause returns an AUTO_INCREMENT clause of an ALTER TABLE statement.
func (cai ChangeAutoIncrement) Clause() string {
	return fmt.Sprintf("AUTO_INCREMENT = %d", cai.NewNextAutoIncrement)
}
