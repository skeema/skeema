package tengo

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

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
	//) ENGINE=%s AUTO_INCREMENT=%d DEFAULT CHARSET=
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
// for a particular table
type StatementModifiers struct {
	NextAutoInc NextAutoIncMode
	Inplace     bool
}

// Main statement prefix for the schema change (ALTER TABLE, CREATE TABLE, etc)
type TableDiff interface {
	Statement(StatementModifiers) string
}

// Specific clause to execute (ADD COLUMN, ADD INDEX, etc)
type TableAlterClause interface {
	Clause() string
}

type SchemaDiff struct {
	FromSchema *Schema
	ToSchema   *Schema
	TableDiffs []TableDiff
	// TODO: schema-level default charset and collation changes
}

func NewSchemaDiff(from, to *Schema) *SchemaDiff {
	result := &SchemaDiff{
		FromSchema: from,
		ToSchema:   to,
		TableDiffs: make([]TableDiff, 0),
	}

	fromTablesByName := from.TablesByName()
	toTablesByName := to.TablesByName()

	toTables := to.Tables()
	for n := range toTables {
		newTable := toTables[n]
		if _, existedBefore := fromTablesByName[newTable.Name]; !existedBefore {
			result.TableDiffs = append(result.TableDiffs, CreateTable{Table: newTable})
		}
	}

	fromTables := from.Tables()
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
			}
		} else {
			result.TableDiffs = append(result.TableDiffs, DropTable{Table: origTable})
		}
	}

	return result
}

func (sd *SchemaDiff) String() string {
	diffStatements := make([]string, len(sd.TableDiffs))
	for n, diff := range sd.TableDiffs {
		diffStatements[n] = fmt.Sprintf("%s;\n", diff.Statement(StatementModifiers{}))
	}
	return strings.Join(diffStatements, "")
}

///// CreateTable //////////////////////////////////////////////////////////////

type CreateTable struct {
	Table *Table
}

func (ct CreateTable) Statement(mods StatementModifiers) string {
	return ct.Table.CreateStatement()
}

///// DropTable ////////////////////////////////////////////////////////////////

type DropTable struct {
	Table *Table
}

func (dt DropTable) Statement(mods StatementModifiers) string {
	panic(fmt.Errorf("Drop Table not yet supported"))
}

///// AlterTable ///////////////////////////////////////////////////////////////

type AlterTable struct {
	Table   *Table
	Clauses []TableAlterClause
}

func (at AlterTable) Statement(mods StatementModifiers) string {
	clauseStrings := make([]string, 0, len(at.Clauses))
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
		}
		clauseStrings = append(clauseStrings, clause.Clause())
	}
	if len(clauseStrings) == 0 {
		return ""
	}
	return fmt.Sprintf("%s %s", at.Table.AlterStatement(), strings.Join(clauseStrings, ", "))
}

///// RenameTable //////////////////////////////////////////////////////////////

type RenameTable struct {
	Table   *Table
	NewName string
}

func (rt RenameTable) Statement(mods StatementModifiers) string {
	panic(fmt.Errorf("Rename Table not yet supported"))
}

///// AddColumn ////////////////////////////////////////////////////////////////

type AddColumn struct {
	Table         *Table
	Column        *Column
	PositionFirst bool
	PositionAfter *Column
}

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

type DropColumn struct {
	Table  *Table
	Column *Column
}

func (dc DropColumn) Clause() string {
	return fmt.Sprintf("DROP COLUMN %s", EscapeIdentifier(dc.Column.Name))
}

///// AddIndex /////////////////////////////////////////////////////////////////

type AddIndex struct {
	Table *Table
	Index *Index
}

func (ai AddIndex) Clause() string {
	return fmt.Sprintf("ADD %s", ai.Index.Definition())
}

///// DropIndex ////////////////////////////////////////////////////////////////

type DropIndex struct {
	Table *Table
	Index *Index
}

func (di DropIndex) Clause() string {
	if di.Index.PrimaryKey {
		return "DROP PRIMARY KEY"
	}
	return fmt.Sprintf("DROP INDEX %s", EscapeIdentifier(di.Index.Name))
}

///// RenameColumn /////////////////////////////////////////////////////////////

type RenameColumn struct {
	Table          *Table
	OriginalColumn *Column
	NewName        string
}

func (rc RenameColumn) Clause() string {
	panic(fmt.Errorf("Rename Column not yet supported"))
}

///// ModifyColumn /////////////////////////////////////////////////////////////
// for changing type, nullable, auto-incr, default, and/or position

type ModifyColumn struct {
	Table          *Table
	OriginalColumn *Column
	NewColumn      *Column
	PositionFirst  bool
	PositionAfter  *Column
}

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

type ChangeAutoIncrement struct {
	Table                *Table
	OldNextAutoIncrement uint64
	NewNextAutoIncrement uint64
}

func (cai ChangeAutoIncrement) Clause() string {
	return fmt.Sprintf("AUTO_INCREMENT = %d", cai.NewNextAutoIncrement)
}
