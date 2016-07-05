package tengo

import (
	"fmt"
	"strings"
)

// Main statement prefix for the schema change (ALTER TABLE, CREATE TABLE, etc)
type TableDiff interface {
	Statement() string
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
		diffStatements[n] = fmt.Sprintf("%s;\n", diff.Statement())
	}
	return strings.Join(diffStatements, "")
}

///// CreateTable //////////////////////////////////////////////////////////////

type CreateTable struct {
	Table *Table
}

func (ct CreateTable) Statement() string {
	return ct.Table.CreateStatement()
}

///// DropTable ////////////////////////////////////////////////////////////////

type DropTable struct {
	Table *Table
}

func (dt DropTable) Statement() string {
	panic(fmt.Errorf("Drop Table not yet supported"))
}

///// AlterTable ///////////////////////////////////////////////////////////////

type AlterTable struct {
	Table   *Table
	Clauses []TableAlterClause
}

func (at AlterTable) Statement() string {
	clauseStrings := make([]string, len(at.Clauses))
	for n, clause := range at.Clauses {
		clauseStrings[n] = clause.Clause()
	}
	return fmt.Sprintf("%s %s", at.Table.AlterStatement(), strings.Join(clauseStrings, ", "))
}

///// RenameTable //////////////////////////////////////////////////////////////

type RenameTable struct {
	Table   *Table
	NewName string
}

func (rt RenameTable) Statement() string {
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
