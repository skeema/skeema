package tengo

import (
	"strings"
	"testing"
)

func TestGeneratedCreateStatement(t *testing.T) {
	for nextAutoInc := uint64(1); nextAutoInc < 3; nextAutoInc++ {
		table := aTable(nextAutoInc)
		if table.GeneratedCreateStatement() != table.createStatement {
			t.Errorf("Generated DDL does not match actual DDL\nExpected:\n%s\nFound:\n%s", table.createStatement, table.GeneratedCreateStatement())
		}
	}

	table := anotherTable()
	if table.GeneratedCreateStatement() != table.createStatement {
		t.Errorf("Generated DDL does not match actual DDL\nExpected:\n%s\nFound:\n%s", table.createStatement, table.GeneratedCreateStatement())
	}

	table = unsupportedTable()
	if table.GeneratedCreateStatement() == table.createStatement {
		t.Error("Expected unsupported table's generated DDL to differ from actual DDL, but they match")
	}
}

func TestTableAlterAddOrDropColumn(t *testing.T) {
	from := aTable(1)
	to := aTable(1)

	// Add a column to an arbitrary position
	newCol := &Column{
		Name:     "age",
		TypeInDB: "int unsigned",
		Nullable: true,
		Default:  ColumnDefaultNull,
	}
	to.Columns = append(to.Columns, newCol)
	colCount := len(to.Columns)
	to.Columns[colCount-2], to.Columns[colCount-1] = to.Columns[colCount-1], to.Columns[colCount-2]
	to.createStatement = to.GeneratedCreateStatement()
	tableAlters, supported := from.Diff(&to)
	if len(tableAlters) != 1 || !supported {
		t.Fatalf("Incorrect number of table alters: expected 1, found %d", len(tableAlters))
	}
	ta, ok := tableAlters[0].(AddColumn)
	if !ok {
		t.Fatalf("Incorrect type of table alter returned: expected %T, found %T", ta, tableAlters[0])
	}
	if ta.Table != &to || ta.Column != newCol {
		t.Error("Pointers in table alter do not point to expected values")
	}
	if ta.PositionFirst || ta.PositionAfter != to.Columns[colCount-3] {
		t.Errorf("Expected new column to be after `%s` / first=false, instead found after `%s` / first=%t", to.Columns[colCount-3].Name, ta.PositionAfter.Name, ta.PositionFirst)
	}

	// Reverse comparison should yield a drop-column
	tableAlters, supported = to.Diff(&from)
	if len(tableAlters) != 1 || !supported {
		t.Fatalf("Incorrect number of table alters: expected 1, found %d", len(tableAlters))
	}
	ta2, ok := tableAlters[0].(DropColumn)
	if !ok {
		t.Fatalf("Incorrect type of table alter returned: expected %T, found %T", ta2, tableAlters[0])
	}
	if ta2.Table != &to || ta2.Column != newCol {
		t.Error("Pointers in table alter do not point to expected values")
	}

	// Add an addition column to first position
	hadColumns := to.Columns
	anotherCol := &Column{
		Name:     "net_worth",
		TypeInDB: "decimal(9,2)",
		Nullable: true,
		Default:  ColumnDefaultNull,
	}
	to.Columns = []*Column{anotherCol}
	to.Columns = append(to.Columns, hadColumns...)
	to.createStatement = to.GeneratedCreateStatement()
	tableAlters, supported = from.Diff(&to)
	if len(tableAlters) != 2 || !supported {
		t.Fatalf("Incorrect number of table alters: expected 2, found %d", len(tableAlters))
	}
	ta, ok = tableAlters[0].(AddColumn)
	if !ok {
		t.Fatalf("Incorrect type of table alter[0] returned: expected %T, found %T", ta, tableAlters[0])
	}
	if ta.Table != &to || ta.Column != anotherCol {
		t.Error("Pointers in table alter[0] do not point to expected values")
	}
	if !ta.PositionFirst || ta.PositionAfter != nil {
		t.Errorf("Expected first new column to be after nil / first=true, instead found after %v / first=%t", ta.PositionAfter, ta.PositionFirst)
	}

	// Add an additional column to the last position
	anotherCol = &Column{
		Name:     "awards_won",
		TypeInDB: "int unsigned",
		Nullable: false,
		Default:  ColumnDefaultValue("0"),
	}
	to.Columns = append(to.Columns, anotherCol)
	to.createStatement = to.GeneratedCreateStatement()
	tableAlters, supported = from.Diff(&to)
	if len(tableAlters) != 3 || !supported {
		t.Fatalf("Incorrect number of table alters: expected 3, found %d", len(tableAlters))
	}
	ta, ok = tableAlters[2].(AddColumn)
	if !ok {
		t.Fatalf("Incorrect type of table alter[2] returned: expected %T, found %T", ta, tableAlters[2])
	}
	if ta.Table != &to || ta.Column != anotherCol {
		t.Error("Pointers in table alter[2] do not point to expected values")
	}
	if ta.PositionFirst || ta.PositionAfter != nil {
		t.Errorf("Expected new column to be after nil / first=false, instead found after %v / first=%t", ta.PositionAfter, ta.PositionFirst)
	}
}

func TestTableAlterAddOrDropIndex(t *testing.T) {
	from := aTable(1)
	to := aTable(1)

	// Add a secondary index
	newSecondary := &Index{
		Name:     "idx_alive_lastname",
		Columns:  []*Column{to.Columns[5], to.Columns[2]},
		SubParts: []uint16{0, 10},
	}
	to.SecondaryIndexes = append(to.SecondaryIndexes, newSecondary)
	to.createStatement = to.GeneratedCreateStatement()
	tableAlters, supported := from.Diff(&to)
	if len(tableAlters) != 1 || !supported {
		t.Fatalf("Incorrect number of table alters: expected 1, found %d", len(tableAlters))
	}
	ta, ok := tableAlters[0].(AddIndex)
	if !ok {
		t.Fatalf("Incorrect type of table alter returned: expected %T, found %T", ta, tableAlters[0])
	}
	if ta.Table != &to || ta.Index != newSecondary {
		t.Error("Pointers in table alter do not point to expected values")
	}

	// Reverse comparison should yield a drop index
	tableAlters, supported = to.Diff(&from)
	if len(tableAlters) != 1 || !supported {
		t.Fatalf("Incorrect number of table alters: expected 1, found %d", len(tableAlters))
	}
	ta2, ok := tableAlters[0].(DropIndex)
	if !ok {
		t.Fatalf("Incorrect type of table alter returned: expected %T, found %T", ta2, tableAlters[0])
	}
	if ta2.Table != &from || ta2.Index != newSecondary {
		t.Error("Pointers in table alter do not point to expected values")
	}

	// Start over; change an existing secondary index
	to = aTable(1)
	to.SecondaryIndexes[0].Unique = false
	to.createStatement = to.GeneratedCreateStatement()
	tableAlters, supported = from.Diff(&to)
	if len(tableAlters) != 2 || !supported {
		t.Fatalf("Incorrect number of table alters: expected 2, found %d", len(tableAlters))
	}
	ta2, ok = tableAlters[0].(DropIndex)
	if !ok {
		t.Fatalf("Incorrect type of table alter[0] returned: expected %T, found %T", ta2, tableAlters[0])
	}
	if ta2.Table != &to || ta2.Index != from.SecondaryIndexes[0] {
		t.Error("Pointers in table alter[0] do not point to expected values")
	}
	ta, ok = tableAlters[1].(AddIndex)
	if !ok {
		t.Fatalf("Incorrect type of table alter[1] returned: expected %T, found %T", ta, tableAlters[1])
	}
	if ta.Table != &to || ta.Index != to.SecondaryIndexes[0] {
		t.Error("Pointers in table alter[1] do not point to expected values")
	}

	// Start over; change the primary key
	to = aTable(1)
	to.PrimaryKey.Columns = append(to.PrimaryKey.Columns, to.Columns[4])
	to.PrimaryKey.SubParts = append(to.PrimaryKey.SubParts, 0)
	to.createStatement = to.GeneratedCreateStatement()
	tableAlters, supported = from.Diff(&to)
	if len(tableAlters) != 2 || !supported {
		t.Fatalf("Incorrect number of table alters: expected 2, found %d", len(tableAlters))
	}
	ta2, ok = tableAlters[0].(DropIndex)
	if !ok {
		t.Fatalf("Incorrect type of table alter[0] returned: expected %T, found %T", ta2, tableAlters[0])
	}
	if ta2.Table != &to || ta2.Index != from.PrimaryKey {
		t.Error("Pointers in table alter[0] do not point to expected values")
	}
	ta, ok = tableAlters[1].(AddIndex)
	if !ok {
		t.Fatalf("Incorrect type of table alter[1] returned: expected %T, found %T", ta, tableAlters[1])
	}
	if ta.Table != &to || ta.Index != to.PrimaryKey {
		t.Error("Pointers in table alter[1] do not point to expected values")
	}

	// Remove the primary key
	to.PrimaryKey = nil
	to.createStatement = to.GeneratedCreateStatement()
	tableAlters, supported = from.Diff(&to)
	if len(tableAlters) != 1 || !supported {
		t.Fatalf("Incorrect number of table alters: expected 1, found %d", len(tableAlters))
	}
	ta2, ok = tableAlters[0].(DropIndex)
	if !ok {
		t.Fatalf("Incorrect type of table alter returned: expected %T, found %T", ta2, tableAlters[0])
	}
	if ta2.Table != &to || ta2.Index != from.PrimaryKey {
		t.Error("Pointers in table alter do not point to expected values")
	}

	// Reverse comparison should yield an add PK
	tableAlters, supported = to.Diff(&from)
	if len(tableAlters) != 1 || !supported {
		t.Fatalf("Incorrect number of table alters: expected 1, found %d", len(tableAlters))
	}
	ta, ok = tableAlters[0].(AddIndex)
	if !ok {
		t.Fatalf("Incorrect type of table alter returned: expected %T, found %T", ta, tableAlters[0])
	}
	if ta.Table != &from || ta.Index != from.PrimaryKey {
		t.Error("Pointers in table alter do not point to expected values")
	}
}

func TestTableAlterAddOrDropConstraint(t *testing.T) {
	from := aCstTestTable(1)
	to := aCstTestTable(1)

	// Add the foreign key constraint
	newSecIndex :=
		&Index{
			Name:     "bID",
			Columns:  []*Column{to.Columns[1]},
			SubParts: []uint16{0},
		}

	newCst := &Constraint{
		Name:                 "cstatable_ibfk_1",
		Column:               to.Columns[1],
		ReferencedSchemaName: "", // LEAVE BLANK TO SIGNAL ITS THE SAME SCHEMA AS THE CURRENT TABLE
		ReferencedTableName:  "cstBTable",
		ReferencedColumnName: "id",
		DeleteRule:           "RESTRICT",
		UpdateRule:           "CASCADE",
	}
	to.SecondaryIndexes = append(to.SecondaryIndexes, newSecIndex)
	to.Constraints = append(to.Constraints, newCst)
	to.createStatement = to.GeneratedCreateStatement()

	// Normal Comparison should yield add Index, add Constraint
	tableAlters, supported := from.Diff(&to)
	if len(tableAlters) != 2 || !supported {
		t.Fatalf("Incorrect number of table alters: expected 2, found %d", len(tableAlters))
	}
	taIx1, ok := tableAlters[0].(AddIndex)
	if !ok {
		t.Fatalf("Incorrect type of table alter returned: expected %T, found %T", taIx1, tableAlters[0])
	}
	if taIx1.Table != &to || taIx1.Index != newSecIndex {
		t.Error("Pointers in table alter do not point to expected values")
	}

	taCst1, ok := tableAlters[1].(AddConstraint)
	if !ok {
		t.Fatalf("Incorrect type of table alter returned: expected %T, found %T", taCst1, tableAlters[0])
	}
	if taCst1.Table != &to || taCst1.Constraint != newCst {
		t.Error("Pointers in table alter do not point to expected values")
	}

	// Reverse comparison should yield a drop index, drop constraint
	tableAlters, supported = to.Diff(&from)
	if len(tableAlters) != 2 || !supported {
		t.Fatalf("Incorrect number of table alters: expected 2, found %d", len(tableAlters))
	}
	taIx2, ok := tableAlters[0].(DropIndex)
	if !ok {
		t.Fatalf("Incorrect type of table alter returned: expected %T, found %T", taIx2, tableAlters[0])
	}
	if taIx2.Table != &from || taIx2.Index != newSecIndex {
		t.Error("Pointers in table alter do not point to expected values")
	}

	taCst2, ok := tableAlters[1].(DropConstraint)
	if !ok {
		t.Fatalf("Incorrect type of table alter returned: expected %T, found %T", taCst2, tableAlters[0])
	}
	if taCst2.Table != &from || taCst2.Constraint != newCst {
		t.Error("Pointers in table alter do not point to expected values")
	}

	// Start over; change an existing constraint
	to = aCstTestTable(1)
	to.Constraints[0].UpdateRule = "SET NULL"
	to.createStatement = to.GeneratedCreateStatement()
	tableAlters, supported = from.Diff(&to)
	if len(tableAlters) != 2 || !supported {
		t.Fatalf("Incorrect number of table alters: expected 2, found %d", len(tableAlters))
	}
	taCst2, ok = tableAlters[0].(DropConstraint)
	if !ok {
		t.Fatalf("Incorrect type of table alter[0] returned: expected %T, found %T", taCst2, tableAlters[0])
	}
	if taCst2.Table != &to || taCst2.Constraint != from.Constraints[0] {
		t.Error("Pointers in table alter[0] do not point to expected values")
	}
	taCst1, ok = tableAlters[1].(AddConstraint)
	if !ok {
		t.Fatalf("Incorrect type of table alter[1] returned: expected %T, found %T", taCst1, tableAlters[1])
	}
	if taCst1.Table != &to || taCst1.Constraint != to.Constraints[0] {
		t.Error("Pointers in table alter[1] do not point to expected values")
	}

	// Remove a constraint
	to.Constraints = []*Constraint{}
	to.createStatement = to.GeneratedCreateStatement()
	tableAlters, supported = from.Diff(&to)
	if len(tableAlters) != 1 || !supported {
		t.Fatalf("Incorrect number of table alters: expected 1, found %d", len(tableAlters))
	}
	taCst2, ok = tableAlters[0].(DropConstraint)
	if !ok {
		t.Fatalf("Incorrect type of table alter returned: expected %T, found %T", taCst2, tableAlters[0])
	}
	if taCst2.Table != &to || taCst2.Constraint != from.Constraints[0] {
		t.Error("Pointers in table alter do not point to expected values")
	}

	// Reverse comparison should yield an add constraint
	tableAlters, supported = to.Diff(&from)
	if len(tableAlters) != 1 || !supported {
		t.Fatalf("Incorrect number of table alters: expected 1, found %d", len(tableAlters))
	}
	taCst1, ok = tableAlters[0].(AddConstraint)
	if !ok {
		t.Fatalf("Incorrect type of table alter returned: expected %T, found %T", taCst1, tableAlters[0])
	}
	if taCst1.Table != &from || taCst1.Constraint != from.Constraints[0] {
		t.Error("Pointers in table alter do not point to expected values")
	}
}

func TestTableAlterModifyColumn(t *testing.T) {
	from := aTable(1)
	to := aTable(1)

	// Reposition a col to first position
	movedColPos := 3
	movedCol := to.Columns[movedColPos]
	to.Columns = append(to.Columns[:movedColPos], to.Columns[movedColPos+1:]...)
	to.Columns = append([]*Column{movedCol}, to.Columns...)
	to.createStatement = to.GeneratedCreateStatement()
	tableAlters, supported := from.Diff(&to)
	if len(tableAlters) != 1 || !supported {
		t.Fatalf("Incorrect number of table alters: expected 1, found %d", len(tableAlters))
	}
	ta, ok := tableAlters[0].(ModifyColumn)
	if !ok {
		t.Fatalf("Incorrect type of table alter returned: expected %T, found %T", ta, tableAlters[0])
	}
	if ta.Table != &to || ta.OldColumn != from.Columns[movedColPos] || ta.NewColumn != movedCol {
		t.Error("Pointers in table alter do not point to expected values")
	}
	if !ta.PositionFirst || ta.PositionAfter != nil {
		t.Errorf("Expected modified column to be after nil / first=true, instead found after %v / first=%t", ta.PositionAfter, ta.PositionFirst)
	}

	// Reposition same col to last position
	to = aTable(1)
	movedCol = to.Columns[movedColPos]
	shouldBeAfter := to.Columns[len(to.Columns)-1]
	to.Columns = append(to.Columns[:movedColPos], to.Columns[movedColPos+1:]...)
	to.Columns = append(to.Columns, movedCol)
	to.createStatement = to.GeneratedCreateStatement()
	tableAlters, supported = from.Diff(&to)
	if len(tableAlters) != 1 || !supported {
		t.Fatalf("Incorrect number of table alters: expected 1, found %d", len(tableAlters))
	}
	ta, ok = tableAlters[0].(ModifyColumn)
	if !ok {
		t.Fatalf("Incorrect type of table alter returned: expected %T, found %T", ta, tableAlters[0])
	}
	if ta.PositionFirst || ta.PositionAfter != shouldBeAfter {
		t.Errorf("Expected modified column to be after %s / first=false, instead found after %v / first=%t", shouldBeAfter.Name, ta.PositionAfter, ta.PositionFirst)
	}
	if !ta.NewColumn.Equals(ta.OldColumn) {
		t.Errorf("Column definition unexpectedly changed: was %s, now %s", ta.OldColumn.Definition(nil), ta.NewColumn.Definition(nil))
	}

	// Repos to last position AND change column definition
	movedCol.Nullable = !movedCol.Nullable
	to.createStatement = to.GeneratedCreateStatement()
	tableAlters, supported = from.Diff(&to)
	if len(tableAlters) != 1 || !supported {
		t.Fatalf("Incorrect number of table alters: expected 1, found %d", len(tableAlters))
	}
	ta, ok = tableAlters[0].(ModifyColumn)
	if !ok {
		t.Fatalf("Incorrect type of table alter returned: expected %T, found %T", ta, tableAlters[0])
	}
	if ta.PositionFirst || ta.PositionAfter != shouldBeAfter {
		t.Errorf("Expected modified column to be after %s / first=false, instead found after %v / first=%t", shouldBeAfter.Name, ta.PositionAfter, ta.PositionFirst)
	}
	if ta.NewColumn.Equals(ta.OldColumn) {
		t.Errorf("Column definition unexpectedly NOT changed: still %s", ta.NewColumn.Definition(nil))
	}

	// Start over; delete a col, move last col to its former position, and add a new col after that
	// FROM: actor_id, first_name, last_name, last_updated, ssn, alive
	// TO:   actor_id, first_name, last_name, alive, age, ssn
	// current move algo treats this as a move of ssn to be after alive, rather than alive to be after last_name
	to = aTable(1)
	newCol := &Column{
		Name:     "age",
		TypeInDB: "int unsigned",
		Nullable: true,
		Default:  ColumnDefaultNull,
	}
	to.Columns = append(to.Columns[0:3], to.Columns[5], newCol, to.Columns[4])
	to.createStatement = to.GeneratedCreateStatement()
	tableAlters, supported = from.Diff(&to)
	if len(tableAlters) != 3 || !supported {
		t.Fatalf("Incorrect number of table alters: expected 3, found %d", len(tableAlters))
	}
	// The alters should always be in this order: drops, modifications, adds
	if drop, ok := tableAlters[0].(DropColumn); ok {
		if drop.Table != &from || drop.Column != from.Columns[3] {
			t.Error("Pointers in table alter[0] do not point to expected values")
		}
	} else {
		t.Errorf("Incorrect type of table alter[0] returned: expected %T, found %T", drop, tableAlters[0])
	}
	if modify, ok := tableAlters[1].(ModifyColumn); ok {
		if modify.NewColumn.Name != "ssn" {
			t.Error("Pointers in table alter[1] do not point to expected values")
		}
		if modify.PositionFirst || modify.PositionAfter.Name != "alive" {
			t.Errorf("Expected moved column to be after alive / first=false, instead found after %v / first=%t", modify.PositionAfter, modify.PositionFirst)
		}
	} else {
		t.Errorf("Incorrect type of table alter[1] returned: expected %T, found %T", modify, tableAlters[1])
	}
	if add, ok := tableAlters[2].(AddColumn); ok {
		if add.PositionFirst || add.PositionAfter.Name != "alive" {
			t.Errorf("Expected new column to be after alive / first=false, instead found after %v / first=%t", add.PositionAfter, add.PositionFirst)
		}
	} else {
		t.Errorf("Incorrect type of table alter[2] returned: expected %T, found %T", add, tableAlters[2])
	}

	// Start over; just change a column definition without moving anything
	to = aTable(1)
	to.Columns[4].TypeInDB = "varchar(10)"
	to.createStatement = to.GeneratedCreateStatement()
	tableAlters, supported = from.Diff(&to)
	if len(tableAlters) != 1 || !supported {
		t.Fatalf("Incorrect number of table alters: expected 1, found %d", len(tableAlters))
	}
	ta, ok = tableAlters[0].(ModifyColumn)
	if !ok {
		t.Fatalf("Incorrect type of table alter returned: expected %T, found %T", ta, tableAlters[0])
	}
	if ta.PositionFirst || ta.PositionAfter != nil {
		t.Errorf("Expected modified column to not be moved, instead found after %v / first=%t", ta.PositionAfter, ta.PositionFirst)
	}
	if ta.NewColumn.Equals(from.Columns[4]) {
		t.Errorf("Column definition unexpectedly NOT changed: still %s", ta.NewColumn.Definition(nil))
	}

	// TODO: once the column-move algorithm is optimal, add a test that confirms
}

func TestTableAlterChangeStorageEngine(t *testing.T) {
	getTableWithEngine := func(engine string) Table {
		t := aTable(1)
		t.Engine = engine
		t.createStatement = t.GeneratedCreateStatement()
		return t
	}
	assertChangeEngine := func(a, b *Table, expected string) {
		tableAlters, supported := a.Diff(b)
		if expected == "" {
			if len(tableAlters) != 0 || !supported {
				t.Fatalf("Incorrect result from Table.Diff(): expected len=0, true; found len=%d, %t", len(tableAlters), supported)
			}
			return
		}
		if len(tableAlters) != 1 || !supported {
			t.Fatalf("Incorrect result from Table.Diff(): expected len=1, supported=true; found len=%d, supported=%t", len(tableAlters), supported)
		}
		ta, ok := tableAlters[0].(ChangeStorageEngine)
		if !ok {
			t.Fatalf("Incorrect type of table alter returned: expected %T, found %T", ta, tableAlters[0])
		}
		if ta.Clause() != expected {
			t.Errorf("Incorrect ALTER TABLE clause returned; expected: %s; found: %s", expected, ta.Clause())
		}
	}

	from := getTableWithEngine("InnoDB")
	to := getTableWithEngine("InnoDB")
	assertChangeEngine(&from, &to, "")
	to = getTableWithEngine("MyISAM")
	assertChangeEngine(&from, &to, "ENGINE=MyISAM")
	assertChangeEngine(&to, &from, "ENGINE=InnoDB")
}

func TestTableAlterChangeAutoIncrement(t *testing.T) {
	// Initial test: change next auto inc from 1 to 2
	from := aTable(1)
	to := aTable(2)
	tableAlters, supported := from.Diff(&to)
	if len(tableAlters) != 1 || !supported {
		t.Fatalf("Incorrect number of table alters: expected 1, found %d", len(tableAlters))
	}
	ta, ok := tableAlters[0].(ChangeAutoIncrement)
	if !ok {
		t.Fatalf("Incorrect type of table alter returned: expected %T, found %T", ta, tableAlters[0])
	}
	if ta.OldNextAutoIncrement != from.NextAutoIncrement || ta.NewNextAutoIncrement != to.NextAutoIncrement {
		t.Error("Incorrect next-auto-increment values in alter clause")
	}

	// Reverse test: should emit an alter clause, even though higher-level caller
	// may decide to ignore it
	tableAlters, supported = to.Diff(&from)
	if len(tableAlters) != 1 || !supported {
		t.Fatalf("Incorrect number of table alters: expected 1, found %d", len(tableAlters))
	}
	ta, ok = tableAlters[0].(ChangeAutoIncrement)
	if !ok {
		t.Fatalf("Incorrect type of table alter returned: expected %T, found %T", ta, tableAlters[0])
	}
	if ta.OldNextAutoIncrement != to.NextAutoIncrement || ta.NewNextAutoIncrement != from.NextAutoIncrement {
		t.Error("Incorrect next-auto-increment values in alter clause")
	}

	// Removing an auto-inc col and changing auto inc next value: should NOT emit
	// an auto-inc change since "to" table no longer has one
	to.Columns = to.Columns[1:]
	to.createStatement = to.GeneratedCreateStatement()
	tableAlters, supported = from.Diff(&to)
	if len(tableAlters) != 1 || !supported {
		t.Errorf("Incorrect number of table alters: expected 1, found %d", len(tableAlters))
	} else {
		ta, ok := tableAlters[0].(DropColumn)
		if !ok {
			t.Fatalf("Incorrect type of table alter returned: expected %T, found %T", ta, tableAlters[0])
		}
	}

	// Reverse of above comparison, adding an auto-inc col with a non-default
	// starting value: one clause for new col, another for auto-inc val (in that order!)
	to.NextAutoIncrement = 0
	from.NextAutoIncrement = 3
	to.createStatement = to.GeneratedCreateStatement()
	from.createStatement = from.GeneratedCreateStatement()
	tableAlters, supported = to.Diff(&from)
	if len(tableAlters) != 2 || !supported {
		t.Errorf("Incorrect number of table alters: expected 2, found %d", len(tableAlters))
	} else {
		if ta, ok := tableAlters[0].(AddColumn); !ok {
			t.Fatalf("Incorrect type of table alter[0] returned: expected %T, found %T", ta, tableAlters[0])
		}
		if ta, ok := tableAlters[1].(ChangeAutoIncrement); !ok {
			t.Fatalf("Incorrect type of table alter[1] returned: expected %T, found %T", ta, tableAlters[1])
		}
	}
}

func TestTableAlterChangeCharSet(t *testing.T) {
	getTableWithCharSet := func(charSet, collation string) Table {
		t := aTable(1)
		t.CharSet = charSet
		t.Collation = collation
		t.createStatement = t.GeneratedCreateStatement()
		return t
	}
	assertChangeCharSet := func(a, b *Table, expected string) {
		tableAlters, supported := a.Diff(b)
		if expected == "" {
			if len(tableAlters) != 0 || !supported {
				t.Fatalf("Incorrect result from Table.Diff(): expected len=0, true; found len=%d, %t", len(tableAlters), supported)
			}
			return
		}
		if len(tableAlters) != 1 || !supported {
			t.Fatalf("Incorrect result from Table.Diff(): expected len=1, supported=true; found len=%d, supported=%t", len(tableAlters), supported)
		}
		ta, ok := tableAlters[0].(ChangeCharSet)
		if !ok {
			t.Fatalf("Incorrect type of table alter returned: expected %T, found %T", ta, tableAlters[0])
		}
		if ta.Clause() != expected {
			t.Errorf("Incorrect ALTER TABLE clause returned; expected: %s; found: %s", expected, ta.Clause())
		}
	}

	from := getTableWithCharSet("utf8mb4", "")
	to := getTableWithCharSet("utf8mb4", "")
	assertChangeCharSet(&from, &to, "")

	to = getTableWithCharSet("utf8mb4", "utf8mb4_swedish_ci")
	assertChangeCharSet(&from, &to, "DEFAULT CHARACTER SET = utf8mb4 COLLATE = utf8mb4_swedish_ci")
	assertChangeCharSet(&to, &from, "DEFAULT CHARACTER SET = utf8mb4")

	to = getTableWithCharSet("latin1", "")
	assertChangeCharSet(&from, &to, "DEFAULT CHARACTER SET = latin1")
	assertChangeCharSet(&to, &from, "DEFAULT CHARACTER SET = utf8mb4")

	to = getTableWithCharSet("latin1", "latin1_general_ci")
	assertChangeCharSet(&from, &to, "DEFAULT CHARACTER SET = latin1 COLLATE = latin1_general_ci")
	assertChangeCharSet(&to, &from, "DEFAULT CHARACTER SET = utf8mb4")
}

func TestTableAlterChangeCreateOptions(t *testing.T) {
	getTableWithCreateOptions := func(createOptions string) Table {
		t := aTable(1)
		t.CreateOptions = createOptions
		t.createStatement = t.GeneratedCreateStatement()
		return t
	}
	assertChangeCreateOptions := func(a, b *Table, expected string) {
		tableAlters, supported := a.Diff(b)
		if expected == "" {
			if len(tableAlters) != 0 || !supported {
				t.Fatalf("Incorrect result from Table.Diff(): expected len=0, true; found len=%d, %t", len(tableAlters), supported)
			}
			return
		}
		if len(tableAlters) != 1 || !supported {
			t.Fatalf("Incorrect result from Table.Diff(): expected len=1, supported=true; found len=%d, supported=%t", len(tableAlters), supported)
		}
		ta, ok := tableAlters[0].(ChangeCreateOptions)
		if !ok {
			t.Fatalf("Incorrect type of table alter returned: expected %T, found %T", ta, tableAlters[0])
		}

		// Order of result isn't predictable, so convert to maps and compare
		indexedClause := make(map[string]bool)
		indexedExpected := make(map[string]bool)
		for _, token := range strings.Split(ta.Clause(), " ") {
			indexedClause[token] = true
		}
		for _, token := range strings.Split(expected, " ") {
			indexedExpected[token] = true
		}

		if len(indexedClause) != len(indexedExpected) {
			t.Errorf("Incorrect ALTER TABLE clause returned; expected: %s; found: %s", expected, ta.Clause())
			return
		}
		for k, v := range indexedExpected {
			if foundv, ok := indexedClause[k]; v != foundv || !ok {
				t.Errorf("Incorrect ALTER TABLE clause returned; expected: %s; found: %s", expected, ta.Clause())
				return
			}
		}
	}

	from := getTableWithCreateOptions("")
	to := getTableWithCreateOptions("")
	assertChangeCreateOptions(&from, &to, "")

	to = getTableWithCreateOptions("ROW_FORMAT=DYNAMIC")
	assertChangeCreateOptions(&from, &to, "ROW_FORMAT=DYNAMIC")
	assertChangeCreateOptions(&to, &from, "ROW_FORMAT=DEFAULT")

	to = getTableWithCreateOptions("STATS_PERSISTENT=1 ROW_FORMAT=DYNAMIC")
	assertChangeCreateOptions(&from, &to, "STATS_PERSISTENT=1 ROW_FORMAT=DYNAMIC")
	assertChangeCreateOptions(&to, &from, "STATS_PERSISTENT=DEFAULT ROW_FORMAT=DEFAULT")

	from = getTableWithCreateOptions("ROW_FORMAT=REDUNDANT AVG_ROW_LENGTH=200 STATS_PERSISTENT=1 MAX_ROWS=1000")
	to = getTableWithCreateOptions("STATS_AUTO_RECALC=1 ROW_FORMAT=DYNAMIC AVG_ROW_LENGTH=200")
	assertChangeCreateOptions(&from, &to, "STATS_AUTO_RECALC=1 ROW_FORMAT=DYNAMIC STATS_PERSISTENT=DEFAULT MAX_ROWS=0")
	assertChangeCreateOptions(&to, &from, "STATS_AUTO_RECALC=DEFAULT ROW_FORMAT=REDUNDANT STATS_PERSISTENT=1 MAX_ROWS=1000")
}

func TestTableAlterChangeComment(t *testing.T) {
	getTableWithComment := func(comment string) Table {
		t := aTable(1)
		t.Comment = comment
		t.createStatement = t.GeneratedCreateStatement()
		return t
	}
	assertChangeComment := func(a, b *Table, expected string) {
		tableAlters, supported := a.Diff(b)
		if expected == "" {
			if len(tableAlters) != 0 || !supported {
				t.Fatalf("Incorrect result from Table.Diff(): expected len=0, true; found len=%d, %t", len(tableAlters), supported)
			}
			return
		}
		if len(tableAlters) != 1 || !supported {
			t.Fatalf("Incorrect result from Table.Diff(): expected len=1, supported=true; found len=%d, supported=%t", len(tableAlters), supported)
		}
		ta, ok := tableAlters[0].(ChangeComment)
		if !ok {
			t.Fatalf("Incorrect type of table alter returned: expected %T, found %T", ta, tableAlters[0])
		}
		if ta.Clause() != expected {
			t.Errorf("Incorrect ALTER TABLE clause returned; expected: %s; found: %s", expected, ta.Clause())
		}
	}

	from := getTableWithComment("")
	to := getTableWithComment("")
	assertChangeComment(&from, &to, "")
	to = getTableWithComment("I'm a table-level comment!")
	assertChangeComment(&from, &to, "COMMENT 'I''m a table-level comment!'")
	assertChangeComment(&to, &from, "COMMENT ''")
}

func TestTableAlterUnsupportedTable(t *testing.T) {
	from, to := unsupportedTable(), unsupportedTable()
	newCol := &Column{
		Name:     "age",
		TypeInDB: "int unsigned",
		Nullable: true,
		Default:  ColumnDefaultNull,
	}
	to.Columns = append(to.Columns, newCol)
	to.createStatement = to.GeneratedCreateStatement()
	if tableAlters, supported := from.Diff(&to); len(tableAlters) != 0 || supported {
		t.Fatalf("Expected diff of unsupported tables to yield no alters; instead found %d", len(tableAlters))
	}

	// Confirm same behavior even if only one side is marked as unsupported
	from, to = anotherTable(), unsupportedTable()
	from.Name = to.Name
	if tableAlters, supported := from.Diff(&to); len(tableAlters) != 0 || supported {
		t.Fatalf("Expected diff of unsupported tables to yield no alters; instead found %d", len(tableAlters))
	}
	from, to = to, from
	if tableAlters, supported := from.Diff(&to); len(tableAlters) != 0 || supported {
		t.Fatalf("Expected diff of unsupported tables to yield no alters; instead found %d", len(tableAlters))
	}
}
