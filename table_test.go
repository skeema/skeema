package tengo

import (
	"fmt"
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
	tableAlters := from.Diff(&to)
	if len(tableAlters) != 1 {
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
	tableAlters = to.Diff(&from)
	if len(tableAlters) != 1 {
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
	tableAlters = from.Diff(&to)
	if len(tableAlters) != 2 {
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

	// Add an addition column to the last position
	anotherCol = &Column{
		Name:     "awards_won",
		TypeInDB: "int unsigned",
		Nullable: false,
		Default:  ColumnDefaultValue("0"),
	}
	to.Columns = append(to.Columns, anotherCol)
	tableAlters = from.Diff(&to)
	if len(tableAlters) != 3 {
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
	tableAlters := from.Diff(&to)
	if len(tableAlters) != 1 {
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
	tableAlters = to.Diff(&from)
	if len(tableAlters) != 1 {
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
	tableAlters = from.Diff(&to)
	if len(tableAlters) != 2 {
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
	tableAlters = from.Diff(&to)
	if len(tableAlters) != 2 {
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
	tableAlters = from.Diff(&to)
	if len(tableAlters) != 1 {
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
	tableAlters = to.Diff(&from)
	if len(tableAlters) != 1 {
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

func TestTableAlterModifyColumn(t *testing.T) {
	from := aTable(1)
	to := aTable(1)

	// Reposition a col to first position
	movedCol := to.Columns[3]
	movedColPos := 3
	to.Columns = append(to.Columns[:movedColPos], to.Columns[movedColPos+1:]...)
	to.Columns = append([]*Column{movedCol}, to.Columns...)
	tableAlters := from.Diff(&to)
	if len(tableAlters) != 1 {
		t.Fatalf("Incorrect number of table alters: expected 1, found %d", len(tableAlters))
	}
	ta, ok := tableAlters[0].(ModifyColumn)
	if !ok {
		t.Fatalf("Incorrect type of table alter returned: expected %T, found %T", ta, tableAlters[0])
	}
	if ta.Table != &to || ta.OriginalColumn != from.Columns[movedColPos] || ta.NewColumn != movedCol {
		t.Error("Pointers in table alter do not point to expected values")
	}
	if !ta.PositionFirst || ta.PositionAfter != nil {
		t.Errorf("Expected first new column to be after nil / first=true, instead found after %v / first=%t", ta.PositionAfter, ta.PositionFirst)
	}

	// Reposition same col to last position

	// Repos to last position AND change column definition

	// Start over; delete and add a col, and move last col to position after the deleted/before the added col

	// Start over; just change column definition

}

func TestTableAlterChangeAutoIncrement(t *testing.T) {

}

func TestTableAlterUnsupportedTable(t *testing.T) {

}
