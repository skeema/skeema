package tengo

import (
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

func TestTableDiffAddColumn(t *testing.T) {
	from := aTable(1)
	to := aTable(1)
	newCol := &Column{
		Name:     "age",
		TypeInDB: "int unsigned",
		Nullable: true,
		Default:  ColumnDefaultNull,
	}
	to.Columns = append(to.Columns, newCol)
	colCount := len(to.Columns)
	to.Columns[colCount-2], to.Columns[colCount-1] = to.Columns[colCount-1], to.Columns[colCount-2]
	tableDiffs := from.Diff(&to)
	if len(tableDiffs) != 1 {
		t.Fatalf("Incorrect number of table diffs: expected 1, found %d", len(tableDiffs))
	}
	td, ok := tableDiffs[0].(AddColumn)
	if !ok {
		t.Fatalf("Incorrect type of table diff returned: expected %T, found %T", td, tableDiffs[0])
	}
	if td.Table != &to || td.Column != newCol {
		t.Error("Pointers in table diff do not point to expected values")
	}
	if td.PositionFirst || td.PositionAfter != to.Columns[colCount-3] {
		t.Errorf("Expected new column to be after `%s` / first=false, instead found after `%s` / first=%t", to.Columns[colCount-3].Name, td.PositionAfter.Name, td.PositionFirst)
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
	tableDiffs = from.Diff(&to)
	if len(tableDiffs) != 2 {
		t.Fatalf("Incorrect number of table diffs: expected 2, found %d", len(tableDiffs))
	}
	td, ok = tableDiffs[0].(AddColumn)
	if !ok {
		t.Fatalf("Incorrect type of table diff[0] returned: expected %T, found %T", td, tableDiffs[0])
	}
	if td.Table != &to || td.Column != anotherCol {
		t.Error("Pointers in table diff[0] do not point to expected values")
	}
	if !td.PositionFirst || td.PositionAfter != nil {
		t.Errorf("Expected first new column to be after nil / first=true, instead found after %v / first=%t", td.PositionAfter, td.PositionFirst)
	}
}

func TestTableDiffDropColumn(t *testing.T) {

}

func TestTableDiffAddIndex(t *testing.T) {

}

func TestTableDiffDropIndex(t *testing.T) {

}

func TestTableDiffModifyColumn(t *testing.T) {

}

func TestTableDiffChangeAutoIncrement(t *testing.T) {

}
