package tengo

import (
	"fmt"
	"strings"
	"testing"
)

func TestTableGeneratedCreateStatement(t *testing.T) {
	for nextAutoInc := uint64(1); nextAutoInc < 3; nextAutoInc++ {
		table := aTable(nextAutoInc)
		if table.GeneratedCreateStatement(FlavorUnknown) != table.CreateStatement {
			t.Errorf("Generated DDL does not match actual DDL\nExpected:\n%s\nFound:\n%s", table.CreateStatement, table.GeneratedCreateStatement(FlavorUnknown))
		}
	}

	table := anotherTable()
	if table.GeneratedCreateStatement(FlavorUnknown) != table.CreateStatement {
		t.Errorf("Generated DDL does not match actual DDL\nExpected:\n%s\nFound:\n%s", table.CreateStatement, table.GeneratedCreateStatement(FlavorUnknown))
	}

	table = unsupportedTable()
	if table.GeneratedCreateStatement(FlavorUnknown) == table.CreateStatement {
		t.Error("Expected unsupported table's generated DDL to differ from actual DDL, but they match")
	}
}

func TestTableClusteredIndexKey(t *testing.T) {
	table := aTable(1)
	if table.ClusteredIndexKey() == nil || table.ClusteredIndexKey() != table.PrimaryKey {
		t.Error("ClusteredIndexKey() did not return primary key when it was supposed to")
	}
	table.Engine = "MyISAM"
	if table.ClusteredIndexKey() != nil {
		t.Errorf("Expected ClusteredIndexKey() to return nil for non-InnoDB table, instead found %+v", table.ClusteredIndexKey())
	}
	table.Engine = "InnoDB"

	table.PrimaryKey = nil
	if table.ClusteredIndexKey() != table.SecondaryIndexes[0] {
		t.Errorf("Expected ClusteredIndexKey() to return %+v, instead found %+v", table.SecondaryIndexes[0], table.ClusteredIndexKey())
	}

	table.SecondaryIndexes[0], table.SecondaryIndexes[1] = table.SecondaryIndexes[1], table.SecondaryIndexes[0]
	if table.ClusteredIndexKey() != table.SecondaryIndexes[1] {
		t.Errorf("Expected ClusteredIndexKey() to return %+v, instead found %+v", table.SecondaryIndexes[1], table.ClusteredIndexKey())
	}

	table.Columns[4].Nullable = true
	if table.ClusteredIndexKey() != nil {
		t.Errorf("Expected ClusteredIndexKey() to return nil for table with unique-but-nullable index, instead found %+v", table.ClusteredIndexKey())
	}
	table.Columns[4].Nullable = false

	table.SecondaryIndexes[0].Unique = true
	if table.ClusteredIndexKey() != table.SecondaryIndexes[1] {
		t.Errorf("Expected ClusteredIndexKey() to return %+v, instead found %+v", table.SecondaryIndexes[1], table.ClusteredIndexKey())
	}

	table.Columns[2].Nullable = false
	if table.ClusteredIndexKey() != table.SecondaryIndexes[0] {
		t.Errorf("Expected ClusteredIndexKey() to return %+v, instead found %+v", table.SecondaryIndexes[0], table.ClusteredIndexKey())
	}

	// Functional indexes cannot be clustered index key
	table.SecondaryIndexes[0].Parts[1] = IndexPart{Expression: "LENGTH(`first_name`)"}
	if table.ClusteredIndexKey() != table.SecondaryIndexes[1] {
		t.Errorf("Expected ClusteredIndexKey() to return %+v, instead found %+v", table.SecondaryIndexes[1], table.ClusteredIndexKey())
	}
}

func TestTableRowFormat(t *testing.T) {
	assertRowFormat := func(createOptions, expectRowFormat string) {
		t.Helper()
		table := aTable(1)
		table.CreateOptions = createOptions
		if actual := table.RowFormat(); actual != expectRowFormat {
			t.Errorf("Unexpected result from RowFormat() with CreateOptions=%s: expected %s, found %s", createOptions, expectRowFormat, actual)
		}
	}
	cases := map[string]string{
		"":                                     "",
		"FOO=BAR":                              "",
		"ROW_FORMAT=DYNAMIC":                   "DYNAMIC",
		"ROW_FORMAT=COMPRESSED":                "COMPRESSED",
		"ROW_FORMAT=COMPACT FOO=BAR":           "COMPACT",
		"FOO=BAR ROW_FORMAT=REDUNDANT BIP=BAP": "REDUNDANT",
		"KEY_BLOCK_SIZE=8":                     "COMPRESSED",
		"ROW_FORMAT=DYNAMIC KEY_BLOCK_SIZE=8":  "DYNAMIC",
	}
	for createOptions, expectRowFormat := range cases {
		assertRowFormat(createOptions, expectRowFormat)
	}
}

func TestTableVirtualColumns(t *testing.T) {
	table := aTable(1)
	virtualCols := table.VirtualColumns()
	if len(virtualCols) > 0 {
		t.Errorf("Expected table to have no virtual columns, instead found %+v", virtualCols)
	}
	newCol := &Column{
		Type:           ParseColumnType("int"),
		GenerationExpr: "`actor_id` * 2",
		Virtual:        true,
	}
	table.Columns = append(table.Columns, newCol)
	virtualCols = table.VirtualColumns()
	if len(virtualCols) != 1 || virtualCols[0] != newCol {
		t.Errorf("Unexpected return value from VirtualColumns(): found %+v", virtualCols)
	}
}

func TestTableIndexesWithColumn(t *testing.T) {
	table := aTable(1)
	for n, col := range table.Columns {
		// Columns 0, 1, 2, 4 are each used in 1 index; other columns are not
		var expectedCount int
		if n < 3 || n == 4 {
			expectedCount++
		}
		actualCount := len(table.IndexesWithColumn(col))
		if expectedCount != actualCount {
			t.Errorf("Expected column %s to be used in %d indexes, instead found %d", col.Name, expectedCount, actualCount)
		}
	}

	// Change SecondaryIndexes[1] to have a functional index part, and confirm
	// it is still detected as using the column
	table.SecondaryIndexes[1].Parts[1] = IndexPart{
		Expression: "LENGTH(" + EscapeIdentifier(table.Columns[1].Name) + ")",
	}
	indexes := table.IndexesWithColumn(table.Columns[1])
	if len(indexes) != 1 || indexes[0].Name != table.SecondaryIndexes[1].Name {
		t.Errorf("Unexpected result from IndexesWithColumn on a functionally-indexed column: %+v", indexes)
	}
}

func TestTableUniqueConstraintsWithColumn(t *testing.T) {
	table := aTable(1)
	ucs := table.UniqueConstraintsWithColumn(table.Columns[0])
	if len(ucs) != 1 || ucs[0].Name != "PRIMARY" {
		t.Errorf("Unexpected return from UniqueConstraintsWithColumn: %v", ucs)
	}
	ucs = table.UniqueConstraintsWithColumn(table.Columns[1])
	if len(ucs) != 0 {
		t.Errorf("Unexpected return from UniqueConstraintsWithColumn: %v", ucs)
	}
	ucs = table.UniqueConstraintsWithColumn(table.Columns[4])
	if len(ucs) != 1 || ucs[0].Name != "idx_ssn" {
		t.Errorf("Unexpected return from UniqueConstraintsWithColumn: %v", ucs)
	}
}

func TestTableHasFulltextIndex(t *testing.T) {
	table := aTable(1)
	if table.HasFulltextIndex() {
		t.Fatal("Expected HasFulltextIndex() to return false, instead found true")
	}
	table.SecondaryIndexes[1].Type = "FULLTEXT"
	if !table.HasFulltextIndex() {
		t.Fatal("Expected HasFulltextIndex() to return true, instead found false")
	}
}

func TestTableAlterAddOrDropColumn(t *testing.T) {
	from := aTable(1)
	to := aTable(1)

	// Add a column to an arbitrary position
	newCol := &Column{
		Name:     "age",
		Type:     ParseColumnType("int unsigned"),
		Nullable: true,
		Default:  "NULL",
	}
	to.Columns = append(to.Columns, newCol)
	colCount := len(to.Columns)
	to.Columns[colCount-2], to.Columns[colCount-1] = to.Columns[colCount-1], to.Columns[colCount-2]
	to.CreateStatement = to.GeneratedCreateStatement(FlavorUnknown)
	tableAlters, supported := from.Diff(&to)
	if len(tableAlters) != 1 || !supported {
		t.Fatalf("Incorrect number of table alters: expected 1, found %d", len(tableAlters))
	}
	ta, ok := tableAlters[0].(AddColumn)
	if !ok {
		t.Fatalf("Incorrect type of table alter returned: expected %T, found %T", ta, tableAlters[0])
	}
	if ta.Column != newCol {
		t.Error("AddColumn.Column field does not point to expected Column")
	}
	if ta.PositionFirst || ta.PositionAfter != to.Columns[colCount-3] || !strings.Contains(ta.Clause(StatementModifiers{}), " AFTER ") {
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
	if ta2.Column != newCol {
		t.Error("Pointer in table alter does not point to expected value")
	}

	// Add an addition column to first position
	hadColumns := to.Columns
	anotherCol := &Column{
		Name:     "net_worth",
		Type:     ParseColumnType("decimal(9,2)"),
		Nullable: true,
		Default:  "NULL",
	}
	to.Columns = []*Column{anotherCol}
	to.Columns = append(to.Columns, hadColumns...)
	to.CreateStatement = to.GeneratedCreateStatement(FlavorUnknown)
	tableAlters, supported = from.Diff(&to)
	if len(tableAlters) != 2 || !supported {
		t.Fatalf("Incorrect number of table alters: expected 2, found %d", len(tableAlters))
	}
	ta, ok = tableAlters[0].(AddColumn)
	if !ok {
		t.Fatalf("Incorrect type of table alter[0] returned: expected %T, found %T", ta, tableAlters[0])
	}
	if ta.Column != anotherCol {
		t.Error("AddColumn.Column field does not point to expected value in alter[0]")
	}
	if !ta.PositionFirst || ta.PositionAfter != nil || !strings.Contains(ta.Clause(StatementModifiers{}), " FIRST") {
		t.Errorf("Expected first new column to be after nil / first=true, instead found after %v / first=%t", ta.PositionAfter, ta.PositionFirst)
	}

	// Add an additional column to the last position
	anotherCol = &Column{
		Name:     "awards_won",
		Type:     ParseColumnType("int unsigned"),
		Nullable: false,
		Default:  "'0'",
	}
	to.Columns = append(to.Columns, anotherCol)
	to.CreateStatement = to.GeneratedCreateStatement(FlavorUnknown)
	tableAlters, supported = from.Diff(&to)
	if len(tableAlters) != 3 || !supported {
		t.Fatalf("Incorrect number of table alters: expected 3, found %d", len(tableAlters))
	}
	ta, ok = tableAlters[2].(AddColumn)
	if !ok {
		t.Fatalf("Incorrect type of table alter[2] returned: expected %T, found %T", ta, tableAlters[2])
	}
	if ta.Column != anotherCol {
		t.Error("AddColumn.Column field does not point to expected value in alter[2]")
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
		Name: "idx_alive_lastname",
		Parts: []IndexPart{
			{ColumnName: to.Columns[5].Name},
			{ColumnName: to.Columns[2].Name, PrefixLength: 10},
		},
		Type: "BTREE",
	}
	to.SecondaryIndexes = append(to.SecondaryIndexes, newSecondary)
	to.CreateStatement = to.GeneratedCreateStatement(FlavorUnknown)
	tableAlters, supported := from.Diff(&to)
	if len(tableAlters) != 1 || !supported {
		t.Fatalf("Incorrect number of table alters: expected 1, found %d", len(tableAlters))
	}
	ta, ok := tableAlters[0].(AddIndex)
	if !ok {
		t.Fatalf("Incorrect type of table alter returned: expected %T, found %T", ta, tableAlters[0])
	}
	if ta.Index != newSecondary {
		t.Error("Pointer in table alter does not point to expected value")
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
	if ta2.Index != newSecondary {
		t.Error("Pointer in table alter does not point to expected value")
	}

	// Start over; change the last existing secondary index
	to = aTable(1)
	to.SecondaryIndexes[1].Unique = true
	to.CreateStatement = to.GeneratedCreateStatement(FlavorUnknown)
	tableAlters, supported = from.Diff(&to)
	if len(tableAlters) != 1 || !supported {
		t.Fatalf("Incorrect number of table alters: expected 1, found %d", len(tableAlters))
	}
	modify, ok := tableAlters[0].(ModifyIndex)
	if !ok {
		t.Fatalf("Incorrect type of alter[0] returned: expected %T, found %T", modify, tableAlters[0])
	}
	if modify.FromIndex != from.SecondaryIndexes[1] || modify.ToIndex != to.SecondaryIndexes[1] {
		t.Error("Pointers in alter[0] do not point to expected values")
	}
	if clause := modify.Clause(StatementModifiers{}); !strings.HasPrefix(clause, "DROP KEY ") || !strings.Contains(clause, ", ADD UNIQUE KEY ") {
		t.Errorf("Clause contents are unexpected: got %s", clause)
	}

	// Start over; change the comment of the last existing secondary index, with or
	// without other changes, and test behavior of StatementModifiers.LaxComments
	to = aTable(1)
	to.SecondaryIndexes[1].Comment = "hello I am an index"
	to.CreateStatement = to.GeneratedCreateStatement(FlavorUnknown)
	tableAlters, supported = from.Diff(&to)
	if len(tableAlters) != 1 || !supported {
		t.Fatalf("Incorrect number of table alters: expected 1, found %d", len(tableAlters))
	}
	if tableAlters[0].Clause(StatementModifiers{}) == "" {
		t.Error("Clause unexpectedly returns blank string")
	}
	if tableAlters[0].Clause(StatementModifiers{LaxComments: true}) != "" {
		t.Error("Clause unexpectedly returns non-blank string")
	}
	mysql8 := Flavor{VendorMySQL, Version{8}, VariantNone}
	to.SecondaryIndexes[1].Invisible = true
	to.CreateStatement = to.GeneratedCreateStatement(mysql8)
	tableAlters, supported = from.Diff(&to)
	if len(tableAlters) != 1 || !supported {
		t.Fatalf("Incorrect number of table alters: expected 1, found %d", len(tableAlters))
	}
	if clause := tableAlters[0].Clause(StatementModifiers{Flavor: mysql8, LaxComments: false}); !strings.HasPrefix(clause, "DROP KEY ") || !strings.Contains(clause, ", ADD KEY ") {
		t.Errorf("Clause returned unexpected string: %s", clause)
	}
	if clause := tableAlters[0].Clause(StatementModifiers{Flavor: mysql8, LaxComments: true}); !strings.HasPrefix(clause, "ALTER") {
		t.Errorf("Clause returned unexpected string: %s", clause)
	}
	to.SecondaryIndexes[1].Unique = true
	to.CreateStatement = to.GeneratedCreateStatement(mysql8)
	tableAlters, supported = from.Diff(&to)
	if len(tableAlters) != 1 || !supported {
		t.Fatalf("Incorrect number of table alters: expected 1, found %d", len(tableAlters))
	}
	if clause := tableAlters[0].Clause(StatementModifiers{Flavor: mysql8, LaxComments: true}); !strings.HasPrefix(clause, "DROP KEY ") || !strings.Contains(clause, ", ADD UNIQUE KEY ") {
		t.Errorf("Clause returned unexpected string: %s", clause)
	}

	// Start over; change the primary key
	to = aTable(1)
	to.PrimaryKey.Parts = append(to.PrimaryKey.Parts, IndexPart{ColumnName: to.Columns[4].Name})
	to.CreateStatement = to.GeneratedCreateStatement(FlavorUnknown)
	tableAlters, supported = from.Diff(&to)
	if len(tableAlters) != 2 || !supported {
		t.Fatalf("Incorrect number of table alters: expected 2, found %d", len(tableAlters))
	}
	ta2, ok = tableAlters[0].(DropIndex)
	if !ok {
		t.Fatalf("Incorrect type of table alter[0] returned: expected %T, found %T", ta2, tableAlters[0])
	}
	if ta2.Index != from.PrimaryKey {
		t.Error("Pointer in table alter[0] does not point to expected value")
	}
	ta, ok = tableAlters[1].(AddIndex)
	if !ok {
		t.Fatalf("Incorrect type of table alter[1] returned: expected %T, found %T", ta, tableAlters[1])
	}
	if ta.Index != to.PrimaryKey {
		t.Error("Pointer in table alter[1] does not point to expected value")
	}

	// Remove the primary key
	to.PrimaryKey = nil
	to.CreateStatement = to.GeneratedCreateStatement(FlavorUnknown)
	tableAlters, supported = from.Diff(&to)
	if len(tableAlters) != 1 || !supported {
		t.Fatalf("Incorrect number of table alters: expected 1, found %d", len(tableAlters))
	}
	ta2, ok = tableAlters[0].(DropIndex)
	if !ok {
		t.Fatalf("Incorrect type of table alter returned: expected %T, found %T", ta2, tableAlters[0])
	}
	if ta2.Index != from.PrimaryKey {
		t.Error("Pointer in table alter does not point to expected value")
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
	if ta.Index != from.PrimaryKey {
		t.Error("Pointer in table alter does not point to expected value")
	}

	// Start over; change a secondary index to FULLTEXT
	to = aTable(1)
	to.SecondaryIndexes[1].Type = "FULLTEXT"
	to.CreateStatement = to.GeneratedCreateStatement(FlavorUnknown)
	tableAlters, supported = from.Diff(&to)
	if len(tableAlters) != 1 || !supported {
		t.Fatalf("Incorrect number of table alters: expected 1, found %d", len(tableAlters))
	}
	modify, ok = tableAlters[0].(ModifyIndex)
	if !ok {
		t.Fatalf("Incorrect type of table alter[0] returned: expected %T, found %T", modify, tableAlters[0])
	}
	if modify.FromIndex != from.SecondaryIndexes[1] || modify.ToIndex != to.SecondaryIndexes[1] {
		t.Error("Pointers in alter[0] do not point to expected values")
	}
	if clause := modify.Clause(StatementModifiers{}); !strings.HasPrefix(clause, "DROP KEY ") || !strings.Contains(clause, ", ADD FULLTEXT KEY ") {
		t.Errorf("Clause returned unexpected string: %s", clause)
	}
}

func TestTableAlterAddOrDropForeignKey(t *testing.T) {
	from := anotherTable()
	to := anotherTable()

	// Add the foreign key constraint
	newFk := &ForeignKey{
		Name:                  "actor_fk",
		ColumnNames:           []string{to.Columns[0].Name},
		ReferencedSchemaName:  "", // leave blank to signal its the same schema as the current table
		ReferencedTableName:   "actor",
		ReferencedColumnNames: []string{"actor_id"},
		DeleteRule:            "RESTRICT",
		UpdateRule:            "CASCADE",
	}
	to.ForeignKeys = append(to.ForeignKeys, newFk)
	to.CreateStatement = to.GeneratedCreateStatement(FlavorUnknown)

	// Normal Comparison should yield add ForeignKey
	tableAlters, supported := from.Diff(&to)
	if len(tableAlters) != 1 || !supported {
		t.Fatalf("Incorrect number of table alters: expected 1, found %d", len(tableAlters))
	}
	taFk1, ok := tableAlters[0].(AddForeignKey)
	if !ok {
		t.Fatalf("Incorrect type of table alter returned: expected %T, found %T", taFk1, tableAlters[0])
	}
	if taFk1.ForeignKey != newFk {
		t.Error("Pointer in table alter does not point to expected value")
	}

	// Reverse comparison should yield a drop foreign key
	tableAlters, supported = to.Diff(&from)
	if len(tableAlters) != 1 || !supported {
		t.Fatalf("Incorrect number of table alters: expected 1, found %d", len(tableAlters))
	}
	taFk2, ok := tableAlters[0].(DropForeignKey)
	if !ok {
		t.Fatalf("Incorrect type of table alter returned: expected %T, found %T", taFk2, tableAlters[0])
	}
	if taFk2.ForeignKey != newFk {
		t.Error("Pointer in table alter does not point to expected value")
	}

	// New situation: changing an existing foreign key
	from = foreignKeyTable()
	to = foreignKeyTable()
	to.ForeignKeys[1].UpdateRule = "SET NULL"
	to.CreateStatement = to.GeneratedCreateStatement(FlavorUnknown)
	tableAlters, supported = from.Diff(&to)
	if len(tableAlters) != 2 || !supported {
		t.Fatalf("Incorrect number of table alters: expected 2, found %d", len(tableAlters))
	}
	taFk2, ok = tableAlters[0].(DropForeignKey)
	if !ok {
		t.Fatalf("Incorrect type of table alter[0] returned: expected %T, found %T", taFk2, tableAlters[0])
	}
	if taFk2.ForeignKey != from.ForeignKeys[1] {
		t.Error("Pointer in table alter[0] does not point to expected value")
	}
	taFk1, ok = tableAlters[1].(AddForeignKey)
	if !ok {
		t.Fatalf("Incorrect type of table alter[1] returned: expected %T, found %T", taFk1, tableAlters[1])
	}
	if taFk1.ForeignKey != to.ForeignKeys[1] {
		t.Error("Pointer in table alter[1] does not point to expected value")
	}

	// Changing the first FK should not affect 2nd FK, since FKs are not ordered
	to = foreignKeyTable()
	to.ForeignKeys[0].ReferencedSchemaName = ""
	to.CreateStatement = to.GeneratedCreateStatement(FlavorUnknown)
	tableAlters, supported = from.Diff(&to)
	if len(tableAlters) != 2 || !supported {
		t.Fatalf("Incorrect number of table alters: expected 2, found %d", len(tableAlters))
	}
	taFk2, ok = tableAlters[0].(DropForeignKey)
	if !ok {
		t.Fatalf("Incorrect type of table alter[0] returned: expected %T, found %T", taFk2, tableAlters[0])
	}
	if taFk2.ForeignKey != from.ForeignKeys[0] {
		t.Error("Pointer in table alter[0] does not point to expected value")
	}
	taFk1, ok = tableAlters[1].(AddForeignKey)
	if !ok {
		t.Fatalf("Incorrect type of table alter[1] returned: expected %T, found %T", taFk1, tableAlters[1])
	}
	if taFk1.ForeignKey != to.ForeignKeys[0] {
		t.Error("Pointer in table alter[1] does not point to expected value")
	}
}

func TestTableAlterAddIndexOrder(t *testing.T) {
	from := aTable(1)
	to := aTable(1)

	// Add 10 secondary indexes, and ensure their order is preserved
	for n := 0; n < 10; n++ {
		to.SecondaryIndexes = append(to.SecondaryIndexes, &Index{
			Name:  fmt.Sprintf("newidx_%d", n),
			Parts: []IndexPart{{ColumnName: to.Columns[0].Name}},
			Type:  "BTREE",
		})
	}
	to.CreateStatement = to.GeneratedCreateStatement(FlavorUnknown)
	tableAlters, supported := from.Diff(&to)
	if len(tableAlters) != 10 || !supported {
		t.Fatalf("Incorrect number of table alters: expected 10, found %d, supported=%t", len(tableAlters), supported)
	}
	for n := 0; n < 10; n++ {
		ta, ok := tableAlters[n].(AddIndex)
		if !ok {
			t.Fatalf("Incorrect type of table alter returned: expected %T, found %T", ta, tableAlters[0])
		}
		expectName := fmt.Sprintf("newidx_%d", n)
		if ta.Index.Name != expectName {
			t.Errorf("Incorrect index order: expected alters[%d] to be index name %s, instead found %s", n, expectName, ta.Index.Name)
		}
	}

	// Also modify an existing index, and ensure its corresponding drop + re-add
	// comes before the other 10 adds
	to.SecondaryIndexes[1].Parts[1].PrefixLength = 6
	to.CreateStatement = to.GeneratedCreateStatement(FlavorUnknown)
	tableAlters, supported = from.Diff(&to)
	if len(tableAlters) != 11 || !supported {
		t.Fatalf("Incorrect number of table alters: expected 11, found %d, supported=%t", len(tableAlters), supported)
	}
	if ta, ok := tableAlters[0].(ModifyIndex); !ok {
		t.Errorf("Expected alters[0] to be a ModifyIndex, instead found %T", ta)
	} else if ta.ToIndex.Name != to.SecondaryIndexes[1].Name {
		t.Errorf("Expected alters[0] to be on index %s, instead found %s", to.SecondaryIndexes[1].Name, ta.ToIndex.Name)
	} else if clause := ta.Clause(StatementModifiers{}); !strings.HasPrefix(clause, "DROP KEY ") || !strings.Contains(clause, ", ADD KEY ") {
		t.Errorf("Expected alters[0] clause to be a DROP and re-ADD, instead found %s", clause)
	}

	// Revert previous change, and instead change visibility on first index. This
	// should be handled by an ALTER INDEX clause, w/o any need to drop or move
	// anything. However, it still generates a spurious modify clause for the
	// second index, which returns a blank string even with strict ordering mod.
	to.SecondaryIndexes[1].Parts[1].PrefixLength = from.SecondaryIndexes[1].Parts[1].PrefixLength
	to.SecondaryIndexes[0].Invisible = true
	mysql8 := Flavor{VendorMySQL, Version{8}, VariantNone}
	maria106 := Flavor{VendorMariaDB, Version{10, 6}, VariantNone}
	to.CreateStatement = to.GeneratedCreateStatement(mysql8)
	tableAlters, supported = from.Diff(&to)
	if len(tableAlters) != 12 || !supported {
		t.Fatalf("Incorrect number of table alters: expected 12, found %d, supported=%t", len(tableAlters), supported)
	}
	expectClause := "ALTER INDEX `idx_ssn` INVISIBLE"
	if ta, ok := tableAlters[0].(ModifyIndex); !ok {
		t.Errorf("Expected alters[0] to be a ModifyIndex, instead found %T", ta)
	} else if ta.ToIndex.Name != to.SecondaryIndexes[0].Name || !ta.ToIndex.Invisible {
		t.Errorf("Unexpected values in ModifyIndex: %+v", ta)
	} else if clauseWithoutFlavor := ta.Clause(StatementModifiers{}); clauseWithoutFlavor != "" {
		t.Errorf("Unexpected result for ModifyIndex.Clause() without a MySQL 8.0+ flavor: %q", clauseWithoutFlavor)
	} else if clauseWithFlavor := ta.Clause(StatementModifiers{Flavor: mysql8}); clauseWithFlavor != expectClause {
		t.Errorf("Unexpected result for ModifyIndex.Clause() with a MySQL 8.0+ flavor: %q", clauseWithFlavor)
	} else if clauseWithFlavor := ta.Clause(StatementModifiers{Flavor: maria106}); clauseWithFlavor != strings.ReplaceAll(expectClause, "INVISIBLE", "IGNORED") {
		t.Errorf("Unexpected result for ModifyIndex.Clause() with a MariaDB 10.6 flavor: %q", clauseWithFlavor)
	}
	if ta, ok := tableAlters[1].(ModifyIndex); !ok {
		t.Errorf("Expected alters[1] to be a ModifyIndex, instead found %T", ta)
	} else if clause := ta.Clause(StatementModifiers{Flavor: mysql8, StrictIndexOrder: true}); clause != "" {
		t.Errorf("Unexpected result for ModifyIndex.Clause(): %q", clause)
	}

	// Also change another aspect of the first index. Now this should be a DROP and
	// re-ADD for index [0], regardless of statement modifiers.
	to.SecondaryIndexes[0].Parts = append(to.SecondaryIndexes[0].Parts, IndexPart{
		ColumnName: "last_name",
		Descending: true,
	})
	to.CreateStatement = to.GeneratedCreateStatement(mysql8)
	tableAlters, supported = from.Diff(&to)
	if len(tableAlters) != 12 || !supported {
		t.Fatalf("Incorrect number of table alters: expected 12, found %d, supported=%t", len(tableAlters), supported)
	}
	if _, ok := tableAlters[0].(ModifyIndex); !ok {
		t.Errorf("Unexpected type of alter clause at position 0: %T", tableAlters[0])
	} else if clause := tableAlters[0].Clause(StatementModifiers{}); !strings.HasPrefix(clause, "DROP KEY ") || !strings.Contains(clause, ", ADD UNIQUE KEY ") {
		t.Errorf("Unexpected clause emitted at position 0: %s", clause)
	} else if clause := tableAlters[0].Clause(StatementModifiers{StrictIndexOrder: true}); !strings.HasPrefix(clause, "DROP KEY ") || !strings.Contains(clause, ", ADD UNIQUE KEY ") {
		t.Errorf("Unexpected clause emitted at position 0: %s", clause)
	}
}

func TestTableAlterIndexReorder(t *testing.T) {
	// Table with three secondary indexes:
	// [0] is UNIQUE KEY `idx_ssn` (`ssn`)
	// [1] is KEY `idx_actor_name` (`last_name`(10),`first_name`(1))
	// [2] is KEY `idx_alive_lastname` (`alive`, `last_name`(10))
	getTableSimple := func() Table {
		table := aTable(1)
		table.SecondaryIndexes = append(table.SecondaryIndexes, &Index{
			Name: "idx_alive_lastname",
			Parts: []IndexPart{
				{ColumnName: table.Columns[5].Name},
				{ColumnName: table.Columns[2].Name, PrefixLength: 10},
			},
			Type: "BTREE",
		})
		table.CreateStatement = table.GeneratedCreateStatement(FlavorUnknown)
		return table
	}

	// Most test logic here uses these MySQL 8 based statement modifiers
	mysql8 := Flavor{VendorMySQL, Version{8}, VariantNone}
	loose8 := StatementModifiers{Flavor: mysql8}
	strict8 := StatementModifiers{Flavor: mysql8, StrictIndexOrder: true}

	assertClauses := func(from, to *Table, mods StatementModifiers, format string, a ...interface{}) {
		t.Helper()
		td := NewAlterTable(from, to)
		var clauses string
		if td != nil {
			var err error
			clauses, err = td.Clauses(mods)
			if err != nil {
				t.Fatalf("Unexpected error result from Clauses(): %s", err)
			}
		}
		expected := fmt.Sprintf(format, a...)
		if clauses != expected {
			t.Errorf("Unexpected result from Clauses()\nExpected:\n  %s\nFound:\n  %s", expected, clauses)
		}
	}

	from, to := getTableSimple(), getTableSimple()
	orig := from.SecondaryIndexes

	// Reorder to's last couple indexes ([1] and [2]). Resulting diff should
	// drop [1] and re-add [1], but should manifest as a no-op statement unless
	// mods.StrictIndexOrder enabled.
	to.SecondaryIndexes[1], to.SecondaryIndexes[2] = to.SecondaryIndexes[2], to.SecondaryIndexes[1]
	to.CreateStatement = to.GeneratedCreateStatement(FlavorUnknown)
	tableAlters, _ := from.Diff(&to)
	if len(tableAlters) != 1 {
		t.Errorf("Expected 1 clause, instead found %d", len(tableAlters))
	} else {
		if modify, ok := tableAlters[0].(ModifyIndex); !ok {
			t.Errorf("Expected tableAlters[0] to be %T, instead found %T", modify, tableAlters[0])
		} else if modify.ToIndex.Name != orig[1].Name {
			t.Errorf("Expected tableAlters[0] to affect %s, instead found %s", orig[1].Name, modify.ToIndex.Name)
		}
		assertClauses(&from, &to, loose8, "")
		assertClauses(&from, &to, strict8, "DROP KEY `%s`, ADD %s", orig[1].Name, orig[1].Definition(mysql8))
	}

	// Clustered index key changes: same effect as mods.StrictIndexOrder
	to.PrimaryKey = nil
	to.CreateStatement = to.GeneratedCreateStatement(FlavorUnknown)
	assertClauses(&from, &to, loose8, "DROP PRIMARY KEY, DROP KEY `%s`, ADD %s", orig[1].Name, orig[1].Definition(mysql8))
	assertClauses(&from, &to, strict8, "DROP PRIMARY KEY, DROP KEY `%s`, ADD %s", orig[1].Name, orig[1].Definition(mysql8))

	// Restore to original state, and then modify definition of [1] and visibility
	// of [2]. Resulting diff should:
	// * drop [1] and re-add the modified [1]
	// * modify visibility of [2] (via DROP/ADD if mods.StrictIndexOrder, or ALTER if not)
	to = getTableSimple()
	to.SecondaryIndexes[1].Parts[1].PrefixLength = 8
	to.SecondaryIndexes[2].Invisible = true
	to.CreateStatement = to.GeneratedCreateStatement(FlavorUnknown)
	tableAlters, _ = from.Diff(&to)
	if len(tableAlters) != 2 {
		t.Errorf("Expected 2 clauses, instead found %d", len(tableAlters))
	} else {
		mod0, ok0 := tableAlters[0].(ModifyIndex)
		mod1, ok1 := tableAlters[1].(ModifyIndex)
		if !ok0 || !ok1 {
			t.Errorf("One or more type mismatches; found types: %T, %T", mod0, mod1)
		}
		assertClauses(&from, &to, loose8, "DROP KEY `%s`, ADD %s, ALTER INDEX `%s` INVISIBLE", orig[1].Name, to.SecondaryIndexes[1].Definition(FlavorUnknown), orig[2].Name)
		assertClauses(&from, &to, strict8, "DROP KEY `%s`, ADD %s, DROP KEY `%s`, ADD %s", orig[1].Name, to.SecondaryIndexes[1].Definition(FlavorUnknown), orig[2].Name, to.SecondaryIndexes[2].Definition(mysql8))
	}

	// Restore to original state, and then add a new index before [1]. This should
	// result in an ADD; then a modify on [1] and [2] if using strict ordering.
	to = getTableSimple()
	newIdx := &Index{
		Name: "idx_firstname",
		Parts: []IndexPart{
			{ColumnName: to.Columns[1].Name},
		},
		Type: "BTREE",
	}
	to.SecondaryIndexes = []*Index{to.SecondaryIndexes[0], newIdx, to.SecondaryIndexes[1], to.SecondaryIndexes[2]}
	to.CreateStatement = to.GeneratedCreateStatement(FlavorUnknown)
	tableAlters, _ = from.Diff(&to)
	if len(tableAlters) != 3 {
		t.Errorf("Expected 3 clauses, instead found %d", len(tableAlters))
	} else {
		assertClauses(&from, &to, loose8, "ADD %s", newIdx.Definition(FlavorUnknown))
		assertClauses(&from, &to, strict8, "ADD %s, DROP KEY `%s`, ADD %s, DROP KEY `%s`, ADD %s", newIdx.Definition(FlavorUnknown), orig[1].Name, orig[1].Definition(FlavorUnknown), orig[2].Name, orig[2].Definition(FlavorUnknown))
	}

	// The opposite operation -- dropping the new index that we put before [1] --
	// should just result in a drop, no need to reorder anything
	tableAlters, _ = to.Diff(&from)
	if len(tableAlters) != 1 {
		t.Errorf("Expected 1 clause, instead found %d", len(tableAlters))
	} else {
		if drop, ok := tableAlters[0].(DropIndex); !ok {
			t.Errorf("Expected tableAlters[0] to be %T, instead found %T", drop, tableAlters[0])
		} else if drop.Index.Name != newIdx.Name {
			t.Errorf("Expected tableAlters[0] to drop %s, instead dropped %s", newIdx.Name, drop.Index.Name)
		}
		assertClauses(&to, &from, loose8, "DROP KEY `%s`", newIdx.Name)
		assertClauses(&to, &from, strict8, "DROP KEY `%s`", newIdx.Name)
	}

	// RENAME KEY related tests need additional statement modifiers, since rename
	// syntax is only in MySQL 5.7+ and MariaDB 10.5+.
	mysql56 := Flavor{VendorMySQL, Version{5, 6}, VariantNone}
	mysql57 := Flavor{VendorMySQL, Version{5, 7}, VariantNone}
	maria104 := Flavor{VendorMariaDB, Version{10, 4}, VariantNone}
	maria105 := Flavor{VendorMariaDB, Version{10, 5}, VariantNone}
	loose56 := StatementModifiers{Flavor: mysql56}
	strict104 := StatementModifiers{Flavor: maria104, StrictIndexOrder: true}
	strict105 := StatementModifiers{Flavor: maria105, StrictIndexOrder: true}
	loose57lc := StatementModifiers{Flavor: mysql57, LaxComments: true}

	// Restore to original state, and then rename index [1]. This should emit a
	// RENAME KEY in flavors that support it, or DROP/ADD in ones that don't.
	// This should also emit a DROP/ADD to move [2] but it is swallowed unless
	// the flavor doesn't support renames AND strict index ordering is enabled.
	to = getTableSimple()
	to.SecondaryIndexes[1].Name = "key_actor_name" // was previously "idx_actor_name"
	to.CreateStatement = to.GeneratedCreateStatement(FlavorUnknown)
	tableAlters, _ = from.Diff(&to)
	if len(tableAlters) != 2 {
		t.Errorf("Expected 2 clauses, instead found %d", len(tableAlters))
	} else {
		assertClauses(&from, &to, loose8, "RENAME KEY `%s` TO `%s`", from.SecondaryIndexes[1].Name, to.SecondaryIndexes[1].Name)
		assertClauses(&from, &to, strict8, "RENAME KEY `%s` TO `%s`", from.SecondaryIndexes[1].Name, to.SecondaryIndexes[1].Name)
		assertClauses(&from, &to, strict105, "RENAME KEY `%s` TO `%s`", from.SecondaryIndexes[1].Name, to.SecondaryIndexes[1].Name)
		assertClauses(&from, &to, loose56, "DROP KEY `%s`, ADD %s", from.SecondaryIndexes[1].Name, to.SecondaryIndexes[1].Definition(mysql56))
		assertClauses(&from, &to, strict104, "DROP KEY `%s`, ADD %s, DROP KEY `%s`, ADD %s", from.SecondaryIndexes[1].Name, to.SecondaryIndexes[1].Definition(maria104), from.SecondaryIndexes[2].Name, from.SecondaryIndexes[2].Definition(maria104))
	}

	// Also change a comment on index [1]. When combined with LaxComments, this
	// still surfaces as in previous case; otherwise it requires a DROP/ADD.
	to.SecondaryIndexes[1].Comment = "hello world" // was previously empty string
	to.CreateStatement = to.GeneratedCreateStatement(FlavorUnknown)
	tableAlters, _ = from.Diff(&to)
	if len(tableAlters) != 2 {
		t.Errorf("Expected 2 clauses, instead found %d", len(tableAlters))
	} else {
		assertClauses(&from, &to, loose8, "DROP KEY `%s`, ADD %s", from.SecondaryIndexes[1].Name, to.SecondaryIndexes[1].Definition(mysql8))
		assertClauses(&from, &to, strict8, "DROP KEY `%s`, ADD %s, DROP KEY `%s`, ADD %s", from.SecondaryIndexes[1].Name, to.SecondaryIndexes[1].Definition(mysql8), from.SecondaryIndexes[2].Name, from.SecondaryIndexes[2].Definition(mysql8))
		assertClauses(&from, &to, loose57lc, "RENAME KEY `%s` TO `%s`", from.SecondaryIndexes[1].Name, to.SecondaryIndexes[1].Name)
	}

	// Revert comment change from previous step; change visibility instead. The
	// emitted ModifyIndex TableAlterClause will only handle the RENAME. But when
	// present in a TableDiff and calling SplitConflicts, a separate TableDiff
	// with the corresponding AlterIndex will be present. This is because the
	// server doesn't allow a rename and visibility change on the same index in
	// the same ALTER TABLE.
	to.SecondaryIndexes[1].Comment = ""
	to.SecondaryIndexes[1].Invisible = true // was previously false
	to.CreateStatement = to.GeneratedCreateStatement(FlavorUnknown)
	tableAlters, _ = from.Diff(&to)
	if len(tableAlters) != 2 {
		t.Errorf("Expected 2 clauses, instead found %d", len(tableAlters))
	} else {
		assertClauses(&from, &to, loose8, "RENAME KEY `%s` TO `%s`", from.SecondaryIndexes[1].Name, to.SecondaryIndexes[1].Name)
		assertClauses(&from, &to, strict8, "RENAME KEY `%s` TO `%s`", from.SecondaryIndexes[1].Name, to.SecondaryIndexes[1].Name)
	}
	tds := NewAlterTable(&from, &to).SplitConflicts()
	header := fmt.Sprintf("ALTER TABLE `%s` ", to.Name)
	if len(tds) != 2 {
		t.Errorf("Expected SplitConflicts to return 2 TableDiffs, instead found %d", len(tds))
	} else if stmt, _ := tds[0].Statement(loose8); stmt != header+fmt.Sprintf("RENAME KEY `%s` TO `%s`", from.SecondaryIndexes[1].Name, to.SecondaryIndexes[1].Name) {
		t.Errorf("Unexpected result of tds[0].Statement() without strict index ordering: found %s", stmt)
	} else if stmt, _ := tds[0].Statement(strict8); stmt != header+fmt.Sprintf("RENAME KEY `%s` TO `%s`", from.SecondaryIndexes[1].Name, to.SecondaryIndexes[1].Name) {
		t.Errorf("Unexpected result of tds[0].Statement() with strict index ordering: found %s", stmt)
	} else if stmt, _ := tds[1].Statement(loose8); stmt != header+fmt.Sprintf("ALTER INDEX `%s` INVISIBLE", to.SecondaryIndexes[1].Name) {
		t.Errorf("Unexpected result of tds[1].Statement() without strict index ordering: found %s", stmt)
	} else if stmt, _ := tds[1].Statement(strict8); stmt != header+fmt.Sprintf("ALTER INDEX `%s` INVISIBLE", to.SecondaryIndexes[1].Name) {
		t.Errorf("Unexpected result of tds[1].Statement() with strict index ordering: found %s", stmt)
	}

	// Revert visibility change, and also swap order of indexes [0] and [2]. Effect
	// depends on use of strict ordering.
	to.SecondaryIndexes[1].Invisible = false
	to.SecondaryIndexes[0], to.SecondaryIndexes[2] = to.SecondaryIndexes[2], to.SecondaryIndexes[0]
	to.CreateStatement = to.GeneratedCreateStatement(FlavorUnknown)
	tableAlters, _ = from.Diff(&to)
	if len(tableAlters) != 2 {
		t.Errorf("Expected 2 clauses, instead found %d", len(tableAlters))
	} else {
		assertClauses(&from, &to, loose8, "RENAME KEY `%s` TO `%s`", from.SecondaryIndexes[1].Name, to.SecondaryIndexes[1].Name)
		assertClauses(&from, &to, strict105, "DROP KEY `%s`, ADD %s, DROP KEY `%s`, ADD %s", from.SecondaryIndexes[1].Name, to.SecondaryIndexes[1].Definition(maria105), to.SecondaryIndexes[2].Name, to.SecondaryIndexes[2].Definition(maria105))
	}

	// Restore the visibility change. Now we're fully testing the combination of an
	// index reorder on [0] / [2] alongside a rename + visibility change on [1].
	to.SecondaryIndexes[1].Invisible = true
	to.CreateStatement = to.GeneratedCreateStatement(FlavorUnknown)
	tds = NewAlterTable(&from, &to).SplitConflicts()
	if len(tds) != 2 {
		t.Errorf("Expected SplitConflicts to return 2 TableDiffs, instead found %d", len(tds))
	} else if stmt, _ := tds[0].Statement(loose8); stmt != header+fmt.Sprintf("RENAME KEY `%s` TO `%s`", from.SecondaryIndexes[1].Name, to.SecondaryIndexes[1].Name) {
		t.Errorf("Unexpected result of tds[0].Statement() without strict index ordering: found %s", stmt)
	} else if stmt, _ := tds[0].Statement(strict105); stmt != header+fmt.Sprintf("DROP KEY `%s`, ADD %s, DROP KEY `%s`, ADD %s", from.SecondaryIndexes[1].Name, to.SecondaryIndexes[1].Definition(maria105), to.SecondaryIndexes[2].Name, to.SecondaryIndexes[2].Definition(maria105)) {
		t.Errorf("Unexpected result of tds[0].Statement() with strict index ordering: found %s", stmt)
	} else if stmt, _ := tds[1].Statement(loose8); stmt != header+fmt.Sprintf("ALTER INDEX `%s` INVISIBLE", to.SecondaryIndexes[1].Name) {
		t.Errorf("Unexpected result of tds[1].Statement() without strict index ordering: found %s", stmt)
	} else if stmt, _ := tds[1].Statement(strict105); stmt != "" {
		t.Errorf("Unexpected result of tds[1].Statement() with strict index ordering: found %s", stmt)
	}

}

func TestTableAlterModifyColumn(t *testing.T) {
	from := aTable(1)
	to := aTable(1)

	// Reposition a col to first position
	movedColPos := 4
	movedCol := to.Columns[movedColPos]
	to.Columns = append(to.Columns[:movedColPos], to.Columns[movedColPos+1:]...)
	to.Columns = append([]*Column{movedCol}, to.Columns...)
	to.CreateStatement = to.GeneratedCreateStatement(FlavorUnknown)
	tableAlters, supported := from.Diff(&to)
	if len(tableAlters) != 1 || !supported {
		t.Fatalf("Incorrect number of table alters: expected 1, found %d", len(tableAlters))
	}
	ta, ok := tableAlters[0].(ModifyColumn)
	if !ok {
		t.Fatalf("Incorrect type of table alter returned: expected %T, found %T", ta, tableAlters[0])
	}
	if ta.OldColumn != from.Columns[movedColPos] || ta.NewColumn != movedCol {
		t.Error("Pointers in table alter do not point to expected values")
	}
	if !ta.PositionFirst || ta.PositionAfter != nil || !strings.Contains(ta.Clause(StatementModifiers{}), " FIRST") {
		t.Errorf("Expected modified column to be after nil / first=true, instead found after %v / first=%t", ta.PositionAfter, ta.PositionFirst)
	}
	laxColOrderMods := StatementModifiers{LaxColumnOrder: true}
	if clauseWithMods := ta.Clause(laxColOrderMods); clauseWithMods != "" {
		t.Errorf("Expected Clause to return a blank string with LaxColumnOrder enabled, instead found: %s", clauseWithMods)
	}

	// Reposition same col to last position
	to = aTable(1)
	movedCol = to.Columns[movedColPos]
	shouldBeAfter := to.Columns[len(to.Columns)-1]
	to.Columns = append(to.Columns[:movedColPos], to.Columns[movedColPos+1:]...)
	to.Columns = append(to.Columns, movedCol)
	to.CreateStatement = to.GeneratedCreateStatement(FlavorUnknown)
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
		t.Errorf("Column definition unexpectedly changed: was %s, now %s", ta.OldColumn.Definition(FlavorUnknown), ta.NewColumn.Definition(FlavorUnknown))
	}
	if clauseWithMods := ta.Clause(laxColOrderMods); clauseWithMods != "" {
		t.Errorf("Expected Clause to return a blank string with LaxColumnOrder enabled, instead found: %s", clauseWithMods)
	}

	// Re-pos to last position AND change column definition, adjusting the
	// collation. Since this column is used in a unique index, the collation
	// change should be detected as unsafe.
	movedCol.Collation = strings.Replace(movedCol.Collation, "general_ci", "unicode_ci", 1)
	to.CreateStatement = to.GeneratedCreateStatement(FlavorUnknown)
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
		t.Errorf("Column definition unexpectedly NOT changed: still %s", ta.NewColumn.Definition(FlavorUnknown))
	}
	if !ta.InUniqueConstraint {
		t.Error("Expected InUniqueConstraint to be true, but it was false")
	}
	if ta.Clause(laxColOrderMods) == "" {
		t.Error("Since non-positioning changes are present, expected Clause to return a non-blank string even with LaxColumnOrder enabled, but it was blank")
	}
	if unsafe, reason := ta.Unsafe(laxColOrderMods); !unsafe || !strings.Contains(reason, movedCol.Name) {
		t.Errorf("Unexpected return from Unsafe(): %t, %q", unsafe, reason)
	}

	// Start over; delete a col, move last col to its former position, and add a new col after that
	// FROM: actor_id, first_name, last_name, last_updated, ssn, alive, alive_bit
	// TO:   actor_id, first_name, last_name, alive, alive_bit, age, ssn
	// current move algo treats this as a move of ssn to be after alive, rather than alive to be after last_name
	to = aTable(1)
	newCol := &Column{
		Name:     "age",
		Type:     ParseColumnType("int unsigned"),
		Nullable: true,
		Default:  "NULL",
	}
	to.Columns = append(to.Columns[0:3], to.Columns[5], to.Columns[6], newCol, to.Columns[4])
	to.CreateStatement = to.GeneratedCreateStatement(FlavorUnknown)
	tableAlters, supported = from.Diff(&to)
	if len(tableAlters) != 3 || !supported {
		t.Fatalf("Incorrect number of table alters: expected 3, found %d", len(tableAlters))
	}
	// The alters should always be in this order: drops, modifications, adds
	if drop, ok := tableAlters[0].(DropColumn); ok {
		if drop.Column != from.Columns[3] {
			t.Error("Pointer in table alter[0] does not point to expected value")
		}
	} else {
		t.Errorf("Incorrect type of table alter[0] returned: expected %T, found %T", drop, tableAlters[0])
	}
	if modify, ok := tableAlters[1].(ModifyColumn); ok {
		if modify.NewColumn.Name != "ssn" {
			t.Error("Pointers in table alter[1] do not point to expected values")
		}
		if modify.PositionFirst || modify.PositionAfter.Name != "alive_bit" {
			t.Errorf("Expected moved column to be after alive_bit / first=false, instead found after %v / first=%t", modify.PositionAfter, modify.PositionFirst)
		}
	} else {
		t.Errorf("Incorrect type of table alter[1] returned: expected %T, found %T", modify, tableAlters[1])
	}
	if add, ok := tableAlters[2].(AddColumn); ok {
		if add.PositionFirst || add.PositionAfter.Name != "alive_bit" {
			t.Errorf("Expected new column to be after alive_bit / first=false, instead found after %v / first=%t", add.PositionAfter, add.PositionFirst)
		}
	} else {
		t.Errorf("Incorrect type of table alter[2] returned: expected %T, found %T", add, tableAlters[2])
	}

	// Start over; just change a column definition without moving anything
	to = aTable(1)
	to.Columns[4].Type = ParseColumnType("varchar(10)")
	to.CreateStatement = to.GeneratedCreateStatement(FlavorUnknown)
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
		t.Errorf("Column definition unexpectedly NOT changed: still %s", ta.NewColumn.Definition(FlavorUnknown))
	}

	// Start over; change one column and move another column
	to = aTable(1)
	to.Columns[4].Type = ParseColumnType("char(12)")
	to.Columns = []*Column{to.Columns[0], to.Columns[6], to.Columns[1], to.Columns[2], to.Columns[3], to.Columns[4], to.Columns[5]}
	to.CreateStatement = to.GeneratedCreateStatement(FlavorUnknown)
	tableAlters, supported = from.Diff(&to)
	if len(tableAlters) != 2 || !supported {
		stmt, _ := NewAlterTable(&from, &to).Statement(StatementModifiers{})
		t.Fatalf("Incorrect number of table alters: expected 2, found %d: %s", len(tableAlters), stmt)
	}
	for _, ta := range tableAlters {
		mc, ok := ta.(ModifyColumn)
		if !ok {
			t.Fatalf("Incorrect type of table alter returned: expected %T, found %T", mc, ta)
		}
		if mc.PositionAfter != nil {
			if mc.PositionAfter.Name != to.Columns[0].Name {
				t.Errorf("Re-ordered column expected to be AFTER %s, instead AFTER %s", to.Columns[0].Name, mc.PositionAfter.Name)
			}
			if mc.OldColumn.Definition(FlavorUnknown) != mc.NewColumn.Definition(FlavorUnknown) {
				t.Error("Expected re-ordered column definition to remain unchanged, but it was modified")
			}
		} else if mc.NewColumn.Type.Base != "char" || mc.NewColumn.Type.Size != 12 || mc.PositionAfter != nil || mc.PositionFirst {
			t.Errorf("Unexpected alter: %s", mc.Clause(StatementModifiers{}))
		}
	}

	// Start over; move 2 columns and verify that each column is only mentioned
	// once in the generated ALTER
	to = aTable(1)
	to.Columns = []*Column{to.Columns[0], to.Columns[1], to.Columns[2], to.Columns[4], to.Columns[6], to.Columns[3], to.Columns[5]}
	to.CreateStatement = to.GeneratedCreateStatement(FlavorUnknown)
	tableAlters, supported = from.Diff(&to)
	if !supported {
		t.Error("Expected diff to be supported, but it was not")
	} else {
		stmt, _ := NewAlterTable(&from, &to).Statement(StatementModifiers{})
		if len(tableAlters) != 2 {
			t.Errorf("Incorrect number of table alters: expected 2, found %d: %s", len(tableAlters), stmt)
		}
		seen := make(map[string]bool, len(to.Columns))
		for _, ta := range tableAlters {
			mc, ok := ta.(ModifyColumn)
			if !ok {
				t.Fatalf("Incorrect type of table alter returned: expected %T, found %T", mc, ta)
			}
			if seen[mc.NewColumn.Name] {
				t.Fatalf("Column %s illegally referenced in generated ALTER multiple times:\n%s", EscapeIdentifier(mc.NewColumn.Name), stmt)
			}
			seen[mc.NewColumn.Name] = true
		}
	}
}

func TestTableAlterNoModify(t *testing.T) {
	// Compare to a table with no common columns, and confirm no MODIFY clauses
	// present
	from := aTable(1)
	to := aTable(1)
	for n := range to.Columns {
		to.Columns[n].Name = fmt.Sprintf("xzy%s", to.Columns[n].Name)
	}
	to.CreateStatement = to.GeneratedCreateStatement(FlavorUnknown)
	if tableAlters, supported := from.Diff(&to); !supported {
		t.Error("Expected diff to be supported, but it was not")
	} else {
		for _, ta := range tableAlters {
			if _, ok := ta.(ModifyColumn); ok {
				t.Errorf("Unexpected ModifyColumn: %+v", ta)
			}
		}
	}
}

// TestTableAlterModifyColumnRandomly performs 20 iterations of semi-random
// modifications to a table's columns ordering and types, and then confirms
// that running the ALTER actually has the expected effect. It is commented out
// because this is overkill for routine testing, but the code can be useful
// when changing deep parts of the diff logic, especially the column reordering
// algorithm.
/*
func (s TengoIntegrationSuite) TestTableAlterModifyColumnRandomly(t *testing.T) {
	exec := func(query string) {
		t.Helper()
		db, err := s.d.Connect("testing", "")
		if err != nil {
			t.Fatalf("Unable to connect to DockerizedInstance: %s", err)
		}
		_, err = db.Exec(query)
		if err != nil {
			t.Fatalf("Error running query on DockerizedInstance.\nQuery: %s\nError: %s", query, err)
		}
	}
	assertCreate := func(table *Table) {
		t.Helper()
		createStatement, err := s.d.ShowCreateTable("testing", table.Name)
		if err != nil {
			t.Fatalf("Unexpected query error: %s", err)
		}
		if createStatement != table.CreateStatement {
			t.Errorf("Mismatch between actual and expected CREATE TABLE.\nActual:\n%s\nExpected:\n%s", createStatement, table.CreateStatement)
		}
	}

	from := aTableForFlavor(s.d.Flavor(), 1)
	for n := 0; n < 20; n++ {
		to := aTableForFlavor(s.d.Flavor(), 1)
		swaps := rand.Intn(len(to.Columns) + 1)
		for swap := 0; swap < swaps; swap++ {
			a := rand.Intn(len(to.Columns))
			b := rand.Intn(len(to.Columns))
			to.Columns[a], to.Columns[b] = to.Columns[b], to.Columns[a]
		}
		mods := rand.Intn(3)
		for mod := 0; mod < mods; mod++ {
			n := rand.Intn(len(to.Columns))
			col := to.Columns[n]
			switch col.Type.String() {
			case "varchar(45)":
				col.Type = ParseColumnType("varchar(55)")
			case "char(10)":
				col.Type = ParseColumnType("char(12)")
			case "tinyint(1)", "bit(1)":
				col.Nullable = true
			case "smallint(5) unsigned":
				col.Type = ParseColumnType("int(10) unsigned")
			}
		}
		to.CreateStatement = to.GeneratedCreateStatement(s.d.Flavor())

		exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", from.Name))
		exec(from.CreateStatement)
		alter := NewAlterTable(&from, &to)
		if alter == nil {
			assertCreate(&to) // assert correct without any change
		} else if !alter.supported {
			t.Fatal("Expected diff to be supported, but it was not")
		} else {
			stmt, _ := alter.Statement(StatementModifiers{})
			seen := make(map[string]bool, len(to.Columns))
			for _, ta := range alter.alterClauses {
				mc, ok := ta.(ModifyColumn)
				if !ok {
					t.Fatalf("Incorrect type of table alter returned: expected %T, found %T", mc, ta)
				}
				if seen[mc.NewColumn.Name] {
					t.Fatalf("Column %s illegally referenced in generated ALTER multiple times:\n%s", EscapeIdentifier(mc.NewColumn.Name), stmt)
				}
				seen[mc.NewColumn.Name] = true
			}
			exec(stmt)
			assertCreate(&to)
		}
	}
}
*/

func TestTableAlterChangeStorageEngine(t *testing.T) {
	getTableWithEngine := func(engine string) Table {
		t := aTable(1)
		t.Engine = engine
		t.CreateStatement = t.GeneratedCreateStatement(FlavorUnknown)
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
		if actual := ta.Clause(StatementModifiers{}); actual != expected {
			t.Errorf("Incorrect ALTER TABLE clause returned; expected: %s; found: %s", expected, actual)
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
	to.CreateStatement = to.GeneratedCreateStatement(FlavorUnknown)
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
	to.CreateStatement = to.GeneratedCreateStatement(FlavorUnknown)
	from.CreateStatement = from.GeneratedCreateStatement(FlavorUnknown)
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
		t.ShowCollation = !collationIsDefault(collation, charSet, FlavorUnknown)
		t.CreateStatement = t.GeneratedCreateStatement(FlavorUnknown)
		return t
	}
	assertChangeCharSet := func(a, b *Table, expected string) {
		t.Helper()
		tableAlters, supported := a.Diff(b)
		if expected == "" && a.Collation == b.Collation && a.CharSet == b.CharSet {
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
		if actual := ta.Clause(StatementModifiers{}); actual != expected {
			t.Errorf("Incorrect ALTER TABLE clause returned; expected: %s; found: %s", expected, actual)
		}
	}

	from := getTableWithCharSet("utf8mb4", "utf8mb4_general_ci")
	to := getTableWithCharSet("utf8mb4", "utf8mb4_general_ci")
	assertChangeCharSet(&from, &to, "")

	to = getTableWithCharSet("utf8mb4", "utf8mb4_swedish_ci")
	assertChangeCharSet(&from, &to, "DEFAULT CHARACTER SET = utf8mb4 COLLATE = utf8mb4_swedish_ci")
	assertChangeCharSet(&to, &from, "DEFAULT CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci")

	to = getTableWithCharSet("latin1", "latin1_swedish_ci")
	assertChangeCharSet(&from, &to, "DEFAULT CHARACTER SET = latin1 COLLATE = latin1_swedish_ci")
	assertChangeCharSet(&to, &from, "DEFAULT CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci")

	to = getTableWithCharSet("latin1", "latin1_general_ci")
	assertChangeCharSet(&from, &to, "DEFAULT CHARACTER SET = latin1 COLLATE = latin1_general_ci")
	assertChangeCharSet(&to, &from, "DEFAULT CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci")

	// Confirm "utf8" and "utf8mb3" are treated as identical, but generate a blank
	// TableAlterClause since the SHOW CREATEs may legitimately differ if comparing
	// tables introspected from different flavors/versions
	from = getTableWithCharSet("utf8", "utf8_general_ci")
	to = getTableWithCharSet("utf8mb3", "utf8_general_ci")
	assertChangeCharSet(&from, &to, "")
	to = getTableWithCharSet("utf8mb3", "utf8mb3_general_ci")
	assertChangeCharSet(&from, &to, "")

	// Confirm "utf8" and "utf8mb4" are not treated as identical
	to = getTableWithCharSet("utf8mb4", "utf8mb4_general_ci")
	assertChangeCharSet(&from, &to, "DEFAULT CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci")
}

func TestTableAlterChangeCreateOptions(t *testing.T) {
	getTableWithCreateOptions := func(createOptions string) Table {
		t := aTable(1)
		t.CreateOptions = createOptions
		t.CreateStatement = t.GeneratedCreateStatement(FlavorUnknown)
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
		for _, token := range strings.Split(ta.Clause(StatementModifiers{}), " ") {
			indexedClause[token] = true
		}
		for _, token := range strings.Split(expected, " ") {
			indexedExpected[token] = true
		}

		if len(indexedClause) != len(indexedExpected) {
			t.Errorf("Incorrect ALTER TABLE clause returned; expected: %s; found: %s", expected, ta.Clause(StatementModifiers{}))
			return
		}
		for k, v := range indexedExpected {
			if foundv, ok := indexedClause[k]; v != foundv || !ok {
				t.Errorf("Incorrect ALTER TABLE clause returned; expected: %s; found: %s", expected, ta.Clause(StatementModifiers{}))
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
		t.CreateStatement = t.GeneratedCreateStatement(FlavorUnknown)
		return t
	}
	assertChangeComment := func(a, b *Table, expected string) {
		t.Helper()
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
		if actual := ta.Clause(StatementModifiers{}); actual != expected {
			t.Errorf("Incorrect ALTER TABLE clause returned; expected: %s; found: %s", expected, actual)
		}
	}

	from := getTableWithComment("")
	to := getTableWithComment("")
	assertChangeComment(&from, &to, "")
	to = getTableWithComment("I'm a table-level comment!")
	assertChangeComment(&from, &to, "COMMENT 'I''m a table-level comment!'")
	assertChangeComment(&to, &from, "COMMENT ''")

	// Test behavior of StatementModifiers.LaxComments: should suppress the comment
	// change only if there are no other alterations to the table. This behavior
	// is implemented at the TableDiff level, rather than AlterClause.
	assertChangeCommentLax := func(a, b *Table, expectDiff bool) {
		t.Helper()
		stmt, err := NewAlterTable(a, b).Statement(StatementModifiers{LaxComments: true, AllowUnsafe: true})
		if err != nil {
			t.Errorf("Unexpected error returned from Statment: %v", err)
		} else if (stmt != "") != expectDiff {
			t.Errorf("Unexpected return string from Statement: %q", stmt)
		}
	}
	assertChangeCommentLax(&from, &to, false)
	assertChangeCommentLax(&to, &from, false)
	to.Columns[0].Type = ParseColumnType("smallint(5)")
	assertChangeCommentLax(&from, &to, true)
	assertChangeCommentLax(&to, &from, true)
}

func TestTableAlterTablespace(t *testing.T) {
	getTableWithTablespace := func(tablespace string) *Table {
		t := aTable(123)
		t.Tablespace = tablespace
		t.CreateStatement = t.GeneratedCreateStatement(FlavorUnknown)
		return &t
	}
	assertChangeTablespace := func(a, b *Table, expectDiff bool, expectClause string) {
		t.Helper()
		tableAlters, supported := a.Diff(b)
		var expectedCount int
		if expectDiff {
			expectedCount++
		}
		if len(tableAlters) != expectedCount || !supported {
			t.Errorf("Incorrect result from Table.Diff(): %d alter clauses, supported=%t", len(tableAlters), supported)
		} else if expectDiff {
			if ta, ok := tableAlters[0].(ChangeTablespace); !ok {
				t.Errorf("Incorrect type of alter returned: expected %T, found %T", ta, tableAlters[0])
			} else if actual := ta.Clause(StatementModifiers{}); actual != expectClause {
				t.Errorf("Incorrect ALTER TABLE clause returned: expected %q, found %q", expectClause, actual)
			}
		}
	}

	noTablespace := getTableWithTablespace("")
	explicitFPT := getTableWithTablespace("innodb_file_per_table")
	explicitSys := getTableWithTablespace("innodb_system")
	assertChangeTablespace(noTablespace, explicitFPT, true, "TABLESPACE `innodb_file_per_table`")
	assertChangeTablespace(explicitFPT, noTablespace, true, "") // no way to remove an explicit tablespace clause, but diff still supported
	assertChangeTablespace(explicitFPT, explicitFPT, false, "")
	assertChangeTablespace(explicitFPT, explicitSys, true, "TABLESPACE `innodb_system`")
}

func TestTableAlterUnsupportedTable(t *testing.T) {
	// Even if a table uses unsupported features, we can generate a diff of just
	// the supported parts of the ALTER, although it still returns !supported in
	// this situation
	from, to := unsupportedTable(), unsupportedTable()
	newCol := &Column{
		Name:     "age",
		Type:     ParseColumnType("int(10) unsigned"),
		Nullable: true,
		Default:  "NULL",
	}
	to.Columns = append(to.Columns, newCol)
	to.CreateStatement = to.GeneratedCreateStatement(FlavorUnknown)
	if tableAlters, supported := from.Diff(&to); len(tableAlters) != 1 || supported {
		t.Fatalf("Expected diff of unsupported tables to yield one alter clause and false; instead found %d alters, %t", len(tableAlters), supported)
	}

	// Diff that only adds unsupported feature doesn't yield any alter clauses
	from, to = supportedTable(), unsupportedTable()
	if tableAlters, supported := from.Diff(&to); len(tableAlters) != 0 || supported {
		t.Fatalf("Expected diff of unsupported tables to yield no alter clauses and false; instead found %d alters, %t", len(tableAlters), supported)
	}

	// However, the opposite is not true:
	// Even though sub-partitioning is not supported, a diff that entirely
	// removes partitioning can be generated successfully, though still with
	// !supported
	from, to = to, from
	if tableAlters, supported := from.Diff(&to); len(tableAlters) != 1 || supported {
		t.Fatalf("Expected diff of unsupported tables to yield one alter clause and false; instead found %d alters, %t", len(tableAlters), supported)
	}
}

func BenchmarkColumnModifications(b *testing.B) {
	// Create two tables: one with 199 cols, other with 200 cols, only differing by
	// that last extra col
	tbl1, tbl2 := &Table{Name: "one"}, &Table{Name: "two"}
	colType := ParseColumnType("int")
	for n := 1; n <= 200; n++ {
		col1 := Column{
			Name: fmt.Sprintf("col_%d", n),
			Type: colType,
		}
		col2 := col1
		if n < 200 {
			tbl1.Columns = append(tbl1.Columns, &col1)
		}
		tbl2.Columns = append(tbl2.Columns, &col2)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cc := compareColumnExistence(tbl1, tbl2)
		cc.columnModifications()
	}
}
