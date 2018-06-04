package tengo

import (
	"fmt"
	"regexp"
	"strings"
	"testing"
)

func TestSchemaDiffEmpty(t *testing.T) {
	assertEmptyDiff := func(a, b *Schema) {
		sd := a.Diff(b)
		if len(sd.TableDiffs) != 0 {
			t.Errorf("Expected no table diffs, instead found %d", len(sd.TableDiffs))
		}
		if sd.SchemaDDL != "" {
			t.Errorf("Expected no SchemaDDL, instead found %s", sd.SchemaDDL)
		}
	}

	s1t1 := anotherTable()
	s2t1 := anotherTable()
	s1t2 := aTable(10)
	s2t2 := aTable(10)
	s1 := aSchema("s1", &s1t1, &s1t2)
	s2 := aSchema("s2", &s2t1, &s2t2)

	assertEmptyDiff(&s1, &s2)
	assertEmptyDiff(&s2, &s1)
	assertEmptyDiff(nil, nil)
}

func TestSchemaDiffSchemaDDL(t *testing.T) {
	assertDiffSchemaDDL := func(a, b *Schema, expectedSchemaDDL string) {
		sd := NewSchemaDiff(a, b)
		if sd.SchemaDDL != expectedSchemaDDL {
			t.Errorf("For a=%s/%s and b=%s/%s, expected SchemaDDL=\"%s\", instead found \"%s\"", a.CharSet, a.Collation, b.CharSet, b.Collation, expectedSchemaDDL, sd.SchemaDDL)
		}
	}

	t1 := aTable(1)
	t2 := anotherTable()
	s1 := aSchema("s1", &t1, &t2)
	s2 := s1
	s2.Name = "s2"

	assertDiffSchemaDDL(&s1, &s1, "")
	assertDiffSchemaDDL(&s1, nil, "DROP DATABASE `s1`")
	assertDiffSchemaDDL(nil, &s1, "CREATE DATABASE `s1` CHARACTER SET latin1 COLLATE latin1_swedish_ci")

	s1.Collation = ""
	assertDiffSchemaDDL(nil, &s1, "CREATE DATABASE `s1` CHARACTER SET latin1")
	assertDiffSchemaDDL(&s1, &s2, "ALTER DATABASE `s1` COLLATE latin1_swedish_ci")
	assertDiffSchemaDDL(&s2, &s1, "")

	s1.CharSet = ""
	assertDiffSchemaDDL(nil, &s1, "CREATE DATABASE `s1`")
	assertDiffSchemaDDL(&s1, &s2, "ALTER DATABASE `s1` CHARACTER SET latin1 COLLATE latin1_swedish_ci")
	assertDiffSchemaDDL(&s2, &s1, "")

	s1.Collation = "utf8mb4_bin"
	assertDiffSchemaDDL(nil, &s1, "CREATE DATABASE `s1` COLLATE utf8mb4_bin")
	assertDiffSchemaDDL(&s2, &s1, "ALTER DATABASE `s2` COLLATE utf8mb4_bin")

	s1.CharSet = "utf8mb4"
	assertDiffSchemaDDL(&s1, &s2, "ALTER DATABASE `s1` CHARACTER SET latin1 COLLATE latin1_swedish_ci")
	assertDiffSchemaDDL(&s2, &s1, "ALTER DATABASE `s2` CHARACTER SET utf8mb4 COLLATE utf8mb4_bin")
}

func TestSchemaDiffAddOrDropTable(t *testing.T) {
	s1t1 := anotherTable()
	s2t1 := anotherTable()
	s2t2 := aTable(1)
	s1 := aSchema("s1", &s1t1)
	s2 := aSchema("s2", &s2t1, &s2t2)

	// Test table create
	sd := NewSchemaDiff(&s1, &s2)
	if len(sd.TableDiffs) != 1 {
		t.Fatalf("Incorrect number of table diffs: expected 1, found %d", len(sd.TableDiffs))
	}
	td, ok := sd.TableDiffs[0].(CreateTable)
	if !ok {
		t.Fatalf("Incorrect type of table diff returned: expected %T, found %T", td, sd.TableDiffs[0])
	}
	if td.Table != &s2t2 {
		t.Error("Pointer in table diff does not point to expected value")
	}

	// Test table drop (opposite diff direction of above)
	sd = NewSchemaDiff(&s2, &s1)
	if len(sd.TableDiffs) != 1 {
		t.Fatalf("Incorrect number of table diffs: expected 1, found %d", len(sd.TableDiffs))
	}
	td2, ok := sd.TableDiffs[0].(DropTable)
	if !ok {
		t.Fatalf("Incorrect type of table diff returned: expected %T, found %T", td2, sd.TableDiffs[0])
	}
	if td2.Table != &s2t2 {
		t.Error("Pointer in table diff does not point to expected value")
	}

	// Test impact of statement modifiers (allowing/forbidding drop) on previous drop
	if stmt, err := td2.Statement(StatementModifiers{AllowUnsafe: false}); err == nil {
		t.Errorf("Modifier AllowUnsafe=false not working; no error returned for %s", stmt)
	}
	if stmt, err := td2.Statement(StatementModifiers{AllowUnsafe: true}); err != nil {
		t.Errorf("Modifier AllowUnsafe=true not working; error (%s) returned for %s", err, stmt)
	}

	// Test impact of statement modifiers on creation of auto-inc table with non-default starting value
	s2t2.NextAutoIncrement = 5
	s2t2.CreateStatement = s2t2.GeneratedCreateStatement()
	sd = NewSchemaDiff(&s1, &s2)
	if len(sd.TableDiffs) != 1 {
		t.Fatalf("Incorrect number of table diffs: expected 1, found %d", len(sd.TableDiffs))
	}
	autoIncPresent := map[NextAutoIncMode]bool{
		NextAutoIncIgnore:      false,
		NextAutoIncIfIncreased: true,
		NextAutoIncIfAlready:   false,
		NextAutoIncAlways:      true,
	}
	for nextAutoInc, expected := range autoIncPresent {
		mods := StatementModifiers{NextAutoInc: nextAutoInc}
		stmt, err := sd.TableDiffs[0].Statement(mods)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(stmt, "AUTO_INCREMENT=") != expected {
			t.Errorf("Auto-inc filtering for new table not working as expected for modifiers=%+v (expect auto_inc to be present = %t)\nStatement: %s", mods, expected, stmt)
		}
	}

	// Test unsupported tables -- still fine for create/drop
	ust := unsupportedTable()
	s1 = aSchema("s1")
	s2 = aSchema("s2", &ust)
	sd = NewSchemaDiff(&s1, &s2)
	if len(sd.TableDiffs) != 1 {
		t.Fatalf("Incorrect number of table diffs: expected 1, found %d", len(sd.TableDiffs))
	}
	td, ok = sd.TableDiffs[0].(CreateTable)
	if !ok {
		t.Fatalf("Incorrect type of table diff returned: expected %T, found %T", td, sd.TableDiffs[0])
	}
	if td.Table != &ust {
		t.Error("Pointer in table diff does not point to expected value")
	}
	sd = NewSchemaDiff(&s2, &s1)
	if len(sd.TableDiffs) != 1 {
		t.Fatalf("Incorrect number of table diffs: expected 1, found %d", len(sd.TableDiffs))
	}
	td2, ok = sd.TableDiffs[0].(DropTable)
	if !ok {
		t.Fatalf("Incorrect type of table diff returned: expected %T, found %T", td2, sd.TableDiffs[0])
	}
	if td2.Table != &ust {
		t.Error("Pointer in table diff does not point to expected value")
	}
}

func TestSchemaDiffAlterTable(t *testing.T) {
	// Helper method for testing various combinations of alters involving next-auto-inc changes
	assertAutoIncAlter := func(from, to uint64, nextAutoInc NextAutoIncMode, expectAlter bool) {
		t1 := aTable(from)
		t2 := aTable(to)
		s1 := aSchema("s1", &t1)
		s2 := aSchema("s2", &t2)
		sd := NewSchemaDiff(&s1, &s2)
		if len(sd.TableDiffs) != 1 {
			t.Fatalf("Incorrect number of table diffs: expected 1, found %d", len(sd.TableDiffs))
		}
		if td, ok := sd.TableDiffs[0].(AlterTable); !ok {
			t.Fatalf("Incorrect type of table diff returned: expected %T, found %T", td, sd.TableDiffs[0])
		}
		mods := StatementModifiers{NextAutoInc: nextAutoInc}
		if stmt, err := sd.TableDiffs[0].Statement(mods); err != nil {
			t.Fatal(err)
		} else if stmt == "" {
			if expectAlter {
				t.Errorf("For next_auto_inc %d -> %d, received blank ALTER with mods=%+v, expected non-blank", from, to, mods)
			}
		} else {
			if !expectAlter {
				t.Errorf("For next_auto_inc %d -> %d, expected blank ALTER with mods=%+v, instead received: %s", from, to, mods, stmt)
			}
			expectClause := fmt.Sprintf("AUTO_INCREMENT = %d", to)
			if !strings.Contains(stmt, expectClause) {
				t.Errorf("For next_auto_inc %d -> %d and mods=%+v, expected statement to contain %s, instead received: %s", from, to, mods, expectClause, stmt)
			}
		}
	}

	// Test auto-inc changes, and effect of statement modifiers on them
	assertAutoIncAlter(1, 4, NextAutoIncIgnore, false)
	assertAutoIncAlter(4, 1, NextAutoIncIgnore, false)
	assertAutoIncAlter(1, 4, NextAutoIncIfIncreased, true)
	assertAutoIncAlter(4, 1, NextAutoIncIfIncreased, false)
	assertAutoIncAlter(1, 4, NextAutoIncIfAlready, false)
	assertAutoIncAlter(2, 4, NextAutoIncIfAlready, true)
	assertAutoIncAlter(4, 2, NextAutoIncIfAlready, true)
	assertAutoIncAlter(1, 4, NextAutoIncAlways, true)
	assertAutoIncAlter(2, 4, NextAutoIncAlways, true)
	assertAutoIncAlter(4, 2, NextAutoIncAlways, true)

	// Helper for testing column adds or drops
	getAlter := func(left, right *Schema) (TableDiff, TableAlterClause) {
		sd := NewSchemaDiff(left, right)
		if len(sd.TableDiffs) != 1 {
			t.Fatalf("Incorrect number of table diffs: expected 1, found %d", len(sd.TableDiffs))
		}
		alter, ok := sd.TableDiffs[0].(AlterTable)
		if !ok {
			t.Fatalf("Incorrect type of table diff returned: expected %T, found %T", alter, sd.TableDiffs[0])
		}
		if len(alter.Clauses) != 1 {
			t.Fatalf("Wrong number of alter clauses: expected 1, found %d", len(alter.Clauses))
		}
		return alter, alter.Clauses[0]
	}

	// Test column adds/drops, and effect of statement modifier on drop col
	t1 := anotherTable()
	t2 := anotherTable()
	s1 := aSchema("s1", &t1)
	s2 := aSchema("s2", &t2)
	t2.Columns = append(t2.Columns, &Column{
		Name:     "something",
		TypeInDB: "smallint(5) unsigned",
		Default:  ColumnDefaultNull,
	})
	t2.CreateStatement = t2.GeneratedCreateStatement()
	alter, clause := getAlter(&s1, &s2)
	if addCol, ok := clause.(AddColumn); !ok {
		t.Errorf("Incorrect type of alter clause returned: expected %T, found %T", addCol, clause)
	}
	if _, err := alter.Statement(StatementModifiers{}); err != nil {
		t.Error(err)
	}
	alter, clause = getAlter(&s2, &s1)
	if dropCol, ok := clause.(DropColumn); !ok {
		t.Errorf("Incorrect type of alter clause returned: expected %T, found %T", dropCol, clause)
	}
	if stmt, err := alter.Statement(StatementModifiers{AllowUnsafe: false}); err == nil {
		t.Errorf("Modifier AllowUnsafe=false not working; no error returned for %s", stmt)
	}
	if stmt, err := alter.Statement(StatementModifiers{AllowUnsafe: true}); err != nil {
		t.Errorf("Modifier AllowUnsafe=true not working; error (%s) returned for %s", err, stmt)
	}
}

func TestAlterTableStatementAllowUnsafeMods(t *testing.T) {
	t1 := aTable(1)
	t2 := aTable(1)
	s1 := aSchema("s1", &t1)
	s2 := aSchema("s2", &t2)

	getAlter := func(a, b *Schema) AlterTable {
		sd := NewSchemaDiff(a, b)
		if len(sd.TableDiffs) != 1 {
			t.Fatalf("Incorrect number of table diffs: expected 1, found %d", len(sd.TableDiffs))
		}
		td, ok := sd.TableDiffs[0].(AlterTable)
		if !ok {
			t.Fatalf("Incorrect type of table diff returned: expected %T, found %T", td, sd.TableDiffs[0])
		}
		return td
	}
	assertSafe := func(a, b *Schema) {
		alter := getAlter(a, b)
		if _, err := alter.Statement(StatementModifiers{AllowUnsafe: false}); err != nil {
			t.Errorf("alter.Statement unexpectedly returned error when AllowUnsafe=false: %s", err)
		} else if _, err := alter.Statement(StatementModifiers{AllowUnsafe: true}); err != nil {
			t.Errorf("alter.Statement unexpectedly returned error yet only when AllowUnsafe=true: %s", err)
		}
	}
	assertUnsafe := func(a, b *Schema) {
		alter := getAlter(a, b)
		if _, err := alter.Statement(StatementModifiers{AllowUnsafe: false}); err == nil {
			t.Error("alter.Statement did not return error when AllowUnsafe=false")
		} else if _, err := alter.Statement(StatementModifiers{AllowUnsafe: true}); err != nil {
			t.Errorf("alter.Statement unexpectedly returned error even with AllowUnsafe=true: %s", err)
		}
	}

	// Removing an index is safe
	t2.SecondaryIndexes = t2.SecondaryIndexes[0 : len(t2.SecondaryIndexes)-1]
	t2.CreateStatement = t2.GeneratedCreateStatement()
	assertSafe(&s1, &s2)

	// Removing a column is unsafe
	t2 = aTable(1)
	t2.Columns = t2.Columns[0 : len(t2.Columns)-1]
	t2.CreateStatement = t2.GeneratedCreateStatement()
	assertUnsafe(&s1, &s2)

	// Changing col type to increase its size is safe
	t2 = aTable(1)
	t2.Columns[0].TypeInDB = "int unsigned"
	t2.CreateStatement = t2.GeneratedCreateStatement()
	assertSafe(&s1, &s2)

	// Changing col type to change to signed is unsafe
	t2 = aTable(1)
	t2.Columns[0].TypeInDB = "smallint(5)"
	t2.CreateStatement = t2.GeneratedCreateStatement()
	assertUnsafe(&s1, &s2)
}

func TestAlterTableStatementOnlineMods(t *testing.T) {
	table := anotherTable()
	col := &Column{
		Name:     "something",
		TypeInDB: "smallint(5) unsigned",
		Default:  ColumnDefaultNull,
	}
	addCol := AddColumn{
		Table:  &table,
		Column: col,
	}
	alter := AlterTable{
		Table:   &table,
		Clauses: []TableAlterClause{addCol},
	}

	assertStatement := func(mods StatementModifiers, middle string) {
		stmt, err := alter.Statement(mods)
		if err != nil {
			t.Errorf("Received unexpected error %s from statement with mods=%v", err, mods)
			return
		}
		expect := fmt.Sprintf("ALTER TABLE `%s` %s%s", table.Name, middle, addCol.Clause())
		if stmt != expect {
			t.Errorf("Generated ALTER doesn't match expectation with mods=%v\n    Expected: %s\n    Found:    %s", mods, expect, stmt)
		}
	}

	mods := StatementModifiers{}
	assertStatement(mods, "")

	mods.LockClause = "none"
	assertStatement(mods, "LOCK=NONE, ")
	mods.AlgorithmClause = "online"
	assertStatement(mods, "ALGORITHM=ONLINE, LOCK=NONE, ")
	mods.LockClause = ""
	assertStatement(mods, "ALGORITHM=ONLINE, ")

	// Confirm that mods are ignored if no actual alter clauses present
	alter.Clauses = []TableAlterClause{}
	if stmt, err := alter.Statement(mods); stmt != "" {
		t.Errorf("Expected blank-string statement if no clauses present, regardless of mods; instead found: %s", stmt)
	} else if err != nil {
		t.Errorf("Expected no error from statement with no clauses present; instead found: %s", err)
	}
}

func TestIgnoreTableMod(t *testing.T) {
	table := anotherTable()
	col := &Column{
		Name:     "something",
		TypeInDB: "smallint(5) unsigned",
		Default:  ColumnDefaultNull,
	}
	addCol := AddColumn{
		Table:  &table,
		Column: col,
	}
	alter := AlterTable{
		Table:   &table,
		Clauses: []TableAlterClause{addCol},
	}
	create := CreateTable{
		Table: &table,
	}
	drop := DropTable{
		Table: &table,
	}
	assertStatement := func(re string, tableName string, expectNonemptyStatement bool) {
		t.Helper()
		mods := StatementModifiers{
			AllowUnsafe: true,
		}
		if re != "" {
			mods.IgnoreTable = regexp.MustCompile(re)
		}
		table.Name = tableName
		if stmt, err := alter.Statement(mods); err != nil || (stmt == "") == expectNonemptyStatement {
			t.Errorf("Unexpected result for alter: re=%s, table=%s, expectNonEmpty=%t, actual=%s, err=%s", re, tableName, expectNonemptyStatement, stmt, err)
		}
		if stmt, err := create.Statement(mods); err != nil || (stmt == "") == expectNonemptyStatement {
			t.Errorf("Unexpected result for create: re=%s, table=%s, expectNonEmpty=%t, actual=%s, err=%s", re, tableName, expectNonemptyStatement, stmt, err)
		}
		if stmt, err := drop.Statement(mods); err != nil || (stmt == "") == expectNonemptyStatement {
			t.Errorf("Unexpected result for drop: re=%s, table=%s, expectNonEmpty=%t, actual=%s, err=%s", re, tableName, expectNonemptyStatement, stmt, err)
		}
	}
	assertStatement("", "testing", true)
	assertStatement("^hello", "testing", true)
	assertStatement("^test", "testing", false)
}
