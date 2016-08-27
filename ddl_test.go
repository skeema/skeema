package tengo

import (
	"fmt"
	"strings"
	"testing"
)

func TestParseCreateAutoInc(t *testing.T) {
	// With auto-inc value <= 1, no AUTO_INCREMENT=%d clause will be put into the
	// test table's create statement
	table := aTable(1)
	stmt := table.createStatement
	if strings.Contains(stmt, "AUTO_INCREMENT=") {
		t.Fatal("Assertion failed in test setup: createStatement unexpectedly contains an AUTO_INCREMENT clause")
	}
	strippedStmt, nextAutoInc := ParseCreateAutoInc(stmt)
	if strippedStmt != stmt || nextAutoInc > 0 {
		t.Error("Incorrect result parsing CREATE TABLE")
	}

	table = aTable(123)
	stmt = table.createStatement
	if !strings.Contains(stmt, "AUTO_INCREMENT=") {
		t.Fatal("Assertion failed in test setup: createStatement does NOT contain expected AUTO_INCREMENT clause")
	}
	strippedStmt, nextAutoInc = ParseCreateAutoInc(stmt)
	if strings.Contains(strippedStmt, "AUTO_INCREMENT=") {
		t.Error("Failed to remove AUTO_INCREMENT clause from create statement")
	}
	if nextAutoInc != 123 {
		t.Errorf("Failed to properly parse AUTO_INCREMENT value: expected 123, found %d", nextAutoInc)
	}
}

func TestSchemaDiffEmpty(t *testing.T) {
	s1t1 := anotherTable()
	s2t1 := anotherTable()
	s1t2 := aTable(10)
	s2t2 := aTable(10)
	s1 := aSchema("s1", &s1t1, &s1t2)
	s2 := aSchema("s2", &s2t1, &s2t2)
	sd, err := NewSchemaDiff(&s1, &s2)
	if err != nil {
		t.Fatal(err)
	}
	if len(sd.TableDiffs) != 0 {
		t.Errorf("Expected no table diffs, instead found %d", len(sd.TableDiffs))
	}
	sd, err = NewSchemaDiff(&s2, &s1)
	if err != nil {
		t.Fatal(err)
	}
	if len(sd.TableDiffs) != 0 {
		t.Errorf("Expected no table diffs, instead found %d", len(sd.TableDiffs))
	}
}

func TestSchemaDiffAddOrDropTable(t *testing.T) {
	s1t1 := anotherTable()
	s2t1 := anotherTable()
	s2t2 := aTable(1)
	s1 := aSchema("s1", &s1t1)
	s2 := aSchema("s2", &s2t1, &s2t2)

	// Test table create
	sd, err := NewSchemaDiff(&s1, &s2)
	if err != nil {
		t.Fatal(err)
	}
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
	sd, err = NewSchemaDiff(&s2, &s1)
	if err != nil {
		t.Fatal(err)
	}
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

	// Test impact of statement modifiers on creation of auto-inc table with non-default starting value
	s2t2.NextAutoIncrement = 5
	s2t2.createStatement = s2t2.GeneratedCreateStatement()
	sd, err = NewSchemaDiff(&s1, &s2)
	if err != nil {
		t.Fatal(err)
	}
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
		stmt := sd.TableDiffs[0].Statement(mods)
		if strings.Contains(stmt, "AUTO_INCREMENT=") != expected {
			t.Errorf("Auto-inc filtering for new table not working as expected for modifiers=%+v (expect auto_inc to be present = %t)\nStatement: %s", mods, expected, stmt)
		}
	}

	// Test unsupported tables -- still fine for create/drop
	ust := unsupportedTable()
	s1 = aSchema("s1")
	s2 = aSchema("s2", &ust)
	sd, err = NewSchemaDiff(&s1, &s2)
	if err != nil {
		t.Fatal(err)
	}
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
	sd, err = NewSchemaDiff(&s2, &s1)
	if err != nil {
		t.Fatal(err)
	}
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
		sd, err := NewSchemaDiff(&s1, &s2)
		if err != nil {
			t.Fatal(err)
		}
		if len(sd.TableDiffs) != 1 {
			t.Fatalf("Incorrect number of table diffs: expected 1, found %d", len(sd.TableDiffs))
		}
		if td, ok := sd.TableDiffs[0].(AlterTable); !ok {
			t.Fatalf("Incorrect type of table diff returned: expected %T, found %T", td, sd.TableDiffs[0])
		}
		mods := StatementModifiers{NextAutoInc: nextAutoInc}
		stmt := sd.TableDiffs[0].Statement(mods)
		if stmt == "" {
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
}
