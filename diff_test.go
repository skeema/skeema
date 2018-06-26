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
		if sd.String() != "" {
			t.Errorf("Expected empty String(), instead found %s", sd.String())
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
	td := sd.TableDiffs[0]
	if td.Type != TableDiffCreate || td.TypeString() != "CREATE" || td.Type.String() != "CREATE" {
		t.Fatalf("Incorrect type of table diff returned: expected %s, found %s", TableDiffCreate, td.TypeString())
	}
	if td.To != &s2t2 {
		t.Error("Pointer in table diff does not point to expected value")
	}

	// Test table drop (opposite diff direction of above)
	sd = NewSchemaDiff(&s2, &s1)
	if len(sd.TableDiffs) != 1 {
		t.Fatalf("Incorrect number of table diffs: expected 1, found %d", len(sd.TableDiffs))
	}
	td2 := sd.TableDiffs[0]
	if td2.Type != TableDiffDrop || td2.TypeString() != "DROP" || td2.Type.String() != "DROP" {
		t.Fatalf("Incorrect type of table diff returned: expected %s, found %s", TableDiffDrop, td2.TypeString())
	}
	if td2.From != &s2t2 {
		t.Error("Pointer in table diff does not point to expected value")
	}
	if sd.String() != fmt.Sprintf("DROP TABLE %s;\n", EscapeIdentifier(s2t2.Name)) {
		t.Errorf("SchemaDiff.String returned unexpected result: %s", sd)
	}

	// Test impact of statement modifiers (allowing/forbidding drop) on previous drop
	if stmt, err := td2.Statement(StatementModifiers{AllowUnsafe: false}); !IsForbiddenDiff(err) {
		t.Errorf("Modifier AllowUnsafe=false not working; expected forbidden diff error for %s, instead err=%v", stmt, err)
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
	td = sd.TableDiffs[0]
	if td.Type != TableDiffCreate {
		t.Fatalf("Incorrect type of table diff returned: expected %s, found %s", TableDiffCreate, td.TypeString())
	}
	if td.To != &ust {
		t.Error("Pointer in table diff does not point to expected value")
	}
	sd = NewSchemaDiff(&s2, &s1)
	if len(sd.TableDiffs) != 1 {
		t.Fatalf("Incorrect number of table diffs: expected 1, found %d", len(sd.TableDiffs))
	}
	td2 = sd.TableDiffs[0]
	if td2.Type != TableDiffDrop {
		t.Fatalf("Incorrect type of table diff returned: expected %s, found %s", TableDiffDrop, td2.TypeString())
	}
	if td2.From != &ust {
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
		td := sd.TableDiffs[0]
		if td.Type != TableDiffAlter || td.TypeString() != "ALTER" || td.Type.String() != "ALTER" {
			t.Fatalf("Incorrect type of table diff returned: expected %s, found %s", TableDiffAlter, td.TypeString())
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
	getAlter := func(left, right *Schema) (*TableDiff, TableAlterClause) {
		sd := NewSchemaDiff(left, right)
		if len(sd.TableDiffs) != 1 {
			t.Fatalf("Incorrect number of table diffs: expected 1, found %d", len(sd.TableDiffs))
		}
		if sd.TableDiffs[0].Type != TableDiffAlter {
			t.Fatalf("Incorrect type of table diff returned: expected %s, found %s", TableDiffAlter, sd.TableDiffs[0].TypeString())
		}
		if len(sd.TableDiffs[0].alterClauses) != 1 {
			t.Fatalf("Wrong number of alter clauses: expected 1, found %d", len(sd.TableDiffs[0].alterClauses))
		}
		return sd.TableDiffs[0], sd.TableDiffs[0].alterClauses[0]
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

func TestSchemaDiffForeignKeys(t *testing.T) {
	t1 := foreignKeyTable()
	t2 := foreignKeyTable()
	s1 := aSchema("s1", &t1)
	s2 := aSchema("s2", &t2)

	// Helper to ensure that AddForeignKey and DropForeignKey clauses get split
	// into separate TableDiffs
	assertDiffs := func(from, to *Schema, expectAddFK, expectDropFK, expectOther int) {
		t.Helper()
		sd := NewSchemaDiff(from, to)
		expectTableDiffs := 1
		if expectAddFK > 0 && expectDropFK > 0 {
			expectTableDiffs++
		}
		if len(sd.TableDiffs) != expectTableDiffs {
			t.Errorf("Incorrect number of TableDiffs: expected %d, found %d", expectTableDiffs, len(sd.TableDiffs))
			return
		}
		for n, td := range sd.TableDiffs {
			if n == 1 && td.From.Name != sd.TableDiffs[0].From.Name {
				t.Errorf("Expected TableDiffs[1] to affect same table as TableDiffs[0] (%s), instead found %s", sd.TableDiffs[0].From.Name, td.From.Name)
				break
			}
			var seenAdd, seenDrop bool
			for _, clause := range td.alterClauses {
				switch clause.(type) {
				case AddForeignKey:
					expectAddFK--
					seenAdd = true
					if seenDrop {
						t.Error("Unexpectedly found AddForeignKey and DropForeignKey clauses in the same TableDiff")
					}
				case DropForeignKey:
					expectDropFK--
					seenDrop = true
					if seenAdd {
						t.Error("Unexpectedly found AddForeignKey and DropForeignKey clauses in the same TableDiff")
					}
					if n == 1 {
						t.Errorf("Expected clauses for second TableDiff of same table to only consist of AddForeignKey, instead found %T", clause)
					}
				default:
					expectOther--
					if n == 1 {
						t.Errorf("Expected clauses for second TableDiff of same table to only consist of AddForeignKey, instead found %T", clause)
					}
				}
			}
		}
		if expectAddFK != 0 || expectDropFK != 0 || expectOther != 0 {
			t.Errorf("Did not find expected count of each clause type; counters remaining: add=%d drop=%d other=%d", expectAddFK, expectDropFK, expectOther)
		}
	}

	// Dropping multiple FKs and making other changes: all one TableDiff
	t2.ForeignKeys = []*ForeignKey{}
	t2.Comment = "Hello world"
	t2.CreateStatement = t2.GeneratedCreateStatement()
	assertDiffs(&s1, &s2, 0, 2, 1)

	// Adding multiple FKs and making other changes: all one TableDiff
	assertDiffs(&s2, &s1, 2, 0, 1)

	// Modifying one FK and making other changes: two TableDiffs
	t2 = foreignKeyTable()
	t2.ForeignKeys[1].ReferencedColumnNames[1] = "model_code"
	t2.Comment = "Hello world"
	t2.CreateStatement = t2.GeneratedCreateStatement()
	assertDiffs(&s1, &s2, 1, 1, 1)

	// Adding and dropping unrelated FKs: two TableDiffs
	t2 = foreignKeyTable()
	t2.ForeignKeys[1] = &ForeignKey{
		Name:                  "actor_fk",
		Columns:               t2.Columns[0:1],
		ReferencedSchemaName:  "",
		ReferencedTableName:   "actor",
		ReferencedColumnNames: []string{"actor_id"},
		DeleteRule:            "RESTRICT",
		UpdateRule:            "CASCADE",
	}
	t2.CreateStatement = t2.GeneratedCreateStatement()
	assertDiffs(&s1, &s2, 1, 1, 0)

	// Renaming an FK: two TableDiffs, but both are blank unless enabling
	// StatementModifiers.StrictForeignKeyNaming
	t2 = foreignKeyTable()
	t2.ForeignKeys[1].Name = fmt.Sprintf("_%s", t2.ForeignKeys[1].Name)
	t2.CreateStatement = t2.GeneratedCreateStatement()
	assertDiffs(&s1, &s2, 1, 1, 0)
	for n, td := range NewSchemaDiff(&s1, &s2).TableDiffs {
		mods := StatementModifiers{}
		if actual, _ := td.Statement(mods); actual != "" {
			t.Errorf("Expected blank ALTER without StrictForeignKeyNaming, instead found %s", actual)
		}
		mods.StrictForeignKeyNaming = true
		actual, _ := td.Statement(mods)
		if (n == 0 && !strings.Contains(actual, "DROP FOREIGN KEY")) || (n == 1 && !strings.Contains(actual, "ADD CONSTRAINT")) {
			t.Errorf("Unexpected statement with StrictForeignKeyNaming for tablediff[%d]: returned %s", n, actual)
		}
	}

	// Renaming an FK but also changing its definition: never blank statement
	t2.ForeignKeys[1].Columns = t2.ForeignKeys[1].Columns[0:1]
	t2.ForeignKeys[1].ReferencedColumnNames = t2.ForeignKeys[1].ReferencedColumnNames[0:1]
	t2.CreateStatement = t2.GeneratedCreateStatement()
	assertDiffs(&s1, &s2, 1, 1, 0)
	for n, td := range NewSchemaDiff(&s1, &s2).TableDiffs {
		actual, _ := td.Statement(StatementModifiers{})
		if (n == 0 && !strings.Contains(actual, "DROP FOREIGN KEY")) || (n == 1 && !strings.Contains(actual, "ADD CONSTRAINT")) {
			t.Errorf("Unexpected statement with StrictForeignKeyNaming for tablediff[%d]: returned %s", n, actual)
		}
	}
}

func TestSchemaDiffFilteredTableDiffs(t *testing.T) {
	s1t1 := anotherTable()
	s1t2 := aTable(1)
	s1 := aSchema("s1", &s1t1, &s1t2)

	s2t1 := anotherTable()
	s2t2 := aTable(5)
	s2t3 := unsupportedTable() // still works for add/drop despite being unsupported
	s2 := aSchema("s2", &s2t1, &s2t2, &s2t3)

	assertFiltered := func(sd *SchemaDiff, expectLen int, types ...TableDiffType) {
		t.Helper()
		diffs := sd.FilteredTableDiffs(types...)
		if len(diffs) != expectLen {
			t.Errorf("Wrong result from FilteredTableDiffs(%v) based on count alone: expect %d, found %d", types, expectLen, len(diffs))
		}
		for _, diff := range diffs {
			var ok bool
			for _, typ := range types {
				if diff.Type == typ {
					ok = true
					break
				}
			}
			if !ok {
				t.Errorf("Unexpected diff %v in result of FilteredTableDiffs(%v)", diff, types)
			}
		}
	}

	sd := NewSchemaDiff(&s1, &s2)
	if len(sd.SameTables) != 1 || sd.SameTables[0].Name != s1t1.Name {
		t.Errorf("Unexpected result for sd.SameTables: %v", sd.SameTables)
	}
	assertFiltered(sd, 1, TableDiffCreate)
	assertFiltered(sd, 1, TableDiffAlter)
	assertFiltered(sd, 0, TableDiffDrop)
	assertFiltered(sd, 1, TableDiffCreate, TableDiffDrop)
	assertFiltered(sd, 2, TableDiffCreate, TableDiffAlter)

	sd = NewSchemaDiff(&s2, &s1)
	assertFiltered(sd, 0, TableDiffCreate)
	assertFiltered(sd, 1, TableDiffAlter)
	assertFiltered(sd, 1, TableDiffDrop)
	assertFiltered(sd, 1, TableDiffCreate, TableDiffDrop)
	assertFiltered(sd, 2, TableDiffDrop, TableDiffAlter)
}

func TestTableDiffUnsupportedAlter(t *testing.T) {
	t1 := supportedTable()
	t2 := unsupportedTable()

	assertUnsupported := func(td *TableDiff) {
		t.Helper()
		if td.supported {
			t.Fatal("Expected diff to be unsupported, but it isn't")
		}
		stmt, err := td.Statement(StatementModifiers{})
		if stmt != "" {
			t.Errorf("Expected blank statement for unsupported diff, instead found %s", stmt)
		}
		if !IsUnsupportedDiff(err) {
			t.Fatalf("Expected unsupported diff error, instead err=%v", err)
		}

		// Confirm extended error message. Regardless of whether the unsupported
		// table was on the "to" or "from" side, the message should show what part
		// of the unsupported table triggered the issue.
		extended := err.(*UnsupportedDiffError).ExtendedError()
		expected := `--- Expected
+++ MySQL-actual
@@ -6 +6,4 @@
-) ENGINE=InnoDB DEFAULT CHARSET=latin1
+) ENGINE=InnoDB DEFAULT CHARSET=latin1 ROW_FORMAT=REDUNDANT
+   /*!50100 PARTITION BY RANGE (customer_id)
+   (PARTITION p0 VALUES LESS THAN (123) ENGINE = InnoDB,
+    PARTITION p1 VALUES LESS THAN MAXVALUE ENGINE = InnoDB) */
`
		if expected != extended {
			t.Errorf("Output of ExtendedError() did not match expectation. Returned value:\n%s", extended)
		}
	}

	assertUnsupported(NewAlterTable(&t1, &t2))
	assertUnsupported(NewAlterTable(&t2, &t1))
}

func TestTableDiffClauses(t *testing.T) {
	mods := StatementModifiers{
		AllowUnsafe: true,
		NextAutoInc: NextAutoIncAlways,
	}
	t1 := aTable(1)

	create := NewCreateTable(&t1)
	clauses, err := create.Clauses(mods)
	offset := len("CREATE TABLE `actor` ")
	if err != nil || clauses != t1.CreateStatement[offset:] {
		t.Errorf("Unexpected result for Clauses on create table: err=%v, output=%s", err, clauses)
	}

	t2 := aTable(5)
	alter := NewAlterTable(&t1, &t2)
	clauses, err = alter.Clauses(mods)
	if err != nil || clauses != "AUTO_INCREMENT = 5" {
		t.Errorf("Unexpected result for Clauses on alter table: err=%v, output=%s", err, clauses)
	}

	drop := NewDropTable(&t1)
	clauses, err = drop.Clauses(mods)
	if err != nil || clauses != "" {
		t.Errorf("Unexpected result for Clauses on drop table: err=%v, output=%s", err, clauses)
	}
}

func TestAlterTableStatementAllowUnsafeMods(t *testing.T) {
	t1 := aTable(1)
	t2 := aTable(1)
	s1 := aSchema("s1", &t1)
	s2 := aSchema("s2", &t2)

	getAlter := func(a, b *Schema) *TableDiff {
		sd := NewSchemaDiff(a, b)
		if len(sd.TableDiffs) != 1 {
			t.Fatalf("Incorrect number of table diffs: expected 1, found %d", len(sd.TableDiffs))
		}
		if sd.TableDiffs[0].Type != TableDiffAlter {
			t.Fatalf("Incorrect type of table diff returned: expected %s, found %s", TableDiffAlter, sd.TableDiffs[0].TypeString())
		}
		return sd.TableDiffs[0]
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
	from := anotherTable()
	to := anotherTable()
	col := &Column{
		Name:     "something",
		TypeInDB: "smallint(5) unsigned",
		Default:  ColumnDefaultNull,
	}
	to.Columns = append(to.Columns, col)
	to.CreateStatement = to.GeneratedCreateStatement()
	alter := NewAlterTable(&from, &to)

	assertStatement := func(mods StatementModifiers, middle string) {
		stmt, err := alter.Statement(mods)
		if err != nil {
			t.Errorf("Received unexpected error %s from statement with mods=%v", err, mods)
			return
		}
		expect := fmt.Sprintf("ALTER TABLE `%s` %s%s", from.Name, middle, alter.alterClauses[0].Clause(mods))
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
	alter.alterClauses = []TableAlterClause{}
	if stmt, err := alter.Statement(mods); stmt != "" {
		t.Errorf("Expected blank-string statement if no clauses present, regardless of mods; instead found: %s", stmt)
	} else if err != nil {
		t.Errorf("Expected no error from statement with no clauses present; instead found: %s", err)
	}
}

func TestIgnoreTableMod(t *testing.T) {
	from := anotherTable()
	to := anotherTable()
	col := &Column{
		Name:     "something",
		TypeInDB: "smallint(5) unsigned",
		Default:  ColumnDefaultNull,
	}
	to.Columns = append(to.Columns, col)
	to.CreateStatement = to.GeneratedCreateStatement()
	alter := NewAlterTable(&from, &to)
	create := NewCreateTable(&from)
	drop := NewDropTable(&from)
	assertStatement := func(re string, tableName string, expectNonemptyStatement bool) {
		t.Helper()
		mods := StatementModifiers{
			AllowUnsafe: true,
		}
		if re != "" {
			mods.IgnoreTable = regexp.MustCompile(re)
		}
		from.Name = tableName
		to.Name = tableName
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
