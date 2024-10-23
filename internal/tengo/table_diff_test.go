package tengo

import (
	"fmt"
	"strings"
	"testing"
)

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
	if td.DiffType() != DiffTypeCreate || td.Type.String() != "CREATE" {
		t.Fatalf("Incorrect type of table diff returned: expected %s, found %s", DiffTypeCreate, td.Type)
	}
	if td.To != &s2t2 || td.ObjectKey().Name != s2t2.Name {
		t.Error("Pointer in table diff does not point to expected value")
	}

	// Test table drop (opposite diff direction of above)
	sd = NewSchemaDiff(&s2, &s1)
	if len(sd.TableDiffs) != 1 {
		t.Fatalf("Incorrect number of table diffs: expected 1, found %d", len(sd.TableDiffs))
	}
	td2 := sd.TableDiffs[0]
	if td2.Type != DiffTypeDrop || td2.Type.String() != "DROP" {
		t.Fatalf("Incorrect type of table diff returned: expected %s, found %s", DiffTypeDrop, td2.Type)
	}
	if td2.From != &s2t2 || td2.ObjectKey().Name != s2t2.Name {
		t.Error("Pointer in table diff does not point to expected value")
	}
	if sd.String() != fmt.Sprintf("DROP TABLE %s;\n", EscapeIdentifier(s2t2.Name)) {
		t.Errorf("SchemaDiff.String returned unexpected result: %s", sd)
	}

	// Test impact of statement modifiers (allowing/forbidding drop) on previous drop
	if stmt, err := td2.Statement(StatementModifiers{AllowUnsafe: false}); !IsUnsafeDiff(err) {
		t.Errorf("Modifier AllowUnsafe=false not working; expected unsafe diff error for %s, instead err=%v", stmt, err)
	}
	if stmt, err := td2.Statement(StatementModifiers{AllowUnsafe: true}); err != nil {
		t.Errorf("Modifier AllowUnsafe=true not working; error (%s) returned for %s", err, stmt)
	}

	// Test impact of statement modifiers on creation of auto-inc table with non-default starting value
	s2t2.NextAutoIncrement = 5
	s2t2.CreateStatement = s2t2.GeneratedCreateStatement(FlavorUnknown)
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
	if td.Type != DiffTypeCreate {
		t.Fatalf("Incorrect type of table diff returned: expected %s, found %s", DiffTypeCreate, td.Type)
	}
	if td.To != &ust {
		t.Error("Pointer in table diff does not point to expected value")
	}
	sd = NewSchemaDiff(&s2, &s1)
	if len(sd.TableDiffs) != 1 {
		t.Fatalf("Incorrect number of table diffs: expected 1, found %d", len(sd.TableDiffs))
	}
	td2 = sd.TableDiffs[0]
	if td2.Type != DiffTypeDrop {
		t.Fatalf("Incorrect type of table diff returned: expected %s, found %s", DiffTypeDrop, td2.Type)
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
		if td.Type != DiffTypeAlter || td.Type.String() != "ALTER" {
			t.Fatalf("Incorrect type of table diff returned: expected %s, found %s", DiffTypeAlter, td.Type)
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
		if sd.TableDiffs[0].Type != DiffTypeAlter {
			t.Fatalf("Incorrect type of table diff returned: expected %s, found %s", DiffTypeAlter, sd.TableDiffs[0].Type)
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
	})
	t2.CreateStatement = t2.GeneratedCreateStatement(FlavorUnknown)
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
	s1t1 := anotherTable()
	s1t2 := foreignKeyTable()
	s2t1 := anotherTable()
	s2t2 := foreignKeyTable()
	s1 := aSchema("s1", &s1t1, &s1t2)
	s2 := aSchema("s2", &s2t1, &s2t2)

	// Helper to ensure that AddForeignKey clauses get split into a separate
	// TableDiff, at the end of the SchemaDiff.TableDiffs
	assertDiffs := func(from, to *Schema, expectAddFKAlters, expectAddFKClauses, expectOtherAlters, expectOtherClauses int) {
		t.Helper()
		sd := NewSchemaDiff(from, to)
		if len(sd.TableDiffs) != expectAddFKAlters+expectOtherAlters {
			t.Errorf("Incorrect number of TableDiffs: expected %d, found %d", expectAddFKAlters+expectOtherAlters, len(sd.TableDiffs))
			return
		}
		for _, td := range sd.TableDiffs {
			var seenAddFK, seenOther bool
			for _, clause := range td.alterClauses {
				if _, ok := clause.(AddForeignKey); ok {
					expectAddFKClauses--
					seenAddFK = true
					if seenOther {
						t.Error("Unexpectedly found AddForeignKey clauses mixed with other clause types in same TableDiff")
					}
				} else {
					expectOtherClauses--
					seenOther = true
					if seenAddFK {
						t.Error("Unexpectedly found AddForeignKey clauses mixed with other clause types in same TableDiff")
					}
				}
			}
			if seenAddFK {
				expectAddFKAlters--
				if expectOtherAlters > 0 {
					t.Error("Unexpectedly found a TableDiff with AddForeignKey before seeing all expected non-AddForeignKey TaleDiffs")
				}
			}
			if seenOther {
				expectOtherAlters--
			}
		}
		if expectAddFKAlters != 0 || expectOtherAlters != 0 {
			t.Errorf("Did not find expected count of each alter type; counters remaining: addfk=%d other=%d", expectAddFKAlters, expectOtherAlters)
		}
		if expectAddFKClauses != 0 || expectOtherClauses != 0 {
			t.Errorf("Did not find expected count of each clause type; counters remaining: addfk=%d other=%d", expectAddFKClauses, expectOtherClauses)
		}
	}

	// Dropping multiple FKs and making other changes
	s2t2.ForeignKeys = []*ForeignKey{}
	s2t2.Comment = "Hello world"
	s2t2.CreateStatement = s2t2.GeneratedCreateStatement(FlavorUnknown)
	assertDiffs(&s1, &s2, 0, 0, 1, 3)

	// Adding multiple FKs and making other changes
	assertDiffs(&s2, &s1, 1, 2, 1, 1)

	// Add an FK to one table; change one FK and make another change to other tbale
	s2t1.ForeignKeys = []*ForeignKey{
		{
			Name:                  "actor_fk",
			ColumnNames:           []string{s2t1.Columns[0].Name},
			ReferencedSchemaName:  "",
			ReferencedTableName:   "actor",
			ReferencedColumnNames: []string{"actor_id"},
			DeleteRule:            "RESTRICT",
			UpdateRule:            "CASCADE",
		},
	}
	s2t1.CreateStatement = s2t1.GeneratedCreateStatement(FlavorUnknown)
	s2t2 = foreignKeyTable()
	s2t2.ForeignKeys[1].ReferencedColumnNames[1] = "model_code"
	s2t2.Comment = "Hello world"
	s2t2.CreateStatement = s2t2.GeneratedCreateStatement(FlavorUnknown)
	assertDiffs(&s1, &s2, 2, 2, 1, 2)

	// Adding and dropping unrelated FKs
	s2t1 = anotherTable()
	s2t2 = foreignKeyTable()
	s2t2.ForeignKeys[1] = &ForeignKey{
		Name:                  "actor_fk",
		ColumnNames:           []string{s2t2.Columns[0].Name},
		ReferencedSchemaName:  "",
		ReferencedTableName:   "actor",
		ReferencedColumnNames: []string{"actor_id"},
		DeleteRule:            "RESTRICT",
		UpdateRule:            "CASCADE",
	}
	s2t2.CreateStatement = s2t2.GeneratedCreateStatement(FlavorUnknown)
	assertDiffs(&s1, &s2, 1, 1, 1, 1)

	// Renaming an FK: two TableDiffs, but both are blank unless enabling
	// StatementModifiers.StrictForeignKeyNaming
	s2t2 = foreignKeyTable()
	s2t2.ForeignKeys[1].Name = fmt.Sprintf("_%s", s2t2.ForeignKeys[1].Name)
	s2t2.CreateStatement = s2t2.GeneratedCreateStatement(FlavorUnknown)
	assertDiffs(&s1, &s2, 1, 1, 1, 1)
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

	// Changing between RESTRICT and NO ACTION:
	// still blank without StatementModifiers.StrictForeignKeyNaming
	s1t2.ForeignKeys[1].DeleteRule = "RESTRICT"
	s1t2.CreateStatement = s1t2.GeneratedCreateStatement(FlavorUnknown)
	s2t2 = foreignKeyTable()
	s2t2.ForeignKeys[1].UpdateRule = "RESTRICT"
	s2t2.ForeignKeys[1].DeleteRule = "NO ACTION"
	s2t2.CreateStatement = s2t2.GeneratedCreateStatement(FlavorUnknown)
	assertDiffs(&s1, &s2, 1, 1, 1, 1)
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

	// Renaming an FK but also changing a rule to one that isn't equivalent: never blank statement
	s1t2.ForeignKeys[1].DeleteRule = "CASCADE"
	s1t2.CreateStatement = s2t1.GeneratedCreateStatement(FlavorUnknown)
	assertDiffs(&s1, &s2, 1, 1, 1, 1)
	for n, td := range NewSchemaDiff(&s1, &s2).TableDiffs {
		actual, _ := td.Statement(StatementModifiers{})
		if (n == 0 && !strings.Contains(actual, "DROP FOREIGN KEY")) || (n == 1 && !strings.Contains(actual, "ADD CONSTRAINT")) {
			t.Errorf("Unexpected statement with StrictForeignKeyNaming for tablediff[%d]: returned %s", n, actual)
		}
	}

	// Renaming an FK but also changing its definition: never blank statement
	s2t2.ForeignKeys[1].UpdateRule = "CASCADE"
	s2t2.ForeignKeys[1].ColumnNames = s2t2.ForeignKeys[1].ColumnNames[0:1]
	s2t2.ForeignKeys[1].ReferencedColumnNames = s2t2.ForeignKeys[1].ReferencedColumnNames[0:1]
	s2t2.CreateStatement = s2t2.GeneratedCreateStatement(FlavorUnknown)
	assertDiffs(&s1, &s2, 1, 1, 1, 1)
	for n, td := range NewSchemaDiff(&s1, &s2).TableDiffs {
		actual, _ := td.Statement(StatementModifiers{})
		if (n == 0 && !strings.Contains(actual, "DROP FOREIGN KEY")) || (n == 1 && !strings.Contains(actual, "ADD CONSTRAINT")) {
			t.Errorf("Unexpected statement with StrictForeignKeyNaming for tablediff[%d]: returned %s", n, actual)
		}
	}
}

func TestSchemaDiffMultiFulltext(t *testing.T) {
	t1 := aTable(0)
	t2 := aTable(0)
	s1 := aSchema("s1", &t1)
	s2 := aSchema("s2", &t2)

	// Add one regular index and two fulltext indexes to s2t.
	newIndexes := []*Index{
		{
			Name: "ft_last",
			Parts: []IndexPart{
				{ColumnName: "last_name"},
			},
			Type: "FULLTEXT",
		},
		{
			Name: "idx_last_update",
			Parts: []IndexPart{
				{ColumnName: "last_update"},
			},
			Type: "BTREE",
		},
		{
			Name: "ft_first",
			Parts: []IndexPart{
				{ColumnName: "first_name"},
			},
			Type: "FULLTEXT",
		},
	}
	t2.SecondaryIndexes = append(t2.SecondaryIndexes, newIndexes...)
	t2.CreateStatement = t2.GeneratedCreateStatement(FlavorUnknown)

	assertClauses := func(td *TableDiff, expectAddFulltext, expectOther int) {
		t.Helper()
		var foundAddFulltext, foundOther int
		for _, clause := range td.alterClauses {
			if addIndex, ok := clause.(AddIndex); ok && addIndex.Index.Type == "FULLTEXT" {
				foundAddFulltext++
			} else {
				foundOther++
			}
		}
		if expectAddFulltext != foundAddFulltext || expectOther != foundOther {
			t.Errorf("Expected to find %d ADD FULLTEXT KEY and %d other clauses, instead found %d ADD FULLTEXT KEY and %d other clauses", expectAddFulltext, expectOther, foundAddFulltext, foundOther)
		}
	}

	// InnoDB doesn't support adding multiple fulltext indexes in a single ALTER.
	// Confirm that the SchemaDiff splits this into two ALTERs.
	sd := NewSchemaDiff(&s1, &s2)
	if len(sd.TableDiffs) != 2 {
		t.Errorf("Incorrect number of TableDiffs: expected 2 due to splitting out multiple ADD FULLTEXT KEY; instead found %d", len(sd.TableDiffs))
	} else {
		assertClauses(sd.TableDiffs[0], 1, 1)
		assertClauses(sd.TableDiffs[1], 1, 0)
	}
}

func TestTableDiffUnsupportedAlter(t *testing.T) {
	t1 := supportedTable()
	t2 := unsupportedTable()

	// Attempt to generate a diff which would add sub-partitioning (an unsupported
	// feature)
	td := NewAlterTable(&t1, &t2)
	if td.supported {
		t.Fatal("Expected diff to be unsupported, but it isn't")
	}
	stmt, err := td.Statement(StatementModifiers{})
	if !IsUnsupportedDiff(err) {
		t.Fatalf("Expected unsupported diff error, instead err=%v", err)
	}
	if stmt != "" {
		t.Errorf("Expected a blank statement string from attempt to add an unsupported feature to a table, but instead generated statement: %s", stmt)
	}
	expected := `The desired state ("to" side of diff) contains unexpected or unsupported clauses in SHOW CREATE TABLE.
--- desired state expected CREATE
+++ desired state actual SHOW CREATE
@@ -8,0 +9,2 @@
+SUBPARTITION BY HASH (post_id)
+SUBPARTITIONS 2
`
	if actual := err.(*UnsupportedDiffError).Error(); actual != expected {
		t.Errorf("Output of Error() did not match expectation. Returned value:\n%s", actual)
	}

	// Attempt to generate a diff which removes sub-partitioning. Note that in
	// this case (*removal* of an unsupported feature) we can actually generate
	// a DDL statement, but still with an unsupported error so that the caller
	// knows to verify the DDL more carefully!
	td = NewAlterTable(&t2, &t1)
	if td.supported {
		t.Fatal("Expected diff to be unsupported, but it isn't")
	}
	stmt, err = td.Statement(StatementModifiers{})
	if !IsUnsupportedDiff(err) {
		t.Fatalf("Expected unsupported diff error, instead err=%v", err)
	}
	if stmt == "" {
		t.Error("Expected non-blank statement for removing an unsupported feature, but statement was blank")
	}
	expected = `The original state ("from" side of diff) contains unexpected or unsupported clauses in SHOW CREATE TABLE.
--- original state expected CREATE
+++ original state actual SHOW CREATE
@@ -8,0 +9,2 @@
+SUBPARTITION BY HASH (post_id)
+SUBPARTITIONS 2
`
	if actual := err.(*UnsupportedDiffError).Error(); actual != expected {
		t.Errorf("Output of Error() did not match expectation. Returned value:\n%s", actual)
	}

	// Test error-handling for when a diff is both unsupported AND unsafe
	t2.Columns = append(t2.Columns, &Column{
		Name:     "foo_id",
		TypeInDB: "bigint(20) unsigned",
	})
	t2.CreateStatement = t2.GeneratedCreateStatement(FlavorUnknown)
	td = NewAlterTable(&t2, &t1)
	if td.supported {
		t.Fatal("Expected diff to be unsupported, but it isn't")
	}
	stmt, err = td.Statement(StatementModifiers{})
	if !IsUnsupportedDiff(err) {
		t.Errorf("Expected unsupported diff error, instead err is type %T, value %v", err, err)
	}
	if !IsUnsafeDiff(err) {
		t.Errorf("Expected unsafe diff error, instead err is type %T, value %v", err, err)
	}

	// Test marking the diff as supported
	if err := td.MarkSupported(); err != nil {
		t.Errorf("Unexpected error from MarkSupported: %v", err)
	} else if !td.supported {
		t.Error("MarkSupported did not mutate td.supported as expected")
	}
	if td.MarkSupported() == nil {
		t.Error("Expected repeated call to MarkSupported to return an error, but error was nil")
	}
	td = NewAlterTable(&t2, &t2)
	if td.MarkSupported() == nil {
		t.Error("Expected error return from MarkSupported on an empty diff, but error was nil")
	}
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
		if sd.TableDiffs[0].Type != DiffTypeAlter {
			t.Fatalf("Incorrect type of table diff returned: expected %s, found %s", DiffTypeAlter, sd.TableDiffs[0].Type)
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
	t2.CreateStatement = t2.GeneratedCreateStatement(FlavorUnknown)
	assertSafe(&s1, &s2)

	// Removing a column is unsafe
	t2 = aTable(1)
	t2.Columns = t2.Columns[0 : len(t2.Columns)-1]
	t2.CreateStatement = t2.GeneratedCreateStatement(FlavorUnknown)
	assertUnsafe(&s1, &s2)

	// Changing col type to increase its size is safe
	t2 = aTable(1)
	t2.Columns[0].TypeInDB = "int unsigned"
	t2.CreateStatement = t2.GeneratedCreateStatement(FlavorUnknown)
	assertSafe(&s1, &s2)

	// Changing col type to change to signed is unsafe
	t2 = aTable(1)
	t2.Columns[0].TypeInDB = "smallint(5)"
	t2.CreateStatement = t2.GeneratedCreateStatement(FlavorUnknown)
	assertUnsafe(&s1, &s2)
}

func TestAlterTableStatementOnlineMods(t *testing.T) {
	from := anotherTable()
	to := anotherTable()
	col := &Column{
		Name:     "something",
		TypeInDB: "smallint(5) unsigned",
	}
	to.Columns = append(to.Columns, col)
	to.CreateStatement = to.GeneratedCreateStatement(FlavorUnknown)
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

func TestAlterTableStatementVirtualColValidation(t *testing.T) {
	from, to := aTable(1), aTable(1)

	assertWithValidation := func(expected bool) {
		t.Helper()
		to.CreateStatement = to.GeneratedCreateStatement(FlavorUnknown)
		alter := NewAlterTable(&from, &to)
		mods := StatementModifiers{}
		stmt, err := alter.Statement(mods)
		if err != nil {
			t.Fatalf("Unexpected error from Statement(): %v", err)
		}
		if strings.Contains(stmt, "WITH VALIDATION") {
			t.Error("Statement unexpectedly contains WITH VALIDATION even without statement modifier?")
			return
		}
		mods.VirtualColValidation = true
		stmt, _ = alter.Statement(mods)
		if actual := strings.Contains(stmt, "WITH VALIDATION"); actual != expected {
			t.Errorf("Expected strings.Contains(%q, \"WITH VALIDATION\") to return %t, instead found %t", stmt, expected, actual)
		}
	}

	// No clauses: VirtualColValidation has no effect
	assertWithValidation(false)

	// Adding a non-virtual column, even if generated: VirtualColValidation has
	// no effect
	col := &Column{
		Name:           "full_name",
		TypeInDB:       "varchar(100)",
		Nullable:       true,
		CharSet:        "utf8",
		Collation:      "utf8_general_ci",
		GenerationExpr: "CONCAT(first_name, ' ', last_name)",
	}
	to.Columns = append(to.Columns, col)
	assertWithValidation(false)

	// Adding a virtual column: VirtualColValidation works as expected
	col.Virtual = true
	assertWithValidation(true)

	// Modifying virtual column: VirtualColValidation works as expected
	col.GenerationExpr = "CONCAT(first_name, ' ', IFNULL(last_name, ''))"
	assertWithValidation(true)

	// Modifying some other col: VirtualColValidation has no effect, even tho
	// virtual col present
	colCopy := *col
	from.Columns = append(from.Columns, &colCopy)
	from.CreateStatement = from.GeneratedCreateStatement(FlavorUnknown)
	to.Columns[4].TypeInDB = "varchar(20)"
	assertWithValidation(false)
}

func TestModifyColumnUnsafe(t *testing.T) {
	assertUnsafeWithMods := func(type1, type2 string, mods StatementModifiers, expected bool) {
		t.Helper()
		mc := ModifyColumn{
			OldColumn: &Column{TypeInDB: type1},
			NewColumn: &Column{TypeInDB: type2},
		}
		if actual, _ := mc.Unsafe(mods); actual != expected {
			t.Errorf("For %s -> %s, expected unsafe=%t, instead found unsafe=%t", type1, type2, expected, actual)
		}
	}
	assertUnsafe := func(type1, type2 string, expected bool) {
		t.Helper()
		assertUnsafeWithMods(type1, type2, StatementModifiers{}, expected)
	}

	expectUnsafe := [][]string{
		{"int unsigned", "int"},
		{"bigint(11)", "bigint(11) unsigned"},
		{"int(11)", "bigint(20) unsigned"},
		{"enum('a','b','c')", "enum('a','aa','b','c'"},
		{"set('abc','def','ghi')", "set('abc','def')"},
		{"decimal(10,5)", "decimal(10,4)"},
		{"decimal(10,5)", "decimal(9,5)"},
		{"decimal(10,5)", "decimal(9,6)"},
		{"decimal(9,4)", "decimal(10,5) unsigned"},
		{"varchar(20)", "varchar(19)"},
		{"varbinary(40)", "varbinary(35)"},
		{"varbinary(256)", "tinyblob"},
		{"blob", "varbinary(2000)"},
		{"varchar(20)", "varbinary(20)"},
		{"timestamp(5)", "timestamp"},
		{"datetime(4)", "datetime(3)"},
		{"timestamp", "time"},
		{"timestamp", "time(3)"},
		{"time", "timestamp"},
		{"time(4)", "timestamp"},
		{"time(4)", "timestamp(5)"},
		{"float", "float(10,5)"},
		{"double", "float"},
		{"float(10,5)", "float(10,4)"},
		{"double(10,5)", "double(9,5)"},
		{"float(10,5)", "double(10,4)"},
		{"float(10,5)", "float(10,5) unsigned"},
		{"mediumint", "smallint"},
		{"mediumint(1)", "tinyint"},
		{"longblob", "blob"},
		{"mediumtext", "tinytext"},
		{"varchar(2000)", "tinytext"},
		{"tinytext", "char(200)"},
		{"tinyblob", "longtext"},
		{"binary(5)", "binary(10)"},
		{"binary(5)", "varbinary(10)"},
		{"tinyblob", "binary(4000)"},
		{"bit(10)", "bit(9)"},
		{"binary(17)", "inet6"},
		{"inet6", "varbinary(16)"},
		{"inet6", "varchar(38)"},
		{"inet6", "inet4"},
		{"inet4", "char(10)"},
		{"inet4", "inet6"}, // unsafe with empty StatementModifiers; see add'l testing later below
		{"char(31)", "uuid"},
		{"uuid", "binary(15)"},
		{"vector(10)", "binary(36)"},
		{"vector(64)", "tinyblob"},
		{"tinyblob", "vector(63)"},
		{"vector(4)", "varchar(4000)"},
	}
	for _, types := range expectUnsafe {
		assertUnsafe(types[0], types[1], true)
	}

	expectSafe := [][]string{
		{"varchar(30)", "varchar(30)"},
		{"mediumint(4)", "mediumint(3)"},
		{"int zerofill", "int"},
		{"int(10) unsigned", "bigint(20)"},
		{"enum('a','b','c')", "enum('a','b','c','d')"},
		{"set('abc','def','ghi')", "set('abc','def','ghi','jkl')"},
		{"decimal(9,4)", "decimal(10,4)"},
		{"decimal(9,4)", "decimal(9,5)"},
		{"decimal(9,4) unsigned", "decimal(9,4)"},
		{"varchar(20)", "varchar(21)"},
		{"varbinary(40)", "varbinary(45)"},
		{"varbinary(255)", "tinyblob"},
		{"tinyblob", "varbinary(255)"},
		{"timestamp", "timestamp(5)"},
		{"time", "time(5)"},
		{"datetime(3)", "datetime(4)"},
		{"float(10,5)", "float"},
		{"float", "double"},
		{"float(10,4)", "float(10,5)"},
		{"double(9,5)", "double(10,5)"},
		{"double(10,5) unsigned", "double(10,5)"},
		{"float(10,4)", "double(11,4)"},
		{"float(10,4)", "double"},
		{"smallint", "mediumint"},
		{"tinyint", "mediumint(1)"},
		{"int(4) unsigned", "int(5) unsigned"},
		{"blob", "longblob"},
		{"tinytext", "mediumtext"},
		{"tinytext", "char(255)"},
		{"char(10)", "char(15)"},
		{"varchar(200)", "tinytext"},
		{"char(30)", "varchar(30)"},
		{"bit(10)", "bit(11)"},
		{"binary(16)", "inet6"},
		{"inet6", "binary(16)"},
		{"char(39)", "inet6"},
		{"inet4", "binary(4)"},
		{"varchar(15)", "inet4"},
		{"uuid", "varchar(32)"},
		{"binary(16)", "uuid"},
		{"vector(10)", "binary(40)"},
		{"vector(63)", "tinyblob"},
		{"tinyblob", "vector(64)"},
	}
	for _, types := range expectSafe {
		assertUnsafe(types[0], types[1], false)
	}

	// Special case: confirm changing the character set of a column is unsafe, but
	// changing collation within same character set is safe (as long as col isn't
	// in a unique index or PK)
	mc := ModifyColumn{
		OldColumn: &Column{TypeInDB: "varchar(30)", CharSet: "latin1"},
		NewColumn: &Column{TypeInDB: "varchar(30)", CharSet: "utf8mb4"},
	}
	if unsafe, _ := mc.Unsafe(StatementModifiers{}); !unsafe {
		t.Error("For changing character set, expected unsafe=true, instead found unsafe=false")
	}
	mc.NewColumn.CharSet = "latin1"
	mc.NewColumn.Collation = "latin1_bin"
	if unsafe, _ := mc.Unsafe(StatementModifiers{}); unsafe {
		t.Error("For changing collation but not character set, expected unsafe=false, instead found unsafe=true")
	}

	// Special case: confirm changing the type of a column is safe for virtual
	// generated columns but not stored generated columns
	mc = ModifyColumn{
		OldColumn: &Column{TypeInDB: "bigint(20)", GenerationExpr: "id * 2", Virtual: true},
		NewColumn: &Column{TypeInDB: "int(11)", GenerationExpr: "id * 2", Virtual: true},
	}
	if unsafe, _ := mc.Unsafe(StatementModifiers{}); unsafe {
		t.Error("Expected virtual column modification to be safe, but Unsafe() returned true")
	}
	mc.OldColumn.Virtual = false
	if unsafe, _ := mc.Unsafe(StatementModifiers{}); !unsafe {
		t.Error("Expected stored column modification to be unsafe, but Unsafe() returned false")
	}

	// Special case: confirm changing SRID, or adding/removing SRID, is unsafe
	mc = ModifyColumn{
		OldColumn: &Column{TypeInDB: "geometry", SpatialReferenceID: 0, HasSpatialReference: false},
		NewColumn: &Column{TypeInDB: "geometry", SpatialReferenceID: 0, HasSpatialReference: true},
	}
	if unsafe, _ := mc.Unsafe(StatementModifiers{}); !unsafe {
		t.Error("Expected addition of SRID to be unsafe even for SRID 0, but Unsafe() returned false")
	}
	mc = ModifyColumn{
		OldColumn: &Column{TypeInDB: "geometry", SpatialReferenceID: 0, HasSpatialReference: true},
		NewColumn: &Column{TypeInDB: "geometry", SpatialReferenceID: 4326, HasSpatialReference: true},
	}
	if unsafe, _ := mc.Unsafe(StatementModifiers{}); !unsafe {
		t.Error("Expected change of SRID to be unsafe, but Unsafe() returned false")
	}

	// Special case: inet4 to inet6 is safe only in MariaDB 11.3+; opposite is
	// still always unsafe
	mods := StatementModifiers{Flavor: ParseFlavor("mariadb:11.2.3")}
	assertUnsafeWithMods("inet4", "inet6", mods, true)
	mods.Flavor = ParseFlavor("mariadb:11.3.2")
	assertUnsafeWithMods("inet4", "inet6", mods, false)
	assertUnsafeWithMods("inet6", "inet4", mods, true)
}

func (s TengoIntegrationSuite) TestAlterPageCompression(t *testing.T) {
	flavor := s.d.Flavor()
	// Skip test if flavor doesn't support page compression
	// Note that although MariaDB 10.1 supports this feature, we exclude it here
	// since it does not seem to work out-of-the-box in Docker images
	if !flavor.MinMySQL(5, 7) && !flavor.MinMariaDB(10, 2) {
		t.Skipf("InnoDB page compression not supported in flavor %s", flavor)
	}

	sqlPath := "pagecompression.sql"
	if flavor.IsMariaDB() {
		sqlPath = "pagecompression-maria.sql"
	}
	s.SourceTestSQL(t, sqlPath)
	schema := s.GetSchema(t, "testing")
	uncompTable := getTable(t, schema, "actor_in_film")
	if uncompTable.CreateOptions != "" {
		t.Fatal("Fixture table has changed without test logic being updated")
	}

	runAlter := func(clause TableAlterClause) {
		t.Helper()
		db, err := s.d.CachedConnectionPool("testing", "")
		if err != nil {
			t.Fatalf("Unable to connect to DockerizedInstance: %s", err)
		}
		tableName := uncompTable.Name
		query := fmt.Sprintf("ALTER TABLE %s %s", EscapeIdentifier(tableName), clause.Clause(StatementModifiers{}))
		if _, err := db.Exec(query); err != nil {
			t.Fatalf("Unexpected error from query %q: %v", query, err)
		}
		schema = s.GetSchema(t, "testing") // re-introspect to reflect changes from the DDL
	}

	compTable := getTable(t, schema, "actor_in_film_comp")
	if compTable.UnsupportedDDL {
		t.Fatal("Table with page compression is unexpectedly unsupported for diff")
	}
	compTable.Name = uncompTable.Name

	// Test diff generation for uncompressed -> compressed
	clauses, supported := uncompTable.Diff(compTable)
	if len(clauses) != 1 || !supported {
		t.Fatalf("Unexpected return from diff: %d clauses, supported=%t", len(clauses), supported)
	}
	runAlter(clauses[0])
	refetchedTable := getTable(t, schema, "actor_in_film")
	// Just comparing string length because the *order* of create options may
	// randomly differ from what was specified in DDL
	if len(refetchedTable.CreateOptions) != len(compTable.CreateOptions) {
		t.Fatalf("Expected refetched table to have create options %q, instead found %q", compTable.CreateOptions, refetchedTable.CreateOptions)
	}

	// Test diff generation and execution for compressed -> uncompressed
	clauses, supported = compTable.Diff(uncompTable)
	if len(clauses) != 1 || !supported {
		t.Fatalf("Unexpected return from diff: %d clauses, supported=%t", len(clauses), supported)
	}
	runAlter(clauses[0])
	refetchedTable = getTable(t, schema, "actor_in_film")
	if refetchedTable.CreateOptions != "" {
		t.Fatalf("Expected refetched table to have create options \"\", instead found %q", refetchedTable.CreateOptions)
	}
}

// TestAlterCheckConstraints provides unit test coverage relating to diffs of
// check constraints.
func TestAlterCheckConstraints(t *testing.T) {
	flavor := ParseFlavor("mysql:8.0.23")
	mods := StatementModifiers{Flavor: flavor}

	// Test addition of checks
	tableNoChecks := aTableForFlavor(flavor, 1)
	tableChecks := aTableForFlavor(flavor, 1)
	tableChecks.Checks = []*Check{
		{Name: "alivecheck", Clause: "alive != 0", Enforced: true},
		{Name: "stringythings", Clause: "ssn <> '000000000'", Enforced: true},
	}
	tableChecks.CreateStatement = tableChecks.GeneratedCreateStatement(flavor)
	td := NewAlterTable(&tableNoChecks, &tableChecks)
	if len(td.alterClauses) != 2 {
		t.Errorf("Expected 2 alter clauses, instead found %d", len(td.alterClauses))
	} else {
		for _, clause := range td.alterClauses {
			str := clause.Clause(mods)
			if _, ok := clause.(AddCheck); !ok || !strings.Contains(str, "ADD CONSTRAINT") {
				t.Errorf("Found unexpected type %T", clause)
			}
		}
	}

	// Test removal of checks
	td = NewAlterTable(&tableChecks, &tableNoChecks)
	if len(td.alterClauses) != 2 {
		t.Errorf("Expected 2 alter clauses, instead found %d", len(td.alterClauses))
	} else {
		for _, clause := range td.alterClauses {
			if _, ok := clause.(DropCheck); !ok {
				t.Errorf("Found unexpected type %T", clause)
			}
			strMySQL := clause.Clause(mods)
			strMaria := clause.Clause(StatementModifiers{Flavor: ParseFlavor("mariadb:10.5")})
			if strMySQL == strMaria || !strings.Contains(strMySQL, "DROP CHECK") || !strings.Contains(strMaria, "DROP CONSTRAINT") {
				t.Errorf("Unexpected clause differences between flavors; found MySQL %q, MariaDB %q", strMySQL, strMaria)
			}
		}
	}

	// Test change in name of first check. This should result in 4 clauses: drop
	// and re-add the first check to rename (only emitted with strict modifier),
	// and drop and re-add the second check but only for ordering (only emitted
	// if MariaDB AND strict modifier)
	maria105 := ParseFlavor("mariadb:10.5")
	strictMySQLMods := StatementModifiers{Flavor: flavor, StrictCheckConstraints: true}
	strictMariaMods := StatementModifiers{Flavor: maria105, StrictCheckConstraints: true}
	tableChecks2 := aTableForFlavor(flavor, 1)
	tableChecks2.Checks = []*Check{
		{Name: "_alivecheck", Clause: "alive != 0", Enforced: true},
		{Name: "stringythings", Clause: "ssn <> '000000000'", Enforced: true},
	}
	tableChecks2.CreateStatement = tableChecks2.GeneratedCreateStatement(flavor)
	td = NewAlterTable(&tableChecks, &tableChecks2)
	if len(td.alterClauses) != 4 {
		t.Errorf("Expected 4 alterClauses, instead found %d", len(td.alterClauses))
	} else {
		if dcc, ok := td.alterClauses[0].(DropCheck); !ok || dcc.Check != tableChecks.Checks[0] || dcc.reorderOnly || !dcc.renameOnly {
			t.Errorf("Found unexpected alterClause %+v", td.alterClauses[0])
		}
		if acc, ok := td.alterClauses[1].(AddCheck); !ok || acc.Check != tableChecks2.Checks[0] || acc.reorderOnly || !acc.renameOnly {
			t.Errorf("Found unexpected alterClause %+v", td.alterClauses[1])
		}
		if dcc, ok := td.alterClauses[2].(DropCheck); !ok || dcc.Check != tableChecks.Checks[1] || !dcc.reorderOnly || dcc.renameOnly {
			t.Errorf("Found unexpected alterClause %+v", td.alterClauses[2])
		}
		if acc, ok := td.alterClauses[3].(AddCheck); !ok || acc.Check != tableChecks2.Checks[1] || !acc.reorderOnly || acc.renameOnly {
			t.Errorf("Found unexpected alterClause %+v", td.alterClauses[3])
		}
		for n, alterClause := range td.alterClauses {
			// Expectations: all blank with regular mods; first two non-blank with strict
			// MySQL mods; all non-blank with strict MariaDB mods
			if clause := alterClause.Clause(mods); clause != "" {
				t.Errorf("Unexpected clause with basic mods at n=%d: expected blank, found %q", n, clause)
			}
			expectStrictBlank := n > 1
			actualStrictBlank := alterClause.Clause(strictMySQLMods) == ""
			if expectStrictBlank != actualStrictBlank {
				t.Errorf("Unexpected result from Clause() with strict MySQL modifier at n=%d", n)
			}
			if clause := alterClause.Clause(strictMariaMods); clause == "" {
				t.Errorf("Unexpected blank result from Clause() with strict MariaDB modifier at n=%d", n)
			}
		}
	}

	// Revert first check's name change, but change its check clause instead.
	// This should result in 4 clauses: drop and re-add the first check to modify
	// it, and drop and re-add the second check but only for ordering (only emitted
	// if MariaDB AND strict modifier)
	tableChecks2.Checks[0].Name = tableChecks.Checks[0].Name
	tableChecks2.Checks[0].Clause = "alive = 1"
	tableChecks2.CreateStatement = tableChecks2.GeneratedCreateStatement(flavor)
	td = NewAlterTable(&tableChecks, &tableChecks2)
	if len(td.alterClauses) != 4 {
		t.Errorf("Expected 4 alterClauses, instead found %d", len(td.alterClauses))
	} else {
		if dcc, ok := td.alterClauses[0].(DropCheck); !ok || dcc.Check != tableChecks.Checks[0] || dcc.reorderOnly || dcc.renameOnly {
			t.Errorf("Found unexpected alterClause %+v", td.alterClauses[0])
		}
		if acc, ok := td.alterClauses[1].(AddCheck); !ok || acc.Check != tableChecks2.Checks[0] || acc.reorderOnly || acc.renameOnly {
			t.Errorf("Found unexpected alterClause %+v", td.alterClauses[1])
		}
		if dcc, ok := td.alterClauses[2].(DropCheck); !ok || dcc.Check != tableChecks.Checks[1] || !dcc.reorderOnly || dcc.renameOnly {
			t.Errorf("Found unexpected alterClause %+v", td.alterClauses[2])
		}
		if acc, ok := td.alterClauses[3].(AddCheck); !ok || acc.Check != tableChecks2.Checks[1] || !acc.reorderOnly || acc.renameOnly {
			t.Errorf("Found unexpected alterClause %+v", td.alterClauses[3])
		}
		for n, alterClause := range td.alterClauses {
			expectBlank := n > 1
			actualBlank := alterClause.Clause(mods) == ""
			if expectBlank != actualBlank {
				t.Errorf("Unexpected result from Clause() at n=%d", n)
			}
		}
	}

	// Test alteration of check enforcement
	tableChecks2.Checks = []*Check{
		{Name: "alivecheck", Clause: "alive != 0", Enforced: true},
		{Name: "stringythings", Clause: "ssn <> '000000000'", Enforced: false},
	}
	tableChecks2.CreateStatement = tableChecks2.GeneratedCreateStatement(flavor)
	td = NewAlterTable(&tableChecks, &tableChecks2)
	if len(td.alterClauses) != 1 {
		t.Errorf("Expected 1 alterClause, instead found %d", len(td.alterClauses))
	} else {
		str := td.alterClauses[0].Clause(mods)
		if _, ok := td.alterClauses[0].(AlterCheck); !ok || !strings.Contains(str, "ALTER CHECK") {
			t.Errorf("Found unexpected type %T", td.alterClauses[0])
		}
	}

	// Create a table with 5 checks. Reorder one of them and confirm result.
	tableChecks, tableChecks2 = aTableForFlavor(maria105, 1), aTableForFlavor(maria105, 1)
	tableChecks.Checks = []*Check{
		{Name: "check1", Clause: "ssn <> '111111111'", Enforced: true},
		{Name: "check2", Clause: "ssn <> '222222222'", Enforced: true},
		{Name: "check3", Clause: "ssn <> '333333333'", Enforced: true},
		{Name: "check4", Clause: "ssn <> '444444444'", Enforced: true},
		{Name: "check5", Clause: "ssn <> '555555555'", Enforced: true},
	}
	tableChecks.CreateStatement = tableChecks.GeneratedCreateStatement(maria105)
	tableChecks2.Checks = []*Check{
		{Name: "check1", Clause: "ssn <> '111111111'", Enforced: true},
		{Name: "check2", Clause: "ssn <> '222222222'", Enforced: true},
		{Name: "check4", Clause: "ssn <> '444444444'", Enforced: true},
		{Name: "check3", Clause: "ssn <> '333333333'", Enforced: true},
		{Name: "check5", Clause: "ssn <> '555555555'", Enforced: true},
	}
	tableChecks2.CreateStatement = tableChecks2.GeneratedCreateStatement(maria105)
	td = NewAlterTable(&tableChecks, &tableChecks2)
	if len(td.alterClauses) != 4 {
		t.Errorf("Expected 4 alterClauses, instead found %d", len(td.alterClauses))
	}
	for n, alterClause := range td.alterClauses {
		str, strStrict := alterClause.Clause(mods), alterClause.Clause(strictMariaMods)
		if str != "" || strStrict == "" {
			t.Errorf("Clauses don't match expectations: %q / %q", str, strStrict)
		}
		if n%2 == 0 {
			if _, ok := alterClause.(DropCheck); !ok {
				t.Errorf("Unexpected type at clause[%d]: %T", n, alterClause)
			}
		} else {
			if _, ok := alterClause.(AddCheck); !ok {
				t.Errorf("Unexpected type at clause[%d]: %T", n, alterClause)
			}
		}
	}

	// Confirm an edge case around behavior of Clause when being renamed AND
	// reordered: output shouldn't ever be blank with strict mods, regardless of
	// MySQL vs MariaDB. The conditionals in Clause should be organized in a way
	// that properly handles this.
	add := AddCheck{
		Check:       tableChecks.Checks[0],
		reorderOnly: true,
		renameOnly:  true,
	}
	drop := DropCheck{
		Check:       tableChecks.Checks[0],
		reorderOnly: true,
		renameOnly:  true,
	}
	for _, ac := range []TableAlterClause{add, drop} {
		if clause := ac.Clause(mods); clause != "" {
			t.Errorf("Expected blank clause with non-strict mods, instead found %q", clause)
		}
		if clause := ac.Clause(strictMySQLMods); clause == "" {
			t.Error("Expected non-blank clause with strict MySQL mods, but it was blank")
		}
		if clause := ac.Clause(strictMariaMods); clause == "" {
			t.Error("Expected non-blank clause with strict MariaDB mods, but it was blank")
		}
	}
}

// TestAlterCheckConstraints provides integration test coverage relating to
// diffs of check constraints. It is similar to the above function, but actually
// executes the generated ALTERs to confirm validity.
func (s TengoIntegrationSuite) TestAlterCheckConstraints(t *testing.T) {
	flavor := s.d.Flavor()
	if !flavor.HasCheckConstraints() {
		t.Skipf("Check constraints not supported in flavor %s", flavor)
	}

	db, err := s.d.ConnectionPool("testing", "")
	if err != nil {
		t.Fatalf("Unable to establish connection pool: %v", err)
	}
	execAlter := func(td *TableDiff) {
		t.Helper()
		if td == nil {
			t.Fatal("diff was unexpectedly nil")
		} else if td.Type != DiffTypeAlter {
			t.Fatalf("Expected Type to be DiffTypeAlter, instead found %s", td.Type)
		}
		stmt, err := td.Statement(StatementModifiers{Flavor: flavor})
		if err != nil {
			t.Fatalf("Unexpected error from Statement: %v", err)
		} else if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("Unexpected error executing statement %q: %v", stmt, err)
		}
	}
	getTableCopy := func(tableName string) *Table {
		t.Helper()
		// Re-introspect the schema so that we can get a pointer to a new
		// Table value, vs normal getTable() which will keep returning a pointer
		// to the same Table value
		schema := s.GetSchema(t, "testing")
		return getTable(t, schema, tableName)
	}

	// Test addition of checks
	tableNoChecks := getTableCopy("grab_bag")
	tableChecks := getTableCopy("grab_bag")
	tableChecks.Checks = []*Check{
		{Name: "alivecheck", Clause: "alive != 0", Enforced: true},
		{Name: "stringythings", Clause: "code != 'ABCD1234' AND owner_id != 123", Enforced: true},
	}
	tableChecks.CreateStatement = tableChecks.GeneratedCreateStatement(flavor)
	td := NewAlterTable(tableNoChecks, tableChecks)
	execAlter(td)
	tableChecks = getTableCopy("grab_bag")
	if tableChecks.UnsupportedDDL {
		t.Fatal("Table is unexpectedly unsupported for diffs now")
	}

	// Confirm that modifying a check's clause = drop and re-add, but changing name
	// doesn't emit anything with non-strict mods
	tableChecks2 := getTableCopy("grab_bag")
	tableChecks2.Checks[0].Clause = strings.ReplaceAll(tableChecks.Checks[0].Clause, "0", "9")
	tableChecks2.Checks[1].Name = "stringycheck"
	tableChecks2.CreateStatement = tableChecks2.GeneratedCreateStatement(flavor)
	td = NewAlterTable(tableChecks, tableChecks2)
	if len(td.alterClauses) != 4 {
		t.Errorf("Expected 4 alterClauses, instead found %d", len(td.alterClauses))
	}
	execAlter(td)
	tableChecks = getTableCopy("grab_bag")
	if tableChecks.UnsupportedDDL {
		t.Fatal("Table is unexpectedly unsupported for diffs now")
	}
	if len(tableChecks.Checks) != 2 {
		t.Errorf("Expected 2 check constraints, instead found %d", len(tableChecks.Checks))
	}
	// Now run an additional diff, which should still have 2 alter clauses relating
	// to the name difference: both clauses should be blank with non-strict mods;
	// but with strict mods, running them should actually update the name
	td = NewAlterTable(tableChecks, tableChecks2)
	if len(td.alterClauses) != 2 {
		t.Errorf("Expected 2 alterClauses, instead found %d", len(td.alterClauses))
	}
	if stmt, err := td.Statement(StatementModifiers{Flavor: flavor}); stmt != "" || err != nil {
		t.Errorf("Unexpected return from Statement: %q / %v", stmt, err)
	}
	if stmt, err := td.Statement(StatementModifiers{Flavor: flavor, StrictCheckConstraints: true}); stmt == "" || err != nil {
		t.Errorf("Unexpected return from Statement: %q / %v", stmt, err)
	} else if _, err := db.Exec(stmt); err != nil {
		t.Fatalf("Unexpected error executing statement %q: %v", stmt, err)
	} else {
		tableChecks = getTableCopy("grab_bag")
		td = NewAlterTable(tableChecks, tableChecks2)
		if td != nil && len(td.alterClauses) > 0 {
			t.Errorf("Expected 0 alterClauses, instead found %d", len(td.alterClauses))
		}
	}

	// Confirm functionality related to MySQL's ALTER CHECK clause and the NOT
	// ENFORCED modifier
	if flavor.Vendor != VendorMariaDB {
		tableChecks2 = getTableCopy("grab_bag")
		tableChecks2.Checks[1].Enforced = false
		tableChecks2.CreateStatement = tableChecks2.GeneratedCreateStatement(flavor)
		td = NewAlterTable(tableChecks, tableChecks2)
		if len(td.alterClauses) != 1 {
			t.Errorf("Expected 1 alterClause, instead found %d", len(td.alterClauses))
		}
		execAlter(td)
		tableChecks2 = getTableCopy("grab_bag")
		if tableChecks2.UnsupportedDDL {
			t.Fatal("Table is unexpectedly unsupported for diffs now")
		}
		if tableChecks2.Checks[0].Enforced == tableChecks2.Checks[1].Enforced {
			t.Error("Altering enforcement of check did not work as expected")
		}

		// Now do the reverse: set the check back to enforced
		td = NewAlterTable(tableChecks2, tableChecks)
		execAlter(td)
		tableChecks = getTableCopy("grab_bag")
		if tableChecks.UnsupportedDDL {
			t.Fatal("Table is unexpectedly unsupported for diffs now")
		}
		if !tableChecks.Checks[0].Enforced || !tableChecks.Checks[1].Enforced {
			t.Error("Altering enforcement of check did not work as expected")
		}
	}
}
