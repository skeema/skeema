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
		if diffs := sd.ObjectDiffs(); len(diffs) != 0 {
			t.Errorf("Expected no object diffs, instead found %d", len(diffs))
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

	s1r1 := aProc("latin1_swedish_ci", "")
	s2r1 := aProc("latin1_swedish_ci", "")
	s1.Routines = append(s1.Routines, &s1r1)
	s2.Routines = append(s2.Routines, &s2r1)

	assertEmptyDiff(&s1, &s2)
	assertEmptyDiff(&s2, &s1)
	assertEmptyDiff(nil, nil)

	var dd *DatabaseDiff
	if dd.DiffType() != DiffTypeNone {
		t.Errorf("expected nil DatabaseDiff to be DiffTypeNone; instead found %s", dd.DiffType())
	}
	if dd.ObjectKey().Name != "" {
		t.Errorf("Unexpected object name: %s", dd.ObjectKey().Name)
	}
}

func TestSchemaDiffDatabaseDiff(t *testing.T) {
	assertDiffSchemaDDL := func(a, b *Schema, expectedSchemaDDL string) {
		sd := NewSchemaDiff(a, b)
		dd := sd.DatabaseDiff()
		schemaDDL, _ := dd.Statement(StatementModifiers{})
		if schemaDDL != expectedSchemaDDL {
			t.Errorf("For a=%s/%s and b=%s/%s, expected SchemaDDL=\"%s\", instead found \"%s\"", a.CharSet, a.Collation, b.CharSet, b.Collation, expectedSchemaDDL, schemaDDL)
		}
		if expectedSchemaDDL != "" {
			var expectKey ObjectKey
			if a != nil {
				expectKey = ObjectKey{Name: a.Name, Type: ObjectTypeDatabase}
			} else {
				expectKey = ObjectKey{Name: b.Name, Type: ObjectTypeDatabase}
			}
			if actualKey := sd.ObjectDiffs()[0].ObjectKey(); actualKey != expectKey {
				t.Errorf("Unexpected object key for diff[0]: %s", actualKey)
			}
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
	if stmt, err := td2.Statement(StatementModifiers{AllowUnsafe: false}); !IsForbiddenDiff(err) {
		t.Errorf("Modifier AllowUnsafe=false not working; expected forbidden diff error for %s, instead err=%v", stmt, err)
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
		Default:  ColumnDefaultNull,
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
			Columns:               s2t1.Columns[0:1],
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
		Columns:               s2t2.Columns[0:1],
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

	// Renaming an FK but also changing its definition: never blank statement
	s2t2.ForeignKeys[1].Columns = s2t2.ForeignKeys[1].Columns[0:1]
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

func TestSchemaDiffRoutines(t *testing.T) {
	s1 := aSchema("s1")
	s2 := aSchema("s2")
	s1r1 := aFunc("latin1_swedish_ci", "")
	s2r1 := aFunc("latin1_swedish_ci", "")
	s2r2 := aProc("latin1_swedish_ci", "")
	s1.Routines = append(s1.Routines, &s1r1)
	s2.Routines = append(s2.Routines, &s2r1, &s2r2)

	// Test create
	sd := NewSchemaDiff(&s1, &s2)
	if len(sd.RoutineDiffs) != 1 {
		t.Fatalf("Incorrect number of routine diffs: expected 1, found %d", len(sd.RoutineDiffs))
	}
	rd := sd.RoutineDiffs[0]
	if rd.DiffType() != DiffTypeCreate {
		t.Fatalf("Incorrect type of diff returned: expected %s, found %s", DiffTypeCreate, rd.DiffType())
	}
	if stmt, err := rd.Statement(StatementModifiers{}); err != nil || !strings.HasPrefix(stmt, "CREATE") {
		t.Errorf("Unexpected return value from Statement(): %s / %s", stmt, err)
	}
	expectKey := ObjectKey{Type: ObjectTypeProc, Name: s2r2.Name}
	if rd.To != &s2r2 || rd.ObjectKey() != expectKey {
		t.Error("Pointer in diff does not point to expected value")
	}

	// Test drop (opposite diff direction of above)
	sd = NewSchemaDiff(&s2, &s1)
	if len(sd.RoutineDiffs) != 1 {
		t.Fatalf("Incorrect number of routine diffs: expected 1, found %d", len(sd.RoutineDiffs))
	}
	rd = sd.RoutineDiffs[0]
	if rd.DiffType() != DiffTypeDrop {
		t.Fatalf("Incorrect type of diff returned: expected %s, found %s", DiffTypeDrop, rd.DiffType())
	}
	if rd.From != &s2r2 || rd.ObjectKey() != expectKey {
		t.Error("Pointer in diff does not point to expected value")
	}
	if sd.String() != fmt.Sprintf("DROP PROCEDURE %s;\n", EscapeIdentifier(s2r2.Name)) {
		t.Errorf("SchemaDiff.String returned unexpected result: %s", sd)
	}

	// Test impact of statement modifiers (allowing/forbidding drop) on previous drop
	if stmt, err := rd.Statement(StatementModifiers{AllowUnsafe: false}); stmt == "" || !IsForbiddenDiff(err) {
		t.Errorf("Modifier AllowUnsafe=false not working; expected forbidden diff error for %s, instead err=%v", stmt, err)
	}
	if stmt, err := rd.Statement(StatementModifiers{AllowUnsafe: true}); stmt == "" || err != nil {
		t.Errorf("Modifier AllowUnsafe=true not working; error (%s) returned for %s", err, stmt)
	}

	// Test alter, which currently always is handled by a drop and re-add.
	// Since this is a creation-time metadata change, also test statement modifier
	// affecting whether or not those changes are suppressed.
	s1r2 := aProc("utf8mb4_general_ci", "")
	s1.Routines = append(s1.Routines, &s1r2)
	sd = NewSchemaDiff(&s2, &s1)
	if len(sd.RoutineDiffs) != 2 {
		t.Fatalf("Incorrect number of routine diffs: expected 2, found %d", len(sd.RoutineDiffs))
	}
	rd = sd.RoutineDiffs[0]
	if rd.DiffType() != DiffTypeDrop {
		t.Fatalf("Incorrect type of diff returned: expected %s, found %s", DiffTypeDrop, rd.DiffType())
	}
	if rd.From != &s2r2 || rd.ObjectKey().Name != s2r2.Name {
		t.Error("Pointer in diff does not point to expected value")
	}
	rd = sd.RoutineDiffs[1]
	if rd.DiffType() != DiffTypeCreate {
		t.Fatalf("Incorrect type of diff returned: expected %s, found %s", DiffTypeCreate, rd.DiffType())
	}
	if rd.To != &s1r2 || rd.ObjectKey().Name != s1r2.Name {
		t.Error("Pointer in diff does not point to expected value")
	}
	mods := StatementModifiers{AllowUnsafe: true}
	for _, od := range sd.ObjectDiffs() {
		stmt, err := od.Statement(mods)
		if stmt != "" || err != nil {
			t.Errorf("Unexpected return from Statement: %s / %v", stmt, err)
		}
	}
	mods.CompareMetadata = true
	for n, od := range sd.ObjectDiffs() {
		stmt, err := od.Statement(mods)
		if stmt == "" || err != nil || (n == 0 && !strings.HasPrefix(stmt, "# ")) {
			t.Errorf("Unexpected return from Statement: %s / %v", stmt, err)
		}
	}

	// Confirm that procs and funcs with same name are handled properly
	s1r2 = aProc("latin1_swedish_ci", "")
	s1.Routines = []*Routine{&s1r2}
	s2r1.Name = s2r2.Name
	sd = NewSchemaDiff(&s1, &s2)
	if len(sd.RoutineDiffs) != 1 {
		t.Fatalf("Incorrect number of routine diffs: expected 1, found %d", len(sd.RoutineDiffs))
	}
	rd = sd.RoutineDiffs[0]
	if rd.DiffType() != DiffTypeCreate {
		t.Fatalf("Incorrect type of diff returned: expected %s, found %s", DiffTypeCreate, rd.DiffType())
	}
	if rd.To != &s2r1 || rd.ObjectKey().Type != ObjectTypeFunc {
		t.Error("Pointer in diff does not point to expected value")
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

	assertFiltered := func(sd *SchemaDiff, expectLen int, types ...DiffType) {
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
	assertFiltered(sd, 1, DiffTypeCreate)
	assertFiltered(sd, 1, DiffTypeAlter)
	assertFiltered(sd, 0, DiffTypeDrop)
	assertFiltered(sd, 1, DiffTypeCreate, DiffTypeDrop)
	assertFiltered(sd, 2, DiffTypeCreate, DiffTypeAlter)

	sd = NewSchemaDiff(&s2, &s1)
	assertFiltered(sd, 0, DiffTypeCreate)
	assertFiltered(sd, 1, DiffTypeAlter)
	assertFiltered(sd, 1, DiffTypeDrop)
	assertFiltered(sd, 1, DiffTypeCreate, DiffTypeDrop)
	assertFiltered(sd, 2, DiffTypeDrop, DiffTypeAlter)
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
		expected := `--- Expected CREATE
+++ MySQL-actual SHOW CREATE
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
		Default:  ColumnDefaultNull,
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
		Name:               "full_name",
		TypeInDB:           "varchar(100)",
		Default:            ColumnDefaultNull,
		Nullable:           true,
		CharSet:            "utf8",
		Collation:          "utf8_general_ci",
		CollationIsDefault: true,
		GenerationExpr:     "CONCAT(first_name, ' ', last_name)",
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

func TestIgnoreTableMod(t *testing.T) {
	from := anotherTable()
	to := anotherTable()
	col := &Column{
		Name:     "something",
		TypeInDB: "smallint(5) unsigned",
		Default:  ColumnDefaultNull,
	}
	to.Columns = append(to.Columns, col)
	to.CreateStatement = to.GeneratedCreateStatement(FlavorUnknown)
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

func TestNilObjectDiff(t *testing.T) {
	var td *TableDiff
	expectKey := ObjectKey{Type: ObjectTypeTable}
	if td.ObjectKey() != expectKey {
		t.Errorf("Unexpected object key: %s", td.ObjectKey())
	}
	if td.DiffType() != DiffTypeNone || td.DiffType().String() != "" {
		t.Errorf("Unexpected diff type: %s", td.DiffType())
	}
	if stmt, err := td.Statement(StatementModifiers{}); stmt != "" || err != nil {
		t.Errorf("Unexpected return from Statement: %s / %v", stmt, err)
	}

	var rd *RoutineDiff
	expectKey = ObjectKey{}
	if rd.ObjectKey() != expectKey {
		t.Errorf("Unexpected object key: %s", rd.ObjectKey())
	}
	if rd.DiffType() != DiffTypeNone {
		t.Errorf("Unexpected diff type: %s", rd.DiffType())
	}
	if stmt, err := rd.Statement(StatementModifiers{}); stmt != "" || err != nil {
		t.Errorf("Unexpected return from Statement: %s / %v", stmt, err)
	}
}
