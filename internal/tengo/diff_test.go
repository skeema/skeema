package tengo

import (
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

func TestNilObjectDiff(t *testing.T) {
	var td *TableDiff
	expectKey := ObjectKey{}
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
