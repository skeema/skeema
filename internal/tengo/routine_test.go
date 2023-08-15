package tengo

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
)

func (s TengoIntegrationSuite) TestInstanceRoutineIntrospection(t *testing.T) {
	schema := s.GetSchema(t, "testing")
	db, err := s.d.CachedConnectionPool("testing", "")
	if err != nil {
		t.Fatalf("Unexpected error from Connect: %s", err)
	}
	sqlMode := s.d.SQLMode()

	procsByName := schema.ProceduresByName()
	actualProc1 := procsByName["proc1"]
	if actualProc1 == nil || len(procsByName) != 1 {
		t.Fatal("Unexpected result from ProceduresByName()")
	}
	expectProc1 := aProc(schema.Collation, sqlMode)
	if !expectProc1.Equals(actualProc1) {
		t.Errorf("Actual proc did not equal expected.\nACTUAL: %+v\nEXPECTED: %+v\n", actualProc1, &expectProc1)
	}

	funcsByName := schema.FunctionsByName()
	actualFunc1 := funcsByName["func1"]
	if actualFunc1 == nil || len(funcsByName) != 2 {
		t.Fatal("Unexpected result from FunctionsByName()")
	}
	expectFunc1 := aFunc(schema.Collation, sqlMode)
	if !expectFunc1.Equals(actualFunc1) {
		t.Errorf("Actual func did not equal expected.\nACTUAL: %+v\nEXPECTED: %+v\n", actualFunc1, &expectFunc1)
	}
	if actualFunc1.Equals(actualProc1) {
		t.Error("Equals not behaving as expected, proc1 and func1 should not be equal")
	}

	// confirm 4-byte characters in the body come through properly
	if func2 := funcsByName["func2"]; !strings.Contains(func2.Body, "\U0001F4A9") {
		t.Errorf("Expected to find 4-byte char \U0001F4A9 in func2.Body, but did not. Body contents:\n%s", func2.Body)
	}

	// If this flavor supports using mysql.proc to bulk-fetch routines, confirm
	// the result is identical to using the individual SHOW CREATE queries
	if !s.d.Flavor().Min(FlavorMySQL80) {
		db, err := s.d.ConnectionPool("testing", "")
		if err != nil {
			t.Fatalf("Unexpected error from ConnectionPool: %v", err)
		}
		fastResults, err := querySchemaRoutines(context.Background(), db, "testing", s.d.Flavor())
		if err != nil {
			t.Fatalf("Unexpected error from querySchemaRoutines: %v", err)
		}
		oldFlavor := s.d.Flavor()
		s.d.ForceFlavor(FlavorMySQL80)
		slowResults, err := querySchemaRoutines(context.Background(), db, "testing", s.d.Flavor())
		s.d.ForceFlavor(oldFlavor)
		if err != nil {
			t.Fatalf("Unexpected error from querySchemaRoutines: %v", err)
		}
		for n, r := range fastResults {
			if !r.Equals(slowResults[n]) {
				t.Errorf("Routine[%d] mismatch\nFast path value: %+v\nSlow path value: %+v\n", n, r, slowResults[n])
			}
		}
	}

	// Coverage for MariaDB 10.8 IN/OUT/INOUT params in funcs
	if fl := s.d.Flavor(); fl.Min(FlavorMariaDB108) {
		s.SourceTestSQL(t, "maria108.sql")
		schema := s.GetSchema(t, "testing")
		funcsByName := schema.FunctionsByName()
		f := funcsByName["maria108func"]
		if defn := f.Definition(fl); defn != f.CreateStatement {
			t.Errorf("Generated function definition does not match SHOW CREATE FUNCTION. Generated definition:\n%s\nSHOW CREATE FUNCTION:\n%s", defn, f.CreateStatement)
		} else if !strings.Contains(defn, "INOUT") {
			t.Errorf("Functions with IN/OUT/INOUT params not introspected properly. Generated definition:\n%s", defn)
		}
	}

	// Coverage for various nil cases and error conditions
	schema = nil
	if procCount := len(schema.ProceduresByName()); procCount != 0 {
		t.Errorf("nil schema unexpectedly contains %d procedures by name", procCount)
	}
	var r *Routine
	if actualFunc1.Equals(r) || !r.Equals(r) {
		t.Error("Equals not behaving as expected")
	}
	if _, err = showCreateRoutine(context.Background(), db, actualProc1.Name, ObjectTypeFunc); err != sql.ErrNoRows {
		t.Errorf("Unexpected error return from showCreateRoutine: expected sql.ErrNoRows, found %s", err)
	}
	if _, err = showCreateRoutine(context.Background(), db, actualFunc1.Name, ObjectTypeProc); err != sql.ErrNoRows {
		t.Errorf("Unexpected error return from showCreateRoutine: expected sql.ErrNoRows, found %s", err)
	}
	if _, err = showCreateRoutine(context.Background(), db, actualFunc1.Name, ObjectTypeTable); err == nil {
		t.Error("Expected non-nil error return from showCreateRoutine with invalid type, instead found nil")
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
	if !rd.IsCompoundStatement() { // s2r2 is a proc with a BEGIN block, so expect this to return true
		t.Error("Unexpected return value from IsCompoundStatement(): found false, expected true")
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
	if rd.IsCompoundStatement() { // DROPs should never be compound statements
		t.Error("Unexpected return value from IsCompoundStatement(): found true, expected false")
	}

	// Test impact of statement modifiers (allowing/forbidding drop) on previous drop
	if stmt, err := rd.Statement(StatementModifiers{AllowUnsafe: false}); stmt == "" || !IsForbiddenDiff(err) {
		t.Errorf("Modifier AllowUnsafe=false not working; expected forbidden diff error for %s, instead err=%v", stmt, err)
	}
	if stmt, err := rd.Statement(StatementModifiers{AllowUnsafe: true}); stmt == "" || err != nil {
		t.Errorf("Modifier AllowUnsafe=true not working; error (%s) returned for %s", err, stmt)
	}

	// Test alter, which is handled by a drop and re-add in MySQL/Percona, and
	// OR REPLACE in MariaDB.
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
	mods := StatementModifiers{Flavor: FlavorMySQL57}
	for _, od := range sd.ObjectDiffs() {
		stmt, err := od.Statement(mods)
		if stmt != "" || err != nil {
			t.Errorf("Unexpected return from Statement: %s / %v", stmt, err)
		}
	}
	mods.AllowUnsafe = true
	mods.CompareMetadata = true
	for n, od := range sd.ObjectDiffs() {
		stmt, err := od.Statement(mods)
		if stmt == "" || err != nil || (n == 0 && !strings.HasPrefix(stmt, "# ")) {
			t.Errorf("Unexpected return from Statement: %s / %v", stmt, err)
		}
	}
	mods.Flavor = FlavorMariaDB101
	mods.AllowUnsafe = false
	for n, od := range sd.ObjectDiffs() {
		stmt, err := od.Statement(mods)
		if err != nil {
			t.Errorf("Unexpected error from Statement[%d]: %v", n, err)
			continue
		}
		if n == 0 && stmt != "" {
			t.Errorf("Expected blank statement from Statement[0], instead found %q", stmt)
		}
		if n == 1 && (!strings.HasPrefix(stmt, "# ") || !strings.Contains(stmt, "CREATE OR REPLACE")) {
			t.Errorf("Unexpected statement from Statement[1]: %q", stmt)
		}
	}

	// Confirm that procs and funcs with same name are handled properly, including
	// with respect to ignore patterns
	s1r2 = aProc("latin1_swedish_ci", "")
	s1.Routines = []*Routine{&s1r2} // s1 has one proc named "proc1" and no funcs
	s2r1.Name = s2r2.Name           // s2 has one proc named "proc1" and one func also named "proc1"
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
	if rd.IsCompoundStatement() { // the function body in aFunc() is just a single RETURN
		t.Error("Unexpected return value from IsCompoundStatement(): found true, expected false")
	}
}
