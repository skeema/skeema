package tengo

import (
	"fmt"
	"strings"
	"testing"
)

func (s TengoIntegrationSuite) TestInstanceRoutineIntrospection(t *testing.T) {
	schema := s.GetSchema(t, "testing")
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

	db, err := s.d.CachedConnectionPool("", "")
	if err != nil {
		t.Fatalf("Unexpected error from CachedConnectionPool: %v", err)
	}

	// If this flavor supports using mysql.proc to bulk-fetch routines, confirm
	// the result is identical to using the individual SHOW CREATE queries
	if !s.d.Flavor().MinMySQL(8) {
		insp := &introspector{
			instance: s.d.Instance,
			db:       db,
			schema:   schema,
		}
		for _, r := range schema.Routines {
			rCopy := *r
			if err := r.introspectShowCreate(t.Context(), insp); err != nil {
				t.Fatalf("Unexpected error from introspectShowCreate: %v", err)
			} else if !rCopy.Equals(r) {
				t.Errorf("Unexpected mutation to %s after introspectShowCreate:\nOriginal: %+v\nAfter: %+v", r.ObjectKey(), rCopy, *r)
			}
		}
	}

	// Coverage for MariaDB 10.8 IN/OUT/INOUT params in funcs
	if fl := s.d.Flavor(); fl.MinMariaDB(10, 8) {
		s.d.SourceSQL(t, "testdata/maria108.sql")
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
	if _, err = showCreateRoutine(t.Context(), db, "testing", ObjectTypeFunc, actualProc1.Name); err == nil {
		t.Error("Expected error return from showCreateRoutine, but err was nil")
	}
	if _, err = showCreateRoutine(t.Context(), db, "testing", ObjectTypeProc, actualFunc1.Name); err == nil {
		t.Error("Expected error return from showCreateRoutine, but err was nil")
	}
	if _, err = showCreateRoutine(t.Context(), db, "testing", ObjectTypeTable, actualFunc1.Name); err == nil {
		t.Error("Expected error return from showCreateRoutine, but err was nil")
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
	if stmt, err := rd.Statement(StatementModifiers{AllowUnsafe: false}); stmt == "" || !IsUnsafeDiff(err) {
		t.Errorf("Modifier AllowUnsafe=false not working; expected unsafe diff error for %s, instead err=%v", stmt, err)
	}
	if stmt, err := rd.Statement(StatementModifiers{AllowUnsafe: true}); stmt == "" || err != nil {
		t.Errorf("Modifier AllowUnsafe=true not working; error (%s) returned for %s", err, stmt)
	}

	// Test modification of a characteristic field, which is handled by ALTER and
	// is always safe.
	s1r2 := aProc("latin1_swedish_ci", "")
	s1r2.SecurityType = "DEFINER"
	s1r2.Comment = "a comment"
	s1r2.CreateStatement = s1r2.Definition(FlavorUnknown)
	s1.Routines = append(s1.Routines, &s1r2)
	sd = NewSchemaDiff(&s2, &s1)
	if len(sd.RoutineDiffs) != 1 {
		t.Fatalf("Incorrect number of routine diffs: expected 1, found %d", len(sd.RoutineDiffs))
	}
	rd = sd.RoutineDiffs[0]
	if rd.DiffType() != DiffTypeAlter {
		t.Fatalf("Incorrect type of diff returned: expected %s, found %s", DiffTypeAlter, rd.DiffType())
	}
	mods := StatementModifiers{Flavor: ParseFlavor("mysql:5.7")}
	if stmt, err := rd.Statement(mods); err != nil || stmt != "ALTER PROCEDURE `"+s1r2.Name+"` SQL SECURITY DEFINER COMMENT 'a comment'" {
		t.Errorf("Unexpected return from Statement: %s, %v", stmt, err)
	}

	// Test modification of a non-characteristic field, which is handled by a drop
	// and re-add in MySQL/Percona, and OR REPLACE in MariaDB.
	// Since this is a creation-time metadata change, also test statement modifier
	// affecting whether or not those changes are suppressed.
	s1r2 = aProc("utf8mb4_general_ci", "")
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
	mods = StatementModifiers{Flavor: ParseFlavor("mysql:5.7")}
	for _, od := range sd.ObjectDiffs() {
		stmt, err := od.Statement(mods)
		if stmt != "" || err != nil {
			t.Errorf("Unexpected return from Statement: %s / %v", stmt, err)
		}
	}
	mods.CompareMetadata = true
	for n, od := range sd.ObjectDiffs() {
		if stmt, err := od.Statement(mods); (err == nil && n == 0) || (err != nil && n == 1) { // expectation: first statement (DROP) is unsafe in MySQL, second (CREATE) is not
			t.Errorf("Unexpected return from Statement: %s / %v", stmt, err)
		}
	}
	mods.AllowUnsafe = true
	for n, od := range sd.ObjectDiffs() {
		stmt, err := od.Statement(mods)
		if stmt == "" || err != nil || (n == 0 && !strings.HasPrefix(stmt, "# ")) {
			t.Errorf("Unexpected return from Statement: %s / %v", stmt, err)
		}
	}
	mods.Flavor = ParseFlavor("mariadb:10.1")
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

	// Confirm that adjusting the param string is considered unsafe, even in mariadb
	s1r2.ParamString = "\n    IN iterations int(10) unsigned\n"
	sd = NewSchemaDiff(&s2, &s1)
	if len(sd.RoutineDiffs) != 2 {
		t.Fatalf("Incorrect number of routine diffs: expected 2, found %d", len(sd.RoutineDiffs))
	}
	if stmt, err := sd.RoutineDiffs[0].Statement(mods); stmt != "" || err != nil {
		t.Errorf("Unexpected return from diff[0].Statement: %s / %v", stmt, err)
	}
	if stmt, err := sd.RoutineDiffs[1].Statement(mods); err == nil {
		t.Errorf("Unexpected return from diff[1].Statement: %s / %v", stmt, err)
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

func aProc(dbCollation, sqlMode string) Routine {
	r := Routine{
		Name: "proc1",
		Type: ObjectTypeProc,
		Body: `BEGIN
  SELECT @iterations + 1, 98.76 INTO iterations, pct;
  END`,
		ParamString:       "\n    IN name varchar(30) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin,\n    INOUT iterations int(10) unsigned,   OUT pct decimal(5, 2)\n",
		ReturnDataType:    "",
		Definer:           "root@%",
		DatabaseCollation: dbCollation,
		Comment:           "",
		Deterministic:     false,
		SQLDataAccess:     "READS SQL DATA",
		SecurityType:      "INVOKER",
		SQLMode:           sqlMode,
	}
	r.CreateStatement = r.Definition(FlavorUnknown)
	return r
}

func aFunc(dbCollation, sqlMode string) Routine {
	r := Routine{
		Name:              "func1",
		Type:              ObjectTypeFunc,
		Body:              "return mult * 2.0",
		ParamString:       "mult float(10,2)",
		ReturnDataType:    "float",
		Definer:           "root@%",
		DatabaseCollation: dbCollation,
		Comment:           "hello world",
		Deterministic:     true,
		SQLDataAccess:     "NO SQL",
		SecurityType:      "DEFINER",
		SQLMode:           sqlMode,
	}
	r.CreateStatement = r.Definition(FlavorUnknown)
	return r
}
