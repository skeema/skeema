package tengo

import (
	"context"
	"database/sql"
	"strings"
	"testing"
)

func (s TengoIntegrationSuite) TestInstanceSchemaIntrospection(t *testing.T) {
	// Ensure our unit test fixtures and integration test fixtures match
	flavor := s.d.Flavor()
	schema, aTableFromDB := s.GetSchemaAndTable(t, "testing", "actor")
	aTableFromUnit := aTableForFlavor(flavor, 1)
	aTableFromUnit.CreateStatement = "" // Prevent diff from short-circuiting on equivalent CREATEs
	clauses, supported := aTableFromDB.Diff(&aTableFromUnit)
	if !supported {
		t.Error("Diff unexpectedly not supported for testing.actor")
	} else if len(clauses) > 0 {
		t.Errorf("Diff of testing.actor unexpectedly found %d clauses; expected 0. Clauses: %+v", len(clauses), clauses)
	}

	aTableFromDB = s.GetTable(t, "testing", "actor_in_film")
	aTableFromUnit = anotherTableForFlavor(flavor)
	aTableFromUnit.CreateStatement = "" // Prevent diff from short-circuiting on equivalent CREATEs
	clauses, supported = aTableFromDB.Diff(&aTableFromUnit)
	if !supported {
		t.Error("Diff unexpectedly not supported for testing.actor_in_film")
	} else if len(clauses) > 0 {
		t.Errorf("Diff of testing.actor_in_film unexpectedly found %d clauses; expected 0", len(clauses))
	}

	// ensure tables in testing schema are all supported (except where known not to be)
	for _, table := range schema.Tables {
		shouldBeUnsupported := (table.Name == unsupportedTable().Name)
		if table.UnsupportedDDL != shouldBeUnsupported {
			t.Errorf("Table %s: expected UnsupportedDDL==%v, instead found %v\nExpected SHOW CREATE TABLE:\n%s\nActual SHOW CREATE TABLE:\n%s", table.Name, shouldBeUnsupported, !shouldBeUnsupported, table.GeneratedCreateStatement(flavor), table.CreateStatement)
		}
	}

	// Test ObjectDefinitions map, which should contain objects of multiple types
	dict := schema.ObjectDefinitions()
	for key, create := range dict {
		var ok bool
		switch key.Type {
		case ObjectTypeTable:
			ok = strings.HasPrefix(create, "CREATE TABLE")
		case ObjectTypeProc, ObjectTypeFunc:
			ok = strings.HasPrefix(create, "CREATE DEFINER")
		}
		if !ok {
			t.Errorf("Unexpected or incorrect key %s found in schema object definitions --> %s", key, create)
		}
	}

	if dict[ObjectKey{Type: ObjectTypeFunc, Name: "func1"}] == "" || dict[ObjectKey{Type: ObjectTypeProc, Name: "func1"}] != "" {
		t.Error("ObjectDefinitions map not populated as expected")
	}

	// ensure character set handling works properly regardless of whether this
	// flavor has a data dictionary, which changed many SHOW CREATE TABLE behaviors
	schema = s.GetSchema(t, "testcharcoll")
	for _, table := range schema.Tables {
		if table.UnsupportedDDL {
			t.Errorf("Table %s unexpectedly not supported for diff.\nExpected SHOW CREATE TABLE:\n%s\nActual SHOW CREATE TABLE:\n%s", table.Name, table.GeneratedCreateStatement(flavor), table.CreateStatement)
		}
	}

	// Test various flavor-specific ordering fixes
	aTableFromDB = s.GetTable(t, "testing", "grab_bag")
	if aTableFromDB.UnsupportedDDL {
		t.Error("Cannot test various order-fixups because testing.grab_bag is unexpectedly not supported for diff")
	} else {
		// Test index order correction, even if no test image is using new data dict
		aTableFromDB.SecondaryIndexes[0], aTableFromDB.SecondaryIndexes[1], aTableFromDB.SecondaryIndexes[2] = aTableFromDB.SecondaryIndexes[2], aTableFromDB.SecondaryIndexes[0], aTableFromDB.SecondaryIndexes[1]
		fixIndexOrder(aTableFromDB)
		if aTableFromDB.GeneratedCreateStatement(flavor) != aTableFromDB.CreateStatement {
			t.Error("fixIndexOrder did not behave as expected")
		}

		// Test foreign key order correction, even if no test image lacks sorted FKs
		aTableFromDB.ForeignKeys[0], aTableFromDB.ForeignKeys[1], aTableFromDB.ForeignKeys[2] = aTableFromDB.ForeignKeys[2], aTableFromDB.ForeignKeys[0], aTableFromDB.ForeignKeys[1]
		fixForeignKeyOrder(aTableFromDB)
		if aTableFromDB.GeneratedCreateStatement(flavor) != aTableFromDB.CreateStatement {
			t.Error("fixForeignKeyOrder did not behave as expected")
		}

		// Test create option order correction, even if no test image is using new data dict
		aTableFromDB.CreateOptions = "ROW_FORMAT=COMPACT DELAY_KEY_WRITE=1 CHECKSUM=1"
		fixCreateOptionsOrder(aTableFromDB, flavor)
		if aTableFromDB.GeneratedCreateStatement(flavor) != aTableFromDB.CreateStatement {
			t.Error("fixCreateOptionsOrder did not behave as expected")
		}
	}

	// Test introspection of default expressions, if flavor supports them
	if flavor.VendorMinVersion(VendorMariaDB, 10, 2) || flavor.MySQLishMinVersion(8, 0, 13) {
		if _, err := s.d.SourceSQL("testdata/default-expr.sql"); err != nil {
			t.Fatalf("Unexpected error sourcing testdata/default-expr.sql: %v", err)
		}
		table := s.GetTable(t, "testing", "testdefaults")
		if table.UnsupportedDDL {
			t.Errorf("Use of default expression unexpectedly triggers UnsupportedDDL.\nExpected SHOW CREATE TABLE:\n%s\nActual SHOW CREATE TABLE:\n%s", table.GeneratedCreateStatement(flavor), table.CreateStatement)
		}
	}

	// Test introspection of generated columns, if flavor supports them
	if flavor.GeneratedColumns() {
		sqlfile := "testdata/generatedcols.sql"
		if flavor.Vendor == VendorMariaDB { // no support for NOT NULL generated cols
			sqlfile = "testdata/generatedcols-maria.sql"
		}
		if _, err := s.d.SourceSQL(sqlfile); err != nil {
			t.Fatalf("Unexpected error sourcing testdata/generatedcols.sql: %v", err)
		}
		table := s.GetTable(t, "testing", "staff")
		if table.UnsupportedDDL {
			t.Errorf("Expected table using generated columns to be supported for diff in flavor %s, but it was not.\nExpected SHOW CREATE TABLE:\n%s\nActual SHOW CREATE TABLE:\n%s", flavor, table.GeneratedCreateStatement(flavor), table.CreateStatement)
		}
		// Test generation expression fix, even if test image isn't MySQL 8
		for _, col := range table.Columns {
			if col.GenerationExpr != "" {
				col.GenerationExpr = "length(_latin1\\'fixme\\')"
			}
		}
		fixGenerationExpr(table, flavor)
		if table.GeneratedCreateStatement(flavor) != table.CreateStatement {
			t.Error("fixGenerationExpr did not behave as expected")
		}
	}

	// Test advanced index functionality in MySQL 8+
	if flavor.MySQLishMinVersion(8, 0) {
		if _, err := s.d.SourceSQL("testdata/index-mysql8.sql"); err != nil {
			t.Fatalf("Unexpected error sourcing testdata/index-mysql8.sql: %v", err)
		}
		table := s.GetTable(t, "testing", "my8idx")
		if table.UnsupportedDDL {
			t.Errorf("Expected table using advanced index functionality to be supported for diff in flavor %s, but it was not.\nExpected SHOW CREATE TABLE:\n%s\nActual SHOW CREATE TABLE:\n%s", flavor, table.GeneratedCreateStatement(flavor), table.CreateStatement)
		}
		idx := table.SecondaryIndexes[0]
		if !idx.Invisible {
			t.Errorf("Expected index %s to be invisible, but it was not", idx.Name)
		}
		if idx.Parts[0].Descending || !idx.Parts[1].Descending {
			t.Errorf("Unexpected index part collations found: [0].Descending=%t, [1].Descending=%t", idx.Parts[0].Descending, !idx.Parts[1].Descending)
		}
		if idx.Parts[0].Expression != "" || idx.Parts[1].Expression == "" {
			t.Errorf("Unexpected index part expressions found: [0].Expression=%q, [1].Expression=%q", idx.Parts[0].Expression, idx.Parts[1].Expression)
		}
	}

	// Test invisible column support in flavors supporting it
	if flavor.VendorMinVersion(VendorMariaDB, 10, 3) || flavor.MySQLishMinVersion(8, 0, 23) {
		if _, err := s.d.SourceSQL("testdata/inviscols.sql"); err != nil {
			t.Fatalf("Unexpected error sourcing testdata/inviscols.sql: %v", err)
		}
		table := s.GetTable(t, "testing", "invistest")
		if table.UnsupportedDDL {
			t.Errorf("Expected table using invisible columns to be supported for diff in flavor %s, but it was not.\nExpected SHOW CREATE TABLE:\n%s\nActual SHOW CREATE TABLE:\n%s", flavor, table.GeneratedCreateStatement(flavor), table.CreateStatement)
		}
		for n, col := range table.Columns {
			expectInvis := (n == 0 || n == 4 || n == 5)
			if col.Invisible != expectInvis {
				t.Errorf("Expected Columns[%d].Invisible == %t, instead found %t", n, expectInvis, !expectInvis)
			}
		}
	}

	// Include coverage for fulltext parsers if MySQL 5.7+. (Although these are
	// supported in other flavors too, no alternative parsers ship with them.)
	if flavor.MySQLishMinVersion(5, 7) {
		if _, err := s.d.SourceSQL("testdata/ft-parser.sql"); err != nil {
			t.Fatalf("Unexpected error sourcing testdata/ft-parser.sql: %v", err)
		}
		table := s.GetTable(t, "testing", "ftparser")
		if table.UnsupportedDDL {
			t.Errorf("Expected table using ngram fulltext parser to be supported for diff in flavor %s, but it was not.\nExpected SHOW CREATE TABLE:\n%s\nActual SHOW CREATE TABLE:\n%s", flavor, table.GeneratedCreateStatement(flavor), table.CreateStatement)
		}
		indexes := table.SecondaryIndexesByName()
		if idx := indexes["ftdesc"]; idx.FullTextParser != "ngram" || idx.Type != "FULLTEXT" {
			t.Errorf("Expected index %s to be FULLTEXT with ngram parser, instead found type=%s / parser=%s", idx.Name, idx.Type, idx.FullTextParser)
		}
		if idx := indexes["ftbody"]; idx.FullTextParser != "" || idx.Type != "FULLTEXT" {
			t.Errorf("Expected index %s to be FULLTEXT with no parser, instead found type=%s / parser=%s", idx.Name, idx.Type, idx.FullTextParser)
		}
		if idx := indexes["name"]; idx.FullTextParser != "" || idx.Type != "BTREE" {
			t.Errorf("Expected index %s to be BTREE with no parser, instead found type=%s / parser=%s", idx.Name, idx.Type, idx.FullTextParser)
		}
	}

	// Coverage for column compression
	if flavor.VendorMinVersion(VendorPercona, 5, 6, 33) {
		if _, err := s.d.SourceSQL("testdata/colcompression-percona.sql"); err != nil {
			t.Fatalf("Unexpected error sourcing testdata/colcompression-percona.sql: %v", err)
		}
		table := s.GetTable(t, "testing", "colcompr")
		if table.UnsupportedDDL {
			t.Errorf("Expected table using column compression to be supported for diff in flavor %s, but it was not.\nExpected SHOW CREATE TABLE:\n%s\nActual SHOW CREATE TABLE:\n%s", flavor, table.GeneratedCreateStatement(flavor), table.CreateStatement)
		}
		if table.Columns[1].Compression != "COMPRESSED" {
			t.Errorf("Unexpected value for compression column attribute: found %q", table.Columns[1].Compression)
		}
	} else if flavor.VendorMinVersion(VendorMariaDB, 10, 3) {
		if _, err := s.d.SourceSQL("testdata/colcompression-maria.sql"); err != nil {
			t.Fatalf("Unexpected error sourcing testdata/colcompression-maria.sql: %v", err)
		}
		table := s.GetTable(t, "testing", "colcompr")
		if table.UnsupportedDDL {
			t.Errorf("Expected table using column compression to be supported for diff in flavor %s, but it was not.\nExpected SHOW CREATE TABLE:\n%s\nActual SHOW CREATE TABLE:\n%s", flavor, table.GeneratedCreateStatement(flavor), table.CreateStatement)
		}
		if table.Columns[1].Compression != "COMPRESSED" {
			t.Errorf("Unexpected value for compression column attribute: found %q", table.Columns[1].Compression)
		}
	}

	// Include tables with check constraints if supported by flavor
	if flavor.HasCheckConstraints() {
		filename := "testdata/check.sql"
		if flavor.Vendor == VendorMariaDB {
			filename = "testdata/check-maria.sql"
		}
		if _, err := s.d.SourceSQL(filename); err != nil {
			t.Fatalf("Unexpected error sourcing %s: %v", filename, err)
		}
		tablesWithChecks := []*Table{s.GetTable(t, "testing", "has_checks1"), s.GetTable(t, "testing", "has_checks2")}
		for _, table := range tablesWithChecks {
			if len(table.Checks) == 0 {
				t.Errorf("Expected table %s to have at least one CHECK constraint, but it did not", table.Name)
			}
			if table.UnsupportedDDL {
				t.Errorf("Expected table %s to be supported for diff in flavor %s, but it was not.\nExpected SHOW CREATE TABLE:\n%s\nActual SHOW CREATE TABLE:\n%s", table.Name, flavor, table.GeneratedCreateStatement(flavor), table.CreateStatement)
			}
		}
	}
}

func (s TengoIntegrationSuite) TestInstanceRoutineIntrospection(t *testing.T) {
	schema := s.GetSchema(t, "testing")
	db, err := s.d.Connect("testing", "")
	if err != nil {
		t.Fatalf("Unexpected error from Connect: %s", err)
	}
	var sqlMode string
	if err = db.QueryRow("SELECT @@sql_mode").Scan(&sqlMode); err != nil {
		t.Fatalf("Unexpected error from Scan: %s", err)
	}

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

	// If this flavor supports using mysql.proc to bulk-fetch routines, confirm
	// the result is identical to using the individual SHOW CREATE queries
	if !s.d.Flavor().HasDataDictionary() {
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
