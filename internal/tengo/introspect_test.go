package tengo

import (
	"context"
	"database/sql"
	"strings"
	"testing"
)

func (s TengoIntegrationSuite) TestInstanceSchemaIntrospection(t *testing.T) {
	// include broad coverage for many flavor-specific features
	flavor := s.d.Flavor()
	s.SourceTestSQL(t, flavorTestFiles(flavor)...)

	// Ensure our unit test fixtures and integration test fixtures match
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

	// Test Objects() map, which should contain objects of multiple types
	dict := schema.Objects()
	for key, obj := range dict {
		var ok bool
		switch key.Type {
		case ObjectTypeTable:
			ok = strings.HasPrefix(obj.Def(), "CREATE TABLE")
		case ObjectTypeProc, ObjectTypeFunc:
			ok = strings.HasPrefix(obj.Def(), "CREATE DEFINER")
		}
		if !ok {
			t.Errorf("Unexpected or incorrect key %s found in schema object definitions --> %s", key, obj.Def())
		}
	}

	if dict[ObjectKey{Type: ObjectTypeFunc, Name: "func1"}] == nil || dict[ObjectKey{Type: ObjectTypeProc, Name: "func1"}] != nil {
		t.Error("Objects map not populated as expected")
	}

	// ensure character set handling works properly. Recent flavors tend to change
	// many SHOW CREATE TABLE behaviors.
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
	if flavor.Min(FlavorMariaDB102) || flavor.Min(FlavorMySQL80.Dot(13)) {
		table := s.GetTable(t, "testing", "testdefaults")
		// Ensure 3-byte chars in default expression are introspected properly
		if !strings.Contains(table.CreateStatement, "\u20AC") {
			t.Errorf("Expected default expression to contain 3-byte char \u20AC, but it did not. CREATE statement:\n%s", table.CreateStatement)
		}

		// In MySQL, ensure 4-byte chars in default expressions are introspected
		// properly. (MariaDB supports them, but does not appear to provide any way
		// to properly introspect them; they're mangled in both I_S and SHOW CREATE!)
		if flavor.Vendor != VendorMariaDB && !strings.Contains(table.CreateStatement, "\U0001F4A9") {
			t.Errorf("Expected default expression to contain 4-byte char \U0001F4A9, but it did not. CREATE statement:\n%s", table.CreateStatement)
		}
	}

	// Test introspection of generated columns, if flavor supports them
	if flavor.GeneratedColumns() {
		table := s.GetTable(t, "testing", "staff")
		// Ensure 3-byte chars in generation expression are introspected properly
		if !strings.Contains(table.CreateStatement, "\u20AC") {
			t.Errorf("Expected generation expression to contain 3-byte char \u20AC, but it did not. CREATE statement:\n%s", table.CreateStatement)
		}

		// In MySQL, ensure 4-byte chars in default expressions are introspected
		// properly. (MariaDB does not appear to handle them properly in generated
		// column expressions at all, it's corrupted at creation time and on a
		// functional level, not just introspection.)
		if flavor.Vendor != VendorMariaDB && !strings.Contains(table.CreateStatement, "\U0001F4A9") {
			t.Errorf("Expected generation expression to contain 4-byte char \U0001F4A9, but it did not. CREATE statement:\n%s", table.CreateStatement)
		}

		// Test generation expression fix, even if test image isn't MySQL 5.7+
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
	if flavor.Min(FlavorMySQL80) {
		table := s.GetTable(t, "testing", "my8idx")
		if !strings.Contains(table.CreateStatement, "\u20AC") {
			t.Errorf("Expected functional index expression to contain 3-byte char \u20AC, but it did not. CREATE statement:\n%s", table.CreateStatement)
		}
		if !strings.Contains(table.CreateStatement, "\U0001F4A9") {
			t.Errorf("Expected functional index expression to contain 4-byte char \U0001F4A9, but it did not. CREATE statement:\n%s", table.CreateStatement)
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
	} else if flavor.Min(FlavorMariaDB106) {
		table := s.GetTable(t, "testing", "maria106idx")
		idx := table.SecondaryIndexes[0]
		if !idx.Invisible {
			t.Errorf("Expected index %s to be ignored, but it was not", idx.Name)
		}
	}

	// Test invisible column support in flavors supporting it
	if flavor.Min(FlavorMariaDB103) || flavor.Min(FlavorMySQL80.Dot(23)) {
		table := s.GetTable(t, "testing", "invistest")
		for n, col := range table.Columns {
			expectInvis := (n == 0 || n == 4 || n == 5)
			if col.Invisible != expectInvis {
				t.Errorf("Expected Columns[%d].Invisible == %t, instead found %t", n, expectInvis, !expectInvis)
			}
		}
	}

	// Include coverage for fulltext parsers if MySQL 5.7+. (Although these are
	// supported in other flavors too, no alternative parsers ship with them.)
	if flavor.Min(FlavorMySQL57) {
		table := s.GetTable(t, "testing", "ftparser")
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
	if flavor.Min(FlavorPercona56.Dot(33)) {
		table := s.GetTable(t, "testing", "colcompr")
		if table.Columns[1].Compression != "COMPRESSED" {
			t.Errorf("Unexpected value for compression column attribute: found %q", table.Columns[1].Compression)
		}
	} else if flavor.Min(FlavorMariaDB103) {
		table := s.GetTable(t, "testing", "colcompr")
		if table.Columns[1].Compression != "COMPRESSED" {
			t.Errorf("Unexpected value for compression column attribute: found %q", table.Columns[1].Compression)
		}
	}

	// Include tables with check constraints if supported by flavor
	if flavor.HasCheckConstraints() {
		tablesWithChecks := []*Table{s.GetTable(t, "testing", "has_checks1"), s.GetTable(t, "testing", "has_checks2")}
		for _, table := range tablesWithChecks {
			if len(table.Checks) == 0 {
				t.Errorf("Expected table %s to have at least one CHECK constraint, but it did not", table.Name)
			}
			if table.Name == "has_checks1" {
				if flavor.IsMariaDB() {
					if !strings.Contains(table.CreateStatement, "\u20AC") {
						t.Errorf("Expected check constraint clause to contain 3-byte char \u20AC, but it did not. CREATE statement:\n%s", table.CreateStatement)
					}
				} else if !strings.Contains(table.CreateStatement, "\U0001F4A9") {
					t.Errorf("Expected check constraint clause to contain 4-byte char \U0001F4A9, but it did not. CREATE statement:\n%s", table.CreateStatement)
				}
			}
		}
	}
}

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

// TestColumnCompression confirms that various logic around compressed columns
// in Percona Server and MariaDB work properly. The syntax and functionality
// differs between these two vendors, and meanwhile MySQL has no equivalent
// feature yet at all.
func TestColumnCompression(t *testing.T) {
	table := supportedTableForFlavor(FlavorPercona57)
	if table.Columns[3].Name != "metadata" || table.Columns[3].Compression != "" {
		t.Fatal("Test fixture has changed without corresponding update to this test's logic")
	}

	table.CreateStatement = strings.Replace(table.CreateStatement, "`metadata` text", "`metadata` text /*!50633 COLUMN_FORMAT COMPRESSED */", 1)
	fixPerconaColCompression(&table)
	if table.Columns[3].Compression != "COMPRESSED" {
		t.Errorf("Expected column's compression to be %q, instead found %q", "COMPRESSED", table.Columns[3].Compression)
	}
	if table.GeneratedCreateStatement(FlavorPercona57) != table.CreateStatement {
		t.Errorf("Unexpected mismatch in generated CREATE TABLE:\nGeneratedCreateStatement:\n%s\nCreateStatement:\n%s", table.GeneratedCreateStatement(FlavorPercona57), table.CreateStatement)
	}

	table.CreateStatement = strings.Replace(table.CreateStatement, "COMPRESSED */", "COMPRESSED WITH COMPRESSION_DICTIONARY `foobar` */", 1)
	fixPerconaColCompression(&table)
	if table.Columns[3].Compression != "COMPRESSED WITH COMPRESSION_DICTIONARY `foobar`" {
		t.Errorf("Expected column's compression to be %q, instead found %q", "COMPRESSED WITH COMPRESSION_DICTIONARY `foobar`", table.Columns[3].Compression)
	}
	if table.GeneratedCreateStatement(FlavorPercona57) != table.CreateStatement {
		t.Errorf("Unexpected mismatch in generated CREATE TABLE:\nGeneratedCreateStatement:\n%s\nCreateStatement:\n%s", table.GeneratedCreateStatement(FlavorPercona57), table.CreateStatement)
	}

	// Now indirectly test Column.Definition() for MariaDB
	table = supportedTableForFlavor(FlavorMariaDB103)
	table.CreateStatement = strings.Replace(table.CreateStatement, "`metadata` text", "`metadata` text /*!100301 COMPRESSED*/", 1)
	table.Columns[3].Compression = "COMPRESSED"
	if table.GeneratedCreateStatement(FlavorMariaDB103) != table.CreateStatement {
		t.Errorf("Unexpected mismatch in generated CREATE TABLE:\nGeneratedCreateStatement:\n%s\nCreateStatement:\n%s", table.GeneratedCreateStatement(FlavorMariaDB103), table.CreateStatement)
	}
}

// TestFixFulltextIndexParsers confirms CREATE TABLE parsing for WITH PARSER
// clauses works properly.
func TestFixFulltextIndexParsers(t *testing.T) {
	table := anotherTableForFlavor(FlavorMySQL57)
	if table.SecondaryIndexes[0].Type != "BTREE" || table.SecondaryIndexes[0].FullTextParser != "" {
		t.Fatal("Test fixture has changed without corresponding update to this test's logic")
	}

	// Confirm no parser = no change from fix
	table.SecondaryIndexes[0].Type = "FULLTEXT"
	table.CreateStatement = table.GeneratedCreateStatement(FlavorMySQL57)
	fixFulltextIndexParsers(&table, FlavorMySQL57)
	if table.SecondaryIndexes[0].FullTextParser != "" {
		t.Errorf("fixFulltextIndexParsers unexpectedly set parser to %q instead of %q", table.SecondaryIndexes[0].FullTextParser, "")
	}

	// Confirm parser extracted correctly from fix
	table.SecondaryIndexes[0].FullTextParser = "ngram"
	table.CreateStatement = table.GeneratedCreateStatement(FlavorMySQL57)
	table.SecondaryIndexes[0].FullTextParser = ""
	fixFulltextIndexParsers(&table, FlavorMySQL57)
	if table.SecondaryIndexes[0].FullTextParser != "ngram" {
		t.Errorf("fixFulltextIndexParsers unexpectedly set parser to %q instead of %q", table.SecondaryIndexes[0].FullTextParser, "ngram")
	}
}

// TestFixBlobDefaultExpression confirms CREATE TABLE parsing works for blob/
// text default expressions in versions which omit them from information_schema.
func TestFixBlobDefaultExpression(t *testing.T) {
	table := aTableForFlavor(FlavorMySQL80, 0)
	defExpr := "(CONCAT('hello ', 'world'))"
	table.Columns[1].Default = defExpr
	table.CreateStatement = table.GeneratedCreateStatement(FlavorMySQL80)
	table.Columns[1].Default = "(!!!BLOBDEFAULT!!!)"
	fixDefaultExpression(&table, FlavorMySQL80)
	if table.Columns[1].Default != defExpr {
		t.Errorf("fixDefaultExpression did not work or set default to unexpected value %q", table.Columns[1].Default)
	}

	// Confirm regex still correct with stuff after the default
	table.Columns[1].Comment = "hi i am a comment"
	table.CreateStatement = table.GeneratedCreateStatement(FlavorMySQL80)
	table.Columns[1].Default = "(!!!BLOBDEFAULT!!!)"
	fixDefaultExpression(&table, FlavorMySQL80)
	if table.Columns[1].Default != defExpr {
		t.Errorf("fixDefaultExpression did not work after adding comment, default is unexpected value %q", table.Columns[1].Default)
	}
}

// TestFixShowCharSets provides unit test coverage for fixShowCharSets
func TestFixShowCharSets(t *testing.T) {
	flavor := FlavorMySQL80.Dot(24)
	stmt := strings.ReplaceAll(`CREATE TABLE ~many_permutations~ (
  ~a~ char(10) COLLATE utf8_unicode_ci DEFAULT NULL,
  ~b~ char(10) CHARACTER SET latin1 DEFAULT NULL,
  ~c~ char(10) CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT NULL,
  ~d~ char(10) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  ~e~ char(10) CHARACTER SET utf8 DEFAULT NULL,
  ~f~ char(10) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  ~g~ char(10) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8_unicode_ci`, "~", "`")
	table := &Table{
		Name:               "many_permutations",
		Engine:             "InnoDB",
		CharSet:            "utf8",
		Collation:          "utf8_unicode_ci",
		CollationIsDefault: false,
		Columns: []*Column{
			{Name: "a", TypeInDB: "char(10)", Nullable: true, Default: "NULL", CharSet: "utf8", Collation: "utf8_unicode_ci", CollationIsDefault: false},
			{Name: "b", TypeInDB: "char(10)", Nullable: true, Default: "NULL", CharSet: "latin1", Collation: "latin1_swedish_ci", CollationIsDefault: true},
			{Name: "c", TypeInDB: "char(10)", Nullable: true, Default: "NULL", CharSet: "latin1", Collation: "latin1_swedish_ci", CollationIsDefault: true},
			{Name: "d", TypeInDB: "char(10)", Nullable: true, Default: "NULL", CharSet: "latin1", Collation: "latin1_bin", CollationIsDefault: false},
			{Name: "e", TypeInDB: "char(10)", Nullable: true, Default: "NULL", CharSet: "utf8", Collation: "utf8_general_ci", CollationIsDefault: true},
			{Name: "f", TypeInDB: "char(10)", Nullable: true, Default: "NULL", CharSet: "utf8", Collation: "utf8_general_ci", CollationIsDefault: true},
			{Name: "g", TypeInDB: "char(10)", Nullable: true, Default: "NULL", CharSet: "utf8", Collation: "utf8_unicode_ci", CollationIsDefault: false},
		},
		CreateStatement: stmt,
	}

	// Verify initial setup: generated create should differ from CreateStatement
	if gen := table.GeneratedCreateStatement(flavor); gen == table.CreateStatement {
		t.Fatalf("Test setup assertion failed. CREATE statement already matches SHOW:\n%s", gen)
	}
	fixShowCharSets(table)
	if gen := table.GeneratedCreateStatement(flavor); gen != table.CreateStatement {
		t.Errorf("Mismatch between generated CREATE statement and SHOW.\nGenerated:\n%s\n\nSHOW:\n%s\n", gen, table.CreateStatement)
	}

	table.CreateStatement = strings.ReplaceAll(`CREATE TABLE ~many_permutations~ (
  ~a~ char(10) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  ~b~ char(10) CHARACTER SET latin1 DEFAULT NULL,
  ~c~ char(10) CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT NULL,
  ~d~ char(10) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  ~e~ char(10) CHARACTER SET utf8 DEFAULT NULL,
  ~f~ char(10) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  ~g~ char(10) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1`, "~", "`")
	table.CharSet = "latin1"
	table.Collation = "latin1_swedish_ci"
	table.CollationIsDefault = true
	table.Columns = []*Column{
		{Name: "a", TypeInDB: "char(10)", Nullable: true, Default: "NULL", CharSet: "latin1", Collation: "latin1_bin", CollationIsDefault: false},
		{Name: "b", TypeInDB: "char(10)", Nullable: true, Default: "NULL", CharSet: "latin1", Collation: "latin1_swedish_ci", CollationIsDefault: true},
		{Name: "c", TypeInDB: "char(10)", Nullable: true, Default: "NULL", CharSet: "latin1", Collation: "latin1_swedish_ci", CollationIsDefault: true},
		{Name: "d", TypeInDB: "char(10)", Nullable: true, Default: "NULL", CharSet: "latin1", Collation: "latin1_bin", CollationIsDefault: false},
		{Name: "e", TypeInDB: "char(10)", Nullable: true, Default: "NULL", CharSet: "utf8", Collation: "utf8_general_ci", CollationIsDefault: true},
		{Name: "f", TypeInDB: "char(10)", Nullable: true, Default: "NULL", CharSet: "utf8", Collation: "utf8_general_ci", CollationIsDefault: true},
		{Name: "g", TypeInDB: "char(10)", Nullable: true, Default: "NULL", CharSet: "utf8", Collation: "utf8_unicode_ci", CollationIsDefault: false},
	}

	// Verify initial setup: generated create should differ from CreateStatement
	if gen := table.GeneratedCreateStatement(flavor); gen == table.CreateStatement {
		t.Fatalf("Test setup assertion failed. CREATE statement already matches SHOW:\n%s", gen)
	}
	fixShowCharSets(table)
	if gen := table.GeneratedCreateStatement(flavor); gen != table.CreateStatement {
		t.Errorf("Mismatch between generated CREATE statement and SHOW.\nGenerated:\n%s\n\nSHOW:\n%s\n", gen, table.CreateStatement)
	}
}
