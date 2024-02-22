package tengo

import (
	"strings"
	"testing"
)

func (s TengoIntegrationSuite) TestInstanceSchemaIntrospection(t *testing.T) {
	// include broad coverage for many flavor-specific features
	flavor := s.d.Flavor()
	s.SourceTestSQL(t, flavorTestFiles(flavor)...)

	// Ensure our unit test fixtures and integration test fixtures match
	schema := s.GetSchema(t, "testing")
	aTableFromDB := getTable(t, schema, "actor")
	aTableFromUnit := aTableForFlavor(flavor, 1)
	aTableFromUnit.CreateStatement = "" // Prevent diff from short-circuiting on equivalent CREATEs
	clauses, supported := aTableFromDB.Diff(&aTableFromUnit)
	if !supported {
		t.Error("Diff unexpectedly not supported for testing.actor")
	} else if len(clauses) > 0 {
		t.Errorf("Diff of testing.actor unexpectedly found %d clauses; expected 0. Clauses: %+v", len(clauses), clauses)
	}

	aTableFromDB = getTable(t, schema, "actor_in_film")
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
	var seenUCA1400 bool
	schema2 := s.GetSchema(t, "testcharcoll")
	for _, table := range schema2.Tables {
		if table.UnsupportedDDL {
			t.Errorf("Table %s unexpectedly not supported for diff.\nExpected SHOW CREATE TABLE:\n%s\nActual SHOW CREATE TABLE:\n%s", table.Name, table.GeneratedCreateStatement(flavor), table.CreateStatement)
		}
		if strings.Contains(table.Collation, "uca1400") {
			seenUCA1400 = true
		}
	}
	if s.d.Flavor().MinMariaDB(10, 10) && !seenUCA1400 {
		t.Error("Failed to introspect table with a uca1400 collation")
	}

	// Test various flavor-specific ordering fixes
	aTableFromDB = getTable(t, schema, "grab_bag")
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
	if flavor.MinMariaDB(10, 2) || flavor.MinMySQL(8, 0, 13) {
		table := getTable(t, schema, "testdefaults")
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
		table := getTable(t, schema, "staff")
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
	if flavor.MinMySQL(8) {
		table := getTable(t, schema, "my8idx")
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
	} else if flavor.MinMariaDB(10, 6) {
		table := getTable(t, schema, "maria106idx")
		idx := table.SecondaryIndexes[0]
		if !idx.Invisible {
			t.Errorf("Expected index %s to be ignored, but it was not", idx.Name)
		}
	}

	// Test invisible column support in flavors supporting it
	if flavor.MinMariaDB(10, 3) || flavor.MinMySQL(8, 0, 23) {
		table := getTable(t, schema, "invistest")
		for n, col := range table.Columns {
			expectInvis := (n == 0 || n == 4 || n == 5)
			if col.Invisible != expectInvis {
				t.Errorf("Expected Columns[%d].Invisible == %t, instead found %t", n, expectInvis, !expectInvis)
			}
		}
	}

	// Include coverage for fulltext parsers if MySQL 5.7+. (Although these are
	// supported in other flavors too, no alternative parsers ship with them.)
	if flavor.MinMySQL(5, 7) {
		table := getTable(t, schema, "ftparser")
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
	if flavor.IsPercona() && flavor.MinMySQL(5, 6, 33) {
		table := getTable(t, schema, "colcompr")
		if table.Columns[1].Compression != "COMPRESSED" {
			t.Errorf("Unexpected value for compression column attribute: found %q", table.Columns[1].Compression)
		}
	} else if flavor.MinMariaDB(10, 3) {
		table := getTable(t, schema, "colcompr")
		if table.Columns[1].Compression != "COMPRESSED" {
			t.Errorf("Unexpected value for compression column attribute: found %q", table.Columns[1].Compression)
		}
	}

	// Include tables with check constraints if supported by flavor
	if flavor.HasCheckConstraints() {
		tablesWithChecks := []*Table{getTable(t, schema, "has_checks1"), getTable(t, schema, "has_checks2")}
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

// TestColumnCompression confirms that various logic around compressed columns
// in Percona Server and MariaDB work properly. The syntax and functionality
// differs between these two vendors, and meanwhile MySQL has no equivalent
// feature yet at all.
func TestColumnCompression(t *testing.T) {
	flavor := ParseFlavor("percona:5.7")
	table := supportedTableForFlavor(flavor)
	if table.Columns[3].Name != "metadata" || table.Columns[3].Compression != "" {
		t.Fatal("Test fixture has changed without corresponding update to this test's logic")
	}

	table.CreateStatement = strings.Replace(table.CreateStatement, "`metadata` text", "`metadata` text /*!50633 COLUMN_FORMAT COMPRESSED */", 1)
	fixPerconaColCompression(&table)
	if table.Columns[3].Compression != "COMPRESSED" {
		t.Errorf("Expected column's compression to be %q, instead found %q", "COMPRESSED", table.Columns[3].Compression)
	}
	if table.GeneratedCreateStatement(flavor) != table.CreateStatement {
		t.Errorf("Unexpected mismatch in generated CREATE TABLE:\nGeneratedCreateStatement:\n%s\nCreateStatement:\n%s", table.GeneratedCreateStatement(flavor), table.CreateStatement)
	}

	table.CreateStatement = strings.Replace(table.CreateStatement, "COMPRESSED */", "COMPRESSED WITH COMPRESSION_DICTIONARY `foobar` */", 1)
	fixPerconaColCompression(&table)
	if table.Columns[3].Compression != "COMPRESSED WITH COMPRESSION_DICTIONARY `foobar`" {
		t.Errorf("Expected column's compression to be %q, instead found %q", "COMPRESSED WITH COMPRESSION_DICTIONARY `foobar`", table.Columns[3].Compression)
	}
	if table.GeneratedCreateStatement(flavor) != table.CreateStatement {
		t.Errorf("Unexpected mismatch in generated CREATE TABLE:\nGeneratedCreateStatement:\n%s\nCreateStatement:\n%s", table.GeneratedCreateStatement(flavor), table.CreateStatement)
	}

	// Now indirectly test Column.Definition() for MariaDB
	flavor = ParseFlavor("mariadb:10.3")
	table = supportedTableForFlavor(flavor)
	table.CreateStatement = strings.Replace(table.CreateStatement, "`metadata` text", "`metadata` text /*!100301 COMPRESSED*/", 1)
	table.Columns[3].Compression = "COMPRESSED"
	if table.GeneratedCreateStatement(flavor) != table.CreateStatement {
		t.Errorf("Unexpected mismatch in generated CREATE TABLE:\nGeneratedCreateStatement:\n%s\nCreateStatement:\n%s", table.GeneratedCreateStatement(flavor), table.CreateStatement)
	}
}

// TestFixFulltextIndexParsers confirms CREATE TABLE parsing for WITH PARSER
// clauses works properly.
func TestFixFulltextIndexParsers(t *testing.T) {
	flavor := ParseFlavor("mysql:5.7")
	table := anotherTableForFlavor(flavor)
	if table.SecondaryIndexes[0].Type != "BTREE" || table.SecondaryIndexes[0].FullTextParser != "" {
		t.Fatal("Test fixture has changed without corresponding update to this test's logic")
	}

	// Confirm no parser = no change from fix
	table.SecondaryIndexes[0].Type = "FULLTEXT"
	table.CreateStatement = table.GeneratedCreateStatement(flavor)
	fixFulltextIndexParsers(&table, flavor)
	if table.SecondaryIndexes[0].FullTextParser != "" {
		t.Errorf("fixFulltextIndexParsers unexpectedly set parser to %q instead of %q", table.SecondaryIndexes[0].FullTextParser, "")
	}

	// Confirm parser extracted correctly from fix
	table.SecondaryIndexes[0].FullTextParser = "ngram"
	table.CreateStatement = table.GeneratedCreateStatement(flavor)
	table.SecondaryIndexes[0].FullTextParser = ""
	fixFulltextIndexParsers(&table, flavor)
	if table.SecondaryIndexes[0].FullTextParser != "ngram" {
		t.Errorf("fixFulltextIndexParsers unexpectedly set parser to %q instead of %q", table.SecondaryIndexes[0].FullTextParser, "ngram")
	}
}

// TestFixBlobDefaultExpression confirms CREATE TABLE parsing works for blob/
// text default expressions in versions which omit them from information_schema.
func TestFixBlobDefaultExpression(t *testing.T) {
	flavor := ParseFlavor("mysql:8.0")
	table := aTableForFlavor(flavor, 0)
	defExpr := "(CONCAT('hello ', 'world'))"
	table.Columns[1].Default = defExpr
	table.CreateStatement = table.GeneratedCreateStatement(flavor)
	table.Columns[1].Default = "(!!!BLOBDEFAULT!!!)"
	fixDefaultExpression(&table, flavor)
	if table.Columns[1].Default != defExpr {
		t.Errorf("fixDefaultExpression did not work or set default to unexpected value %q", table.Columns[1].Default)
	}

	// Confirm regex still correct with stuff after the default
	table.Columns[1].Comment = "hi i am a comment"
	table.CreateStatement = table.GeneratedCreateStatement(flavor)
	table.Columns[1].Default = "(!!!BLOBDEFAULT!!!)"
	fixDefaultExpression(&table, flavor)
	if table.Columns[1].Default != defExpr {
		t.Errorf("fixDefaultExpression did not work after adding comment, default is unexpected value %q", table.Columns[1].Default)
	}
}

// TestFixShowCharSets provides unit test coverage for fixShowCharSets
func TestFixShowCharSets(t *testing.T) {
	flavor := ParseFlavor("mysql:8.0.24")
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
		Name:          "many_permutations",
		Engine:        "InnoDB",
		CharSet:       "utf8",
		Collation:     "utf8_unicode_ci",
		ShowCollation: true,
		Columns: []*Column{
			{Name: "a", TypeInDB: "char(10)", Nullable: true, Default: "NULL", CharSet: "utf8", Collation: "utf8_unicode_ci", ShowCharSet: false, ShowCollation: true},
			{Name: "b", TypeInDB: "char(10)", Nullable: true, Default: "NULL", CharSet: "latin1", Collation: "latin1_swedish_ci", ShowCharSet: true, ShowCollation: false},
			{Name: "c", TypeInDB: "char(10)", Nullable: true, Default: "NULL", CharSet: "latin1", Collation: "latin1_swedish_ci", ShowCharSet: true, ShowCollation: false},
			{Name: "d", TypeInDB: "char(10)", Nullable: true, Default: "NULL", CharSet: "latin1", Collation: "latin1_bin", ShowCharSet: true, ShowCollation: true},
			{Name: "e", TypeInDB: "char(10)", Nullable: true, Default: "NULL", CharSet: "utf8", Collation: "utf8_general_ci", ShowCharSet: true, ShowCollation: false},
			{Name: "f", TypeInDB: "char(10)", Nullable: true, Default: "NULL", CharSet: "utf8", Collation: "utf8_general_ci", ShowCharSet: true, ShowCollation: false},
			{Name: "g", TypeInDB: "char(10)", Nullable: true, Default: "NULL", CharSet: "utf8", Collation: "utf8_unicode_ci", ShowCharSet: false, ShowCollation: true},
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
	table.ShowCollation = false
	table.Columns = []*Column{
		{Name: "a", TypeInDB: "char(10)", Nullable: true, Default: "NULL", CharSet: "latin1", Collation: "latin1_bin", ShowCharSet: true, ShowCollation: true},
		{Name: "b", TypeInDB: "char(10)", Nullable: true, Default: "NULL", CharSet: "latin1", Collation: "latin1_swedish_ci", ShowCharSet: false, ShowCollation: false},
		{Name: "c", TypeInDB: "char(10)", Nullable: true, Default: "NULL", CharSet: "latin1", Collation: "latin1_swedish_ci", ShowCharSet: false, ShowCollation: false},
		{Name: "d", TypeInDB: "char(10)", Nullable: true, Default: "NULL", CharSet: "latin1", Collation: "latin1_bin", ShowCharSet: true, ShowCollation: true},
		{Name: "e", TypeInDB: "char(10)", Nullable: true, Default: "NULL", CharSet: "utf8", Collation: "utf8_general_ci", ShowCharSet: true, ShowCollation: false},
		{Name: "f", TypeInDB: "char(10)", Nullable: true, Default: "NULL", CharSet: "utf8", Collation: "utf8_general_ci", ShowCharSet: true, ShowCollation: false},
		{Name: "g", TypeInDB: "char(10)", Nullable: true, Default: "NULL", CharSet: "utf8", Collation: "utf8_unicode_ci", ShowCharSet: true, ShowCollation: true},
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
