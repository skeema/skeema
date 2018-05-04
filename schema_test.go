package tengo

import (
	"testing"
)

// TestSchemaTables tests the input and output of Tables(), TablesByName(),
// HasTable(), and Table(). It does not explicitly validate the introspection
// logic though; that's handled in TestSchemaIntrospection.
func (s TengoIntegrationSuite) TestSchemaTables(t *testing.T) {
	schema := s.GetSchema(t, "testing")

	// Currently at least 7 tables in testing schema in testdata/integration.sql
	tables, err := schema.Tables()
	if err != nil || len(tables) < 7 {
		t.Errorf("Expected at least 7 tables, instead found %d, err=%s", len(tables), err)
	}

	// Ensure TablesByName is returning the same set of tables
	byName, err := schema.TablesByName()
	if err != nil {
		t.Errorf("TablesByName returned error: %s", err)
	} else if len(byName) != len(tables) {
		t.Errorf("len(byName) != len(tables): %d vs %d", len(byName), len(tables))
	}
	seen := make(map[string]bool, len(byName))
	for _, table := range tables {
		if seen[table.Name] {
			t.Errorf("Table %s returned multiple times from call to instance.Tables", table.Name)
		}
		seen[table.Name] = true
		if table != byName[table.Name] {
			t.Errorf("Mismatch for table %s between Tables and TablesByName", table.Name)
		}
		if table2, err := schema.Table(table.Name); err != nil || table2 != table {
			t.Errorf("Mismatch for table %s vs schema.Table(%s); error=%s", table.Name, table.Name, err)
		}
		if !schema.HasTable(table.Name) {
			t.Errorf("Expected HasTable(%s)==true, instead found false", table.Name)
		}
	}

	// Test negative responses
	if schema.HasTable("doesnt_exist") {
		t.Error("HasTable(doesnt_exist) unexpectedly returning true")
	}
	if table, err := schema.Table("doesnt_exist"); table != nil || err != nil {
		t.Errorf("Expected Table(doesnt_exist) to return nil,nil; instead found %v,%s", table, err)
	}
}

func (s TengoIntegrationSuite) TestSchemaIntrospection(t *testing.T) {
	// Ensure our unit test fixtures and integration test fixtures match
	schema, aTableFromDB := s.GetSchemaAndTable(t, "testing", "actor")
	aTableFromUnit := aTable(1)
	clauses, supported := aTableFromDB.Diff(&aTableFromUnit)
	if !supported {
		t.Error("Diff unexpectedly not supported for testing.actor")
	} else if len(clauses) > 0 {
		t.Errorf("Diff of testing.actor unexpectedly found %d clauses; expected 0", len(clauses))
	}
	aTableFromDB = s.GetTable(t, "testing", "actor_in_film")
	aTableFromUnit = anotherTable()
	clauses, supported = aTableFromDB.Diff(&aTableFromUnit)
	if !supported {
		t.Error("Diff unexpectedly not supported for testing.actor_in_film")
	} else if len(clauses) > 0 {
		t.Errorf("Diff of testing.actor_in_film unexpectedly found %d clauses; expected 0", len(clauses))
	}

	// ensure tables are all supported (except where known not to be)
	tables, err := schema.Tables()
	if err != nil {
		t.Fatalf("Unexpected error from schema.Tables(): %s", err)
	}
	for _, table := range tables {
		shouldBeUnsupported := (table.Name == unsupportedTable().Name)
		if table.UnsupportedDDL != shouldBeUnsupported {
			t.Errorf("Table %s: expected UnsupportedDDL==%v, instead found %v", table.Name, shouldBeUnsupported, !shouldBeUnsupported)
		}
	}
}

func (s TengoIntegrationSuite) TestSchemaOverridesServerCharSet(t *testing.T) {
	testingSchema := s.GetSchema(t, "testing")
	testcollateSchema := s.GetSchema(t, "testcollate")
	testcharsetSchema := s.GetSchema(t, "testcharset")
	testcharcollSchema := s.GetSchema(t, "testcharcoll")

	// Test logic assumes default of latin1 / latin1_swedish_ci
	defCharSet, defCollation, err := s.d.DefaultCharSetAndCollation()
	if err != nil {
		t.Skip("Unable to obtain instance default charset and collation")
	}
	if defCharSet != "latin1" || defCollation != "latin1_swedish_ci" {
		t.Skipf("Unexpected instance default charset and collation (%s / %s)", defCharSet, defCollation)
	}

	testTable := []struct {
		schema                  *Schema
		expectOverrideCharset   bool
		expectOverrideCollation bool
	}{
		{testingSchema, false, false},
		{testcollateSchema, false, true},
		{testcharsetSchema, true, true},
		{testcharcollSchema, true, true},
	}
	for _, testRow := range testTable {
		overrideCharset, overrideCollation, err := testRow.schema.OverridesServerCharSet()
		if err != nil {
			t.Errorf("Unexpected error from OverridesServerCharSet: %s", err)
		} else {
			if overrideCharset != testRow.expectOverrideCharset {
				t.Errorf("Schema %s: Expected overrideCharset=%v, instead found %v", testRow.schema.Name, testRow.expectOverrideCharset, overrideCharset)
			}
			if overrideCollation != testRow.expectOverrideCollation {
				t.Errorf("Schema %s: Expected overrideCollation=%v, instead found %v", testRow.schema.Name, testRow.expectOverrideCollation, overrideCollation)
			}
		}
	}

	// Confirm error returned from calling method on a nil schema
	var nilSchema *Schema
	if _, _, err = nilSchema.OverridesServerCharSet(); err == nil {
		t.Error("Expected OverridesServerCharSet to return error for nil schema, but it did not")
	}
}

func (s TengoIntegrationSuite) TestSchemaCachedCopy(t *testing.T) {
	schema := s.GetSchema(t, "testing")

	clone, err := schema.CachedCopy()
	if err != nil {
		t.Errorf("Unexpected error from schema.CachedCopy(): %s", err)
	}

	// Confirm diff still works
	sd, err := clone.Diff(schema)
	if err != nil {
		t.Errorf("Unexpected error from diff on a cached-copy schema: %s", err)
	} else if len(sd.TableDiffs) > 0 {
		t.Error("Non-empty diff unexpectedly returned")
	}

	// Confirm PurgeTableCache intentionally does not purge anything on a detached
	// instance
	if clone.tables == nil {
		t.Fatal("Incorrect assumption of test (cached copy of schema should already have table cache pre-populated)")
	}
	clone.PurgeTableCache()
	if clone.tables == nil {
		t.Error("Expected PurgeTableCache to be a no-op on a detached schema, but it was not")
	}

	// Confirm that methods requiring an instance return an error
	if _, _, err = clone.OverridesServerCharSet(); err == nil {
		t.Error("Expected OverridesServerCharSet to fail with detached instance, but it did not")
	}
	clone.tables = nil
	if _, err = clone.Tables(); err == nil {
		t.Error("Expected Tables() to fail with detached instance with artificially purged table cache, but it did not")
	}

	// Confirm CachedCopy of nil schema is nil
	var nilSchema *Schema
	if clone, err = nilSchema.CachedCopy(); clone != nil || err != nil {
		t.Error("Cached copy of nil schema did work as expected")
	}
}
