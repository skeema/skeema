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
	if len(schema.Tables) < 7 {
		t.Errorf("Expected at least 7 tables, instead found %d", len(schema.Tables))
	}

	// Ensure TablesByName is returning the same set of tables
	byName := schema.TablesByName()
	if len(byName) != len(schema.Tables) {
		t.Errorf("len(byName) != len(tables): %d vs %d", len(byName), len(schema.Tables))
	}
	seen := make(map[string]bool, len(byName))
	for _, table := range schema.Tables {
		if seen[table.Name] {
			t.Errorf("Table %s returned multiple times from call to instance.Tables", table.Name)
		}
		seen[table.Name] = true
		if table != byName[table.Name] {
			t.Errorf("Mismatch for table %s between Tables and TablesByName", table.Name)
		}
		if table2 := schema.Table(table.Name); table2 != table {
			t.Errorf("Mismatch for table %s vs schema.Table(%s)", table.Name, table.Name)
		}
		if !schema.HasTable(table.Name) {
			t.Errorf("Expected HasTable(%s)==true, instead found false", table.Name)
		}
	}

	// Test negative responses
	if schema.HasTable("doesnt_exist") {
		t.Error("HasTable(doesnt_exist) unexpectedly returning true")
	}
	if table := schema.Table("doesnt_exist"); table != nil {
		t.Errorf("Expected Table(doesnt_exist) to return nil; instead found %v", table)
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
	for _, table := range schema.Tables {
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
		overrideCharset, overrideCollation, err := testRow.schema.OverridesServerCharSet(s.d.Instance)
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
	if _, _, err = nilSchema.OverridesServerCharSet(s.d.Instance); err == nil {
		t.Error("Expected OverridesServerCharSet to return error for nil schema, but it did not")
	}
}
