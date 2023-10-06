package tengo

import (
	"encoding/json"
	"regexp"
	"testing"
)

// TestSchemaTables tests the input and output of Tables, TablesByName(),
// HasTable(), and Table(). It does not explicitly validate the introspection
// logic though; that's handled in TestInstanceSchemaIntrospection.
func (s TengoIntegrationSuite) TestSchemaTables(t *testing.T) {
	s.SourceTestSQL(t, "integration-ext.sql")
	schema := s.GetSchema(t, "testing")

	// Currently at least 7 tables in testing schema from testdata/integration.sql
	// and testdata/introspection-ext.sql
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

// TestSchemaJSON confirms that a schema can be JSON encoded and decoded to
// yield the exact same schema.
func (s TengoIntegrationSuite) TestSchemaJSON(t *testing.T) {
	// include broad coverage for many flavor-specific features
	flavor := s.d.Flavor()
	s.SourceTestSQL(t, flavorTestFiles(flavor)...)

	for _, schemaName := range []string{"testing", "testcharcoll", "partitionparty"} {
		schema := s.GetSchema(t, schemaName)
		jsonBytes, err := json.Marshal(schema)
		if err != nil {
			t.Fatalf("Unexpected error from Marshal: %v", err)
		}
		var result Schema
		if err := json.Unmarshal(jsonBytes, &result); err != nil {
			t.Fatalf("Unexpected error from Unmarshal: %v", err)
		}
		diff := schema.Diff(&result)
		if objDiffs := diff.ObjectDiffs(); len(objDiffs) != 0 {
			t.Errorf("Expected no object diff in schema %s, but instead found %d: %+v", schemaName, len(objDiffs), objDiffs)
		}
		for n := range schema.Tables {
			a, b := schema.Tables[n].GeneratedCreateStatement(flavor), result.Tables[n].GeneratedCreateStatement(flavor)
			if a != b {
				t.Errorf("Mismatched GeneratedCreateStatement for table %s", schema.Tables[n].Name)
			}
		}
	}
}

func (s TengoIntegrationSuite) TestSchemaStripMatches(t *testing.T) {
	s.SourceTestSQL(t, "integration-ext.sql")
	schema := s.GetSchema(t, "testing")

	origTableCount, origRoutineCount := len(schema.Tables), len(schema.Routines)

	// Confirm nothing is stripped when no matching patterns are supplied
	schema.StripMatches(nil)
	if len(schema.Tables) != origTableCount || len(schema.Routines) != origRoutineCount {
		t.Fatal("StripMatches with nil arg unexpectedly stripped objects from schema")
	}
	noMatch1 := ObjectPattern{Type: ObjectTypeTable, Pattern: regexp.MustCompile("wont_match_anything")}
	noMatch2 := ObjectPattern{Type: ObjectTypeProc, Pattern: regexp.MustCompile("^func")}
	schema.StripMatches([]ObjectPattern{noMatch1, noMatch2})
	if len(schema.Tables) != origTableCount || len(schema.Routines) != origRoutineCount {
		t.Fatal("StripMatches with non-matching patterns unexpectedly stripped objects from schema")
	}

	// Confirm behavior stripping a table
	matchTable := ObjectPattern{Type: ObjectTypeTable, Pattern: regexp.MustCompile("^grab_bag$")}
	schema.StripMatches([]ObjectPattern{matchTable})
	if len(schema.Tables) != origTableCount-1 {
		t.Errorf("StripMatches not working correctly; expected %d tables remaining, instead found %d", origTableCount-1, len(schema.Tables))
	}
	if len(schema.Routines) != origRoutineCount {
		t.Errorf("StripMatches not working correctly; expected %d routines, instead found %d", origRoutineCount, len(schema.Routines))
	}

	// Confirm behavior stripping a func
	matchFunc := ObjectPattern{Type: ObjectTypeFunc, Pattern: regexp.MustCompile("func1")}
	schema.StripMatches([]ObjectPattern{matchFunc})
	if len(schema.Routines) != origRoutineCount-1 {
		t.Errorf("StripMatches not working correctly; expected %d routines remaining, instead found %d", origRoutineCount-1, len(schema.Routines))
	}

	// Confirm no panic if called on nil schema
	schema = nil
	schema.StripMatches([]ObjectPattern{matchFunc})
}

func (s TengoIntegrationSuite) TestSchemaStripTablePartitioning(t *testing.T) {
	s.SourceTestSQL(t, "partition.sql")

	// testing.followed_posts uses sub-partitioning and is unsupported for diffs.
	// After stripping partitioning, it should now be supported for diffs.
	schema, table := s.GetSchemaAndTable(t, "testing", "followed_posts")
	if !table.UnsupportedDDL {
		t.Fatal("Test setup assertion failed: testing.followed_posts is already supported for diff operations")
	}
	schema.StripTablePartitioning(s.d.Flavor())
	table = schema.Table("followed_posts")
	if table.Partitioning != nil || table.UnsupportedDDL {
		t.Errorf("StripTablePartitioning did not have expected effect on %s", table.ObjectKey())
	}

	// Many partitioned tables in schema partitionparty from partition.sql.
	// Confirm stripping works as expected.
	schema = s.GetSchema(t, "partitionparty")
	schema.StripTablePartitioning(s.d.Flavor())
	for _, table := range schema.Tables {
		if table.Partitioning != nil {
			t.Errorf("StripTablePartitioning did not have expected effect on %s", table.ObjectKey())
		}
	}
}
