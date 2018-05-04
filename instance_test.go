package tengo

import (
	"net/url"
	"reflect"
	"testing"

	"github.com/jmoiron/sqlx"
)

// Give tests a way to avoid stomping on each others' cached instances, since
// currently we don't permit creation of instances that only differ by user,
// pass, or defaultParams
func nukeInstanceCache() {
	allInstances.byDSN = make(map[string]*Instance)
}

func TestNewInstance(t *testing.T) {
	nukeInstanceCache()
	assertError := func(driver, dsn string) {
		instance, err := NewInstance(driver, dsn)
		if instance != nil || err == nil {
			t.Errorf("Expected NewInstance(\"%s\", \"%s\") to return nil,err; instead found %v, %v", driver, dsn, instance, err)
		}
	}
	assertError("btrieve", "username:password@tcp(some.host)/dbname?param=value")
	assertError("", "username:password@tcp(some.host:1234)/dbname?param=value")
	assertError("mysql", "username:password@tcp(some.host:1234) i like zebras")

	assertInstance := func(dsn string, expectedInstance Instance) {
		expectedInstance.connectionPool = make(map[string]*sqlx.DB)
		instance, err := NewInstance("mysql", dsn)
		if err != nil {
			t.Fatalf("Unexpectedly received error %s from NewInstance(\"mysql\", \"%s\")", err, dsn)
		}
		expectedInstance.RWMutex = instance.RWMutex // cheat to satisfy DeepEqual
		if !reflect.DeepEqual(expectedInstance, *instance) {
			t.Errorf("NewInstance(\"mysql\", \"%s\"): Returned instance %#v does not match expected instance %#v", dsn, *instance, expectedInstance)
		}
	}

	dsn := "username:password@tcp(some.host:1234)/dbname"
	expected := Instance{
		BaseDSN:       "username:password@tcp(some.host:1234)/",
		Driver:        "mysql",
		User:          "username",
		Password:      "password",
		Host:          "some.host",
		Port:          1234,
		defaultParams: map[string]string{},
	}
	assertInstance(dsn, expected)

	dsn = "username:password@tcp(1.2.3.4:3306)/?param1=value1&readTimeout=5s&interpolateParams=0"
	expected = Instance{
		BaseDSN:  "username:password@tcp(1.2.3.4:3306)/",
		Driver:   "mysql",
		User:     "username",
		Password: "password",
		Host:     "1.2.3.4",
		Port:     3306,
		defaultParams: map[string]string{
			"param1":            "value1",
			"readTimeout":       "5s",
			"interpolateParams": "0",
		},
	}
	assertInstance(dsn, expected)

	dsn = "root@unix(/var/lib/mysql/mysql.sock)/dbname?param1=value1"
	expected = Instance{
		BaseDSN:    "root@unix(/var/lib/mysql/mysql.sock)/",
		Driver:     "mysql",
		User:       "root",
		Host:       "localhost",
		SocketPath: "/var/lib/mysql/mysql.sock",
		defaultParams: map[string]string{
			"param1": "value1",
		},
	}
	assertInstance(dsn, expected)
}

func TestNewInstanceDedupes(t *testing.T) {
	nukeInstanceCache()
	dsn1 := "username:password@tcp(some.host:1234)/dbname"
	dsn2 := "username:password@tcp(some.host:1234)/otherdb"
	dsn3 := "username:password@tcp(some.host:1234)/"
	dsn4 := "username:password@tcp(some.host:123)/dbname"
	dsn5 := "username:password@tcp(some.host:1234)/otherdb?foo=bar"

	newInstance := func(dsn string) *Instance {
		inst, err := NewInstance("mysql", dsn)
		if err != nil {
			t.Fatalf("Unexpectedly received error %s from NewInstance(\"mysql\", \"%s\")", err, dsn)
		}
		return inst
	}

	inst1 := newInstance(dsn1)
	if newInstance(dsn1) != inst1 {
		t.Errorf("Expected NewInstance to return same pointer for duplicate DSN, but it did not")
	}
	if newInstance(dsn2) != inst1 {
		t.Errorf("Expected NewInstance to return same pointer for DSN that only differed by schema, but it did not")
	}
	if newInstance(dsn3) != inst1 {
		t.Errorf("Expected NewInstance to return same pointer for DSN that only differed by lack of schema, but it did not")
	}
	if newInstance(dsn4) == inst1 {
		t.Errorf("Expected NewInstance to return different pointer for DSN that has different port, but it did not")
	}
	if inst5, err := NewInstance("mysql", dsn5); inst5 != nil || err == nil {
		t.Errorf("Expected NewInstance to return an error upon using DSN that only differs by schema or params, but it did not")
	}

}

func TestInstanceBuildParamString(t *testing.T) {
	assertParamString := func(defaultOptions, addOptions, expectOptions string) {
		dsn := "username:password@tcp(1.2.3.4:3306)/"
		if defaultOptions != "" {
			dsn += "?" + defaultOptions
		}
		nukeInstanceCache()
		instance, err := NewInstance("mysql", dsn)
		if err != nil {
			t.Fatalf("NewInstance(\"mysql\", \"%s\") returned error: %s", dsn, err)
		}

		// can't compare strings directly since order may be different
		result := instance.buildParamString(addOptions)
		parsedResult, err := url.ParseQuery(result)
		if err != nil {
			t.Fatalf("url.ParseQuery(\"%s\") returned error: %s", result, err)
		}
		parsedExpected, err := url.ParseQuery(expectOptions)
		if err != nil {
			t.Fatalf("url.ParseQuery(\"%s\") returned error: %s", expectOptions, err)
		}
		if !reflect.DeepEqual(parsedResult, parsedExpected) {
			t.Errorf("Expected param map %v, instead found %v", parsedExpected, parsedResult)
		}

		// nuke the Instance cache
		nukeInstanceCache()
	}

	assertParamString("", "", "")
	assertParamString("param1=value1", "", "param1=value1")
	assertParamString("", "param1=value1", "param1=value1")
	assertParamString("param1=value1", "param1=value1", "param1=value1")
	assertParamString("param1=value1", "param1=hello", "param1=hello")
	assertParamString("param1=value1&readTimeout=5s&interpolateParams=0", "param2=value2", "param1=value1&readTimeout=5s&interpolateParams=0&param2=value2")
	assertParamString("param1=value1&readTimeout=5s&interpolateParams=0", "param1=value3", "param1=value3&readTimeout=5s&interpolateParams=0")
}

func (s TengoIntegrationSuite) TestInstanceConnect(t *testing.T) {
	// Connecting to invalid schema should return an error
	db, err := s.d.Connect("does-not-exist", "")
	if err == nil {
		t.Error("err is unexpectedly nil")
	} else if db != nil {
		t.Error("db is unexpectedly non-nil")
	}

	// Connecting without specifying a default schema should be successful
	db, err = s.d.Connect("", "")
	if err != nil {
		t.Errorf("Unexpected connection error: %s", err)
	} else if db == nil {
		t.Error("db is unexpectedly nil")
	}

	// Connecting again with same schema and params should return the existing connection pool
	db2, err := s.d.Connect("", "")
	if err != nil {
		t.Errorf("Unexpected connection error: %s", err)
	} else if db2 != db {
		t.Errorf("Expected same DB pool to be returned from identical Connect call; instead db=%v and db2=%v", db, db2)
	}

	// Connecting again with different schema should return a different connection pool
	db3, err := s.d.Connect("information_schema", "")
	if err != nil {
		t.Errorf("Unexpected connection error: %s", err)
	} else if db3 == db {
		t.Error("Expected different DB pool to be returned from Connect with different default db; instead was same")
	}

	// Connecting again with different params should return a different connection pool
	db4, err := s.d.Connect("information_schema", "foreign_key_checks=0")
	if err != nil {
		t.Errorf("Unexpected connection error: %s", err)
	} else if db4 == db || db4 == db3 {
		t.Error("Expected different DB pool to be returned from Connect with different params; instead was same")
	}
}

func (s TengoIntegrationSuite) TestInstanceSchemas(t *testing.T) {
	// Currently at least 4 schemas in testdata/integration.sql
	schemas, err := s.d.Schemas()
	if err != nil || len(schemas) < 4 {
		t.Errorf("Expected at least 4 schemas, instead found %d, err=%s", len(schemas), err)
	}

	// Ensure SchemasByName is returning the same set of schemas
	byName, err := s.d.SchemasByName()
	if err != nil {
		t.Errorf("SchemasByName returned error: %s", err)
	} else if len(byName) != len(schemas) {
		t.Errorf("len(byName) != len(schemas): %d vs %d", len(byName), len(schemas))
	}
	seen := make(map[string]bool, len(byName))
	for _, schema := range schemas {
		if seen[schema.Name] {
			t.Errorf("Schema %s returned multiple times from call to instance.Schemas", schema.Name)
		}
		seen[schema.Name] = true
		if schema != byName[schema.Name] {
			t.Errorf("Mismatch for schema %s between Schemas and SchemasByName", schema.Name)
		}
		if schema2, err := s.d.Schema(schema.Name); err != nil || schema2 != schema {
			t.Errorf("Mismatch for schema %s vs instance.Schema(%s); error=%s", schema.Name, schema.Name, err)
		}
		if !s.d.HasSchema(schema.Name) {
			t.Errorf("Expected HasSchema(%s)==true, instead found false", schema.Name)
		}
	}

	// Test negative responses
	if s.d.HasSchema("doesnt_exist") {
		t.Error("HasSchema(doesnt_exist) unexpectedly returning true")
	}
	if schema, err := s.d.Schema("doesnt_exist"); schema != nil || err != nil {
		t.Errorf("Expected Schema(doesnt_exist) to return nil,nil; instead found %v,%s", schema, err)
	}
}

func (s TengoIntegrationSuite) TestInstanceShowCreateTable(t *testing.T) {
	schema, t1actual := s.GetSchemaAndTable(t, "testing", "actor")
	t2actual := s.GetTable(t, "testing", "actor_in_film")

	t1create, err1 := s.d.ShowCreateTable(schema, t1actual)
	t2create, err2 := s.d.ShowCreateTable(schema, t2actual)
	if err1 != nil || err2 != nil || t1create == "" || t2create == "" {
		t.Fatalf("Unable to obtain SHOW CREATE TABLE output: err1=%s, err2=%s", err1, err2)
	}

	t1expected := aTable(1)
	if t1create != t1expected.createStatement {
		t.Errorf("Mismatch for SHOW CREATE TABLE\nActual return from %s:\n%s\n----------\nExpected output: %s", s.d.Image, t1create, t1expected.createStatement)
	}
	t2expected := anotherTable()
	if t2create != t2expected.createStatement {
		t.Errorf("Mismatch for SHOW CREATE TABLE\nActual return from %s:\n%s\n----------\nExpected output: %s", s.d.Image, t2create, t2expected.createStatement)
	}

	// Test nonexistent table
	t3 := aTable(123)
	t3.Name = "doesnt_exist"
	t3create, err3 := s.d.ShowCreateTable(schema, &t3)
	if t3create != "" || err3 == nil {
		t.Errorf("Expected ShowCreateTable on invalid table to return empty string and error, instead err=%s, output=%s", err3, t3create)
	}
}

func (s TengoIntegrationSuite) TestInstanceTableSize(t *testing.T) {
	schema, table := s.GetSchemaAndTable(t, "testing", "has_rows")
	size, err := s.d.TableSize(schema, table)
	if err != nil {
		t.Errorf("Error from TableSize: %s", err)
	} else if size < 1 {
		t.Errorf("TableSize returned a non-positive result: %d", size)
	}

	// Test nonexistent table
	doesntExist := aTable(123)
	doesntExist.Name = "doesnt_exist"
	size, err = s.d.TableSize(schema, &doesntExist)
	if size > 0 || err == nil {
		t.Errorf("Expected TableSize to return 0 size and non-nil err for missing table, instead size=%d and err=%s", size, err)
	}
}

func (s TengoIntegrationSuite) TestInstanceTableHasRows(t *testing.T) {
	schema, tableWithRows := s.GetSchemaAndTable(t, "testing", "has_rows")
	if hasRows, err := s.d.TableHasRows(schema, tableWithRows); err != nil {
		t.Errorf("Error from TableHasRows: %s", err)
	} else if !hasRows {
		t.Error("Expected TableHasRows to return true for has_rows, instead returned false")
	}

	tableNoRows := s.GetTable(t, "testing", "no_rows")
	if hasRows, err := s.d.TableHasRows(schema, tableNoRows); err != nil {
		t.Errorf("Error from TableHasRows: %s", err)
	} else if hasRows {
		t.Error("Expected TableHasRows to return false for no_rows, instead returned true")
	}

	// Test nonexistent table
	doesntExist := aTable(123)
	doesntExist.Name = "doesnt_exist"
	if _, err := s.d.TableHasRows(schema, &doesntExist); err == nil {
		t.Error("Expected TableHasRows to return error for nonexistent table, but it did not")
	}

}

func (s TengoIntegrationSuite) TestInstanceCreateSchema(t *testing.T) {
	_, err := s.d.CreateSchema("foobar", "utf8mb4", "utf8mb4_unicode_ci")
	if err != nil {
		t.Fatalf("CreateSchema returned unexpected error: %s", err)
	}
	if refetch, err := s.d.Schema("foobar"); err != nil {
		t.Errorf("Unable to fetch newly created schema: %s", err)
	} else if refetch.CharSet != "utf8mb4" || refetch.Collation != "utf8mb4_unicode_ci" {
		t.Errorf("Unexpected charset or collation on refetched schema: %+v", refetch)
	}

	// Ensure creation of duplicate schema fails with error
	if _, err := s.d.CreateSchema("foobar", "utf8mb4", "utf8mb4_unicode_ci"); err == nil {
		t.Error("Expected creation of duplicate schema to return an error, but it did not")
	}

	// Creation of schema without specifying charset and collation should use
	// instance defaults
	defCharSet, defCollation, err := s.d.DefaultCharSetAndCollation()
	if err != nil {
		t.Fatalf("Unable to obtain instance default charset and collation")
	}
	if schema, err := s.d.CreateSchema("barfoo", "", ""); err != nil {
		t.Errorf("Failed to create schema with default charset and collation: %s", err)
	} else if schema.CharSet != defCharSet || schema.Collation != defCollation {
		t.Errorf("Expected charset/collation to be %s/%s, instead found %s/%s", defCharSet, defCollation, schema.CharSet, schema.Collation)
	}
}

func (s TengoIntegrationSuite) TestInstanceDropSchema(t *testing.T) {
	schema1 := s.GetSchema(t, "testing")      // has tables, and one has rows
	schema2 := s.GetSchema(t, "testcollate")  // does not have tables
	schema3 := s.GetSchema(t, "testcharcoll") // has tables, but none have rows

	// Dropping a schema with non-empty tables when onlyIfEmpty==true should fail
	if err := s.d.DropSchema(schema1, true); err == nil {
		t.Error("Expected dropping a schema with tables to fail when onlyIfEmpty==true, but it did not")
	}

	// Dropping a schema without tables when onlyIfEmpty==true should succeed
	if err := s.d.DropSchema(schema2, true); err != nil {
		t.Errorf("Expected dropping a schema without tables to succeed when onlyIfEmpty==true, but error=%s", err)
	}

	// Dropping a schema with only empty tables when onlyIfEmpty==true should succeed
	if err := s.d.DropSchema(schema3, true); err != nil {
		t.Errorf("Expected dropping a schema with only empty tables to succeed when onlyIfEmpty==true, but error=%s", err)
	}

	// Dropping a schema with non-empty tables when onlyIfEmpty==false should succeed
	if err := s.d.DropSchema(schema1, false); err != nil {
		t.Errorf("Expected dropping a schema with tables to succeed when onlyIfEmpty==false, but error=%s", err)
	}

	// Dropping a schema that doesn't exist should fail
	if err := s.d.DropSchema(schema1, false); err == nil {
		t.Error("Expected dropping a nonexistent schema to fail, but error was nil")
	}
}

func (s TengoIntegrationSuite) TestInstanceAlterSchema(t *testing.T) {
	assertNoError := func(schema *Schema, newCharSet, newCollation, expectCharSet, expectCollation string) {
		t.Helper()
		if err := s.d.AlterSchema(schema, newCharSet, newCollation); err != nil {
			t.Errorf("Expected alter of %s to (%s,%s) would not error, but returned %s", schema.Name, newCharSet, newCollation, err)
		} else {
			if schema.CharSet != expectCharSet {
				t.Errorf("Expected post-alter charset to be %s, instead found %s", expectCharSet, schema.CharSet)
			}
			if schema.Collation != expectCollation {
				t.Errorf("Expected post-alter collation to be %s, instead found %s", expectCollation, schema.Collation)
			}
		}
	}
	assertError := func(schema *Schema, newCharSet, newCollation string) {
		t.Helper()
		if err := s.d.AlterSchema(schema, newCharSet, newCollation); err == nil {
			t.Errorf("Expected alter of %s to (%s,%s) would return error, but returned nil instead", schema.Name, newCharSet, newCollation)
		}
	}

	schema1 := s.GetSchema(t, "testing")      // instance-default charset and collation
	schema2 := s.GetSchema(t, "testcharset")  // utf8mb4 charset with its default collation (utf8mb4_general_ci)
	schema3 := s.GetSchema(t, "testcharcoll") // utf8mb4 with utf8mb4_unicode_ci
	schema4 := aSchema("nonexistent")

	instCharSet, instCollation, err := s.d.DefaultCharSetAndCollation()
	if err != nil {
		t.Fatalf("Unable to fetch instance default charset and collation: %s", err)
	}

	// Test no-op conditions
	assertNoError(schema1, "", "", instCharSet, instCollation)
	assertNoError(schema2, "utf8mb4", "", "utf8mb4", "utf8mb4_general_ci")
	assertNoError(schema2, "", "utf8mb4_general_ci", "utf8mb4", "utf8mb4_general_ci")
	assertNoError(schema3, "utf8mb4", "utf8mb4_unicode_ci", "utf8mb4", "utf8mb4_unicode_ci")

	// Test known error conditions
	assertError(schema1, "badcharset", "badcollation") // charset and collation are invalid
	assertError(schema2, "utf8", "latin1_swedish_ci")  // charset and collation do not match
	assertError(&schema4, "utf8mb4", "")               // schema does not actually exist in instance

	// Test successful alters
	assertNoError(schema2, "", "utf8mb4_unicode_ci", "utf8mb4", "utf8mb4_unicode_ci")
	assertNoError(schema3, "latin1", "", "latin1", "latin1_swedish_ci")
	assertNoError(schema1, "utf8mb4", "utf8mb4_general_ci", "utf8mb4", "utf8mb4_general_ci")
}

func (s TengoIntegrationSuite) TestInstanceCloneSchema(t *testing.T) {
	src := s.GetSchema(t, "testing")
	dst, err := s.d.CreateSchema("clone", "", "")
	if err != nil {
		t.Fatalf("Failed to create new empty schema: %s", err)
	}
	if tables, err := dst.Tables(); err != nil || len(tables) > 0 {
		t.Fatalf("Failed to verify no tables in new schema: err=%s, len(tables)=%d", err, len(tables))
	}
	srcTables, err := src.Tables()
	if err != nil || len(srcTables) < 1 {
		t.Fatalf("Failed to obtain non-empty table list for source: err=%s, len(tables)=%d", err, len(srcTables))
	}

	if err := s.d.CloneSchema(src, dst); err != nil {
		t.Fatalf("Failed to clone schema: %s", err)
	}
	if tables, err := dst.Tables(); err != nil || len(tables) != len(srcTables) {
		t.Errorf("Failed to verify table count in cloned schema: err=%s, len(srcTables)=%d, len(dstTables)=%d", err, len(srcTables), len(tables))
	}

	// Cloning again should fail due to table name already existing
	if err := s.d.CloneSchema(src, dst); err == nil {
		t.Error("Expected second clone attempt to fail, but error is nil")
	}

	// Verify that tables were cloned but data was not: dropping dest with
	// onlyIfEmpty==true should still succeed
	if err := s.d.DropSchema(dst, true); err != nil {
		t.Errorf("Error dropping destination schema: %s", err)
	}
}
