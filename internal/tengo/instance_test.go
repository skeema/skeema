package tengo

import (
	"database/sql"
	"fmt"
	"net/url"
	"reflect"
	"strings"
	"testing"

	"github.com/jmoiron/sqlx"
)

func TestNewInstance(t *testing.T) {
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
		expectedInstance.m = instance.m // cheat to satisfy DeepEqual
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

func TestInstanceBuildParamString(t *testing.T) {
	assertParamString := func(defaultOptions, addOptions, expectOptions string) {
		t.Helper()
		dsn := "username:password@tcp(1.2.3.4:3306)/"
		if defaultOptions != "" {
			dsn += "?" + defaultOptions
		}
		instance, err := NewInstance("mysql", dsn)
		if err != nil {
			t.Fatalf("NewInstance(\"mysql\", \"%s\") returned error: %s", dsn, err)
		}

		// can't compare strings directly since order may be different
		result := instance.BuildParamString(addOptions)
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
	}

	assertParamString("", "", "")
	assertParamString("param1=value1", "", "param1=value1")
	assertParamString("", "param1=value1", "param1=value1")
	assertParamString("param1=value1", "param1=value1", "param1=value1")
	assertParamString("param1=value1", "param1=hello", "param1=hello")
	assertParamString("param1=value1&timeout=5s&interpolateParams=0", "param2=value2", "param1=value1&timeout=5s&interpolateParams=0&param2=value2")
	assertParamString("param1=value1&timeout=5s&interpolateParams=0", "param1=value3", "param1=value3&timeout=5s&interpolateParams=0")

	// Test special interactions between readTimeout, lock_wait_timeout, innodb_lock_wait_timeout
	assertParamString("lock_wait_timeout=20", "", "lock_wait_timeout=20")
	assertParamString("lock_wait_timeout=20", "readTimeout=10s", "readTimeout=10s&lock_wait_timeout=9&innodb_lock_wait_timeout=LEAST(9,CONVERT(@@innodb_lock_wait_timeout,SIGNED))")
	assertParamString("readTimeout=10500ms", "innodb_lock_wait_timeout=7", "readTimeout=10500ms&innodb_lock_wait_timeout=7&lock_wait_timeout=LEAST(9,CONVERT(@@lock_wait_timeout,SIGNED))")
	assertParamString("readTimeout=2.5s", "readTimeout=1.5s", "readTimeout=2s&lock_wait_timeout=1&innodb_lock_wait_timeout=1")
}

func TestInstanceIntrospectionParams(t *testing.T) {
	instance, err := NewInstance("mysql", "username:password@tcp(1.2.3.4:3306)/")
	if err != nil {
		t.Fatalf("NewInstance returned unexpected error: %v", err)
	}
	instance.valid = true // prevent calls like Flavor() from actually attempting a conn
	assertParams := func(flavor string, sqlMode, expectOptions string) {
		t.Helper()
		instance.flavor = ParseFlavor(flavor)
		instance.sqlMode = strings.Split(sqlMode, ",")

		// can't compare strings directly since order may be different
		result := instance.introspectionParams()
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
	}
	assertParams("mysql:5.7", "", "sql_quote_show_create=1&collation=binary")
	assertParams("mysql:8.0", "", "sql_quote_show_create=1&information_schema_stats_expiry=0&collation=binary")
	assertParams("mysql:5.7", "STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE", "sql_quote_show_create=1&collation=binary")
	assertParams("mysql:8.0", "STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE", "sql_quote_show_create=1&information_schema_stats_expiry=0&collation=binary")
	assertParams("mariadb:10.5", "ANSI_QUOTES", "sql_quote_show_create=1&sql_mode=%27%27")
	assertParams("mysql:5.7", "REAL_AS_FLOAT,PIPES_AS_CONCAT,ANSI_QUOTES,IGNORE_SPACE,ONLY_FULL_GROUP_BY,ANSI", "sql_quote_show_create=1&collation=binary&sql_mode=%27REAL_AS_FLOAT%2CPIPES_AS_CONCAT%2CIGNORE_SPACE%2CONLY_FULL_GROUP_BY%27")
	assertParams("mysql:8.0", "NO_FIELD_OPTIONS,NO_BACKSLASH_ESCAPES,NO_KEY_OPTIONS,NO_TABLE_OPTIONS", "sql_quote_show_create=1&collation=binary&information_schema_stats_expiry=0&sql_mode=%27NO_BACKSLASH_ESCAPES%27")
}

func (s TengoIntegrationSuite) TestInstanceConnect(t *testing.T) {
	// Connecting to invalid schema should return an error
	db, err := s.d.CachedConnectionPool("does-not-exist", "")
	if err == nil {
		t.Error("err is unexpectedly nil")
	} else if db != nil {
		t.Error("db is unexpectedly non-nil")
	}

	// Connecting without specifying a default schema should be successful
	db, err = s.d.CachedConnectionPool("", "")
	if err != nil {
		t.Errorf("Unexpected connection error: %s", err)
	} else if db == nil {
		t.Error("db is unexpectedly nil")
	}

	// Connecting again with same schema and params should return the existing connection pool
	db2, err := s.d.CachedConnectionPool("", "")
	if err != nil {
		t.Errorf("Unexpected connection error: %s", err)
	} else if db2 != db {
		t.Errorf("Expected same DB pool to be returned from identical Connect call; instead db=%v and db2=%v", db, db2)
	}

	// Connecting again with different schema should return a different connection pool
	db3, err := s.d.CachedConnectionPool("information_schema", "")
	if err != nil {
		t.Errorf("Unexpected connection error: %s", err)
	} else if db3 == db {
		t.Error("Expected different DB pool to be returned from Connect with different default db; instead was same")
	}

	// Connecting again with different params should return a different connection pool
	db4, err := s.d.CachedConnectionPool("information_schema", "foreign_key_checks=0&wait_timeout=20")
	if err != nil {
		t.Errorf("Unexpected connection error: %s", err)
	} else if db4 == db || db4 == db3 {
		t.Error("Expected different DB pool to be returned from Connect with different params; instead was same")
	}
}

func (s TengoIntegrationSuite) TestInstanceCanConnect(t *testing.T) {
	// Force a connection that has defaultParams
	dsn := s.d.BaseDSN + "?wait_timeout=5&timeout=1s"
	inst, err := NewInstance("mysql", dsn)
	if err != nil {
		t.Fatalf("Unexpected error from NewInstance: %s", err)
	}
	defer inst.CloseAll()

	if ok, err := inst.CanConnect(); !ok || err != nil {
		t.Fatalf("Unexpected return from CanConnect(): %t / %s", ok, err)
	}
	if ok, err := inst.Valid(); !ok || err != nil {
		t.Fatalf("Unexpected return from Valid(): %t / %s", ok, err)
	}

	// Stop the DockerizedInstance and confirm CanConnect result matches
	// expectation
	// This logic is commented out because it is slow and disruptive to test
	// containers that are using tmpfs: it involves stopping and restarting the
	// container, and that requires a full db re-init with a tmpfs data dir. The
	// test is known to work correctly, and CanConnect is quite simple anyway and
	// unlikely to change.
	/*
		if err := s.d.Stop(); err != nil {
			t.Fatalf("Failed to Stop instance: %s", err)
		}
		ok, connErr := inst.CanConnect()
		valid, validErr := inst.Valid()
		if err := s.d.Start(); err != nil {
			t.Fatalf("Failed to re-Start() instance: %s", err)
		}
		if err := s.d.TryConnect(); err != nil {
			t.Fatalf("Failed to reconnect after restarting instance: %s", err)
		}
		if ok || connErr == nil {
			t.Errorf("Unexpected return from TryConnect(): %t / %s", ok, connErr)
		}
		if !valid || validErr != nil { // Instance is still considered Valid since it was reachable earlier
			t.Errorf("Unexpected return from Valid(): %t / %s", ok, connErr)
		}
	*/
}

func (s TengoIntegrationSuite) TestInstanceValid(t *testing.T) {
	if ok, err := s.d.Valid(); !ok || err != nil {
		t.Fatalf("Valid() unexpectedly returned %t / %v", ok, err)
	}

	dsn := s.d.BaseDSN
	dsn = strings.Replace(dsn, s.d.Password, "wrongpass", 1)
	inst, err := NewInstance("mysql", dsn)
	if err != nil {
		t.Fatalf("Unexpected error from NewInstance: %s", err)
	}
	if ok, err := inst.Valid(); ok || err == nil {
		t.Fatalf("Valid() unexpectedly returned %t / %v despite wrong password", ok, err)
	}
	inst, err = NewInstance("mysql", s.d.BaseDSN)
	if err != nil {
		t.Fatalf("Unexpected error from NewInstance: %s", err)
	}
	defer inst.CloseAll()
	if ok, err := inst.Valid(); !ok || err != nil {
		t.Fatalf("Valid() unexpectedly returned %t / %v", ok, err)
	}
}

func (s TengoIntegrationSuite) TestInstanceNameCaseMode(t *testing.T) {
	// The instance in TengoIntegrationSuite is started without any extra command-
	// line args, so we know it will have lower_case_table_names=0 as this is the
	// default for Linux
	if lctn := s.d.NameCaseMode(); lctn != NameCaseAsIs {
		t.Errorf("Expected lower_case_table_names=0 for a valid Linux containerized DB; instead found %d", lctn)
	}

	// An invalid Instance should return a special value since it cannot be
	// introspected
	dsn := s.d.BaseDSN
	dsn = strings.Replace(dsn, s.d.Password, "wrongpass", 1)
	inst, err := NewInstance("mysql", dsn)
	if err != nil {
		t.Fatalf("Unexpected error from NewInstance: %s", err)
	}
	if lctn := inst.NameCaseMode(); lctn != NameCaseUnknown {
		t.Errorf("Expected NameCaseUnknown for a valid Linux containerized DB; instead found %d", lctn)
	}
}

func (s TengoIntegrationSuite) TestInstanceLockWaitTimeout(t *testing.T) {
	var expected int
	// lock_wait_timeout defaults to a ridiculous 1 year in MySQL. MariaDB lowered
	// it to a slightly-less-ridiculous 1 day in MariaDB 10.2.
	if s.d.Flavor().MinMariaDB(10, 2) {
		expected = 86400
	} else {
		expected = 86400 * 365
	}
	if actual := s.d.LockWaitTimeout(); actual != expected {
		t.Errorf("Expected LockWaitTimeout to return %d, instead found %d", expected, actual)
	}
}

func (s TengoIntegrationSuite) TestInstanceCloseAll(t *testing.T) {
	makePool := func(defaultSchema, params string) {
		t.Helper()
		db, err := s.d.CachedConnectionPool(defaultSchema, params)
		if err != nil {
			t.Fatalf("Unexpected connection error: %s", err)
		} else if db == nil {
			t.Fatal("db is unexpectedly nil")
		}
	}
	assertPoolCount := func(expected int) {
		t.Helper()
		if actual := len(s.d.Instance.connectionPool); actual != expected {
			t.Errorf("Expected instance to have %d connection pools; instead found %d", expected, actual)
		}
	}

	s.d.CloseAll()
	assertPoolCount(0)
	makePool("", "")
	makePool("information_schema", "")
	assertPoolCount(2)
	s.d.CloseAll()
	assertPoolCount(0)
	makePool("", "")
	assertPoolCount(1)
}

func (s TengoIntegrationSuite) TestInstanceSetFlavor(t *testing.T) {
	// Ensure flavor is hydrated already
	actualFlavor := s.d.Flavor()
	if actualFlavor == FlavorUnknown {
		t.Fatal("Unable to determine test instance flavor")
	}

	// Confirm that SetFlavor does not work once flavor hydrated
	if err := s.d.SetFlavor(ParseFlavor("mariadb:10.2")); err == nil {
		t.Error("Expected SetFlavor to return an error, but it was nil")
	}

	// Nuke the hydrated flavor, and confirm SetFlavor now works
	s.d.ForceFlavor(FlavorUnknown)
	if err := s.d.SetFlavor(actualFlavor.Family()); err != nil || s.d.Flavor() != actualFlavor.Family() {
		t.Errorf("Unexpected outcome from SetFlavor: error=%v, flavor=%s", err, s.d.Flavor())
	}
	s.d.ForceFlavor(actualFlavor)
}

// TestInstanceGrantChecks covers CanSkipBinlog on an actual
// Instance.
func (s TengoIntegrationSuite) TestInstanceGrantChecks(t *testing.T) {
	// The dockerized instance in the test should always use root creds
	if !s.d.CanSkipBinlog() {
		t.Fatal("Expected all Dockerized instances to be able to skip binlogs, but CanSkipBinlogs returned false")
	}
	if _, err := s.d.CachedConnectionPool("", "sql_log_bin=0"); err != nil {
		t.Errorf("Error connecting with sql_log_bin=0: %v", err)
	}

	// Now create a less-privileged user, and confirm behavior. Note that for
	// MariaDB 11.0, we intentionally grant SUPER since this no longer confers
	// fine-grained privs.
	var morePrivs string
	if s.d.Flavor().MinMariaDB(11, 0) {
		morePrivs = ", SUPER"
	}
	db, err := s.d.CachedConnectionPool("", "")
	if err != nil {
		t.Fatalf("Unable to establish connection pool: %v", err)
	}
	dbExec := func(query string) {
		t.Helper()
		if _, err := db.Exec(query); err != nil {
			t.Fatalf("Error running query on DockerizedInstance.\nQuery: %s\nError: %s", query, err)
		}
	}
	t.Cleanup(func() {
		dbExec("DROP USER 'notroot'@'%'")
	})
	dbExec(fmt.Sprintf("CREATE USER 'notroot'@'%%' IDENTIFIED BY '%s'", s.d.Password))
	dbExec(fmt.Sprintf("GRANT SELECT, CREATE, DROP, ALTER, INDEX, CREATE ROUTINE, ALTER ROUTINE%s ON *.* TO 'notroot'@'%%'", morePrivs))
	unprivDSN := strings.Replace(s.d.BaseDSN, s.d.User, "notroot", 1)
	unprivInst, _ := NewInstance("mysql", unprivDSN)
	if ok, err := unprivInst.Valid(); !ok {
		t.Fatalf("Valid returned false, %v", err)
	}
	defer unprivInst.CloseAll()
	if unprivInst.CanSkipBinlog() {
		t.Error("CanSkipBinlog unexpectedly returned true for unprivileged user")
	}
	if _, err := unprivInst.ConnectionPool("", "sql_log_bin=0"); err == nil {
		t.Error("Expected error connecting with sql_log_bin=0, but err was nil")
	}
}

// TestInstanceGrantChecksRegexes provides unit testing coverage of the regexes
// used by CanSkipBinlog.
func TestInstanceGrantChecksRegexes(t *testing.T) {
	inst, err := NewInstance("mysql", "username:password@tcp(1.2.3.4:3306)/")
	if err != nil {
		t.Fatalf("NewInstance returned unexpected error: %v", err)
	}
	inst.valid = true // prevent calls like Flavor() from actually attempting a conn

	// Empty grants should cause methods to return false
	inst.grants = []string{}
	if inst.CanSkipBinlog() {
		t.Error("Expected empty grants to cause Can methods to return false, but it did not")
	}

	// This set of grants should not contain anything causing CanSkipBinlog to return true
	noBinlogSkipGrants := []string{
		"GRANT USAGE ON *.* TO `foo`@`%`",
		"GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, RELOAD, SHUTDOWN, PROCESS, FILE, REFERENCES, INDEX, ALTER, SHOW DATABASES, CREATE TEMPORARY TABLES, LOCK TABLES, EXECUTE, REPLICATION SLAVE, REPLICATION CLIENT, CREATE VIEW, SHOW VIEW, CREATE ROUTINE, ALTER ROUTINE, CREATE USER, EVENT, TRIGGER, CREATE TABLESPACE, CREATE ROLE, DROP ROLE ON *.* TO `foo`@`%`",
		"GRANT APPLICATION_PASSWORD_ADMIN,AUDIT_ADMIN,BACKUP_ADMIN,BINLOG_ADMIN,BINLOG_ENCRYPTION_ADMIN,CLONE_ADMIN,CONNECTION_ADMIN,ENCRYPTION_KEY_ADMIN,GROUP_REPLICATION_ADMIN,INNODB_REDO_LOG_ARCHIVE,PERSIST_RO_VARIABLES_ADMIN,REPLICATION_APPLIER,REPLICATION_SLAVE_ADMIN,RESOURCE_GROUP_ADMIN,RESOURCE_GROUP_USER,ROLE_ADMIN,SERVICE_CONNECTION_ADMIN,SET_USER_ID,SET_ANY_DEFINER,SYSTEM_USER,TABLE_ENCRYPTION_ADMIN,XA_RECOVER_ADMIN ON *.* TO `foo`@`%`",
		"GRANT ALL PRIVILEGES ON `blarg`.* TO `foo`@`%`",
		"GRANT PROXY ON ''@'' TO 'foo'@'%' WITH GRANT OPTION",
	}
	inst.grants = noBinlogSkipGrants
	if inst.CanSkipBinlog() {
		t.Fatal("Expected CanSkipBinlog to return false, but it returned true")
	}

	// Any of these grants should be sufficient for CanSkipBinlog to return true
	binlogSkipGrants := []string{
		"GRANT ALL PRIVILEGES ON *.* TO `foo`@`%`",
		"GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, RELOAD, SHUTDOWN, PROCESS, FILE, REFERENCES, INDEX, ALTER, SHOW DATABASES, SUPER, CREATE TEMPORARY TABLES, LOCK TABLES, EXECUTE, REPLICATION SLAVE, REPLICATION CLIENT, CREATE VIEW, SHOW VIEW, CREATE ROUTINE, ALTER ROUTINE, CREATE USER, EVENT, TRIGGER, CREATE TABLESPACE, CREATE ROLE, DROP ROLE ON *.* TO `foo`@`%`",
		"GRANT APPLICATION_PASSWORD_ADMIN,AUDIT_ADMIN,BACKUP_ADMIN,BINLOG_ADMIN,BINLOG_ENCRYPTION_ADMIN,CLONE_ADMIN,CONNECTION_ADMIN,ENCRYPTION_KEY_ADMIN,GROUP_REPLICATION_ADMIN,INNODB_REDO_LOG_ARCHIVE,PERSIST_RO_VARIABLES_ADMIN,REPLICATION_APPLIER,REPLICATION_SLAVE_ADMIN,RESOURCE_GROUP_ADMIN,RESOURCE_GROUP_USER,ROLE_ADMIN,SERVICE_CONNECTION_ADMIN,SESSION_VARIABLES_ADMIN,SET_USER_ID,SYSTEM_USER,SYSTEM_VARIABLES_ADMIN,TABLE_ENCRYPTION_ADMIN,XA_RECOVER_ADMIN ON *.* TO `foo`@`%`",
		"GRANT BINLOG ADMIN ON *.* TO 'foo'@'%'", // MariaDB 10.5+, not to be confused with MySQL 8.0's BINLOG_ADMIN with an underscore!
		"GRANT SUPER ON *.* TO 'foo'@'%'",        // inst is FlavorUnknown, so it won't trip on MariaDB 11's SUPER change
	}
	for n, grant := range binlogSkipGrants {
		inst.grants = []string{}
		inst.grants = append(inst.grants, noBinlogSkipGrants...)
		inst.grants = append(inst.grants, grant)
		if !inst.CanSkipBinlog() {
			t.Errorf("Expected binlogSkipGrants[%d] to cause CanSkipBinlogs to return true, but it did not", n)
		}
	}
}

func (s TengoIntegrationSuite) TestInstanceSchemas(t *testing.T) {
	s.SourceTestSQL(t, "integration-ext.sql")

	assertSame := func(s1, s2 *Schema) {
		t.Helper()
		if s1.Name != s2.Name {
			t.Errorf("Schema names do not match: %q vs %q", s1.Name, s2.Name)
		} else {
			diff := s1.Diff(s2)
			if diffCount := len(diff.ObjectDiffs()); diffCount > 0 {
				t.Errorf("Schemas do not match: %d object diffs found", diffCount)
			}
		}
	}

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
		assertSame(schema, byName[schema.Name])
		if schema2, err := s.d.Schema(schema.Name); err != nil {
			t.Errorf("Unexpected error from Schema(%q): %v", schema.Name, err)
		} else {
			assertSame(schema, schema2)
		}
		if has, err := s.d.HasSchema(schema.Name); !has || err != nil {
			t.Errorf("Expected HasSchema(%s)==true, instead found false / %v", schema.Name, err)
		}
	}

	// Test SchemasByName with args
	byName, err = s.d.SchemasByName("testcharset", "doesnt_exist", "testcharcoll")
	if err != nil {
		t.Errorf("SchemasByName returned error: %s", err)
	}
	if len(byName) != 2 {
		t.Errorf("SchemasByName returned wrong number of results; expected 2, found %d", len(byName))
	}
	for name, schema := range byName {
		if name != schema.Name || (name != "testcharset" && name != "testcharcoll") {
			t.Errorf("SchemasByName returned mismatching schema: key=%s, name=%s", name, schema.Name)
		}
	}

	// Test negative responses
	if has, err := s.d.HasSchema("doesnt_exist"); has || err != nil {
		t.Error("HasSchema(doesnt_exist) unexpectedly returning true")
	}
	if schema, err := s.d.Schema("doesnt_exist"); schema != nil || err != sql.ErrNoRows {
		t.Errorf("Expected Schema(doesnt_exist) to return nil,sql.ErrNoRows; instead found %v,%s", schema, err)
	}
}

func (s TengoIntegrationSuite) TestInstanceShowCreateTable(t *testing.T) {
	t1create, err1 := s.d.ShowCreateTable("testing", "actor")
	t2create, err2 := s.d.ShowCreateTable("testing", "actor_in_film")
	if err1 != nil || err2 != nil || t1create == "" || t2create == "" {
		t.Fatalf("Unable to obtain SHOW CREATE TABLE output: err1=%s, err2=%s", err1, err2)
	}

	t1expected := aTableForFlavor(s.d.Flavor(), 1)
	if t1create != t1expected.CreateStatement {
		t.Errorf("Mismatch for SHOW CREATE TABLE\nActual return from %s:\n%s\n----------\nExpected output: %s", s.d.Flavor(), t1create, t1expected.CreateStatement)
	}

	t2expected := anotherTableForFlavor(s.d.Flavor())
	if t2create != t2expected.CreateStatement {
		t.Errorf("Mismatch for SHOW CREATE TABLE\nActual return from %s:\n%s\n----------\nExpected output: %s", s.d.Flavor(), t2create, t2expected.CreateStatement)
	}

	// Test nonexistent table
	t3create, err3 := s.d.ShowCreateTable("testing", "doesnt_exist")
	if t3create != "" || err3 == nil {
		t.Errorf("Expected ShowCreateTable on invalid table to return empty string and error, instead err=%s, output=%s", err3, t3create)
	}
}

func (s TengoIntegrationSuite) TestInstanceTableSize(t *testing.T) {
	s.SourceTestSQL(t, "rows.sql")

	// Test table with at least one row
	size, err := s.d.TableSize("testing", "has_rows")
	if err != nil || size < 1 {
		t.Errorf("Unexpected return from TableSize: %d, %v", size, err)
	}

	// Test table with no rows: TableSize should return 0 as a special case
	size, err = s.d.TableSize("testing", "no_rows")
	if err != nil || size != 0 {
		t.Errorf("Unexpected return from TableSize: %d, %v", size, err)
	}

	// Test nonexistent table
	size, err = s.d.TableSize("testing", "doesnt_exist")
	if err == nil || size != 0 {
		t.Errorf("Unexpected return from TableSize: %d, %v", size, err)
	}
}

func (s TengoIntegrationSuite) TestInstanceFindNonEmptyTables(t *testing.T) {
	s.SourceTestSQL(t, "rows.sql")
	if nonEmpties, err := s.d.FindNonEmptyTables("testing", []string{"has_rows", "no_rows"}); err != nil {
		t.Errorf("Unexpected error from FindNonEmptyTables: %v", err)
	} else if len(nonEmpties) != 1 || nonEmpties[0] != "has_rows" {
		t.Errorf("Unexpected result from FindNonEmptyTables: %v", nonEmpties)
	}

	if nonEmpties, err := s.d.FindNonEmptyTables("testing", []string{"actor", "actor_in_film", "has_rows", "no_rows"}); err != nil {
		t.Errorf("Unexpected error from FindNonEmptyTables: %v", err)
	} else if len(nonEmpties) != 1 || nonEmpties[0] != "has_rows" {
		t.Errorf("Unexpected result from FindNonEmptyTables: %v", nonEmpties)
	}

	// Test error condition via nonexistent table
	if _, err := s.d.FindNonEmptyTables("testing", []string{"actor", "doesnt_exist", "has_rows", "no_rows"}); err == nil {
		t.Error("Expected error from FindNonEmptyTables, but err was nil")
	}
}

func (s TengoIntegrationSuite) TestInstanceCreateSchema(t *testing.T) {
	opts := SchemaCreationOptions{
		DefaultCharSet:   "utf8mb4",
		DefaultCollation: "utf8mb4_unicode_ci",
	}
	_, err := s.d.CreateSchema("foobar", opts)
	if err != nil {
		t.Fatalf("CreateSchema returned unexpected error: %s", err)
	}
	if refetch, err := s.d.Schema("foobar"); err != nil {
		t.Errorf("Unable to fetch newly created schema: %s", err)
	} else if refetch.CharSet != "utf8mb4" || refetch.Collation != "utf8mb4_unicode_ci" {
		t.Errorf("Unexpected charset or collation on refetched schema: %+v", refetch)
	}

	// Ensure creation of duplicate schema fails with error
	if _, err := s.d.CreateSchema("foobar", opts); err == nil {
		t.Error("Expected creation of duplicate schema to return an error, but it did not")
	}

	// Creation of schema without specifying charset and collation should use
	// instance defaults
	defCharSet, defCollation, err := s.d.DefaultCharSetAndCollation()
	if err != nil {
		t.Fatalf("Unable to obtain instance default charset and collation")
	}
	if schema, err := s.d.CreateSchema("barfoo", SchemaCreationOptions{}); err != nil {
		t.Errorf("Failed to create schema with default charset and collation: %s", err)
	} else if schema.CharSet != defCharSet || schema.Collation != defCollation {
		t.Errorf("Expected charset/collation to be %s/%s, instead found %s/%s", defCharSet, defCollation, schema.CharSet, schema.Collation)
	}
}

func (s TengoIntegrationSuite) TestInstanceDropSchema(t *testing.T) {
	s.SourceTestSQL(t, "integration-ext.sql", "rows.sql")

	opts := BulkDropOptions{
		ChunkSize:       6,
		OnlyIfEmpty:     true,
		PartitionsFirst: true,
	}
	// Dropping a schema with non-empty tables when OnlyIfEmpty==true should fail,
	// with and without OneShot
	if err := s.d.DropSchema("testing", opts); err == nil {
		t.Error("Expected dropping a schema with tables to fail when OnlyIfEmpty==true, but it did not")
	}
	opts.OneShot = true
	if err := s.d.DropSchema("testing", opts); err == nil {
		t.Error("Expected dropping a schema with tables to fail when OnlyIfEmpty==true, but it did not")
	}
	opts.OneShot = false

	// Dropping a schema without tables when OnlyIfEmpty==true should succeed
	if err := s.d.DropSchema("testcollate", opts); err != nil {
		t.Errorf("Expected dropping a schema without tables to succeed when OnlyIfEmpty==true, but error=%s", err)
	}

	// Dropping a schema with only empty tables when OnlyIfEmpty==true should succeed
	if err := s.d.DropSchema("testcharcoll", opts); err != nil {
		t.Errorf("Expected dropping a schema with only empty tables to succeed when OnlyIfEmpty==true, but error=%s", err)
	}

	// Dropping a schema with non-empty tables when OnlyIfEmpty==false should succeed
	opts.OnlyIfEmpty = false
	if err := s.d.DropSchema("testing", opts); err != nil {
		t.Errorf("Expected dropping a schema with tables to succeed when OnlyIfEmpty==false, but error=%s", err)
	}

	// Dropping a schema that doesn't exist should fail
	if err := s.d.DropSchema("testing", opts); err == nil {
		t.Error("Expected dropping a nonexistent schema to fail, but error was nil")
	}
}

func (s TengoIntegrationSuite) TestInstanceDropTablesInSchemaByRef(t *testing.T) {
	schema := s.GetSchema(t, "testing")
	if len(schema.Tables) == 0 {
		t.Fatal("Assertion failure: schema `testing` has no tables to start")
	}
	opts := BulkDropOptions{
		ChunkSize: 3,
		Schema:    schema,
	}
	if err := s.d.DropTablesInSchema("testing", opts); err != nil {
		t.Fatalf("Unexpected error from DropTablesInSchema: %v", err)
	}
	if schema = s.GetSchema(t, "testing"); len(schema.Tables) > 0 {
		t.Errorf("Expected schema `testing` to have no tables after DropTablesInSchema, instead found %d", len(schema.Tables))
	}

	// Repeated call SHOULD error, because the tables in schema.Tables no
	// longer exist!
	if err := s.d.DropTablesInSchema("testing", opts); err == nil {
		t.Error("Expected error from DropTablesInSchema, but return was nil")
	}
}

// TestInstanceDropTablesSkipsViews tests the behavior of
// Instance.DropTablesInSchema when views are present in the schema. Although
// this package does not support views, presence of them should not break
// behavior. This test also confirms some assumptions regarding views and
// information_schema.partitions for the current flavor.
func (s TengoIntegrationSuite) TestInstanceDropTablesSkipsViews(t *testing.T) {
	// Create two views, including one with an invalid DEFINER, which intentionally
	// prevents queries on the view from working.
	s.SourceTestSQL(t, "views.sql")

	// Confirm I_S assumptions present in logic similar to tablesToPartitions:
	// no views there except in MySQL 8 / PS 8; if views are there, they have
	// data_length 0.
	// Explicit AS clauses needed for compatibility with MySQL 8 data dictionary,
	// otherwise results come back with uppercase col names, breaking Select
	var partitions []struct {
		TableName  string `db:"table_name"`
		DataLength int64  `db:"data_length"`
	}
	query := `
		SELECT   p.table_name AS table_name,
		         p.data_length AS data_length
		FROM     information_schema.partitions p
		WHERE    p.table_schema = 'testing' AND
		         p.table_name LIKE 'view%'`
	db, err := s.d.CachedConnectionPool("", "")
	if err != nil {
		t.Fatalf("Unexpected error obtaining connection pool: %v", err)
	}
	if err := db.Select(&partitions, query); err != nil {
		t.Fatalf("Unexpected error querying information_schema.partitions: %v", err)
	}
	if flavor := s.d.Flavor(); flavor.MinMySQL(8) {
		if len(partitions) != 2 {
			t.Fatalf("Expected flavor %s to have 2 views present in information_schema.partitions; instead found %d", flavor, len(partitions))
		}
		for _, part := range partitions {
			if part.DataLength != 0 {
				t.Fatalf("Expected view %s to have data_length of 0, instead found %d", part.TableName, part.DataLength)
			}
		}
	} else if len(partitions) != 0 {
		t.Fatalf("Expected flavor %s to have no views present in information_schema.partitions; instead found %d", flavor, len(partitions))
	}

	// Now drop all tables in testing, and confirm this does not return an error.
	// If views weren't ignored, there are two ways this could error: DROP TABLE
	// will error on a view; and even before that, since we use OnlyIfEmpty, the
	// SELECT on a view with a bad definer will also error.
	opts := BulkDropOptions{
		ChunkSize:       5,
		OnlyIfEmpty:     true,
		PartitionsFirst: true,
	}
	if err := s.d.DropTablesInSchema("testing", opts); err != nil {
		t.Errorf("Unexpected error from DropTablesInSchema with views present: %v", err)
	}
}

func (s TengoIntegrationSuite) TestInstanceDropRoutinesInSchema(t *testing.T) {
	// testing schema contains several routines in testdata/integration.sql
	schema := s.GetSchema(t, "testing")
	if len(schema.Routines) == 0 {
		t.Fatal("Assertion failure: schema `testing` has no routines to start")
	}
	opts := BulkDropOptions{
		ChunkSize: 2,
	}
	if err := s.d.DropRoutinesInSchema("testing", opts); err != nil {
		t.Fatalf("Unexpected error from DropRoutinesInSchema: %v", err)
	}
	if schema = s.GetSchema(t, "testing"); len(schema.Routines) > 0 {
		t.Errorf("Expected schema `testing` to have no routines after DropRoutinesInSchema, instead found %d", len(schema.Routines))
	}

	// Repeated calls should have no effect, no error.
	if err := s.d.DropRoutinesInSchema("testing", opts); err != nil {
		t.Errorf("Unexpected error from DropRoutinesInSchema: %v", err)
	}

	// Calling on a nonexistent schema name should return an error.
	if err := s.d.DropRoutinesInSchema("doesntexist", opts); err == nil {
		t.Error("Expected error from DropRoutinesInSchema on nonexistent schema; instead err was nil")
	}
}

func (s TengoIntegrationSuite) TestInstanceDropRoutinesInSchemaByRef(t *testing.T) {
	// testing schema contains several routines in testdata/integration.sql
	schema := s.GetSchema(t, "testing")
	if len(schema.Routines) == 0 {
		t.Fatal("Assertion failure: schema `testing` has no routines to start")
	}
	opts := BulkDropOptions{
		Schema: schema,
	}
	if err := s.d.DropRoutinesInSchema("testing", opts); err != nil {
		t.Fatalf("Unexpected error from DropRoutinesInSchema: %v", err)
	}
	if schema = s.GetSchema(t, "testing"); len(schema.Routines) > 0 {
		t.Errorf("Expected schema `testing` to have no routines after DropRoutinesInSchema, instead found %d", len(schema.Routines))
	}

	// Repeated calls should have no effect, no error despite passing routines in
	// opts.Schema, because IF EXISTS is used in the queries
	if err := s.d.DropRoutinesInSchema("testing", opts); err != nil {
		t.Errorf("Unexpected error from DropRoutinesInSchema: %v", err)
	}
}

func (s TengoIntegrationSuite) TestInstanceAlterSchema(t *testing.T) {
	s.SourceTestSQL(t, "integration-ext.sql")

	assertNoError := func(schemaName, newCharSet, newCollation, expectCharSet, expectCollation string) {
		t.Helper()
		opts := SchemaCreationOptions{
			DefaultCharSet:   newCharSet,
			DefaultCollation: newCollation,
		}
		if err := s.d.AlterSchema(schemaName, opts); err != nil {
			t.Errorf("Expected alter of %s to (%s,%s) would not error, but returned %s", schemaName, newCharSet, newCollation, err)
		} else {
			schema, err := s.d.Schema(schemaName)
			if err != nil {
				t.Fatalf("Unexpected error fetching schema: %s", err)
			}
			if schema.CharSet != expectCharSet {
				t.Errorf("Expected post-alter charset to be %s, instead found %s", expectCharSet, schema.CharSet)
			}
			if schema.Collation != expectCollation {
				t.Errorf("Expected post-alter collation to be %s, instead found %s", expectCollation, schema.Collation)
			}
		}
	}
	assertError := func(schemaName, newCharSet, newCollation string) {
		t.Helper()
		opts := SchemaCreationOptions{
			DefaultCharSet:   newCharSet,
			DefaultCollation: newCollation,
		}
		if err := s.d.AlterSchema(schemaName, opts); err == nil {
			t.Errorf("Expected alter of %s to (%s,%s) would return error, but returned nil instead", schemaName, newCharSet, newCollation)
		}
	}

	instCharSet, instCollation, err := s.d.DefaultCharSetAndCollation()
	if err != nil {
		t.Fatalf("Unable to fetch instance default charset and collation: %s", err)
	}

	// Default collation for utf8mb4 depends on the flavor
	defaultCollationMB4 := DefaultCollationForCharset("utf8mb4", s.d.Instance)

	// `testing` has instance-default charset and collation
	// `testcharset` has utf8mb4 charset with its default collation (varies by flavor)
	// `testcharcoll` has utf8mb4 with utf8mb4_unicode_ci

	// Test no-op conditions
	assertNoError("testing", "", "", instCharSet, instCollation)
	assertNoError("testcharset", "utf8mb4", "", "utf8mb4", defaultCollationMB4)
	assertNoError("testcharset", "", defaultCollationMB4, "utf8mb4", defaultCollationMB4)
	assertNoError("testcharcoll", "utf8mb4", "utf8mb4_unicode_ci", "utf8mb4", "utf8mb4_unicode_ci")

	// Test known error conditions
	assertError("testing", "badcharset", "badcollation")    // charset and collation are invalid
	assertError("testcharset", "utf8", "latin1_swedish_ci") // charset and collation do not match
	assertError("nonexistent", "utf8mb4", "")               // schema does not actually exist in instance

	// Test successful alters
	assertNoError("testcharset", "", "utf8mb4_unicode_ci", "utf8mb4", "utf8mb4_unicode_ci")
	assertNoError("testcharcoll", "latin1", "", "latin1", "latin1_swedish_ci")
	assertNoError("testing", "utf8mb4", "utf8mb4_general_ci", "utf8mb4", "utf8mb4_general_ci")
}

func (s TengoIntegrationSuite) TestInstanceProcessList(t *testing.T) {
	// Since ProcessList is only intended to be used ad-hoc for debugging in CI,
	// we don't currently include test coverage of it beyond confirming the method
	// returns at least one entry (we should always at least see the connection
	// that queried the processlist; there may or may not be other system threads)
	processes, err := s.d.ProcessList()
	if err != nil {
		t.Fatalf("Unexpected error from Instance.ProcessList: %v", err)
	}
	if len(processes) == 0 {
		t.Error("Instance.ProcessList unexpectedly returned 0 rows")
	}
}
