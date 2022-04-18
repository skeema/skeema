package main

import (
	"fmt"
	"os"
	"runtime"
	"strings"
	"testing"

	"github.com/skeema/skeema/internal/fs"
	"github.com/skeema/skeema/internal/tengo"
)

// This file contains integration tests relating to MySQL/MariaDB's
// lower_case_table_names ("LCTN") global variable. These tests are designed
// to model situations of running the database server natively on MacOS or
// Windows, which have case-insensitive filesystems.

// TestLowerCaseTableNames1 covers testing with lower_case_table_names=1
// using a separate Dockerized database. (Normally LCTN defaults to 0 on Linux,
// and it can only be changed upon instance creation.)
// This test is run in CI, or when SKEEMA_TEST_LCTN env var is set to any non-
// blank value.
func (s SkeemaIntegrationSuite) TestLowerCaseTableNames1(t *testing.T) {
	if os.Getenv("SKEEMA_TEST_LCTN") == "" && (os.Getenv("CI") == "" || os.Getenv("CI") == "0" || os.Getenv("CI") == "false") {
		t.Skip("Skipping lower_case_table_names=1 testing. To run, set env var SKEEMA_TEST_LCTN=true and/or CI=1.")
	}

	// Create an instance with lctn=1
	opts := s.d.DockerizedInstanceOptions
	opts.Name = strings.Replace(opts.Name, "skeema-test-", "skeema-test-lctn1-", 1)
	opts.CommandArgs = []string{"--lower-case-table-names=1"}
	dinst, err := s.manager.GetOrCreateInstance(opts)
	if err != nil {
		t.Fatalf("Unable to create Dockerized instance with lower-case-table-names=1: %v", err)
	}
	defer func() {
		if err := dinst.Destroy(); err != nil {
			t.Errorf("Unable to destroy test instance with LCTN=1: %v", err)
		}
	}()
	if lctnActual := dinst.NameCaseMode(); lctnActual != tengo.NameCaseLower {
		t.Fatalf("Expected Dockerized instance to have lower-case-table-names=1, instead found lower-case-table-names=%d", int(lctnActual))
	}

	// On the normal integration test db (lctn=0 as per Linux default), create a
	// mixed-case-named database and fill it with objects with mixed-case names.
	// Run `skeema init` from this db, so that we have a skeema dir that maintains
	// the mixed-case names. Afterwards, `skeema diff` should show no differences.
	s.sourceSQL(t, "lctn.sql")
	s.handleCommand(t, CodeSuccess, ".", "skeema init --dir mydb --schema NameCase -h %s -P %d lctn0", s.d.Instance.Host, s.d.Instance.Port)
	s.handleCommand(t, CodeSuccess, ".", "skeema diff lctn0")

	// Add an environment for the lctn=2 instance, and then push to it. Afterwards,
	// diff should show no differences.
	s.handleCommand(t, CodeSuccess, ".", "skeema add-environment --dir mydb -h %s -P %d lctn1", dinst.Instance.Host, dinst.Instance.Port)
	s.handleCommand(t, CodeSuccess, ".", "skeema push lctn1")
	s.handleCommand(t, CodeSuccess, ".", "skeema diff lctn1")

	// Confirm all tables on the LCTN=1 db are supported for diff operations
	schema, err := dinst.Schema("NameCase")
	if err != nil {
		t.Fatalf("Unexpected error from Instance.Schema: %v", err)
	}
	if schema.Name != "namecase" {
		t.Errorf("Expected schema name to come back lower-case from introspection, instead found %q", schema.Name)
	}
	for _, table := range schema.Tables {
		if table.UnsupportedDDL {
			t.Errorf("Table %s is unexpectedly not supported for diff operations", table.Name)
		}
	}

	// pull --skip-format should do nothing (since there's no diff) even though
	// the name capitalization differs between the FS and the DB
	s.handleCommand(t, CodeSuccess, ".", "skeema pull --skip-format lctn1")
	if contents := fs.ReadTestFile(t, "mydb/Users.sql"); !strings.Contains(contents, "`Users`") {
		t.Errorf("Expected contents of mydb/Users.sql to still have capitalized identifier, but it did not. Contents:\n%s", contents)
	}
	if contents := fs.ReadTestFile(t, "mydb/.skeema"); !strings.Contains(contents, "NameCase") {
		t.Errorf("Expected contents of mydb/.skeema to still have capitalized schema name, but it did not. Contents:\n%s", contents)
	}

	// Format should rewrite the sql files to use lower-case table names; however
	// re-running diff shouldn't yield any differences from that
	s.handleCommand(t, CodeDifferencesFound, ".", "skeema format lctn1")
	s.handleCommand(t, CodeSuccess, ".", "skeema diff lctn1")
	if contents := fs.ReadTestFile(t, "mydb/Users.sql"); !strings.Contains(contents, "`users`") {
		t.Errorf("Expected contents of mydb/Users.sql to have downcased identifier after format, but it did not. Contents:\n%s", contents)
	}

	// Diff against the *original* instance SHOULD yield an error unless
	// allow-unsafe is used, because LCTN=0 treats these all as different objects!
	s.handleCommand(t, CodeFatalError, ".", "skeema diff lctn0")
	s.handleCommand(t, CodeDifferencesFound, ".", "skeema diff --allow-unsafe lctn0")

	// Diff or push against BOTH instances (at once) should yield a fatal error,
	// even with allow-unsafe, because they have different LCTN values.  With
	// --first-only, this error should not happen.
	configFileContents := fs.ReadTestFile(t, "mydb/.skeema")
	configFileContents += fmt.Sprintf("\n\n[both]\nhost=%s:%d,%s:%d\n", s.d.Instance.Host, s.d.Instance.Port, dinst.Instance.Host, dinst.Instance.Port)
	fs.WriteTestFile(t, "mydb/.skeema", configFileContents)
	s.handleCommand(t, CodeFatalError, ".", "skeema diff --allow-unsafe both")
	s.handleCommand(t, CodeFatalError, ".", "skeema push --allow-unsafe both")
	s.handleCommand(t, CodeDifferencesFound, ".", "skeema diff --allow-unsafe --first-only both")

	// Start fresh: now we init from the LCTN=1 instance and test the opposite
	// behaviors. Note that we're explicitly using --schema on init; otherwise
	// the .skeema file schema name and subdir would both be "namecase", which
	// complicates cross-platform testing (esp since MacOS has case-insensitive
	// FS but Linux does not).
	// Also note that below, when testing pull, this confirms that dumper's FS
	// operations work properly on a case-insensitive FS (e.g. MacOS or Windows on
	// the CLIENT side running skeema / this test suite).
	fs.RemoveTestDirectory(t, "mydb")
	s.handleCommand(t, CodeSuccess, ".", "skeema init --dir mydb --schema NameCase -h %s -P %d lctn1", dinst.Instance.Host, dinst.Instance.Port)
	s.handleCommand(t, CodeSuccess, ".", "skeema diff lctn1")
	if contents := fs.ReadTestFile(t, "mydb/.skeema"); !strings.Contains(contents, "NameCase") {
		t.Fatalf("Expected contents of mydb/.skeema to maintain schema name casing as supplied, but it did not. Contents:\n%s", contents)
	}
	s.handleCommand(t, CodeSuccess, ".", "skeema add-environment --dir mydb -h %s -P %d lctn0", s.d.Instance.Host, s.d.Instance.Port)
	s.handleCommand(t, CodeFatalError, ".", "skeema diff lctn0")
	s.handleCommand(t, CodeSuccess, ".", "skeema pull lctn0")
	s.handleCommand(t, CodeSuccess, ".", "skeema diff lctn0")

	// At this point the fs reflects the LCTN=0 instance. Create a "duplicate"
	// object there with lowercase name; pull from the LCTN=0 instance to bring it
	// into the FS, and confirm a diff is clean. Then try operations on the LCTN=1
	// instance, which should error due to the "duplicate" definition in the
	// situation of a case-insensitive server.
	s.dbExec(t, "NameCase", "CREATE TABLE `users` (`ID` int) ENGINE=InnoDB")
	s.handleCommand(t, CodeSuccess, ".", "skeema pull lctn0")
	s.handleCommand(t, CodeSuccess, ".", "skeema diff lctn0")
	s.handleCommand(t, CodeFatalError, ".", "skeema diff lctn1")
	s.handleCommand(t, CodeFatalError, ".", "skeema format lctn1")
	s.handleCommand(t, CodeFatalError, ".", "skeema lint --skip-format lctn1")
	s.handleCommand(t, CodeFatalError, ".", "skeema pull --skip-format lctn1")
	s.handleCommand(t, CodeFatalError, ".", "skeema pull lctn1")
}

// TestLowerCaseTableNames2 covers testing with lower_case_table_names=2 using
// a separate Dockerized database using a bind mount. This test only works when
// executed from a MacOS host, in order for the the bind mount to have a case-
// insensitive filesystem. This test is only run when GOOS is darwin and
// additionally the SKEEMA_TEST_LCTN env var is set to any non-blank value.
func (s SkeemaIntegrationSuite) TestLowerCaseTableNames2(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skipf("Skipping lower_case_table_names=2 testing GOOS=%s (test logic requires GOOS=darwin)", runtime.GOOS)
	} else if os.Getenv("SKEEMA_TEST_LCTN") == "" {
		t.Skip("Skipping lower_case_table_names=2 testing. To run, set env var SKEEMA_TEST_LCTN=true.")
	}

	// Create an instance with lctn=2
	opts := s.d.DockerizedInstanceOptions
	opts.Name = strings.Replace(opts.Name, "skeema-test-", "skeema-test-lctn2-", 1)
	opts.CommandArgs = []string{"--lower-case-table-names=2"}
	opts.DataBindMount = t.TempDir()
	dinst, err := s.manager.GetOrCreateInstance(opts)
	if err != nil {
		t.Fatalf("Unable to create Dockerized instance with lower-case-table-names=2: %v", err)
	}
	defer func() {
		if err := dinst.Destroy(); err != nil {
			t.Errorf("Unable to destroy test instance with LCTN=2: %v", err)
		}
	}()
	if lctnActual := dinst.NameCaseMode(); lctnActual != tengo.NameCaseInsensitive {
		t.Fatalf("Expected Dockerized instance to have lower-case-table-names=2, instead found lower-case-table-names=%d", int(lctnActual))
	}

	// On the normal integration test db (lctn=0 as per Linux default), create a
	// mixed-case-named database and fill it with objects with mixed-case names.
	// Run `skeema init` from this db, so that we have a skeema dir that maintains
	// the mixed-case names. Afterwards, `skeema diff` should show no differences.
	s.sourceSQL(t, "lctn.sql")
	s.handleCommand(t, CodeSuccess, ".", "skeema init --dir mydb --schema NameCase -h %s -P %d lctn0", s.d.Instance.Host, s.d.Instance.Port)
	s.handleCommand(t, CodeSuccess, ".", "skeema diff lctn0")

	// Add an environment for the lctn=2 instance, and then push to it. Afterwards,
	// diff should show no differences.
	s.handleCommand(t, CodeSuccess, ".", "skeema add-environment --dir mydb -h %s -P %d lctn2", dinst.Instance.Host, dinst.Instance.Port)
	s.handleCommand(t, CodeSuccess, ".", "skeema push lctn2")
	s.handleCommand(t, CodeSuccess, ".", "skeema diff lctn2")

	// Confirm all tables on the LCTN=2 db are supported for diff operations, and
	// confirm the schema name comes back with original casing
	schema, err := dinst.Schema("NameCase")
	if err != nil {
		t.Fatalf("Unexpected error from Instance.Schema: %v", err)
	}
	if schema.Name != "NameCase" {
		t.Errorf("Expected schema name to come back mixed-case from introspection, instead found %q", schema.Name)
	}
	for _, table := range schema.Tables {
		if table.UnsupportedDDL {
			t.Errorf("Table %s is unexpectedly not supported for diff operations", table.Name)
		}
	}

	// pull --skip-format should do nothing (since there's no diff)
	s.handleCommand(t, CodeSuccess, ".", "skeema pull --skip-format lctn2")

	// Format should do nothing, tables maintain name casing. Ditto for pull.
	// Schema name in .skeema should also maintain original mixed casing.
	s.handleCommand(t, CodeSuccess, ".", "skeema format lctn2")
	s.handleCommand(t, CodeSuccess, ".", "skeema pull lctn2")
	if contents := fs.ReadTestFile(t, "mydb/Users.sql"); !strings.Contains(contents, "`Users`") {
		t.Errorf("Expected contents of mydb/Users.sql to still have capitalized identifier, but it did not. Contents:\n%s", contents)
	}
	if contents := fs.ReadTestFile(t, "mydb/.skeema"); !strings.Contains(contents, "NameCase") {
		t.Errorf("Expected contents of mydb/.skeema to still have capitalized schema name, but it did not. Contents:\n%s", contents)
	}

	// Diff against the *original* instance shouldn't yield an error or show any
	// differences
	s.handleCommand(t, CodeSuccess, ".", "skeema diff lctn0")

	// Diff or push against BOTH instances (at once) should yield a fatal error,
	// even with allow-unsafe, because they have different LCTN values. With
	// --first-only, this error should not happen.
	configFileContents := fs.ReadTestFile(t, "mydb/.skeema")
	configFileContents += fmt.Sprintf("\n\n[both]\nhost=%s:%d,%s:%d\n", s.d.Instance.Host, s.d.Instance.Port, dinst.Instance.Host, dinst.Instance.Port)
	fs.WriteTestFile(t, "mydb/.skeema", configFileContents)
	s.handleCommand(t, CodeFatalError, ".", "skeema diff --allow-unsafe both")
	s.handleCommand(t, CodeFatalError, ".", "skeema push --allow-unsafe both")
	s.handleCommand(t, CodeSuccess, ".", "skeema diff --first-only both")

	// Start fresh: now we init from the LCTN=2 instance and test the opposite
	// behaviors.
	fs.RemoveTestDirectory(t, "mydb")
	s.handleCommand(t, CodeSuccess, ".", "skeema init --dir mydb -h %s -P %d lctn2", dinst.Instance.Host, dinst.Instance.Port)
	s.handleCommand(t, CodeSuccess, ".", "skeema diff lctn2")
	if contents := fs.ReadTestFile(t, "mydb/NameCase/.skeema"); !strings.Contains(contents, "NameCase") {
		t.Fatalf("Expected contents of mydb/NameCase/.skeema to have mixed-case schema name, but it did not. Contents:\n%s", contents)
	}
	s.handleCommand(t, CodeSuccess, ".", "skeema add-environment --dir mydb -h %s -P %d lctn0", s.d.Instance.Host, s.d.Instance.Port)
	s.handleCommand(t, CodeSuccess, ".", "skeema diff lctn0")
	s.handleCommand(t, CodeSuccess, ".", "skeema pull lctn0")
	s.handleCommand(t, CodeSuccess, ".", "skeema diff lctn0")

	// At this point the fs reflects the LCTN=0 instance. Create a "duplicate"
	// object there with lowercase name; pull from the LCTN=0 instance to bring it
	// into the FS, and confirm a diff is clean. Then try operations on the LCTN=2
	// instance, which should error due to the "duplicate" definition in the
	// situation of a case-insensitive server.
	s.dbExec(t, "NameCase", "CREATE TABLE `users` (`ID` int) ENGINE=InnoDB")
	s.handleCommand(t, CodeSuccess, ".", "skeema pull lctn0")
	s.handleCommand(t, CodeSuccess, ".", "skeema diff lctn0")
	s.handleCommand(t, CodeFatalError, ".", "skeema diff lctn2")
	s.handleCommand(t, CodeFatalError, ".", "skeema format lctn2")
	s.handleCommand(t, CodeFatalError, ".", "skeema lint --skip-format lctn2")
	s.handleCommand(t, CodeFatalError, ".", "skeema pull --skip-format lctn2")
	s.handleCommand(t, CodeFatalError, ".", "skeema pull lctn2")
}
