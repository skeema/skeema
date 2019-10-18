package main

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/skeema/mybase"
	"github.com/skeema/skeema/fs"
	"github.com/skeema/tengo"
)

func (s SkeemaIntegrationSuite) TestInitHandler(t *testing.T) {
	s.handleCommand(t, CodeBadConfig, ".", "skeema init") // no host

	// Invalid environment name
	s.handleCommand(t, CodeBadConfig, ".", "skeema init --dir mydb -h %s -P %d '[nope]'", s.d.Instance.Host, s.d.Instance.Port)

	// Specifying a single schema that doesn't exist on the instance
	s.handleCommand(t, CodeBadConfig, ".", "skeema init --dir mydb -h %s -P %d --schema doesntexist", s.d.Instance.Host, s.d.Instance.Port)

	// Specifying a single schema that is a system schema
	s.handleCommand(t, CodeBadConfig, ".", "skeema init --dir mydb -h %s -P %d --schema mysql", s.d.Instance.Host, s.d.Instance.Port)

	// Successful standard execution. Also confirm user is not persisted to .skeema
	// since not specified on CLI.
	cfg := s.handleCommand(t, CodeSuccess, ".", "skeema init --dir mydb -h %s -P %d", s.d.Instance.Host, s.d.Instance.Port)
	s.verifyFiles(t, cfg, "../golden/init")
	if _, setsOption := getOptionFile(t, "mydb", cfg).OptionValue("user"); setsOption {
		t.Error("Did not expect user to be persisted to .skeema, but it was")
	}

	// Specifying an unreachable host should fail with fatal error
	s.handleCommand(t, CodeFatalError, ".", "skeema init --dir baddb -h %s -P %d", s.d.Instance.Host, s.d.Instance.Port-100)

	// host-wrapper with no output should fail
	s.handleCommand(t, CodeBadConfig, ".", "skeema init --dir baddb -h xyz --host-wrapper='echo'")

	// Test successful init with --user specified on CLI, persisting to .skeema
	cfg = s.handleCommand(t, CodeSuccess, ".", "skeema init --dir withuser -h %s -P %d --user root", s.d.Instance.Host, s.d.Instance.Port)
	if _, setsOption := getOptionFile(t, "withuser", cfg).OptionValue("user"); !setsOption {
		t.Error("Expected user to be persisted to .skeema, but it was not")
	}

	// Can't init into a dir with existing option file
	s.handleCommand(t, CodeBadConfig, ".", "skeema init --dir mydb -h %s -P %d", s.d.Instance.Host, s.d.Instance.Port)

	// Can't init off of base dir that already specifies a schema
	s.handleCommand(t, CodeBadConfig, "mydb/product", "skeema init --dir mydb -h %s -P %d", s.d.Instance.Host, s.d.Instance.Port)

	// Test successful init for a single schema. Source a SQL file first that,
	// among other things, changes the default charset and collation for the
	// schema in question.
	s.sourceSQL(t, "push1.sql")
	cfg = s.handleCommand(t, CodeSuccess, ".", "skeema init --dir combined -h %s -P %d --schema product ", s.d.Instance.Host, s.d.Instance.Port)
	dir, err := fs.ParseDir("combined", cfg)
	if err != nil {
		t.Fatalf("Unexpected error from ParseDir: %s", err)
	}
	mybase.AssertFileSetsOptions(t, dir.OptionFile, "host", "schema", "default-character-set", "default-collation")
	if subdirs, err := dir.Subdirs(); err != nil {
		t.Fatalf("Unexpected error listing subdirs of %s: %v", dir, err)
	} else if len(subdirs) > 0 {
		t.Errorf("Expected %s to have no subdirs, but it has %d", dir, len(subdirs))
	} else {
		for _, sub := range subdirs {
			if sub.ParseError != nil {
				t.Errorf("Unexpected parse error in %s: %v", sub, sub.ParseError)
			}
		}
	}
	if len(dir.SQLFiles) < 1 {
		t.Errorf("Expected %s to have *.sql files, but it does not", dir)
	}

	// Test successful init without a --dir. Also test persistence of --connect-options.
	expectDir := fmt.Sprintf("%s:%d", s.d.Instance.Host, s.d.Instance.Port)
	if _, err = os.Stat(expectDir); err == nil {
		t.Fatalf("Expected dir %s to not exist yet, but it does", expectDir)
	}
	cfg = s.handleCommand(t, CodeSuccess, ".", "skeema init -h %s -P %d --connect-options='wait_timeout=3'", s.d.Instance.Host, s.d.Instance.Port)
	if dir, err = fs.ParseDir(expectDir, cfg); err != nil {
		t.Fatalf("Unexpected error from ParseDir: %s", err)
	}
	mybase.AssertFileSetsOptions(t, dir.OptionFile, "host", "port", "connect-options")
	mybase.AssertFileMissingOptions(t, dir.OptionFile, "schema", "default-character-set", "default-collation")

	// Test successful init on a schema that isn't strict-mode compliant
	s.dbExecWithParams(t, "product", "sql_mode=%27NO_ENGINE_SUBSTITUTION%27", "ALTER TABLE posts MODIFY COLUMN created_at datetime NOT NULL DEFAULT '0000-00-00 00:00:00'")
	cfg = s.handleCommand(t, CodeSuccess, ".", "skeema init -h %s -P %d --dir nonstrict", s.d.Instance.Host, s.d.Instance.Port)
	if dir, err = fs.ParseDir("nonstrict", cfg); err != nil {
		t.Fatalf("Unexpected error from ParseDir: %s", err)
	}
	value, _ := dir.OptionFile.OptionValue("connect-options")
	if !strings.Contains(value, "innodb_strict_mode=0") {
		t.Errorf("Expected non-strict-compliant schema to use relaxed connect-options; instead found connect-options=%s", value)
	}

	// init should fail if a parent dir has an invalid .skeema file
	fs.MakeTestDirectory(t, "hasbadoptions")
	fs.WriteTestFile(t, "hasbadoptions/.skeema", "invalid file will not parse")
	s.handleCommand(t, CodeFatalError, "hasbadoptions", "skeema init -h %s -P %d", s.d.Instance.Host, s.d.Instance.Port)

	// init should fail if the --dir specifies an existing non-directory file; or
	// if the --dir already contains a subdir matching a schema name; or if the
	// --dir already contains a .sql file and --schema was used to only do 1 level
	fs.WriteTestFile(t, "nondir", "foo bar")
	s.handleCommand(t, CodeBadConfig, ".", "skeema init --dir nondir -h %s -P %d", s.d.Instance.Host, s.d.Instance.Port)
	fs.WriteTestFile(t, "alreadyexists/product/.skeema", "schema=product\n")
	s.handleCommand(t, CodeCantCreate, ".", "skeema init --dir alreadyexists -h %s -P %d", s.d.Instance.Host, s.d.Instance.Port)
	fs.MakeTestDirectory(t, "hassql")
	fs.WriteTestFile(t, "hassql/foo.sql", "foo")
	s.handleCommand(t, CodeBadConfig, ".", "skeema init --dir hassql --schema product -h %s -P %d", s.d.Instance.Host, s.d.Instance.Port)
}

func (s SkeemaIntegrationSuite) TestAddEnvHandler(t *testing.T) {
	cfg := s.handleCommand(t, CodeSuccess, ".", "skeema init --dir mydb -h %s -P %d", s.d.Instance.Host, s.d.Instance.Port)

	// add-environment should fail on a dir that does not exist
	s.handleCommand(t, CodeBadConfig, ".", "skeema add-environment --host my.staging.db.com --dir does/not/exist staging")

	// add-environment should fail on a dir that does not already contain a .skeema file
	s.handleCommand(t, CodeBadConfig, ".", "skeema add-environment --host my.staging.db.com staging")

	// bad environment name should fail
	s.handleCommand(t, CodeBadConfig, ".", "skeema add-environment --host my.staging.db.com --dir mydb '[staging]'")

	// preexisting environment name should fail
	s.handleCommand(t, CodeBadConfig, ".", "skeema add-environment --host my.staging.db.com --dir mydb production")

	// non-host-level directory should fail
	s.handleCommand(t, CodeBadConfig, ".", "skeema add-environment --host my.staging.db.com --dir mydb/product staging")

	// lack of host on CLI should fail
	s.handleCommand(t, CodeBadConfig, ".", "skeema add-environment --dir mydb staging")

	// None of the above failed commands should have modified any files
	s.verifyFiles(t, cfg, "../golden/init")
	origFile := getOptionFile(t, "mydb", cfg)

	// valid dir should succeed and add the section to the .skeema file
	// Intentionally using a low connection timeout here to avoid delaying the
	// test with the invalid hostname
	cfg = s.handleCommand(t, CodeSuccess, ".", "skeema add-environment --host my.staging.invalid --dir mydb staging --connect-options='timeout=10ms'")
	file := getOptionFile(t, "mydb", cfg)
	origFile.SetOptionValue("staging", "host", "my.staging.invalid")
	origFile.SetOptionValue("staging", "port", "3306")
	origFile.SetOptionValue("staging", "connect-options", "timeout=10ms")
	if !origFile.SameContents(file) {
		t.Fatalf("File contents of %s do not match expectation", file.Path())
	}

	// Nonstandard port should work properly; ditto for user option persisting
	cfg = s.handleCommand(t, CodeSuccess, ".", "skeema add-environment --host my.ci.invalid -P 3307 -ufoobar --dir mydb ci  --connect-options='timeout=10ms'")
	file = getOptionFile(t, "mydb", cfg)
	origFile.SetOptionValue("ci", "host", "my.ci.invalid")
	origFile.SetOptionValue("ci", "port", "3307")
	origFile.SetOptionValue("ci", "user", "foobar")
	origFile.SetOptionValue("ci", "connect-options", "timeout=10ms")
	if !origFile.SameContents(file) {
		t.Fatalf("File contents of %s do not match expectation", file.Path())
	}

	// localhost and socket should work properly
	s.handleCommand(t, CodeSuccess, ".", "skeema add-environment -h localhost -S /var/lib/mysql/mysql.sock --dir mydb development")
	file = getOptionFile(t, "mydb", cfg)
	origFile.SetOptionValue("development", "host", "localhost")
	origFile.SetOptionValue("development", "socket", "/var/lib/mysql/mysql.sock")
	if !origFile.SameContents(file) {
		t.Fatalf("File contents of %s do not match expectation", file.Path())
	}

	// valid instance should work properly and even populate flavor. Also confirm
	// persistence of ignore-schema and ignore-table.
	cfg = s.handleCommand(t, CodeSuccess, "mydb", "skeema add-environment --ignore-schema='^test' --ignore-table='^_' --host %s:%d cloud", s.d.Instance.Host, s.d.Instance.Port)
	file = getOptionFile(t, "mydb", cfg)
	origFile.SetOptionValue("cloud", "host", s.d.Instance.Host)
	origFile.SetOptionValue("cloud", "port", fmt.Sprintf("%d", s.d.Instance.Port))
	origFile.SetOptionValue("cloud", "ignore-schema", "^test")
	origFile.SetOptionValue("cloud", "ignore-table", "^_")
	origFile.SetOptionValue("cloud", "flavor", s.d.Flavor().String())
	if !origFile.SameContents(file) {
		t.Fatalf("File contents of %s do not match expectation", file.Path())
	}
}

func (s SkeemaIntegrationSuite) TestPullHandler(t *testing.T) {
	s.handleCommand(t, CodeSuccess, ".", "skeema init --dir mydb -h %s -P %d", s.d.Instance.Host, s.d.Instance.Port)

	// In product db, alter one table and drop one table;
	// In analytics db, add one table and alter the schema's charset and collation;
	// Create a new db and put one table in it
	s.sourceSQL(t, "pull1.sql")
	cfg := s.handleCommand(t, CodeSuccess, ".", "skeema pull")
	s.verifyFiles(t, cfg, "../golden/pull1")

	// Revert db back to previous state, and pull again to test the opposite
	// behaviors: delete dir for new schema, restore charset/collation in .skeema,
	// etc. Also edit the host .skeema file to remove flavor, to test logic that
	// adds/updates flavor on pull.
	s.cleanData(t, "setup.sql")
	fs.WriteTestFile(t, "mydb/.skeema", strings.Replace(fs.ReadTestFile(t, "mydb/.skeema"), "flavor", "#flavor", 1))
	cfg = s.handleCommand(t, CodeSuccess, ".", "skeema pull")
	s.verifyFiles(t, cfg, "../golden/init")

	// Files with invalid SQL should still be corrected upon pull. Files with
	// nonstandard formatting of their CREATE TABLE should be normalized, even if
	// there was an ignored auto-increment change. Files with extraneous text
	// before/after the CREATE TABLE should remain as-is, regardless of whether
	// there were other changes triggering a file rewrite. Files containing
	// commands plus a table that doesn't exist should be deleted, instead of
	// leaving a file with lingering commands.
	contents := fs.ReadTestFile(t, "mydb/analytics/activity.sql")
	fs.WriteTestFile(t, "mydb/analytics/activity.sql", strings.Replace(contents, "DEFAULT", "DEFALUT", 1))
	s.dbExec(t, "product", "INSERT INTO comments (post_id, user_id) VALUES (555, 777)")
	contents = fs.ReadTestFile(t, "mydb/product/comments.sql")
	fs.WriteTestFile(t, "mydb/product/comments.sql", strings.Replace(contents, "`", "", -1))
	contents = fs.ReadTestFile(t, "mydb/product/posts.sql")
	fs.WriteTestFile(t, "mydb/product/posts.sql", fmt.Sprintf("# random comment\n%s", contents))
	fs.WriteTestFile(t, "mydb/product/noexist.sql", "DELIMITER //\nCREATE TABLE noexist (id int)//\nDELIMITER ;\n")
	cfg = s.handleCommand(t, CodeSuccess, ".", "skeema pull --debug")
	s.verifyFiles(t, cfg, "../golden/init")
	contents = fs.ReadTestFile(t, "mydb/product/posts.sql")
	if !strings.Contains(contents, "# random comment") {
		t.Error("Expected mydb/product/posts.sql to retain its extraneous comment, but it was removed")
	}
	fs.WriteTestFile(t, "mydb/product/posts.sql", strings.Replace(contents, "`", "", -1))
	cfg = s.handleCommand(t, CodeSuccess, ".", "skeema pull --debug")
	s.verifyFiles(t, cfg, "../golden/init")
	if !strings.Contains(contents, "# random comment") {
		t.Error("Expected mydb/product/posts.sql to retain its extraneous comment, but it was removed")
	}

	// Test behavior with --skip-new-schemas: new schema should not have a dir in
	// fs, but changes to existing schemas should still be made
	s.sourceSQL(t, "pull1.sql")
	s.handleCommand(t, CodeSuccess, ".", "skeema pull --skip-new-schemas")
	if _, err := os.Stat("mydb/archives"); !os.IsNotExist(err) {
		t.Errorf("Expected os.Stat to return IsNotExist error for mydb/archives; instead err=%v", err)
	}
	if _, err := os.Stat("mydb/analytics/widget_counts.sql"); err != nil {
		t.Errorf("Expected os.Stat to return nil error for mydb/analytics/widget_counts.sql; instead err=%v", err)
	}

	// If a dir has a bad option file, new schema detection should also be skipped,
	// since we don't know what schemas the bad subdir maps to
	fs.WriteTestFile(t, "mydb/analytics/.skeema", "this won't parse anymore")
	s.handleCommand(t, CodePartialError, ".", "skeema pull")
	if _, err := os.Stat("mydb/archives"); !os.IsNotExist(err) {
		t.Errorf("Expected os.Stat to return IsNotExist error for mydb/archives; instead err=%v", err)
	}

	// Test pull behavior on a "flat" layout (single dir defining host and schema):
	// ensure flavor updated; ensure deleted sql file brought back
	s.handleCommand(t, CodeSuccess, ".", "skeema init --schema product --dir flat -h %s -P %d", s.d.Instance.Host, s.d.Instance.Port)
	fs.RemoveTestFile(t, "flat/users.sql")
	fs.WriteTestFile(t, "flat/.skeema", strings.Replace(fs.ReadTestFile(t, "flat/.skeema"), "flavor", "####", 1))
	s.handleCommand(t, CodeSuccess, "flat", "skeema pull")
	if _, err := os.Stat("flat/users.sql"); err != nil {
		t.Errorf("Expected os.Stat to return nil error for flat/users.sql; instead err=%v", err)
	}
	if contents := fs.ReadTestFile(t, "flat/.skeema"); !strings.Contains(contents, "flavor") {
		t.Error("Expected flat/.skeema to contain flavor after pull, but it does not")
	}
}

func (s SkeemaIntegrationSuite) TestLintHandler(t *testing.T) {
	s.handleCommand(t, CodeSuccess, ".", "skeema init --dir mydb -h %s -P %d", s.d.Instance.Host, s.d.Instance.Port)

	// Initial lint should be a no-op that returns exit code 0
	cfg := s.handleCommand(t, CodeSuccess, ".", "skeema lint")
	s.verifyFiles(t, cfg, "../golden/init")

	// Invalid options should error with CodeBadConfig
	s.handleCommand(t, CodeBadConfig, ".", "skeema lint --workspace=doesnt-exist")
	s.handleCommand(t, CodeBadConfig, "mydb/product", "skeema lint --password=wrong")

	// Alter a few files in a way that is still valid SQL, but doesn't match
	// the database's native format. Lint with --skip-format should do nothing;
	// otherwise lint with default of format should rewrite these files and then
	// return exit code CodeDifferencesFound.
	productDir, err := fs.ParseDir("mydb/product", cfg)
	if err != nil {
		t.Fatalf("Unable to obtain dir for mydb/product: %s", err)
	}
	if len(productDir.SQLFiles) < 4 {
		t.Fatalf("Unable to obtain *.sql files from %s", productDir)
	}
	rewriteFiles := func(includeSyntaxError bool) {
		for n, sf := range productDir.SQLFiles {
			contents := fs.ReadTestFile(t, sf.Path())
			switch n {
			case 0:
				if includeSyntaxError {
					contents = strings.Replace(contents, "DEFAULT", "DEFALUT", 1)
				}
			case 1:
				contents = strings.ToLower(contents)
			case 2:
				contents = strings.Replace(contents, "`", "", -1)
			case 3:
				contents = strings.Replace(contents, " ", "  ", -1)
			}
			fs.WriteTestFile(t, sf.Path(), contents)
		}
	}
	rewriteFiles(false)
	s.handleCommand(t, CodeSuccess, ".", "skeema lint --skip-format")
	s.handleCommand(t, CodeDifferencesFound, ".", "skeema lint")
	s.verifyFiles(t, cfg, "../golden/init")

	// Add a new file with invalid SQL, and also make the previous valid rewrites.
	// Lint should rewrite the valid files but return exit code CodeFatalError due
	// to there being at least 1 file with invalid SQL.
	rewriteFiles(true)
	s.handleCommand(t, CodeFatalError, ".", "skeema lint")

	// Manually restore the file with invalid SQL; the files should now verify,
	// confirming that the fatal error did not prevent the other files from being
	// reformatted; re-linting should yield no changes.
	contents := fs.ReadTestFile(t, productDir.SQLFiles[0].Path())
	fs.WriteTestFile(t, productDir.SQLFiles[0].Path(), strings.Replace(contents, "DEFALUT", "DEFAULT", 1))
	s.verifyFiles(t, cfg, "../golden/init")
	s.handleCommand(t, CodeSuccess, ".", "skeema lint")

	// Files with SQL statements unsupported by this package should yield a
	// warning, resulting in CodeDifferencesFound
	fs.WriteTestFile(t, productDir.SQLFiles[0].Path(), "INSERT INTO foo (col1, col2) VALUES (123, 456)")
	s.handleCommand(t, CodeDifferencesFound, ".", "skeema lint")

	// Directories that have invalid options should yield CodeBadConfig
	fs.WriteTestFile(t, "mydb/uhoh/.skeema", "this is not a valid .skeema file")
	s.handleCommand(t, CodeBadConfig, ".", "skeema lint")
}

func (s SkeemaIntegrationSuite) TestFormatHandler(t *testing.T) {
	s.handleCommand(t, CodeSuccess, ".", "skeema init --dir mydb -h %s -P %d", s.d.Instance.Host, s.d.Instance.Port)

	// Initial format should be a no-op that returns exit code 0
	cfg := s.handleCommand(t, CodeSuccess, ".", "skeema format")
	s.verifyFiles(t, cfg, "../golden/init")

	// Invalid options should error with CodeBadConfig
	s.handleCommand(t, CodeBadConfig, ".", "skeema format --workspace=doesnt-exist")
	s.handleCommand(t, CodeBadConfig, "mydb/product", "skeema format --password=wrong")

	// Alter a few files in a way that is still valid SQL, but doesn't match
	// the database's native format. Format with --skip-write should return exit
	// CodeDifferencesFound repeatedly; format with default (--write) should return
	// CodeDifferencesFound followed by CodeSuccess.
	productDir, err := fs.ParseDir("mydb/product", cfg)
	if err != nil {
		t.Fatalf("Unable to obtain dir for mydb/product: %s", err)
	}
	if len(productDir.SQLFiles) < 4 {
		t.Fatalf("Unable to obtain *.sql files from %s", productDir)
	}
	rewriteFiles := func(includeSyntaxError bool) {
		for n, sf := range productDir.SQLFiles {
			contents := fs.ReadTestFile(t, sf.Path())
			switch n {
			case 0:
				if includeSyntaxError {
					contents = strings.Replace(contents, "DEFAULT", "DEFALUT", 1)
				}
			case 1:
				contents = strings.ToLower(contents)
			case 2:
				contents = strings.Replace(contents, "`", "", -1)
			case 3:
				contents = strings.Replace(contents, " ", "  ", -1)
			}
			fs.WriteTestFile(t, sf.Path(), contents)
		}
	}
	rewriteFiles(false)
	s.handleCommand(t, CodeDifferencesFound, ".", "skeema format --skip-write")
	s.handleCommand(t, CodeDifferencesFound, ".", "skeema format --skip-write")
	s.handleCommand(t, CodeDifferencesFound, ".", "skeema format")
	s.handleCommand(t, CodeSuccess, ".", "skeema format")
	s.verifyFiles(t, cfg, "../golden/init")

	// Change a file to contain invalid SQL; format should return
	// CodeDifferencesFound repeatedly
	rewriteFiles(true)
	s.handleCommand(t, CodeDifferencesFound, ".", "skeema format")
	s.handleCommand(t, CodeDifferencesFound, ".", "skeema format")

	// Manually restore the file with invalid SQL; the files should now verify,
	// confirming that the fatal error did not prevent the other files from being
	// reformatted; re-formatting again should yield no changes.
	contents := fs.ReadTestFile(t, productDir.SQLFiles[0].Path())
	fs.WriteTestFile(t, productDir.SQLFiles[0].Path(), strings.Replace(contents, "DEFALUT", "DEFAULT", 1))
	s.verifyFiles(t, cfg, "../golden/init")
	s.handleCommand(t, CodeSuccess, ".", "skeema format")

	// Files with SQL statements unsupported by this package should not affect
	// exit code
	fs.WriteTestFile(t, productDir.SQLFiles[0].Path(), "INSERT INTO foo (col1, col2) VALUES (123, 456)")
	s.handleCommand(t, CodeSuccess, ".", "skeema format --debug")

	// Directories that have invalid options should yield CodeBadConfig
	fs.WriteTestFile(t, "mydb/uhoh/.skeema", "this is not a valid .skeema file")
	s.handleCommand(t, CodeBadConfig, ".", "skeema format")
}

func (s SkeemaIntegrationSuite) TestDiffHandler(t *testing.T) {
	s.handleCommand(t, CodeSuccess, ".", "skeema init --dir mydb -h %s -P %d", s.d.Instance.Host, s.d.Instance.Port)

	// no-op diff should yield no differences
	s.handleCommand(t, CodeSuccess, ".", "skeema diff")

	// --host and --schema should error if supplied on CLI
	s.handleCommand(t, CodeBadConfig, ".", "skeema diff --host=1.2.3.4 --schema=whatever")

	// It isn't possible to disable --dry-run with diff
	cfg := s.handleCommand(t, CodeSuccess, ".", "skeema diff --skip-dry-run")
	if !cfg.GetBool("dry-run") {
		t.Error("Expected --skip-dry-run to have no effect on `skeema diff`, but it disabled dry-run")
	}

	s.dbExec(t, "analytics", "ALTER TABLE pageviews DROP COLUMN domain")
	s.handleCommand(t, CodeDifferencesFound, ".", "skeema diff")

	// Confirm --brief works as expected
	oldStdout := os.Stdout
	if outFile, err := os.Create("diff-brief.out"); err != nil {
		t.Fatalf("Unable to redirect stdout to a file: %s", err)
	} else {
		os.Stdout = outFile
		s.handleCommand(t, CodeDifferencesFound, ".", "skeema diff --brief")
		outFile.Close()
		os.Stdout = oldStdout
		expectOut := fmt.Sprintf("%s\n", s.d.Instance)
		actualOut := fs.ReadTestFile(t, "diff-brief.out")
		if actualOut != expectOut {
			t.Errorf("Unexpected output from `skeema diff --brief`\nExpected:\n%sActual:\n%s", expectOut, actualOut)
		}
		if err := os.Remove("diff-brief.out"); err != nil {
			t.Fatalf("Unable to delete diff-brief.out: %s", err)
		}
	}
}

func (s SkeemaIntegrationSuite) TestPushHandler(t *testing.T) {
	s.handleCommand(t, CodeSuccess, ".", "skeema init --dir mydb -h %s -P %d", s.d.Instance.Host, s.d.Instance.Port)

	// Verify clean-slate operation: wipe the DB; push; wipe the files; re-init
	// the files; verify the files match. The push inherently verifies creation of
	// schemas and tables.
	s.cleanData(t)
	s.handleCommand(t, CodeSuccess, ".", "skeema push")
	s.reinitAndVerifyFiles(t, "", "")

	// Test bad option values
	s.handleCommand(t, CodeBadConfig, ".", "skeema push --concurrent-instances=0")
	s.handleCommand(t, CodeBadConfig, ".", "skeema push --alter-algorithm=invalid")
	s.handleCommand(t, CodeBadConfig, ".", "skeema push --alter-lock=invalid")
	s.handleCommand(t, CodeBadConfig, ".", "skeema push --ignore-table='+'")
	s.handleCommand(t, CodeBadConfig, ".", "skeema push --lint-charset=gentle-nudge")

	// Make some changes on the db side, mix of safe and unsafe changes to
	// multiple schemas. Remember, subsequent pushes will effectively be UN-DOING
	// what push1.sql did, since we updated the db but not the filesystem.
	s.sourceSQL(t, "push1.sql")

	// push from base dir, without any args, should succeed for schemas with safe
	// changes (analytics) but not for schemas with 1 or more unsafe changes
	// (product). It shouldn't not affect the `bonus` schema (which exists on db
	// but not on filesystem, but push should never drop schemas)
	s.handleCommand(t, CodeFatalError, "", "skeema push")            // CodeFatalError due to unsafe changes not being allowed
	s.handleCommand(t, CodeSuccess, "mydb/analytics", "skeema diff") // analytics dir was pushed fine tho
	s.assertTableExists(t, "analytics", "pageviews", "")             // re-created by push
	s.assertTableMissing(t, "product", "users", "credits")           // product DDL skipped due to unsafe stmt
	s.assertTableExists(t, "product", "posts", "featured")           // product DDL skipped due to unsafe stmt
	s.assertTableExists(t, "bonus", "placeholder", "")               // not affected by push (never drops schemas)

	// The "skip whole schema upon unsafe stmt" rule also affects schema-level DDL
	if product, err := s.d.Schema("product"); err != nil || product == nil {
		t.Fatalf("Unexpected error obtaining schema: %s", err)
	} else {
		if product.CharSet != "utf8" || product.Collation != "utf8_swedish_ci" {
			t.Errorf("Expected schema should have charset/collation=utf8/utf8_swedish_ci from push1.sql, instead found %s/%s", product.CharSet, product.Collation)
		}
	}

	// Delete *.sql file for analytics.rollups. Push from analytics dir with
	// --safe-below-size=1 should fail since it has a row. Delete that row and
	// try again, should succeed that time.
	if err := os.Remove("mydb/analytics/rollups.sql"); err != nil {
		t.Fatalf("Unexpected error removing a file: %s", err)
	}
	s.handleCommand(t, CodeFatalError, "mydb/analytics", "skeema push --safe-below-size=1")
	s.assertTableExists(t, "analytics", "rollups", "")
	s.dbExec(t, "analytics", "DELETE FROM rollups")
	s.handleCommand(t, CodeSuccess, "mydb/analytics", "skeema push --safe-below-size=1")
	s.assertTableMissing(t, "analytics", "rollups", "")

	// push from base dir, with --allow-unsafe, will permit the changes to product
	// schema to proceed
	s.handleCommand(t, CodeSuccess, ".", "skeema push --allow-unsafe")
	s.assertTableMissing(t, "product", "posts", "featured")
	s.assertTableExists(t, "product", "users", "credits")
	s.assertTableExists(t, "bonus", "placeholder", "")
	if product, err := s.d.Schema("product"); err != nil || product == nil {
		t.Fatalf("Unexpected error obtaining schema: %s", err)
	} else {
		serverCharSet, serverCollation, err := s.d.DefaultCharSetAndCollation()
		if err != nil {
			t.Fatalf("Unable to obtain server default charset and collation: %s", err)
		}
		if serverCharSet != product.CharSet || serverCollation != product.Collation {
			t.Errorf("Expected schema should have charset/collation=%s/%s, instead found %s/%s", serverCharSet, serverCollation, product.CharSet, product.Collation)
		}
	}

	// invalid SQL prevents push from working in an entire dir, but not in a
	// dir for a different schema
	contents := fs.ReadTestFile(t, "mydb/product/comments.sql")
	fs.WriteTestFile(t, "mydb/product/comments.sql", strings.Replace(contents, "PRIMARY KEY", "foo int,\nPRIMARY KEY", 1))
	contents = fs.ReadTestFile(t, "mydb/product/users.sql")
	fs.WriteTestFile(t, "mydb/product/users.sql", strings.Replace(contents, "PRIMARY KEY", "foo int INVALID SQL HERE,\nPRIMARY KEY", 1))
	fs.WriteTestFile(t, "mydb/bonus/.skeema", "schema=bonus\n")
	fs.WriteTestFile(t, "mydb/bonus/placeholder.sql", "CREATE TABLE placeholder (id int unsigned NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB")
	fs.WriteTestFile(t, "mydb/bonus/table2.sql", "CREATE TABLE table2 (name varchar(20) NOT NULL, PRIMARY KEY (name))")
	s.handleCommand(t, CodeFatalError, ".", "skeema push")
	s.assertTableMissing(t, "product", "comments", "foo")
	s.assertTableMissing(t, "product", "users", "foo")
	s.assertTableExists(t, "bonus", "table2", "")

	// confirm that lint errors (in modified objects only) prevent push:
	// drop a PK from a table in bonus schema in the db;
	// pull to restore valid filesystem state after prev test;
	// remove PKs from 2 tables in the filesystem in product dir;
	// add a col to a different table in bonus schema;
	// try pushing and confirm no changes are made in product schema (due to
	// lint failure), but bonus change proceeds (since the PK-less table there was
	// not modified in this diff)
	s.dbExec(t, "bonus", "ALTER TABLE placeholder DROP PRIMARY KEY")
	s.handleCommand(t, CodeSuccess, ".", "skeema pull")
	fs.WriteTestFile(t, "mydb/bonus/table2.sql", "CREATE TABLE table2 (name varchar(20) NOT NULL, newcol int, PRIMARY KEY (name))")
	contents = fs.ReadTestFile(t, "mydb/product/users.sql")
	fs.WriteTestFile(t, "mydb/product/users.sql", strings.Replace(contents, "PRIMARY KEY", "KEY", 1))
	contents = fs.ReadTestFile(t, "mydb/product/posts.sql")
	fs.WriteTestFile(t, "mydb/product/posts.sql", strings.Replace(contents, "PRIMARY KEY", "KEY", 1))
	s.handleCommand(t, CodeFatalError, ".", "skeema push --lint-pk=error")
	s.assertTableExists(t, "bonus", "table2", "newcol")
	s.handleCommand(t, CodeDifferencesFound, "mydb/product", "skeema diff")

	// Confirm behavior of --skip-lint even with --lint-pk=error
	s.handleCommand(t, CodeSuccess, ".", "skeema push --lint-pk=error --skip-lint")
	s.handleCommand(t, CodeSuccess, ".", "skeema diff --lint-pk=error")
}

func (s SkeemaIntegrationSuite) TestHelpHandler(t *testing.T) {
	// Simple tests just to confirm the commands don't error
	fs.WriteTestFile(t, "fake-etc/skeema", "# hello world")
	s.handleCommand(t, CodeSuccess, ".", "skeema")
	s.handleCommand(t, CodeSuccess, ".", "skeema help")
	s.handleCommand(t, CodeSuccess, ".", "skeema --help")
	s.handleCommand(t, CodeSuccess, ".", "skeema --help=add-environment")
	s.handleCommand(t, CodeSuccess, ".", "skeema help add-environment")
	s.handleCommand(t, CodeSuccess, ".", "skeema add-environment --help")
	s.handleCommand(t, CodeFatalError, ".", "skeema help doesntexist")
	s.handleCommand(t, CodeFatalError, ".", "skeema --help=doesntexist")
}

func (s SkeemaIntegrationSuite) TestIndexOrdering(t *testing.T) {
	s.handleCommand(t, CodeSuccess, ".", "skeema init --dir mydb -h %s -P %d", s.d.Instance.Host, s.d.Instance.Port)

	// Add 6 new redundant indexes to posts.sql. Place them before the existing
	// secondary index.
	contentsOrig := fs.ReadTestFile(t, "mydb/product/posts.sql")
	lines := make([]string, 6)
	for n := range lines {
		lines[n] = fmt.Sprintf("KEY `idxnew_%d` (`created_at`)", n)
	}
	joinedLines := strings.Join(lines, ",\n  ")
	contentsIndexesFirst := strings.Replace(contentsOrig, "PRIMARY KEY (`id`),\n", fmt.Sprintf("PRIMARY KEY (`id`),\n  %s,\n", joinedLines), 1)
	fs.WriteTestFile(t, "mydb/product/posts.sql", contentsIndexesFirst)

	// push should add the indexes, and afterwards diff should report no
	// differences, even though the index order in the file differs from what is
	// in mysql
	s.handleCommand(t, CodeSuccess, "", "skeema push")
	s.handleCommand(t, CodeSuccess, "", "skeema diff")

	// however, diff --exact-match can see the differences
	s.handleCommand(t, CodeDifferencesFound, "", "skeema diff --exact-match")

	// pull should re-write the file such that the indexes are now last, just like
	// what's actually in mysql
	s.handleCommand(t, CodeSuccess, "", "skeema pull")
	contentsIndexesLast := strings.Replace(contentsOrig, ")\n", fmt.Sprintf("),\n  %s\n", joinedLines), 1)
	if fileContents := fs.ReadTestFile(t, "mydb/product/posts.sql"); fileContents == contentsIndexesFirst {
		t.Error("Expected skeema pull to rewrite mydb/product/posts.sql to put indexes last, but file remained unchanged")
	} else if fileContents != contentsIndexesLast {
		t.Errorf("Expected skeema pull to rewrite mydb/product/posts.sql to put indexes last, but it did something else entirely. Contents:\n%s\nExpected:\n%s\n", fileContents, contentsIndexesLast)
	}

	// Edit posts.sql to put the new indexes first again, and ensure
	// push --exact-match actually reorders them.
	fs.WriteTestFile(t, "mydb/product/posts.sql", contentsIndexesFirst)
	if major, minor, _ := s.d.Version(); major == 5 && minor == 5 {
		s.handleCommand(t, CodeSuccess, "", "skeema push --exact-match")
	} else {
		s.handleCommand(t, CodeSuccess, "", "skeema push --exact-match --alter-algorithm=copy")
	}
	s.handleCommand(t, CodeSuccess, "", "skeema diff")
	s.handleCommand(t, CodeSuccess, "", "skeema diff --exact-match")
	s.handleCommand(t, CodeSuccess, "", "skeema pull")
	if fileContents := fs.ReadTestFile(t, "mydb/product/posts.sql"); fileContents != contentsIndexesFirst {
		t.Errorf("Expected skeema pull to have no effect at this point, but instead file now looks like this:\n%s", fileContents)
	}
}

func (s SkeemaIntegrationSuite) TestForeignKeys(t *testing.T) {
	s.sourceSQL(t, "foreignkey.sql")
	s.handleCommand(t, CodeSuccess, ".", "skeema init --dir mydb -h %s -P %d", s.d.Instance.Host, s.d.Instance.Port)

	// Renaming an FK should not be considered a difference by default
	oldContents := fs.ReadTestFile(t, "mydb/product/posts.sql")
	contents1 := strings.Replace(oldContents, "user_fk", "usridfk", 1)
	if oldContents == contents1 {
		t.Fatal("Expected mydb/product/posts.sql to contain foreign key definition, but it did not")
	}
	fs.WriteTestFile(t, "mydb/product/posts.sql", contents1)
	s.handleCommand(t, CodeSuccess, ".", "skeema diff")

	// pull won't update the file unless reformatting
	s.handleCommand(t, CodeSuccess, ".", "skeema pull --skip-format")
	if fs.ReadTestFile(t, "mydb/product/posts.sql") != contents1 {
		t.Error("Expected skeema pull --skip-format to leave file untouched, but it rewrote it")
	}
	s.handleCommand(t, CodeSuccess, ".", "skeema pull")
	if fs.ReadTestFile(t, "mydb/product/posts.sql") != oldContents {
		t.Error("Expected skeema pull to rewrite file, but it did not")
	}

	// Renaming an FK should be considered a difference with --exact-match
	fs.WriteTestFile(t, "mydb/product/posts.sql", contents1)
	s.handleCommand(t, CodeDifferencesFound, ".", "skeema diff --exact-match")
	s.handleCommand(t, CodeSuccess, ".", "skeema push --exact-match")
	s.handleCommand(t, CodeSuccess, ".", "skeema diff")

	// Changing an FK definition should not break push or pull, even though this
	// will be two non-noop ALTERs to the same table
	contents2 := strings.Replace(contents1,
		"FOREIGN KEY (`user_id`) REFERENCES `users` (`id`)",
		"FOREIGN KEY (`user_id`, `byline`) REFERENCES `users` (`id`, `name`)",
		1)
	if contents2 == contents1 {
		t.Fatal("Failed to update contents as expected")
	}
	fs.WriteTestFile(t, "mydb/product/posts.sql", contents2)
	s.handleCommand(t, CodeDifferencesFound, ".", "skeema diff")
	s.handleCommand(t, CodeSuccess, ".", "skeema push")
	fs.WriteTestFile(t, "mydb/product/posts.sql", contents1)
	s.handleCommand(t, CodeSuccess, ".", "skeema pull --skip-format")
	if fs.ReadTestFile(t, "mydb/product/posts.sql") != contents2 {
		t.Error("Expected skeema pull to rewrite file, but it did not")
	}

	// Confirm that adding foreign keys occurs after other changes: construct
	// a scenario where we're adding an FK that needs a new index on the "parent"
	// (referenced) table, where the parent table name is alphabetically after
	// the child table
	s.dbExec(t, "product", "ALTER TABLE posts DROP FOREIGN KEY usridfk")
	s.dbExec(t, "product", "ALTER TABLE users DROP KEY idname")
	s.handleCommand(t, CodeSuccess, ".", "skeema push")

	// Test handling of unsafe operations combined with FK operations:
	// Drop FK + add different FK + drop another col
	// Confirm that if an unsafe operation is blocked, but there's also a 2nd
	// ALTER for same table (due to splitting of drop FK + add FK into separate
	// ALTERs) that both ALTERs are skipped.
	contents3 := strings.Replace(oldContents, "`body` text,\n", "", 1)
	contents3 = strings.Replace(contents3, "`body` text DEFAULT NULL,\n", "", 1) // MariaDB 10.2+
	if strings.Contains(contents3, "`body`") || !strings.Contains(contents3, "`user_fk`") {
		t.Fatal("Failed to update contents as expected")
	}
	fs.WriteTestFile(t, "mydb/product/posts.sql", contents3)
	s.handleCommand(t, CodeFatalError, ".", "skeema push")
	s.handleCommand(t, CodeSuccess, ".", "skeema pull")
	checkContents := fs.ReadTestFile(t, "mydb/product/posts.sql")
	if !strings.Contains(checkContents, "`body`") || strings.Contains(checkContents, "`user_fk`") {
		t.Error("Unsafe status did not properly affect both ALTERs on the table")
	}

	// Test adding an FK where the existing data does not meet the constraint:
	// should fail if foreign_key_checks=1, succeed if foreign_key_checks=0
	s.dbExec(t, "product", "ALTER TABLE posts DROP FOREIGN KEY usridfk")
	s.dbExec(t, "product", "INSERT INTO posts (user_id, byline) VALUES (1234, 'someone')")
	fs.WriteTestFile(t, "mydb/product/posts.sql", contents1)
	s.handleCommand(t, CodeFatalError, ".", "skeema push --foreign-key-checks")
	s.handleCommand(t, CodeSuccess, ".", "skeema push")
}

func (s SkeemaIntegrationSuite) TestAutoInc(t *testing.T) {
	// Insert 2 rows into product.users, so that next auto-inc value is now 3
	s.dbExec(t, "product", "INSERT INTO users (name) VALUES (?), (?)", "foo", "bar")

	// Normal init omits auto-inc values. diff views this as no differences.
	s.reinitAndVerifyFiles(t, "", "")
	s.handleCommand(t, CodeSuccess, ".", "skeema diff")

	// pull and lint should make no changes
	cfg := s.handleCommand(t, CodeSuccess, ".", "skeema pull")
	s.verifyFiles(t, cfg, "../golden/init")
	cfg = s.handleCommand(t, CodeSuccess, ".", "skeema lint")
	s.verifyFiles(t, cfg, "../golden/init")

	// pull with --include-auto-inc should include auto-inc values greater than 1
	cfg = s.handleCommand(t, CodeSuccess, ".", "skeema pull --include-auto-inc")
	s.verifyFiles(t, cfg, "../golden/autoinc")
	s.handleCommand(t, CodeSuccess, ".", "skeema diff")

	// Inserting another row should still be ignored by diffs
	s.dbExec(t, "product", "INSERT INTO users (name) VALUES (?)", "something")
	s.handleCommand(t, CodeSuccess, ".", "skeema diff")

	// However, if table's next auto-inc is LOWER than sqlfile's, this is a
	// difference.
	s.dbExec(t, "product", "DELETE FROM users WHERE id > 1")
	s.dbExec(t, "product", "ALTER TABLE users AUTO_INCREMENT=2")
	s.handleCommand(t, CodeDifferencesFound, ".", "skeema diff")
	s.handleCommand(t, CodeSuccess, ".", "skeema push")
	s.handleCommand(t, CodeSuccess, ".", "skeema diff")

	// init with --include-auto-inc should include auto-inc values greater than 1
	s.reinitAndVerifyFiles(t, "--include-auto-inc", "../golden/autoinc")

	// now that the file has a next auto-inc value, subsequent pull operations
	// should update the value, even without --include-auto-inc
	s.dbExec(t, "product", "INSERT INTO users (name) VALUES (?)", "something")
	s.handleCommand(t, CodeSuccess, ".", "skeema pull")
	if !strings.Contains(fs.ReadTestFile(t, "mydb/product/users.sql"), "AUTO_INCREMENT=4") {
		t.Error("Expected mydb/product/users.sql to contain AUTO_INCREMENT=4 after pull, but it did not")
	}

}

func (s SkeemaIntegrationSuite) TestUnsupportedAlter(t *testing.T) {
	s.sourceSQL(t, "unsupported1.sql")

	// init should work fine with an unsupported table
	s.reinitAndVerifyFiles(t, "", "../golden/unsupported")

	// Back to clean slate for db and files
	s.cleanData(t, "setup.sql")
	s.reinitAndVerifyFiles(t, "", "../golden/init")

	// apply change to db directly, and confirm pull still works
	s.sourceSQL(t, "unsupported1.sql")
	cfg := s.handleCommand(t, CodeSuccess, ".", "skeema pull --debug")
	s.verifyFiles(t, cfg, "../golden/unsupported")

	// back to clean slate for db only
	s.cleanData(t, "setup.sql")

	// lint should be able to fix formatting problems in unsupported table files
	contents := fs.ReadTestFile(t, "mydb/product/subscriptions.sql")
	fs.WriteTestFile(t, "mydb/product/subscriptions.sql", strings.Replace(contents, "`", "", -1))
	s.handleCommand(t, CodeDifferencesFound, ".", "skeema lint")
	s.verifyFiles(t, cfg, "../golden/unsupported")

	// diff should return CodeDifferencesFound, vs push should return
	// CodePartialError
	s.handleCommand(t, CodeDifferencesFound, ".", "skeema diff --debug")
	s.handleCommand(t, CodePartialError, ".", "skeema push")

	// diff/push still ok if *creating* or *dropping* unsupported table
	s.dbExec(t, "product", "DROP TABLE subscriptions")
	s.assertTableMissing(t, "product", "subscriptions", "")
	s.handleCommand(t, CodeDifferencesFound, ".", "skeema diff")
	s.handleCommand(t, CodeSuccess, ".", "skeema push")
	s.assertTableExists(t, "product", "subscriptions", "")
	if err := os.Remove("mydb/product/subscriptions.sql"); err != nil {
		t.Fatalf("Unexpected error removing a file: %s", err)
	}
	s.handleCommand(t, CodeDifferencesFound, ".", "skeema diff --allow-unsafe")
	s.handleCommand(t, CodeSuccess, ".", "skeema push --allow-unsafe")
	s.assertTableMissing(t, "product", "subscriptions", "")
}

func (s SkeemaIntegrationSuite) TestIgnoreOptions(t *testing.T) {
	s.sourceSQL(t, "ignore1.sql")

	// init: valid regexes should work properly and persist to option files
	cfg := s.handleCommand(t, CodeSuccess, ".", "skeema init --dir mydb -h %s -P %d --ignore-schema='^archives$' --ignore-table='^_'", s.d.Instance.Host, s.d.Instance.Port)
	s.verifyFiles(t, cfg, "../golden/ignore")

	// pull: nothing should be updated due to ignore options. Ditto even if we add
	// a dir with schema name corresponding to ignored schema.
	cfg = s.handleCommand(t, CodeSuccess, ".", "skeema pull")
	s.verifyFiles(t, cfg, "../golden/ignore")
	fs.WriteTestFile(t, "mydb/archives/.skeema", "schema=archives")
	s.handleCommand(t, CodeSuccess, ".", "skeema pull")
	if _, err := os.Stat("mydb/archives/foo.sql"); err == nil {
		t.Error("ignore-options not affecting `skeema pull` as expected")
	}

	// diff/push: no differences. This should still be the case even if we add a
	// file corresponding to an ignored table, with a different definition than
	// the db has.
	s.handleCommand(t, CodeSuccess, ".", "skeema diff")
	fs.WriteTestFile(t, "mydb/product/_widgets.sql", "CREATE TABLE _widgets (id int) ENGINE=InnoDB;\n")
	fs.WriteTestFile(t, "mydb/analytics/_newtable.sql", "CREATE TABLE _newtable (id int) ENGINE=InnoDB;\n")
	fs.WriteTestFile(t, "mydb/archives/bar.sql", "CREATE TABLE bar (id int) ENGINE=InnoDB;\n")
	s.handleCommand(t, CodeSuccess, ".", "skeema diff")

	// pull should also ignore that file corresponding to an ignored table
	s.handleCommand(t, CodeSuccess, ".", "skeema pull")
	if fs.ReadTestFile(t, "mydb/product/_widgets.sql") != "CREATE TABLE _widgets (id int) ENGINE=InnoDB;\n" {
		t.Error("Expected pull to ignore mydb/product/_widgets.sql entirely, but it did not")
	}

	// lint: ignored tables should be ignored
	// To set up this test, we do a pull that overrides the previous ignore options
	// and then edit those files so that they contain formatting mistakes or even
	// invalid SQL.
	s.handleCommand(t, CodeSuccess, ".", "skeema pull --ignore-table=''")
	contents := fs.ReadTestFile(t, "mydb/analytics/_trending.sql")
	newContents := strings.Replace(contents, "`", "", -1)
	fs.WriteTestFile(t, "mydb/analytics/_trending.sql", newContents)
	fs.WriteTestFile(t, "mydb/analytics/_hmm.sql", "CREATE TABLE _hmm uhoh this is not valid;\n")
	fs.RemoveTestFile(t, "mydb/archives/bar.sql")
	s.handleCommand(t, CodeSuccess, ".", "skeema lint")
	if fs.ReadTestFile(t, "mydb/analytics/_trending.sql") != newContents {
		t.Error("Expected `skeema lint` to ignore mydb/analytics/_trending.sql, but it did not")
	}

	// push, pull, lint, format, init: invalid regexes should error. Error is
	// CodeBadConfig except for cases of invalid ignore-schema being hit in
	// fs.Dir.SchemaNames().
	s.handleCommand(t, CodeBadConfig, ".", "skeema lint --ignore-table='+'")
	s.handleCommand(t, CodeBadConfig, ".", "skeema format --ignore-table='+'")
	s.handleCommand(t, CodeBadConfig, ".", "skeema pull --ignore-table='+'")
	s.handleCommand(t, CodeFatalError, ".", "skeema pull --ignore-schema='+'")
	s.handleCommand(t, CodeBadConfig, ".", "skeema push --ignore-table='+'")
	s.handleCommand(t, CodeFatalError, ".", "skeema push --ignore-schema='+'")
	s.handleCommand(t, CodeBadConfig, ".", "skeema init --dir badre1 -h %s -P %d --ignore-schema='+'", s.d.Instance.Host, s.d.Instance.Port)
	s.handleCommand(t, CodeBadConfig, ".", "skeema init --dir badre2 -h %s -P %d --ignore-table='+'", s.d.Instance.Host, s.d.Instance.Port)
}

func (s SkeemaIntegrationSuite) TestDirEdgeCases(t *testing.T) {
	s.handleCommand(t, CodeSuccess, ".", "skeema init --dir mydb -h %s -P %d", s.d.Instance.Host, s.d.Instance.Port)

	// Invalid option file should break all commands
	oldContents := fs.ReadTestFile(t, "mydb/.skeema")
	fs.WriteTestFile(t, "mydb/.skeema", "invalid contents\n")
	s.handleCommand(t, CodeFatalError, "mydb", "skeema pull")
	s.handleCommand(t, CodeFatalError, "mydb", "skeema diff")
	s.handleCommand(t, CodeFatalError, "mydb", "skeema lint")
	s.handleCommand(t, CodeFatalError, "mydb", "skeema format")
	s.handleCommand(t, CodeFatalError, ".", "skeema add-environment --host my.staging.db.com --dir mydb staging")
	fs.WriteTestFile(t, "mydb/.skeema", oldContents)

	// Hidden directories are ignored, even if they contain a .skeema file, whether
	// valid or invalid.
	fs.WriteTestFile(t, ".hidden/.skeema", "invalid contents\n")
	fs.WriteTestFile(t, ".hidden/whatever.sql", "CREATE TABLE whatever (this is not valid SQL oh well)")
	fs.WriteTestFile(t, "mydb/.hidden/.skeema", "schema=whatever\n")
	fs.WriteTestFile(t, "mydb/.hidden/whatever.sql", "CREATE TABLE whatever (this is not valid SQL oh well)")
	fs.WriteTestFile(t, "mydb/product/.hidden/.skeema", "schema=whatever\n")
	fs.WriteTestFile(t, "mydb/product/.hidden/whatever.sql", "CREATE TABLE whatever (this is not valid SQL oh well)")
	s.handleCommand(t, CodeSuccess, ".", "skeema pull")
	s.handleCommand(t, CodeSuccess, ".", "skeema diff")
	s.handleCommand(t, CodeSuccess, ".", "skeema lint")

	// Extra subdirs with .skeema files and *.sql files don't inherit "schema"
	// option value from parent dir, and are ignored by diff/push/pull as long
	// as they don't specify a schema value directly. lint still works since its
	// execution model does not require a schema to be defined.
	fs.WriteTestFile(t, "mydb/product/subdir/.skeema", "# nothing relevant here\n")
	fs.WriteTestFile(t, "mydb/product/subdir/hello.sql", "CREATE TABLE hello (id int);\n")
	s.handleCommand(t, CodeSuccess, ".", "skeema pull")
	s.handleCommand(t, CodeSuccess, ".", "skeema diff")
	s.handleCommand(t, CodeDifferencesFound, ".", "skeema lint") // should rewrite hello.sql
	s.handleCommand(t, CodeSuccess, ".", "skeema diff")

	// Dirs with no *.sql files, but have a schema defined in .skeema, should
	// be interpreted as a logical schema without any objects
	s.dbExec(t, "", "CREATE DATABASE otherdb")
	s.handleCommand(t, CodeSuccess, ".", "skeema pull")
	s.handleCommand(t, CodeSuccess, ".", "skeema diff")
	if contents := fs.ReadTestFile(t, "mydb/otherdb/.skeema"); contents == "" {
		t.Error("Unexpectedly found no contents in mydb/otherdb/.skeema")
	}
	s.dbExec(t, "otherdb", "CREATE TABLE othertable (id int)")
	s.handleCommand(t, CodeSuccess, ".", "skeema pull")
	if contents := fs.ReadTestFile(t, "mydb/otherdb/othertable.sql"); contents == "" {
		t.Error("Unexpectedly found no contents in mydb/otherdb/othertable.sql")
	}
	fs.RemoveTestFile(t, "mydb/otherdb/othertable.sql")
	s.handleCommand(t, CodeFatalError, ".", "skeema diff")
	s.handleCommand(t, CodeDifferencesFound, ".", "skeema diff --allow-unsafe")
	s.handleCommand(t, CodeSuccess, ".", "skeema push --allow-unsafe")
}

// This test covers usage of clauses that have no effect in InnoDB, but are still
// shown by MySQL in SHOW CREATE TABLE, despite not being reflected anywhere in
// information_schema. Skeema ignores/strips these clauses so that they do not
// trip up its "unsupported table" validation logic.
func (s SkeemaIntegrationSuite) TestNonInnoClauses(t *testing.T) {
	// MariaDB does not consider STORAGE or COLUMN_FORMAT clauses as valid SQL.
	// Ditto for MySQL 5.5.
	if s.d.Flavor().Vendor == tengo.VendorMariaDB {
		t.Skip("Test not relevant for MariaDB-based image", s.d.Image)
	} else if major, minor, _ := s.d.Version(); major == 5 && minor == 5 {
		t.Skip("Test not relevant for 5.5-based image", s.d.Image)
	}

	// By default, Skeema uses innodb_strict_mode=1 in its connections. MySQL 5.6
	// interprets that option to prevent use of KEY_BLOCK_SIZE in nonsensical ways,
	// so we must disable it for this test.
	var connectOpts string
	if major, minor, _ := s.d.Version(); major == 5 && minor == 6 {
		connectOpts = " --connect-options=\"innodb_strict_mode=0\""
	}

	withClauses := "CREATE TABLE `problems` (\n" +
		"  `name` varchar(30) /*!50606 STORAGE MEMORY */ /*!50606 COLUMN_FORMAT DYNAMIC */ DEFAULT NULL,\n" +
		"  `num` int(10) unsigned NOT NULL /*!50606 STORAGE DISK */ /*!50606 COLUMN_FORMAT FIXED */,\n" +
		"  KEY `idx1` (`name`) USING HASH KEY_BLOCK_SIZE=4 COMMENT 'lol',\n" +
		"  KEY `idx2` (`num`) USING BTREE\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=latin1 KEY_BLOCK_SIZE=8;\n"
	withoutClauses := "CREATE TABLE `problems` (\n" +
		"  `name` varchar(30) DEFAULT NULL,\n" +
		"  `num` int(10) unsigned NOT NULL,\n" +
		"  KEY `idx1` (`name`) COMMENT 'lol',\n" +
		"  KEY `idx2` (`num`)\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=latin1 KEY_BLOCK_SIZE=8;\n"
	assertFileNormalized := func() {
		t.Helper()
		if contents := fs.ReadTestFile(t, "mydb/product/problems.sql"); contents != withoutClauses {
			t.Errorf("File mydb/product/problems.sql not normalized. Expected:\n%s\nFound:\n%s", withoutClauses, contents)
		}
	}
	s.handleCommand(t, CodeSuccess, ".", "skeema init --dir mydb -h %s -P %d", s.d.Instance.Host, s.d.Instance.Port)

	// pull strips the clauses from new table
	s.dbExec(t, "product", withClauses)
	s.handleCommand(t, CodeSuccess, ".", "skeema pull")
	assertFileNormalized()

	// pull normalizes files to remove the clauses from an unchanged table
	fs.WriteTestFile(t, "mydb/product/problems.sql", withClauses)
	s.handleCommand(t, CodeSuccess, ".", "skeema pull")
	assertFileNormalized()

	// lint normalizes files to remove the clauses
	fs.WriteTestFile(t, "mydb/product/problems.sql", withClauses)
	s.handleCommand(t, CodeDifferencesFound, ".", "skeema lint%s --errors=''", connectOpts)
	assertFileNormalized()

	// diff views the clauses as no-ops if present in file but not db, or vice versa
	s.dbExec(t, "product", "DROP TABLE `problems`")
	s.dbExec(t, "product", withoutClauses)
	fs.WriteTestFile(t, "mydb/product/problems.sql", withClauses)
	s.handleCommand(t, CodeSuccess, ".", "skeema diff%s", connectOpts)
	s.dbExec(t, "product", "DROP TABLE `problems`")
	s.dbExec(t, "product", withClauses)
	fs.WriteTestFile(t, "mydb/product/problems.sql", withoutClauses)
	s.handleCommand(t, CodeSuccess, ".", "skeema diff%s", connectOpts)

	// init strips the clauses when it writes files
	// (current db state: file still has extra clauses from previous)
	if err := os.RemoveAll("mydb"); err != nil {
		t.Fatalf("Unable to clean directory: %s", err)
	}
	s.handleCommand(t, CodeSuccess, ".", "skeema init --dir mydb -h %s -P %d", s.d.Instance.Host, s.d.Instance.Port)
	assertFileNormalized()

	// push with other changes to same table ignores clauses / does not break
	// validation, in either direction
	newFileContents := strings.Replace(withoutClauses, "  KEY `idx1`", "  newcol int COLUMN_FORMAT FIXED,\n  KEY `idx1`", 1)
	fs.WriteTestFile(t, "mydb/product/problems.sql", newFileContents)
	s.handleCommand(t, CodeSuccess, ".", "skeema push%s", connectOpts)
	s.dbExec(t, "product", "DROP TABLE `problems`")
	s.dbExec(t, "product", withoutClauses)
	s.dbExec(t, "product", "ALTER TABLE `problems` DROP KEY `idx2`")
	fs.WriteTestFile(t, "mydb/product/problems.sql", withClauses)
	s.handleCommand(t, CodeSuccess, ".", "skeema push%s", connectOpts)
}

func (s SkeemaIntegrationSuite) TestReuseTempSchema(t *testing.T) {
	s.handleCommand(t, CodeSuccess, ".", "skeema init --dir mydb -h %s -P %d", s.d.Instance.Host, s.d.Instance.Port)

	// Ensure that re-using temp schema works as expected, and does not confuse
	// subsequent commands
	for n := 0; n < 2; n++ {
		// Need --skip-format in order for pull to use temp schema
		cfg := s.handleCommand(t, CodeSuccess, ".", "skeema pull --skip-format --reuse-temp-schema --temp-schema=verytemp")
		s.assertTableExists(t, "verytemp", "", "")
		s.verifyFiles(t, cfg, "../golden/init")
	}

	// Invalid workspace option should error
	s.handleCommand(t, CodeBadConfig, ".", "skeema pull --workspace=doesnt-exist --skip-format --reuse-temp-schema --temp-schema=verytemp")
}

func (s SkeemaIntegrationSuite) TestShardedSchemas(t *testing.T) {
	s.handleCommand(t, CodeSuccess, ".", "skeema init --dir mydb -h %s -P %d", s.d.Instance.Host, s.d.Instance.Port)

	// Make product dir now map to 3 schemas: product, product2, product3
	contents := fs.ReadTestFile(t, "mydb/product/.skeema")
	contents = strings.Replace(contents, "schema=product", "schema=product,product2,product3,product4", 1)
	fs.WriteTestFile(t, "mydb/product/.skeema", contents)

	// push that ignores 4$ should now create product2 and product3
	s.handleCommand(t, CodeSuccess, ".", "skeema push --ignore-schema=4$")
	s.assertTableExists(t, "product2", "", "")
	s.assertTableExists(t, "product3", "posts", "")

	// diff should be clear after
	s.handleCommand(t, CodeSuccess, ".", "skeema diff --ignore-schema=4$")

	// pull should not create separate dirs for the new schemas or mess with
	// the .skeema file
	assertDirMissing := func(dirPath string) {
		t.Helper()
		if _, err := os.Stat(dirPath); !os.IsNotExist(err) {
			t.Errorf("Expected dir %s to not exist, but it does (or other err=%v)", dirPath, err)
		}
	}
	assertDirMissing("mydb/product1")
	assertDirMissing("mydb/product2")
	if fs.ReadTestFile(t, "mydb/product/.skeema") != contents {
		t.Error("Unexpected change to mydb/product/.skeema contents")
	}

	// pull should still reflect changes properly, if made to the first sharded
	// product schema or to the unsharded analytics schema
	s.dbExec(t, "product", "ALTER TABLE comments ADD COLUMN `approved` tinyint(1) unsigned NOT NULL")
	s.dbExec(t, "analytics", "ALTER TABLE activity ADD COLUMN `rolled_up` tinyint(1) unsigned NOT NULL")
	s.handleCommand(t, CodeSuccess, ".", "skeema pull --ignore-schema=4$")
	sfContents := fs.ReadTestFile(t, "mydb/product/comments.sql")
	if !strings.Contains(sfContents, "`approved` tinyint(1) unsigned") {
		t.Error("Pull did not update mydb/product/comments.sql as expected")
	}
	sfContents = fs.ReadTestFile(t, "mydb/analytics/activity.sql")
	if !strings.Contains(sfContents, "`rolled_up` tinyint(1) unsigned") {
		t.Error("Pull did not update mydb/analytics/activity.sql as expected")
	}

	// push should re-apply the changes to the other 2 product shards; diff
	// should be clean after
	s.handleCommand(t, CodeSuccess, ".", "skeema push --ignore-schema=4$")
	s.assertTableExists(t, "product2", "comments", "approved")
	s.assertTableExists(t, "product3", "comments", "approved")
	s.handleCommand(t, CodeSuccess, ".", "skeema diff --ignore-schema=4$")

	// schema shellouts should also work properly. First get rid of product schema
	// manually (since push won't ever drop a db) and then push should create
	// product1 as a new schema.
	contents = strings.Replace(contents, "schema=product,product2,product3,product4", "schema=`/usr/bin/printf 'product1 product2 product3 product4'`", 1)
	fs.WriteTestFile(t, "mydb/product/.skeema", contents)
	s.dbExec(t, "", "DROP DATABASE product")
	s.handleCommand(t, CodeSuccess, ".", "skeema push --ignore-schema=4$")
	s.assertTableExists(t, "product1", "posts", "")
	s.handleCommand(t, CodeSuccess, ".", "skeema diff --ignore-schema=4$")
	s.handleCommand(t, CodeSuccess, ".", "skeema pull")
	assertDirMissing("mydb/product1") // dir is still called mydb/product
	assertDirMissing("mydb/product2")
	if fs.ReadTestFile(t, "mydb/product/.skeema") != contents {
		t.Error("Unexpected change to mydb/product/.skeema contents")
	}

	// Test schema=* behavior, which should map to all the schemas, meaning that
	// a push will replace the previous tables of the analytics schema with the
	// tables of the product schemas
	if err := os.RemoveAll("mydb/analytics"); err != nil {
		t.Fatalf("Unable to delete mydb/analytics/: %s", err)
	}
	contents = strings.Replace(contents, "schema=`/usr/bin/printf 'product1 product2 product3 product4'`", "schema=*", 1)
	fs.WriteTestFile(t, "mydb/product/.skeema", contents)
	s.handleCommand(t, CodeSuccess, ".", "skeema push --allow-unsafe")
	s.assertTableExists(t, "product1", "posts", "")
	s.assertTableExists(t, "analytics", "posts", "")
	s.assertTableMissing(t, "analytics", "pageviews", "")
	s.handleCommand(t, CodeSuccess, ".", "skeema diff")

	// Since analytics is the first alphabetically, it is now the prototype
	// as far as pull is concerned
	s.dbExec(t, "analytics", "CREATE TABLE `foo` (id int)")
	s.handleCommand(t, CodeSuccess, ".", "skeema pull")
	fs.ReadTestFile(t, "mydb/product/foo.sql")                   // just confirming it exists
	s.handleCommand(t, CodeDifferencesFound, ".", "skeema diff") // since 3 schemas missing foo
	s.handleCommand(t, CodeSuccess, ".", "skeema push")
	s.assertTableExists(t, "product1", "foo", "")
	s.assertTableExists(t, "product2", "foo", "")
	s.handleCommand(t, CodeSuccess, ".", "skeema diff")

	// Test combination of ignore-schema and schema=*
	fs.WriteTestFile(t, "mydb/product/foo2.sql", "CREATE TABLE `foo2` (id int);\n")
	s.handleCommand(t, CodeSuccess, ".", "skeema push --ignore-schema=2$")
	s.assertTableExists(t, "product1", "foo2", "")
	s.assertTableMissing(t, "product2", "foo2", "")
	s.assertTableExists(t, "product3", "foo2", "")
}

func (s SkeemaIntegrationSuite) TestFlavorConfig(t *testing.T) {
	// Set up dir mydb to have flavor set, and then remove the flavor from
	// the cached Instance, so that we can test the ability of the flavor option
	// to be a fallback.
	cfg := s.handleCommand(t, CodeSuccess, ".", "skeema init --dir mydb -h %s -P %d", s.d.Instance.Host, s.d.Instance.Port)
	dir, err := fs.ParseDir("mydb", cfg)
	if err != nil {
		t.Fatalf("Unexpected error from ParseDir: %s", err)
	}
	inst, err := dir.FirstInstance()
	if inst == nil || err != nil {
		t.Fatalf("No instances returned for %s: %s", dir, err)
	}

	realFlavor := inst.Flavor()
	badFlavor := tengo.Flavor{Vendor: tengo.VendorUnknown, Major: 10, Minor: 3}

	// diff should return no differences
	inst.ForceFlavor(badFlavor)
	s.handleCommand(t, CodeSuccess, "mydb", "skeema diff --debug")

	// pull should keep the flavor override in place
	// (note that we need to keep re-forcing the bad flavor each time, since some
	// commands will forcibly override it using the dir config one!)
	inst.ForceFlavor(badFlavor)
	cfg = s.handleCommand(t, CodeSuccess, "mydb", "skeema pull --debug")
	s.verifyFiles(t, cfg, "../golden/init")

	// Doing init again to new dir mydbnf, confirm no flavor in mydbnf/.skeema
	inst.ForceFlavor(badFlavor)
	var extraFlag string
	if realFlavor.HasDataDictionary() {
		// Very counter-intuitive, but we need to ensure init is reusing the
		// cached Instance that has param information_schema_stats_expiry=0 in the
		// DSN, since that's what "inst" points to! Supplying the flavor on the CLI
		// won't otherwise affect anything in init except for this one piece of logic
		// in fs.Dir.InstanceDefaultParams().
		// Note that supplying flavor on the CLI is undocumented. Behavior changes
		// may break this test in the future; find a less hacky solution here if so!
		extraFlag = " --flavor mysql:8.0"
	}
	s.handleCommand(t, CodeSuccess, ".", "skeema init --dir mydbnf -h %s -P %d%s", s.d.Instance.Host, s.d.Instance.Port, extraFlag)
	contents := fs.ReadTestFile(t, "mydbnf/.skeema")
	if strings.Contains(contents, "flavor") {
		t.Error("Expected init to skip flavor, but it was found")
	}
	fs.RemoveTestDirectory(t, "mydbnf")

	// Restore the instance's correct flavor, and set a different flavor back in
	// mydb/.skeema. Confirm diff behavior unaffected, meaning the instance flavor
	// takes precedence over the dir one if both are known.
	inst.ForceFlavor(realFlavor)
	newFlavor := tengo.FlavorMariaDB103
	if realFlavor.Vendor == tengo.VendorMariaDB {
		newFlavor = tengo.FlavorMySQL57
	}
	contents = fs.ReadTestFile(t, "mydb/.skeema")
	if !strings.Contains(contents, realFlavor.String()) {
		t.Fatal("Could not find flavor line in mydb/.skeema")
	}
	contents = strings.Replace(contents, realFlavor.String(), newFlavor.String(), 1)
	fs.WriteTestFile(t, "mydb/.skeema", contents)
	s.handleCommand(t, CodeSuccess, "mydb", "skeema diff --debug")

	// pull should fix flavor line
	cfg = s.handleCommand(t, CodeSuccess, ".", "skeema pull --debug")
	s.verifyFiles(t, cfg, "../golden/init")
}

func (s SkeemaIntegrationSuite) TestRoutines(t *testing.T) {
	origCreate := `CREATE definer=root@localhost FUNCTION routine1(a int,
  b int)
RETURNS int
DETERMINISTIC
BEGIN
	return a * b;
END`
	create := origCreate
	s.dbExec(t, "product", create)

	// Confirm init works properly with one function present
	s.reinitAndVerifyFiles(t, "", "../golden/routines")

	// diff, pull, lint should all be no-ops at this point
	s.handleCommand(t, CodeSuccess, ".", "skeema diff")
	cfg := s.handleCommand(t, CodeSuccess, ".", "skeema pull")
	s.verifyFiles(t, cfg, "../golden/routines")
	cfg = s.handleCommand(t, CodeSuccess, ".", "skeema lint")
	s.verifyFiles(t, cfg, "../golden/routines")

	// Change routine1.sql to use Windows-style CRLF line-end in two spots. No
	// diff should be present. Pull should restore UNIX-style LFs.
	routine1 := fs.ReadTestFile(t, "mydb/product/routine1.sql")
	routine1 = strings.Replace(routine1, "a int,\n", "a int,\r\n", 1)
	routine1 = strings.Replace(routine1, "BEGIN\n", "BEGIN\r\n", 1)
	fs.WriteTestFile(t, "mydb/product/routine1.sql", routine1)
	s.handleCommand(t, CodeSuccess, ".", "skeema diff")
	cfg = s.handleCommand(t, CodeSuccess, ".", "skeema pull")
	s.verifyFiles(t, cfg, "../golden/routines")

	// Modify the db representation of the routine; diff/push should work, but only
	// with --allow-unsafe (and not with --safe-below-size)
	s.dbExec(t, "product", "DROP FUNCTION routine1")
	create = strings.Replace(create, "a * b", "b * a", 1)
	if create == origCreate {
		t.Fatal("Test setup incorrect")
	}
	s.dbExec(t, "product", create)
	s.handleCommand(t, CodeFatalError, ".", "skeema diff")
	s.handleCommand(t, CodeDifferencesFound, ".", "skeema diff --allow-unsafe")
	s.handleCommand(t, CodeFatalError, ".", "skeema push --safe-below-size=10000")
	cfg = s.handleCommand(t, CodeSuccess, ".", "skeema push --allow-unsafe")
	s.verifyFiles(t, cfg, "../golden/routines")
	s.handleCommand(t, CodeSuccess, ".", "skeema diff")

	// Delete that file and do a pull; file should be back, even with
	// --skip-format
	fs.RemoveTestFile(t, "mydb/product/routine1.sql")
	cfg = s.handleCommand(t, CodeSuccess, ".", "skeema pull --skip-format")
	s.verifyFiles(t, cfg, "../golden/routines")

	// Confirm changing the db's collation counts as a diff for routines if (and
	// only if) --compare-metadata is used
	s.dbExec(t, "", "ALTER DATABASE product DEFAULT COLLATE = latin1_general_ci")
	s.handleCommand(t, CodeSuccess, ".", "skeema pull")
	s.handleCommand(t, CodeSuccess, ".", "skeema diff")
	s.handleCommand(t, CodeFatalError, ".", "skeema diff --compare-metadata")
	s.handleCommand(t, CodeDifferencesFound, ".", "skeema diff --compare-metadata --allow-unsafe")
	s.handleCommand(t, CodeSuccess, ".", "skeema push --compare-metadata --allow-unsafe")
	s.handleCommand(t, CodeSuccess, ".", "skeema diff --compare-metadata")
	s.d.CloseAll() // avoid mysql bug where ALTER DATABASE doesn't affect existing sessions

	// Add a file creating another routine. Push it and confirm the routine is
	// using the sql_mode of the server.
	origContents := "CREATE FUNCTION routine2() returns varchar(30) DETERMINISTIC return 'abc''def';\n"
	fs.WriteTestFile(t, "mydb/product/routine2.sql", origContents)
	s.handleCommand(t, CodeSuccess, ".", "skeema push")
	schema, err := s.d.Schema("product")
	if err != nil || schema == nil {
		t.Fatal("Unexpected error obtaining product schema")
	}
	funcs := schema.FunctionsByName()
	if r2, ok := funcs["routine2"]; !ok {
		t.Fatal("Unable to locate routine2")
	} else {
		var serverSQLMode string
		db, _ := s.d.Connect("", "")
		if err := db.QueryRow("SELECT @@global.sql_mode").Scan(&serverSQLMode); err != nil {
			t.Fatalf("Unexpected error querying sql_mode: %s", err)
		}
		if r2.SQLMode != serverSQLMode {
			t.Errorf("Expected routine2 to have sql_mode %s, instead found %s", serverSQLMode, r2.SQLMode)
		}
	}

	// Lint that new file; confirm new formatting matches expectation.
	s.handleCommand(t, CodeDifferencesFound, ".", "skeema lint")
	normalizedContents := `CREATE DEFINER=~root~@~%~ FUNCTION ~routine2~() RETURNS varchar(30) CHARSET latin1 COLLATE latin1_general_ci
    DETERMINISTIC
return 'abc''def';
`
	normalizedContents = strings.Replace(normalizedContents, "~", "`", -1)
	if contents := fs.ReadTestFile(t, "mydb/product/routine2.sql"); contents != normalizedContents {
		t.Errorf("Unexpected contents after linting; found:\n%s", contents)
	}

	// Restore old formatting and test pull, with and without --format
	fs.WriteTestFile(t, "mydb/product/routine2.sql", origContents)
	s.handleCommand(t, CodeSuccess, ".", "skeema pull --skip-format")
	if contents := fs.ReadTestFile(t, "mydb/product/routine2.sql"); contents != origContents {
		t.Errorf("Expected contents unchanged from pull with --skip-format; instead found:\n%s", contents)
	}
	s.handleCommand(t, CodeSuccess, ".", "skeema pull")
	if contents := fs.ReadTestFile(t, "mydb/product/routine2.sql"); contents != normalizedContents {
		t.Errorf("Expected contents to be normalized from pull with --format; instead found:\n%s", contents)
	}

	// Add a *procedure* called routine2. pull should place this in same file as
	// the function with same name.
	r2dupe := `CREATE PROCEDURE routine2(a int, b int)
BEGIN
	SELECT a;
	SELECT b;
END`
	s.dbExec(t, "product", r2dupe)
	s.handleCommand(t, CodeSuccess, ".", "skeema pull")
	normalizedContents += "DELIMITER //\nCREATE DEFINER=`root`@`%` PROCEDURE `routine2`(a int, b int)\nBEGIN\n\tSELECT a;\n\tSELECT b;\nEND//\nDELIMITER ;\n"
	if contents := fs.ReadTestFile(t, "mydb/product/routine2.sql"); contents != normalizedContents {
		t.Errorf("Unexpected contents after pull; expected:\n%s\nfound:\n%s", normalizedContents, contents)
	}

	// diff and lint should both be no-ops
	s.handleCommand(t, CodeSuccess, ".", "skeema diff")
	s.handleCommand(t, CodeSuccess, ".", "skeema lint")

	// Drop the *function* routine2 and do a push. This should properly recreate
	// the dropped func.
	s.dbExec(t, "product", "DROP FUNCTION routine2")
	s.handleCommand(t, CodeSuccess, ".", "skeema push")
	for _, otype := range []tengo.ObjectType{tengo.ObjectTypeFunc, tengo.ObjectTypeProc} {
		exists, phrase, err := s.objectExists("product", otype, "routine2", "")
		if !exists || err != nil {
			t.Errorf("Expected %s to exist, instead found %t, err=%v", phrase, exists, err)
		}
	}
}

func (s SkeemaIntegrationSuite) TestPartitioning(t *testing.T) {
	s.handleCommand(t, CodeSuccess, ".", "skeema init --dir mydb -h %s -P %d", s.d.Instance.Host, s.d.Instance.Port)

	contentsNoPart := fs.ReadTestFile(t, "mydb/analytics/activity.sql")
	contents2Part := strings.Replace(contentsNoPart, ";\n",
		"\nPARTITION BY RANGE (ts)\n(PARTITION p0 VALUES LESS THAN (1571678000),\n PARTITION pN VALUES LESS THAN MAXVALUE);\n",
		1)
	contents3Part := strings.Replace(contentsNoPart, ";\n",
		"\nPARTITION BY RANGE (ts)\n(PARTITION p0 VALUES LESS THAN (1571678000),\n PARTITION p1 VALUES LESS THAN (1571679000),\n PARTITION pN VALUES LESS THAN MAXVALUE);\n",
		1)
	contents3PartPlusNewCol := strings.Replace(contents3Part, "  `target_id`", "  `somenewcol` int,\n  `target_id`", 1)
	contentsHashPart := strings.Replace(contentsNoPart, ";\n",
		"\nPARTITION BY HASH (action_id) PARTITIONS 4;\n",
		1)

	// Rewrite activity.sql to be partitioned by range with 2 partitions, and then
	// test diff behavior with each value of partitioning option
	fs.WriteTestFile(t, "mydb/analytics/activity.sql", contents2Part)
	s.handleCommand(t, CodeSuccess, "mydb/analytics", "skeema diff --partitioning=remove")
	s.handleCommand(t, CodeDifferencesFound, "mydb/analytics", "skeema diff --partitioning=keep")
	s.handleCommand(t, CodeDifferencesFound, "mydb/analytics", "skeema diff") // default is keep
	s.handleCommand(t, CodeDifferencesFound, "mydb/analytics", "skeema diff --partitioning=modify")
	s.handleCommand(t, CodeBadConfig, "mydb/analytics", "skeema diff --partitioning=invalid")

	// Push to execute the ALTER to partition by range with 2 partitions.
	// Confirm no differences with keep, but some differences with remove.
	s.handleCommand(t, CodeSuccess, "mydb/analytics", "skeema push") // default is keep
	s.handleCommand(t, CodeSuccess, "mydb/analytics", "skeema diff")
	s.handleCommand(t, CodeDifferencesFound, "mydb/analytics", "skeema diff --partitioning=remove")

	// Rewrite activity.sql to now have 3 partitions, still by range. This should
	// not show differences for keep or modify.
	fs.WriteTestFile(t, "mydb/analytics/activity.sql", contents3Part)
	s.handleCommand(t, CodeSuccess, "mydb/analytics", "skeema diff --partitioning=keep")
	s.handleCommand(t, CodeSuccess, "mydb/analytics", "skeema diff --partitioning=modify")
	// Note: didn't push the above change

	// Rewrite activity.sql to be unpartitioned. This should not show differences
	// for keep, but should for remove or modify.
	fs.WriteTestFile(t, "mydb/analytics/activity.sql", contentsNoPart)
	s.handleCommand(t, CodeSuccess, "mydb/analytics", "skeema diff --partitioning=keep")
	s.handleCommand(t, CodeDifferencesFound, "mydb/analytics", "skeema diff --partitioning=remove")
	s.handleCommand(t, CodeDifferencesFound, "mydb/analytics", "skeema diff --partitioning=modify")
	// Note: didn't push the above change

	// Rewrite activity.sql to have 3 partitions, still by range, as well as a new
	// column. Pushing this with remove should add the new column but remove the
	// partitioning.
	fs.WriteTestFile(t, "mydb/analytics/activity.sql", contents3PartPlusNewCol)
	s.handleCommand(t, CodeDifferencesFound, "mydb/analytics", "skeema diff --partitioning=remove")
	s.handleCommand(t, CodeSuccess, "mydb/analytics", "skeema push --partitioning=remove")
	s.handleCommand(t, CodeSuccess, "mydb/analytics", "skeema pull")
	newContents := fs.ReadTestFile(t, "mydb/analytics/activity.sql")
	if strings.Contains(newContents, "PARTITION BY") || !strings.Contains(newContents, "somenewcol") {
		t.Errorf("Previous push did not have intended effect; current table structure: %s", newContents)
	}

	// Remove the new col and restore partitioning
	fs.WriteTestFile(t, "mydb/analytics/activity.sql", contents3Part)
	s.handleCommand(t, CodeSuccess, "mydb/analytics", "skeema push --allow-unsafe --partitioning=keep")
	s.handleCommand(t, CodeSuccess, "mydb/analytics", "skeema diff --partitioning=keep")

	// Rewrite activity.sql to be partitioned by hash. This should be ignored with
	// keep, repartition with modify, or departition with remove. If pushed with
	// remove and then pulled, files should be back to initial state.
	fs.WriteTestFile(t, "mydb/analytics/activity.sql", contentsHashPart)
	s.handleCommand(t, CodeSuccess, "mydb/analytics", "skeema diff --partitioning=keep")
	s.handleCommand(t, CodeDifferencesFound, "mydb/analytics", "skeema diff --partitioning=modify")
	s.handleCommand(t, CodeDifferencesFound, "mydb/analytics", "skeema diff --partitioning=remove")
	s.handleCommand(t, CodeSuccess, "mydb/analytics", "skeema push --partitioning=remove")
	cfg := s.handleCommand(t, CodeSuccess, "mydb/analytics", "skeema pull")
	s.verifyFiles(t, cfg, "../golden/init")

	// Repartition with 2 partitions and push. Confirm that dropping the table
	// works correctly regardless of partitioning option.
	for _, value := range []string{"keep", "modify", "remove"} {
		fs.WriteTestFile(t, "mydb/analytics/activity.sql", contents2Part)
		s.handleCommand(t, CodeSuccess, "mydb/analytics", "skeema push") // default is keep
		fs.RemoveTestFile(t, "mydb/analytics/activity.sql")
		s.handleCommand(t, CodeDifferencesFound, "mydb/analytics", "skeema diff --allow-unsafe --partitioning=%s", value)
		s.handleCommand(t, CodeSuccess, "mydb/analytics", "skeema push --allow-unsafe --partitioning=%s", value)
	}
}
