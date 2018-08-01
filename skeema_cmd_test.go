package main

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/skeema/tengo"
)

func (s *SkeemaIntegrationSuite) TestInitHandler(t *testing.T) {
	s.handleCommand(t, CodeBadConfig, ".", "skeema init") // no host

	// Invalid environment name
	s.handleCommand(t, CodeBadConfig, ".", "skeema init --dir mydb -h %s -P %d '[nope]'", s.d.Instance.Host, s.d.Instance.Port)

	// Specifying a single schema that doesn't exist on the instance
	s.handleCommand(t, CodeBadConfig, ".", "skeema init --dir mydb -h %s -P %d --schema doesntexist", s.d.Instance.Host, s.d.Instance.Port)

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
	cfg = s.handleCommand(t, CodeSuccess, ".", "skeema init --dir combined -h %s -P %d --schema product", s.d.Instance.Host, s.d.Instance.Port)
	dir, err := NewDir("combined", cfg)
	if err != nil {
		t.Fatalf("Unexpected error from NewDir: %s", err)
	}
	optionFile := getOptionFile(t, "combined", cfg)
	for _, option := range []string{"host", "schema", "default-character-set", "default-collation"} {
		if _, setsOption := optionFile.OptionValue(option); !setsOption {
			t.Errorf("Expected .skeema to contain %s, but it does not", option)
		}
	}
	if subdirs, err := dir.Subdirs(); err != nil {
		t.Fatalf("Unexpected error listing subdirs of %s: %s", dir, err)
	} else if len(subdirs) > 0 {
		t.Errorf("Expected %s to have no subdirs, but it has %d", dir, len(subdirs))
	}
	if sqlFiles, err := dir.SQLFiles(); err != nil {
		t.Fatalf("Unexpected error listing *.sql in %s: %s", dir, err)
	} else if len(sqlFiles) < 1 {
		t.Errorf("Expected %s to have *.sql files, but it does not", dir)
	}

	// Test successful init without a --dir
	expectDir := fmt.Sprintf("%s:%d", s.d.Instance.Host, s.d.Instance.Port)
	if _, err = os.Stat(expectDir); err == nil {
		t.Fatalf("Expected dir %s to not exist yet, but it does", expectDir)
	}
	s.handleCommand(t, CodeSuccess, ".", "skeema init -h %s -P %d", s.d.Instance.Host, s.d.Instance.Port)
	if _, err = os.Stat(expectDir); err != nil {
		t.Fatalf("Expected dir %s to exist now, but it does not", expectDir)
	}

	// init should fail if a parent dir has an invalid .skeema file
	makeDir(t, "hasbadoptions")
	writeFile(t, "hasbadoptions/.skeema", "invalid file will not parse")
	s.handleCommand(t, CodeFatalError, "hasbadoptions", "skeema init -h %s -P %d", s.d.Instance.Host, s.d.Instance.Port)

	// init should fail if the --dir specifies an existing non-directory file; or
	// if the --dir already contains a subdir matching a schema name; or if the
	// --dir already contains a .sql file and --schema was used to only do 1 level
	writeFile(t, "nondir", "foo bar")
	s.handleCommand(t, CodeCantCreate, ".", "skeema init --dir nondir -h %s -P %d", s.d.Instance.Host, s.d.Instance.Port)
	makeDir(t, "alreadyexists/product")
	s.handleCommand(t, CodeCantCreate, ".", "skeema init --dir alreadyexists -h %s -P %d", s.d.Instance.Host, s.d.Instance.Port)
	makeDir(t, "hassql")
	writeFile(t, "hassql/foo.sql", "foo")
	s.handleCommand(t, CodeFatalError, ".", "skeema init --dir hassql --schema product -h %s -P %d", s.d.Instance.Host, s.d.Instance.Port)
}

func (s *SkeemaIntegrationSuite) TestAddEnvHandler(t *testing.T) {
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
	if !origFile.SameContents(file) {
		t.Fatalf("File contents of %s do not match expectation", file.Path())
	}

	// Nonstandard port should work properly; ditto for user option persisting
	cfg = s.handleCommand(t, CodeSuccess, ".", "skeema add-environment --host my.ci.invalid -P 3307 -ufoobar --dir mydb ci  --connect-options='timeout=10ms'")
	file = getOptionFile(t, "mydb", cfg)
	origFile.SetOptionValue("ci", "host", "my.ci.invalid")
	origFile.SetOptionValue("ci", "port", "3307")
	origFile.SetOptionValue("ci", "user", "foobar")
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
}

func (s *SkeemaIntegrationSuite) TestPullHandler(t *testing.T) {
	s.handleCommand(t, CodeSuccess, ".", "skeema init --dir mydb -h %s -P %d", s.d.Instance.Host, s.d.Instance.Port)

	// In product db, alter one table and drop one table;
	// In analytics db, add one table and alter the schema's charset and collation;
	// Create a new db and put one table in it
	s.sourceSQL(t, "pull1.sql")
	cfg := s.handleCommand(t, CodeSuccess, ".", "skeema pull")
	s.verifyFiles(t, cfg, "../golden/pull1")

	// Revert db back to previous state, and pull again to test the opposite
	// behaviors: delete dir for new schema, remove charset/collation from .skeema,
	// etc. Also edit the host .skeema file to remove flavor, to test logic that
	// adds/updates flavor on pull.
	s.cleanData(t, "setup.sql")
	writeFile(t, "mydb/.skeema", strings.Replace(readFile(t, "mydb/.skeema"), "flavor", "#flavor", 1))
	cfg = s.handleCommand(t, CodeSuccess, ".", "skeema pull")
	s.verifyFiles(t, cfg, "../golden/init")

	// Files with invalid SQL should still be corrected upon pull. Files with
	// nonstandard formatting of their CREATE TABLE should be normalized, even if
	// there was an ignored auto-increment change. However, files with extraneous
	// text before/after the CREATE TABLE should remain as-is UNLESS there were
	// other changes triggering a file rewrite.
	contents := readFile(t, "mydb/analytics/activity.sql")
	writeFile(t, "mydb/analytics/activity.sql", strings.Replace(contents, "DEFAULT", "DEFALUT", 1))
	s.dbExec(t, "product", "INSERT INTO comments (post_id, user_id) VALUES (555, 777)")
	contents = readFile(t, "mydb/product/comments.sql")
	writeFile(t, "mydb/product/comments.sql", strings.Replace(contents, "`", "", -1))
	contents = readFile(t, "mydb/product/posts.sql")
	writeFile(t, "mydb/product/posts.sql", fmt.Sprintf("# random comment\n%s", contents))
	cfg = s.handleCommand(t, CodeSuccess, ".", "skeema pull --debug")
	s.verifyFiles(t, cfg, "../golden/init")
	contents = readFile(t, "mydb/product/posts.sql")
	if !strings.Contains(contents, "# random comment") {
		t.Error("Expected mydb/product/posts.sql to retain its extraneous comment, but it was removed")
	}
	writeFile(t, "mydb/product/posts.sql", strings.Replace(contents, "`", "", -1))
	cfg = s.handleCommand(t, CodeSuccess, ".", "skeema pull --debug")
	s.verifyFiles(t, cfg, "../golden/init")
	if strings.Contains(readFile(t, "mydb/product/posts.sql"), "# random comment") {
		t.Error("Expected mydb/product/posts.sql to lose its extraneous comment due to other rewrite, but it was retained")
	}
}

func (s *SkeemaIntegrationSuite) TestLintHandler(t *testing.T) {
	s.handleCommand(t, CodeSuccess, ".", "skeema init --dir mydb -h %s -P %d", s.d.Instance.Host, s.d.Instance.Port)

	// Initial lint should be a no-op that returns exit code 0
	cfg := s.handleCommand(t, CodeSuccess, ".", "skeema lint")
	s.verifyFiles(t, cfg, "../golden/init")

	// Alter a few files in a way that is still valid SQL, but doesn't match
	// the database's native format. Lint should rewrite these files and then
	// return exit code CodeDifferencesFound.
	productDir, err := NewDir("mydb/product", cfg)
	if err != nil {
		t.Fatalf("Unable to obtain dir for mydb/product: %s", err)
	}
	sqlFiles, err := productDir.SQLFiles()
	if err != nil || len(sqlFiles) < 4 {
		t.Fatalf("Unable to obtain *.sql files from %s", productDir)
	}
	rewriteFiles := func(includeSyntaxError bool) {
		for n, sf := range sqlFiles {
			if sf.Error != nil {
				t.Fatalf("Unexpected error in file %s: %s", sf.Path(), sf.Error)
			}
			switch n {
			case 0:
				if includeSyntaxError {
					sf.Contents = strings.Replace(sf.Contents, "DEFAULT", "DEFALUT", 1)
				}
			case 1:
				sf.Contents = strings.ToLower(sf.Contents)
			case 2:
				sf.Contents = strings.Replace(sf.Contents, "`", "", -1)
			case 3:
				sf.Contents = strings.Replace(sf.Contents, "\n", " ", -1)
			}
			if _, err := sf.Write(); err != nil {
				t.Fatalf("Unable to rewrite %s: %s", sf.Path(), err)
			}
		}
	}
	rewriteFiles(false)
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
	writeFile(t, sqlFiles[0].Path(), strings.Replace(sqlFiles[0].Contents, "DEFALUT", "DEFAULT", 1))
	s.verifyFiles(t, cfg, "../golden/init")
	s.handleCommand(t, CodeSuccess, ".", "skeema lint")

	// Files with valid SQL, but not CREATE TABLE statements, should also trigger
	// CodeFatalError.
	writeFile(t, sqlFiles[0].Path(), "INSERT INTO foo (col1, col2) VALUES (123, 456)")
	s.handleCommand(t, CodeFatalError, ".", "skeema lint")

	// Files with wrong table name should yield a fatal error, by virtue of
	// SQLFile.Read() failing
	writeFile(t, sqlFiles[0].Path(), "CREATE TABLE whatever (id int)")
	s.handleCommand(t, CodeFatalError, ".", "skeema lint --debug")
}

func (s *SkeemaIntegrationSuite) TestDiffHandler(t *testing.T) {
	s.handleCommand(t, CodeSuccess, ".", "skeema init --dir mydb -h %s -P %d", s.d.Instance.Host, s.d.Instance.Port)

	// no-op diff should yield no differences
	s.handleCommand(t, CodeSuccess, ".", "skeema diff")

	// --host and --schema have no effect if supplied on CLI
	s.handleCommand(t, CodeSuccess, ".", "skeema diff --host=1.2.3.4 --schema=whatever")

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
		actualOut := readFile(t, "diff-brief.out")
		if actualOut != expectOut {
			t.Errorf("Unexpected output from `skeema diff --brief`\nExpected:\n%sActual:\n%s", expectOut, actualOut)
		}
		if err := os.Remove("diff-brief.out"); err != nil {
			t.Fatalf("Unable to delete diff-brief.out: %s", err)
		}
	}
}

func (s *SkeemaIntegrationSuite) TestPushHandler(t *testing.T) {
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
	s.assertExists(t, "analytics", "pageviews", "")                  // re-created by push
	s.assertMissing(t, "product", "users", "credits")                // product DDL skipped due to unsafe stmt
	s.assertExists(t, "product", "posts", "featured")                // product DDL skipped due to unsafe stmt
	s.assertExists(t, "bonus", "placeholder", "")                    // not affected by push (never drops schemas)

	// One exception to the "skip whole schema upon unsafe stmt" rule is schema-
	// level DDL, which is executed separately as a special case
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

	// Delete *.sql file for analytics.rollups. Push from analytics dir with
	// --safe-below-size=1 should fail since it has a row. Delete that row and
	// try again, should succeed that time.
	if err := os.Remove("mydb/analytics/rollups.sql"); err != nil {
		t.Fatalf("Unexpected error removing a file: %s", err)
	}
	s.handleCommand(t, CodeFatalError, "mydb/analytics", "skeema push --safe-below-size=1")
	s.assertExists(t, "analytics", "rollups", "")
	s.dbExec(t, "analytics", "DELETE FROM rollups")
	s.handleCommand(t, CodeSuccess, "mydb/analytics", "skeema push --safe-below-size=1")
	s.assertMissing(t, "analytics", "rollups", "")

	// push from base dir, with --allow-unsafe, will permit the changes to product
	// schema to proceed
	s.handleCommand(t, CodeSuccess, ".", "skeema push --allow-unsafe")
	s.assertMissing(t, "product", "posts", "featured")
	s.assertExists(t, "product", "users", "credits")
	s.assertExists(t, "bonus", "placeholder", "")

	// invalid SQL prevents push from working in an entire dir, but not in a
	// dir for a different schema
	contents := readFile(t, "mydb/product/comments.sql")
	writeFile(t, "mydb/product/comments.sql", strings.Replace(contents, "PRIMARY KEY", "foo int,\nPRIMARY KEY", 1))
	contents = readFile(t, "mydb/product/users.sql")
	writeFile(t, "mydb/product/users.sql", strings.Replace(contents, "PRIMARY KEY", "foo int INVALID SQL HERE,\nPRIMARY KEY", 1))
	writeFile(t, "mydb/bonus/.skeema", "schema=bonus\n")
	writeFile(t, "mydb/bonus/placeholder.sql", "CREATE TABLE placeholder (id int unsigned NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB")
	writeFile(t, "mydb/bonus/table2.sql", "CREATE TABLE table2 (name varchar(20) NOT NULL, PRIMARY KEY (name))")
	s.handleCommand(t, CodeFatalError, ".", "skeema push")
	s.assertMissing(t, "product", "comments", "foo")
	s.assertMissing(t, "product", "users", "foo")
	s.assertExists(t, "bonus", "table2", "")
}

func (s *SkeemaIntegrationSuite) TestIndexOrdering(t *testing.T) {
	s.handleCommand(t, CodeSuccess, ".", "skeema init --dir mydb -h %s -P %d", s.d.Instance.Host, s.d.Instance.Port)

	// Add 6 new redundant indexes to posts.sql. Place them before the existing
	// secondary index.
	contentsOrig := readFile(t, "mydb/product/posts.sql")
	lines := make([]string, 6)
	for n := range lines {
		lines[n] = fmt.Sprintf("KEY `idxnew_%d` (`created_at`)", n)
	}
	joinedLines := strings.Join(lines, ",\n  ")
	contentsIndexesFirst := strings.Replace(contentsOrig, "PRIMARY KEY (`id`),\n", fmt.Sprintf("PRIMARY KEY (`id`),\n  %s,\n", joinedLines), 1)
	writeFile(t, "mydb/product/posts.sql", contentsIndexesFirst)

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
	if fileContents := readFile(t, "mydb/product/posts.sql"); fileContents == contentsIndexesFirst {
		t.Error("Expected skeema pull to rewrite mydb/product/posts.sql to put indexes last, but file remained unchanged")
	} else if fileContents != contentsIndexesLast {
		t.Errorf("Expected skeema pull to rewrite mydb/product/posts.sql to put indexes last, but it did something else entirely. Contents:\n%s\nExpected:\n%s\n", fileContents, contentsIndexesLast)
	}

	// Edit posts.sql to put the new indexes first again, and ensure
	// push --exact-match actually reorders them.
	writeFile(t, "mydb/product/posts.sql", contentsIndexesFirst)
	if major, minor, _ := s.d.Version(); major == 5 && minor == 5 {
		s.handleCommand(t, CodeSuccess, "", "skeema push --exact-match")
	} else {
		s.handleCommand(t, CodeSuccess, "", "skeema push --exact-match --alter-algorithm=COPY")
	}
	s.handleCommand(t, CodeSuccess, "", "skeema diff")
	s.handleCommand(t, CodeSuccess, "", "skeema diff --exact-match")
	s.handleCommand(t, CodeSuccess, "", "skeema pull")
	if fileContents := readFile(t, "mydb/product/posts.sql"); fileContents != contentsIndexesFirst {
		t.Errorf("Expected skeema pull to have no effect at this point, but instead file now looks like this:\n%s", fileContents)
	}
}

func (s *SkeemaIntegrationSuite) TestForeignKeys(t *testing.T) {
	s.sourceSQL(t, "foreignkey.sql")
	s.handleCommand(t, CodeSuccess, ".", "skeema init --dir mydb -h %s -P %d", s.d.Instance.Host, s.d.Instance.Port)

	// Renaming an FK should not be considered a difference by default
	oldContents := readFile(t, "mydb/product/posts.sql")
	contents1 := strings.Replace(oldContents, "user_fk", "usridfk", 1)
	if oldContents == contents1 {
		t.Fatal("Expected mydb/product/posts.sql to contain foreign key definition, but it did not")
	}
	writeFile(t, "mydb/product/posts.sql", contents1)
	s.handleCommand(t, CodeSuccess, ".", "skeema diff")

	// pull won't update the file unless normalizing
	s.handleCommand(t, CodeSuccess, ".", "skeema pull --skip-normalize")
	if readFile(t, "mydb/product/posts.sql") != contents1 {
		t.Error("Expected skeema pull --skip-normalize to leave file untouched, but it rewrote it")
	}
	s.handleCommand(t, CodeSuccess, ".", "skeema pull")
	if readFile(t, "mydb/product/posts.sql") != oldContents {
		t.Error("Expected skeema pull to rewrite file, but it did not")
	}

	// Renaming an FK should be considered a difference with --exact-match
	writeFile(t, "mydb/product/posts.sql", contents1)
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
	writeFile(t, "mydb/product/posts.sql", contents2)
	s.handleCommand(t, CodeDifferencesFound, ".", "skeema diff")
	s.handleCommand(t, CodeSuccess, ".", "skeema push")
	writeFile(t, "mydb/product/posts.sql", contents1)
	s.handleCommand(t, CodeSuccess, ".", "skeema pull --skip-normalize")
	if readFile(t, "mydb/product/posts.sql") != contents2 {
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
	writeFile(t, "mydb/product/posts.sql", contents3)
	s.handleCommand(t, CodeFatalError, ".", "skeema push")
	s.handleCommand(t, CodeSuccess, ".", "skeema pull")
	checkContents := readFile(t, "mydb/product/posts.sql")
	if !strings.Contains(checkContents, "`body`") || strings.Contains(checkContents, "`user_fk`") {
		t.Error("Unsafe status did not properly affect both ALTERs on the table")
	}

	// Test adding an FK where the existing data does not meet the constraint:
	// should fail if foreign_key_checks=1, succeed if foreign_key_checks=0
	s.dbExec(t, "product", "ALTER TABLE posts DROP FOREIGN KEY usridfk")
	s.dbExec(t, "product", "INSERT INTO posts (user_id, byline) VALUES (1234, 'someone')")
	writeFile(t, "mydb/product/posts.sql", contents1)
	s.handleCommand(t, CodeFatalError, ".", "skeema push --foreign-key-checks")
	s.handleCommand(t, CodeSuccess, ".", "skeema push")
}

func (s *SkeemaIntegrationSuite) TestAutoInc(t *testing.T) {
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
	if !strings.Contains(readFile(t, "mydb/product/users.sql"), "AUTO_INCREMENT=4") {
		t.Error("Expected mydb/product/users.sql to contain AUTO_INCREMENT=4 after pull, but it did not")
	}

}

func (s *SkeemaIntegrationSuite) TestUnsupportedAlter(t *testing.T) {
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
	contents := readFile(t, "mydb/product/subscriptions.sql")
	writeFile(t, "mydb/product/subscriptions.sql", strings.Replace(contents, "`", "", -1))
	s.handleCommand(t, CodeDifferencesFound, ".", "skeema lint")
	s.verifyFiles(t, cfg, "../golden/unsupported")

	// diff should return CodeDifferencesFound, vs push should return
	// CodePartialError
	s.handleCommand(t, CodeDifferencesFound, ".", "skeema diff --debug")
	s.handleCommand(t, CodePartialError, ".", "skeema push")

	// diff/push still ok if *creating* or *dropping* unsupported table
	s.dbExec(t, "product", "DROP TABLE subscriptions")
	s.assertMissing(t, "product", "subscriptions", "")
	s.handleCommand(t, CodeDifferencesFound, ".", "skeema diff")
	s.handleCommand(t, CodeSuccess, ".", "skeema push")
	s.assertExists(t, "product", "subscriptions", "")
	if err := os.Remove("mydb/product/subscriptions.sql"); err != nil {
		t.Fatalf("Unexpected error removing a file: %s", err)
	}
	s.handleCommand(t, CodeDifferencesFound, ".", "skeema diff --allow-unsafe")
	s.handleCommand(t, CodeSuccess, ".", "skeema push --allow-unsafe")
	s.assertMissing(t, "product", "subscriptions", "")
}

func (s *SkeemaIntegrationSuite) TestIgnoreOptions(t *testing.T) {
	s.sourceSQL(t, "ignore1.sql")

	// init: valid regexes should work properly and persist to option files
	cfg := s.handleCommand(t, CodeSuccess, ".", "skeema init --dir mydb -h %s -P %d --ignore-schema='^archives$' --ignore-table='^_'", s.d.Instance.Host, s.d.Instance.Port)
	s.verifyFiles(t, cfg, "../golden/ignore")

	// pull: nothing should be updated due to ignore options
	cfg = s.handleCommand(t, CodeSuccess, ".", "skeema pull")
	s.verifyFiles(t, cfg, "../golden/ignore")

	// diff/push: no differences. This should still be the case even if we add a
	// file corresponding to an ignored table, with a different definition than
	// the db has.
	s.handleCommand(t, CodeSuccess, ".", "skeema diff")
	writeFile(t, "mydb/product/_widgets.sql", "CREATE TABLE _widgets (id int) ENGINE=InnoDB;\n")
	writeFile(t, "mydb/analytics/_newtable.sql", "CREATE TABLE _newtable (id int) ENGINE=InnoDB;\n")
	s.handleCommand(t, CodeSuccess, ".", "skeema diff")

	// pull should also ignore that file corresponding to an ignored table
	s.handleCommand(t, CodeSuccess, ".", "skeema pull")
	if readFile(t, "mydb/product/_widgets.sql") != "CREATE TABLE _widgets (id int) ENGINE=InnoDB;\n" {
		t.Error("Expected pull to ignore mydb/product/_widgets.sql entirely, but it did not")
	}

	// lint: ignored schemas and tables should be ignored
	// To set up this test, we do a pull that overrides the previous ignore options
	// and then edit those files so that they contain formatting mistakes or even
	// invalid SQL.
	cfg = s.handleCommand(t, CodeSuccess, ".", "skeema pull --ignore-schema='' --ignore-table=''")
	contents := readFile(t, "mydb/analytics/_trending.sql")
	newContents := strings.Replace(contents, "`", "", -1)
	writeFile(t, "mydb/analytics/_trending.sql", newContents)
	writeFile(t, "mydb/analytics/_hmm.sql", "lolololol no valid sql here")
	writeFile(t, "mydb/archives/bar.sql", "CREATE TABLE bar (this is not valid SQL whatever)")
	s.handleCommand(t, CodeSuccess, ".", "skeema lint")
	if readFile(t, "mydb/analytics/_trending.sql") != newContents {
		t.Error("Expected `skeema lint` to ignore mydb/analytics/_trending.sql, but it did not")
	}
	if readFile(t, "mydb/archives/bar.sql") != "CREATE TABLE bar (this is not valid SQL whatever)" {
		t.Error("Expected `skeema lint` to ignore mydb/archives/bar.sql, but it did not")
	}

	// pull, lint, init: invalid regexes should error
	s.handleCommand(t, CodeFatalError, ".", "skeema lint --ignore-schema='+'")
	s.handleCommand(t, CodeFatalError, ".", "skeema lint --ignore-table='+'")
	s.handleCommand(t, CodeFatalError, ".", "skeema pull --ignore-table='+'")
	s.handleCommand(t, CodeFatalError, ".", "skeema init --dir badre1 -h %s -P %d --ignore-schema='+'", s.d.Instance.Host, s.d.Instance.Port)
	s.handleCommand(t, CodeFatalError, ".", "skeema init --dir badre2 -h %s -P %d --ignore-table='+'", s.d.Instance.Host, s.d.Instance.Port)
}

func (s *SkeemaIntegrationSuite) TestDirEdgeCases(t *testing.T) {
	s.handleCommand(t, CodeSuccess, ".", "skeema init --dir mydb -h %s -P %d", s.d.Instance.Host, s.d.Instance.Port)

	// Invalid option file should break all commands
	oldContents := readFile(t, "mydb/.skeema")
	writeFile(t, "mydb/.skeema", "invalid contents\n")
	s.handleCommand(t, CodeFatalError, "mydb", "skeema pull")
	s.handleCommand(t, CodeFatalError, "mydb", "skeema diff")
	s.handleCommand(t, CodeFatalError, "mydb", "skeema lint")
	s.handleCommand(t, CodeFatalError, ".", "skeema add-environment --host my.staging.db.com --dir mydb staging")
	writeFile(t, "mydb/.skeema", oldContents)

	// Hidden directories are ignored, even if they contain a .skeema file, whether
	// valid or invalid. Extra directories are also ignored if they contain no
	// .skeema file.
	writeFile(t, ".hidden/.skeema", "invalid contents\n")
	writeFile(t, ".hidden/whatever.sql", "CREATE TABLE whatever (this is not valid SQL oh well)")
	writeFile(t, "whatever/whatever.sql", "CREATE TABLE whatever (this is not valid SQL oh well)")
	writeFile(t, "mydb/.hidden/.skeema", "schema=whatever\n")
	writeFile(t, "mydb/.hidden/whatever.sql", "CREATE TABLE whatever (this is not valid SQL oh well)")
	writeFile(t, "mydb/whatever/whatever.sql", "CREATE TABLE whatever (this is not valid SQL oh well)")
	writeFile(t, "mydb/product/.hidden/.skeema", "schema=whatever\n")
	writeFile(t, "mydb/product/.hidden/whatever.sql", "CREATE TABLE whatever (this is not valid SQL oh well)")
	writeFile(t, "mydb/product/whatever/whatever.sql", "CREATE TABLE whatever (this is not valid SQL oh well)")
	s.handleCommand(t, CodeSuccess, ".", "skeema pull")
	s.handleCommand(t, CodeSuccess, ".", "skeema diff")
	s.handleCommand(t, CodeSuccess, ".", "skeema lint")
}

// This test covers usage of clauses that have no effect in InnoDB, but are still
// shown by MySQL in SHOW CREATE TABLE, despite not being reflected anywhere in
// information_schema. Skeema ignores/strips these clauses so that they do not
// trip up its "unsupported table" validation logic.
func (s *SkeemaIntegrationSuite) TestNonInnoClauses(t *testing.T) {
	// MariaDB does not consider STORAGE or COLUMN_FORMAT clauses as valid SQL.
	// Ditto for MySQL 5.5.
	if s.d.Flavor().Vendor == tengo.VendorMariaDB {
		t.Skip("Test not relevant for MariaDB-based image", s.d.Image)
	} else if major, minor, _ := s.d.Version(); major == 5 && minor == 5 {
		t.Skip("Test not relevant for 5.5-based image", s.d.Image)
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
		if contents := readFile(t, "mydb/product/problems.sql"); contents != withoutClauses {
			t.Errorf("File mydb/product/problems.sql not normalized. Expected:\n%s\nFound:\n%s", withoutClauses, contents)
		}
	}
	s.handleCommand(t, CodeSuccess, ".", "skeema init --dir mydb -h %s -P %d", s.d.Instance.Host, s.d.Instance.Port)

	// pull strips the clauses from new table
	s.dbExec(t, "product", withClauses)
	s.handleCommand(t, CodeSuccess, ".", "skeema pull")
	assertFileNormalized()

	// pull normalizes files to remove the clauses from an unchanged table
	writeFile(t, "mydb/product/problems.sql", withClauses)
	s.handleCommand(t, CodeSuccess, ".", "skeema pull")
	assertFileNormalized()

	// lint normalizes files to remove the clauses
	writeFile(t, "mydb/product/problems.sql", withClauses)
	s.handleCommand(t, CodeDifferencesFound, ".", "skeema lint")
	assertFileNormalized()

	// diff views the clauses as no-ops if present in file but not db, or vice versa
	s.dbExec(t, "product", "DROP TABLE `problems`")
	s.dbExec(t, "product", withoutClauses)
	writeFile(t, "mydb/product/problems.sql", withClauses)
	s.handleCommand(t, CodeSuccess, ".", "skeema diff")
	s.dbExec(t, "product", "DROP TABLE `problems`")
	s.dbExec(t, "product", withClauses)
	writeFile(t, "mydb/product/problems.sql", withoutClauses)
	s.handleCommand(t, CodeSuccess, ".", "skeema diff")

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
	writeFile(t, "mydb/product/problems.sql", newFileContents)
	s.handleCommand(t, CodeSuccess, ".", "skeema push")
	s.dbExec(t, "product", "DROP TABLE `problems`")
	s.dbExec(t, "product", withoutClauses)
	s.dbExec(t, "product", "ALTER TABLE `problems` DROP KEY `idx2`")
	writeFile(t, "mydb/product/problems.sql", withClauses)
	s.handleCommand(t, CodeSuccess, ".", "skeema push")
}

func (s *SkeemaIntegrationSuite) TestReuseTempSchema(t *testing.T) {
	s.handleCommand(t, CodeSuccess, ".", "skeema init --dir mydb -h %s -P %d", s.d.Instance.Host, s.d.Instance.Port)

	// Ensure that re-using temp schema works as expected, and does not confuse
	// subsequent commands
	for n := 0; n < 2; n++ {
		cfg := s.handleCommand(t, CodeSuccess, ".", "skeema pull --reuse-temp-schema --temp-schema=verytemp")
		s.assertExists(t, "verytemp", "", "")
		s.verifyFiles(t, cfg, "../golden/init")
	}
}

func (s *SkeemaIntegrationSuite) TestShardedSchemas(t *testing.T) {
	s.handleCommand(t, CodeSuccess, ".", "skeema init --dir mydb -h %s -P %d", s.d.Instance.Host, s.d.Instance.Port)

	// Make product dir now map to 3 schemas: product, product2, product3
	contents := readFile(t, "mydb/product/.skeema")
	contents = strings.Replace(contents, "schema=product", "schema=product,product2,product3", 1)
	writeFile(t, "mydb/product/.skeema", contents)

	// push should now create product2 and product3
	s.handleCommand(t, CodeSuccess, ".", "skeema push")
	s.assertExists(t, "product2", "", "")
	s.assertExists(t, "product3", "posts", "")

	// diff should be clear after
	s.handleCommand(t, CodeSuccess, ".", "skeema diff")

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
	if readFile(t, "mydb/product/.skeema") != contents {
		t.Error("Unexpected change to mydb/product/.skeema contents")
	}

	// pull should still reflect changes properly, if made to the first sharded
	// product schema or to the unsharded analytics schema
	s.dbExec(t, "product", "ALTER TABLE comments ADD COLUMN `approved` tinyint(1) unsigned NOT NULL")
	s.dbExec(t, "analytics", "ALTER TABLE activity ADD COLUMN `rolled_up` tinyint(1) unsigned NOT NULL")
	s.handleCommand(t, CodeSuccess, ".", "skeema pull")
	sfContents := readFile(t, "mydb/product/comments.sql")
	if !strings.Contains(sfContents, "`approved` tinyint(1) unsigned") {
		t.Error("Pull did not update mydb/product/comments.sql as expected")
	}
	sfContents = readFile(t, "mydb/analytics/activity.sql")
	if !strings.Contains(sfContents, "`rolled_up` tinyint(1) unsigned") {
		t.Error("Pull did not update mydb/analytics/activity.sql as expected")
	}

	// push should re-apply the changes to the other 2 product shards; diff
	// should be clean after
	s.handleCommand(t, CodeSuccess, ".", "skeema push")
	s.assertExists(t, "product2", "comments", "approved")
	s.assertExists(t, "product3", "comments", "approved")
	s.handleCommand(t, CodeSuccess, ".", "skeema diff")

	// schema shellouts should also work properly. First get rid of product schema
	// manually (since push won't ever drop a db) and then push should create
	// product1 as a new schema.
	contents = strings.Replace(contents, "schema=product,product2,product3", "schema=`/usr/bin/printf 'product1 product2 product3'`", 1)
	writeFile(t, "mydb/product/.skeema", contents)
	s.dbExec(t, "", "DROP DATABASE product")
	s.handleCommand(t, CodeSuccess, ".", "skeema push")
	s.assertExists(t, "product1", "posts", "")
	s.handleCommand(t, CodeSuccess, ".", "skeema diff")
	s.handleCommand(t, CodeSuccess, ".", "skeema pull")
	assertDirMissing("mydb/product1") // dir is still called mydb/product
	assertDirMissing("mydb/product2")
	if readFile(t, "mydb/product/.skeema") != contents {
		t.Error("Unexpected change to mydb/product/.skeema contents")
	}

	// Test schema=* behavior, which should map to all the schemas, meaning that
	// a push will replace the previous tables of the analytics schema with the
	// tables of the product schemas
	if err := os.RemoveAll("mydb/analytics"); err != nil {
		t.Fatalf("Unable to delete mydb/analytics/: %s", err)
	}
	contents = strings.Replace(contents, "schema=`/usr/bin/printf 'product1 product2 product3'`", "schema=*", 1)
	writeFile(t, "mydb/product/.skeema", contents)
	s.handleCommand(t, CodeSuccess, ".", "skeema push --allow-unsafe")
	s.assertExists(t, "product1", "posts", "")
	s.assertExists(t, "analytics", "posts", "")
	s.assertMissing(t, "analytics", "pageviews", "")
	s.handleCommand(t, CodeSuccess, ".", "skeema diff")

	// Since analytics is the first alphabetically, it is now the prototype
	// as far as pull is concerned
	s.dbExec(t, "analytics", "CREATE TABLE `foo` (id int)")
	s.handleCommand(t, CodeSuccess, ".", "skeema pull")
	readFile(t, "mydb/product/foo.sql")                          // just confirming it exists
	s.handleCommand(t, CodeDifferencesFound, ".", "skeema diff") // since 3 schemas missing foo
	s.handleCommand(t, CodeSuccess, ".", "skeema push")
	s.assertExists(t, "product1", "foo", "")
	s.assertExists(t, "product2", "foo", "")
	s.handleCommand(t, CodeSuccess, ".", "skeema diff")

	// Test combination of ignore-schema and schema=*
	contents = strings.Replace(contents, "schema=*", "schema=*\nignore-schema=2$", 1)
	writeFile(t, "mydb/product/.skeema", contents)
	writeFile(t, "mydb/product/foo2.sql", "CREATE TABLE `foo2` (id int);\n")
	s.handleCommand(t, CodeSuccess, ".", "skeema push")
	s.assertExists(t, "product1", "foo2", "")
	s.assertMissing(t, "product2", "foo2", "")
	s.assertExists(t, "product3", "foo2", "")
}
