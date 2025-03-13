package applier

import (
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/skeema/mybase"
	"github.com/skeema/skeema/internal/fs"
	"github.com/skeema/skeema/internal/tengo"
	"github.com/skeema/skeema/internal/workspace"
)

func (s ApplierIntegrationSuite) TestNewDDLStatement(t *testing.T) {
	sourceSQL := func(filename string) {
		t.Helper()
		if _, err := s.d[0].SourceSQL(filepath.Join("testdata", filename)); err != nil {
			t.Fatalf("Unexpected error from SourceSQL on %s: %s", filename, err)
		}
	}
	dbExec := func(schemaName, query string, args ...interface{}) {
		t.Helper()
		db, err := s.d[0].CachedConnectionPool(schemaName, "")
		if err != nil {
			t.Fatalf("Unable to connect to DockerizedInstance: %s", err)
		}
		_, err = db.Exec(query, args...)
		if err != nil {
			t.Fatalf("Error running query on DockerizedInstance.\nSchema: %s\nQuery: %s\nError: %s", schemaName, query, err)
		}
	}
	getSchema := func(schemaName string) *tengo.Schema {
		t.Helper()
		schema, err := s.d[0].Schema(schemaName)
		if err != nil {
			t.Fatalf("Unable to obtain schema %s: %s", schemaName, err)
		}
		return schema
	}

	// DB setup: init files and then change the DB so that we have some
	// differences to generate. We first add a default value to domain col
	// to later test that quoting rules are working properly for shellouts.
	sourceSQL("setup.sql")
	dbExec("analytics", "ALTER TABLE pageviews MODIFY COLUMN domain varchar(40) NOT NULL DEFAULT 'skeema.io'")
	fsSchema := getSchema("analytics")
	sourceSQL("ddlstatement.sql")
	instSchema := getSchema("analytics")

	// Hackily set up test args manually
	flavor := s.d[0].Flavor()
	configMap := map[string]string{
		"user":                   "root",
		"password":               s.d[0].Instance.Password,
		"debug":                  "1",
		"allow-unsafe":           "1",
		"ddl-wrapper":            "/bin/echo ddl-wrapper {SCHEMA}.{NAME} {TYPE} {CLASS}",
		"alter-wrapper":          "/bin/echo alter-wrapper {SCHEMA}.{TABLE} {TYPE} {CLAUSES}",
		"alter-wrapper-min-size": "1",
		"alter-algorithm":        "inplace",
		"alter-lock":             "none",
		"safe-below-size":        "0",
		"connect-options":        "",
		"environment":            "production",
	}
	if flavor.IsMySQL(5, 5) {
		delete(configMap, "alter-algorithm")
		delete(configMap, "alter-lock")
	}
	if runtime.GOOS == "windows" {
		configMap["ddl-wrapper"] = `echo "ddl-wrapper {SCHEMA}.{NAME} {TYPE} {CLASS}"`
		configMap["alter-wrapper"] = `echo "alter-wrapper {SCHEMA}.{TABLE} {TYPE} {CLAUSES}"`
	}
	cfg := mybase.SimpleConfig(configMap)
	dir := &fs.Dir{
		Path:   "/var/tmp/fakedir",
		Config: cfg,
	}
	target := &Target{
		Instance:   s.d[0].Instance,
		Dir:        dir,
		SchemaName: "analytics",
		DesiredSchema: &workspace.Schema{
			Schema: fsSchema,
		},
	}

	sd := tengo.NewSchemaDiff(instSchema, fsSchema)
	objDiffs := sd.ObjectDiffs()
	if len(objDiffs) != 5 {
		// modifications in ddlstatement.sql should have yielded 5 diffs: one alter
		// database, one drop table, one create table, and two alter tables (one to
		// a table with rows and one to a table without rows)
		t.Fatalf("Expected 5 object diffs, instead found %d %#v", len(objDiffs), objDiffs)
	}

	mods := tengo.StatementModifiers{AllowUnsafe: true}
	if !flavor.IsMySQL(5, 5) {
		mods.LockClause, mods.AlgorithmClause = "none", "inplace"
	}
	for _, diff := range objDiffs {
		ddl, err := NewDDLStatement(diff, mods, target)
		if err != nil {
			t.Errorf("Unexpected DDLStatement error: %s", err)
		}
		if ddl.shellOut == nil {
			t.Fatalf("Expected this configuration to result in all DDLs being shellouts, but %v is not", ddl)
		}
		expected := objectDiffExpected(t, diff, ddl, s.d[0].Flavor())
		if ddl.shellOut.String() != expected {
			t.Errorf("Expected shellout:\n%s\nActual shellout:\n%s\n", expected, ddl.shellOut)
		}
		if expectedString := "\\! " + expected; ddl.Statement() != expectedString {
			t.Errorf("Expected String():\n%s\nActual String():\n%s\n", expectedString, ddl.Statement())
		}
	}
}

// helper for TestNewDDLStatement; return value is specific to the setup of
// that test
func objectDiffExpected(t *testing.T, diff tengo.ObjectDiff, ddl *DDLStatement, flavor tengo.Flavor) (expected string) {
	switch diff := diff.(type) {
	case *tengo.DatabaseDiff:
		expected = "/bin/echo ddl-wrapper .analytics ALTER DATABASE"
		if ddl.schemaName != "" {
			t.Errorf("Unexpected DDLStatement.schemaName: %s", ddl.schemaName)
		}
	case *tengo.TableDiff:
		if ddl.schemaName != "analytics" {
			t.Errorf("Unexpected DDLStatement.schemaName: %s", ddl.schemaName)
		}
		switch diff.DiffType() {
		case tengo.DiffTypeAlter:
			if diff.To.Name == "rollups" {
				// no rows, so ddl-wrapper used. verify the statement separately.
				expected = "/bin/echo ddl-wrapper analytics.rollups ALTER TABLE"
				expectedStmt := "ALTER TABLE `rollups` ALGORITHM=INPLACE, LOCK=NONE, ADD COLUMN `value` bigint(20) DEFAULT NULL"
				if strings.Contains(flavor.String(), ":5.5") {
					expectedStmt = "ALTER TABLE `rollups` ADD COLUMN `value` bigint(20) DEFAULT NULL"
				} else if flavor.OmitIntDisplayWidth() {
					expectedStmt = strings.ReplaceAll(expectedStmt, "bigint(20)", "bigint")
				}
				if ddl.stmt != expectedStmt {
					t.Errorf("Expected statement:\n%s\nActual statement:\n%s\n", expectedStmt, ddl.stmt)
				}
			} else if diff.To.Name == "pageviews" {
				// has 1 row, so alter-wrapper used. verify the execution separately to
				// sanity-check the quoting rules.
				expected = "/bin/echo alter-wrapper analytics.pageviews ALTER 'ADD COLUMN `domain` varchar(40) NOT NULL DEFAULT '\"'\"'skeema.io'\"'\"''"
				expectedOutput := "alter-wrapper analytics.pageviews ALTER ADD COLUMN `domain` varchar(40) NOT NULL DEFAULT 'skeema.io'\n"
				if runtime.GOOS != "windows" { // skipping on Windows due to quoting insanity / differences in how echo works
					if actualOutput, err := ddl.shellOut.RunCapture(); err != nil || actualOutput != expectedOutput {
						t.Errorf("Expected output:\n%sActual output:\n%sErr:\n%v\n", expectedOutput, actualOutput, err)
					}
				}
			} else {
				t.Fatalf("Unexpected AlterTable for %s; perhaps test fixture changed without updating this test?", diff.To.Name)
			}
		case tengo.DiffTypeDrop:
			expected = "/bin/echo ddl-wrapper analytics.widget_counts DROP TABLE"
		case tengo.DiffTypeCreate:
			expected = "/bin/echo ddl-wrapper analytics.activity CREATE TABLE"
		}
	}
	if runtime.GOOS == "windows" {
		expected = strings.ReplaceAll(expected, "/bin/echo ", `echo "`) + `"`
		expected = strings.ReplaceAll(expected, "'\"'\"'", "''")
	}
	return
}
