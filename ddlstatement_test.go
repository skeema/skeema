package main

import (
	"testing"

	"github.com/skeema/mybase"
	"github.com/skeema/tengo"
)

func (s *SkeemaIntegrationSuite) TestNewDDLStatement(t *testing.T) {
	// Setup: init files and then change the DB so that we have some
	// differences to generate. We first add a default value to domain col
	// to later test that quoting rules are working properly for shellouts.
	s.dbExec(t, "analytics", "ALTER TABLE pageviews MODIFY COLUMN domain varchar(40) NOT NULL DEFAULT 'skeema.net'")
	s.handleCommand(t, CodeSuccess, ".", "skeema init --dir mydb -h %s -P %d", s.d.Instance.Host, s.d.Instance.Port)
	s.sourceSQL(t, "ddlstatement.sql")

	// Generate a diff a bit manually
	fakeFileSource := mybase.SimpleSource(map[string]string{
		"password":               s.d.Instance.Password,
		"debug":                  "1",
		"allow-unsafe":           "1",
		"ddl-wrapper":            "/bin/echo ddl-wrapper {SCHEMA}.{TABLE} {TYPE}",
		"alter-wrapper":          "/bin/echo alter-wrapper {SCHEMA}.{TABLE} {TYPE} {CLAUSES}",
		"alter-wrapper-min-size": "1",
		"alter-algorithm":        "INPLACE",
		"alter-lock":             "NONE",
	})
	cfg := mybase.ParseFakeCLI(t, CommandSuite, "skeema diff ", fakeFileSource)
	AddGlobalConfigFiles(cfg)
	dir, err := NewDir(".", cfg)
	if err != nil {
		t.Fatalf("Unexpected error from NewDir: %s", err)
	}
	var target *Target
	for _, thisTarget := range dir.Targets() {
		if thisTarget.SchemaFromInstance.Name == "analytics" {
			target = thisTarget
			break
		}
	}
	sd := tengo.NewSchemaDiff(target.SchemaFromInstance, target.SchemaFromDir)
	if len(sd.TableDiffs) != 4 {
		// modifications in ddlstatement.sql should have yielded 4 diffs: one drop
		// table, one create table, and two alter tables (one to a table with rows
		// and one to a table without rows)
		t.Fatalf("Expected 4 table diffs, instead found %d", len(sd.TableDiffs))
	}

	mods := tengo.StatementModifiers{
		AllowUnsafe:     true,
		LockClause:      "NONE",
		AlgorithmClause: "INPLACE",
	}
	for _, diff := range sd.TableDiffs {
		ddl := NewDDLStatement(diff, mods, target)
		if ddl.Err != nil {
			t.Errorf("Unexpected DDLStatement error: %s", ddl.Err)
		}
		if !ddl.IsShellOut() {
			t.Fatalf("Expected this configuration to result in all DDLs being shellouts, but %v is not", ddl)
		}
		var expected string
		switch diff.Type {
		case tengo.TableDiffAlter:
			if diff.To.Name == "rollups" {
				// no rows, so ddl-wrapper used. verify the statement separately.
				expected = "/bin/echo ddl-wrapper analytics.rollups ALTER"
				expectedStmt := "ALTER TABLE `rollups` ALGORITHM=INPLACE, LOCK=NONE, ADD COLUMN `value` bigint(20) DEFAULT NULL"
				if ddl.stmt != expectedStmt {
					t.Errorf("Expected statement:\n%s\nActual statement:\n%s\n", expectedStmt, ddl.stmt)
				}
			} else if diff.To.Name == "pageviews" {
				// has 1 row, so alter-wrapper used. verify the execution separately to
				// sanity-check the quoting rules.
				expected = "/bin/echo alter-wrapper analytics.pageviews ALTER 'ADD COLUMN `domain` varchar(40) NOT NULL DEFAULT '\"'\"'skeema.net'\"'\"''"
				expectedOutput := "alter-wrapper analytics.pageviews ALTER ADD COLUMN `domain` varchar(40) NOT NULL DEFAULT 'skeema.net'\n"
				if actualOutput, err := ddl.shellOut.RunCapture(); err != nil || actualOutput != expectedOutput {
					t.Errorf("Expected output:\n%sActual output:\n%sErr:\n%v\n", expectedOutput, actualOutput, err)
				}
			} else {
				t.Fatalf("Unexpected AlterTable for %s; perhaps test fixture changed without updating this test?", diff.To.Name)
			}
		case tengo.TableDiffDrop:
			expected = "/bin/echo ddl-wrapper analytics.widget_counts DROP"
		case tengo.TableDiffCreate:
			expected = "/bin/echo ddl-wrapper analytics.activity CREATE"
		}
		if ddl.shellOut.Command != expected {
			t.Errorf("Expected shellout:\n%s\nActual shellout:\n%s\n", expected, ddl.shellOut.Command)
		}
	}
}
