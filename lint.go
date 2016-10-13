package main

import (
	"errors"
	"fmt"

	"github.com/skeema/mycli"
)

func init() {
	summary := "Verify table files and reformat them in a standardized way"
	desc := `Reformats the filesystem representation of tables to match the format of SHOW
CREATE TABLE. Verifies that all table files contain valid SQL in their CREATE
TABLE statements.

This command relies on accessing database instances to test the SQL DDL. All DDL
will be run against a temporary schema, with no impact on the real schema.

You may optionally pass an environment name as a CLI option. This will affect
which section of .skeema config files is used for obtaining a database instance
to test the SQL DDL against. For example, running ` + "`" + `skeema lint staging` + "`" + ` will
apply config directives from the [staging] section of config files, as well as
any sectionless directives at the top of the file. If no environment name is 
supplied, the default is "production".`

	cmd := mycli.NewCommand("lint", summary, desc, LintHandler)
	cmd.AddArg("environment", "production", false)
	CommandSuite.AddSubCommand(cmd)
}

func LintHandler(cfg *mycli.Config) error {
	AddGlobalConfigFiles(cfg)
	dir, err := NewDir(".", cfg)
	if err != nil {
		return err
	}

	var errCount, sqlErrCount int
	for t := range dir.Targets(false, false) {

		if t.Err != nil { // we only skip on fatal errors (t.Err), not SQL file errors (t.SQLFileErrors or t.HasError())
			fmt.Printf("Skipping %s: %s\n", t.Dir, t.Err)
			errCount++
			continue
		}

		fmt.Printf("Linting %s...\n", t.Dir)

		for _, err := range t.SQLFileErrors {
			fmt.Printf("    %s\n", err)
			sqlErrCount++
		}

		tables, _ := t.SchemaFromDir.Tables() // can ignore error since table list already guaranteed to be cached

		for _, table := range tables {
			sf := SQLFile{
				Dir:      t.Dir,
				FileName: fmt.Sprintf("%s.sql", table.Name),
			}
			if _, err := sf.Read(); err != nil {
				return err
			}
			if table.CreateStatement() != sf.Contents {
				sf.Contents = table.CreateStatement()
				if length, err := sf.Write(); err != nil {
					return fmt.Errorf("Unable to write to %s: %s", sf.Path(), err)
				} else {
					fmt.Printf("    Wrote %s (%d bytes) -- updated file to normalize format\n", sf.Path(), length)
				}
			}
		}
	}

	switch {
	case errCount == 0 && sqlErrCount == 0:
		return nil
	case errCount == 1:
		return errors.New("Skipped 1 operation due to error")
	case errCount > 1:
		return fmt.Errorf("Skipped %d operations due to errors", errCount)
	case sqlErrCount == 1:
		return errors.New("Found syntax errors in 1 SQL file")
	default:
		return fmt.Errorf("Found syntax errors in %d SQL files", sqlErrCount)
	}
}
