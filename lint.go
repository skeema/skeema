package main

import (
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

	for t := range dir.Targets(false, false) {
		if t.Err != nil {
			fmt.Printf("Skipping %s: %s\n", t.Dir, t.Err)
			continue
		}

		fmt.Printf("Linting %s...\n", t.Dir)

		// Can ignore errors on t.SchemaFromDir.Tables() since it is guaranteed to already be pre-cached
		tables, _ := t.SchemaFromDir.Tables()
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

	return nil
}
