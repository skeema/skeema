package main

import (
	"fmt"

	"github.com/skeema/mycli"
)

func init() {
	summary := "Verify table files and reformat them in a standardized way"
	desc := `Reformats the filesystem representation of tables to match the format of SHOW
CREATE TABLE. Verifies that all table files contain valid SQL in their CREATE
TABLE statements.`

	cmd := mycli.NewCommand("lint", summary, desc, LintCommand, 0, 1, "environment")
	CommandSuite.AddSubCommand(cmd)
}

func LintCommand(cfg *mycli.Config) error {
	environment := "production"
	if len(cfg.CLI.Args) > 0 {
		environment = cfg.CLI.Args[0]
	}
	AddGlobalConfigFiles(cfg, environment)

	dir, err := NewDir(".", cfg, environment)
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
