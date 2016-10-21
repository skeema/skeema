package main

import (
	"errors"
	"fmt"

	"github.com/skeema/mycli"
	"github.com/skeema/tengo"
)

func init() {
	summary := "Compare a DB instance's schemas and tables to the filesystem"
	desc := `Compares the schemas on database instance(s) to the corresponding filesystem
representation of them. The output is a series of DDL commands that, if run on
the instance, would cause the instances' schemas to now match the ones in the
filesystem.

You may optionally pass an environment name as a CLI option. This will affect
which section of .skeema config files is used for processing. For example,
running ` + "`" + `skeema diff staging` + "`" + ` will apply config directives from the
[staging] section of config files, as well as any sectionless directives at the
top of the file. If no environment name is supplied, the default is
"production".`

	cmd := mycli.NewCommand("diff", summary, desc, DiffHandler)
	cmd.AddOption(mycli.BoolOption("verify", 0, true, "Test all generated ALTER statements on temporary schema to verify correctness"))
	cmd.AddOption(mycli.BoolOption("allow-drop-table", 0, false, "In output, include a DROP TABLE for any table without a corresponding *.sql file"))
	cmd.AddOption(mycli.BoolOption("allow-drop-column", 0, false, "In output, include DROP COLUMN clauses where appropriate"))
	cmd.AddArg("environment", "production", false)
	CommandSuite.AddSubCommand(cmd)
}

func DiffHandler(cfg *mycli.Config) error {
	AddGlobalConfigFiles(cfg)
	dir, err := NewDir(".", cfg)
	if err != nil {
		return err
	}

	var errCount int
	mods := tengo.StatementModifiers{
		NextAutoInc: tengo.NextAutoIncIfIncreased,
	}

	for t := range dir.Targets(false, false) {
		if hasErrors, firstErr := t.HasErrors(); hasErrors {
			fmt.Printf("-- Skipping %s:\n--    %s\n\n", t.Dir, firstErr)
			errCount++
			continue
		}

		fmt.Printf("-- Diff of %s %s vs %s/*.sql\n", t.Instance, t.SchemaFromDir.Name, t.Dir)
		diff, err := tengo.NewSchemaDiff(t.SchemaFromInstance, t.SchemaFromDir)
		if err != nil {
			return err
		}
		if t.SchemaFromInstance == nil {
			// TODO: support CREATE DATABASE schema-level options
			fmt.Printf("%s;\n", t.SchemaFromDir.CreateStatement())
		}
		if cfg.GetBool("verify") && len(diff.TableDiffs) > 0 {
			if err := t.verifyDiff(diff); err != nil {
				return err
			}
		}

		mods.AllowDropTable = t.Dir.Config.GetBool("allow-drop-table")
		mods.AllowDropColumn = t.Dir.Config.GetBool("allow-drop-column")
		for _, tableDiff := range diff.TableDiffs {
			if stmt, err := tableDiff.Statement(mods); err != nil {
				fmt.Printf("-- %s. See --help for how to override.\n-- %s;\n", err, stmt)
			} else if stmt != "" {
				fmt.Printf("%s;\n", stmt)
			}
		}
		fmt.Println()
	}

	switch errCount {
	case 0:
		return nil
	case 1:
		return errors.New("Skipped 1 operation due to error")
	default:
		return fmt.Errorf("Skipped %d operations due to errors", errCount)
	}
}
