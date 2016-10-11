package main

import (
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
running ` + "`" + `skeema diff production` + "`" + ` will apply config directives from the
[production] section of config files, as well as any sectionless directives
at the top of the file. If no environment name is supplied, only the sectionless
directives alone will be applied.`

	cmd := mycli.NewCommand("diff", summary, desc, DiffCommand, 0, 1, "environment")
	cmd.AddOption(mycli.BoolOption("verify", 0, true, "Test all generated ALTER statements on temporary schema to verify correctness"))
	CommandSuite.AddSubCommand(cmd)
}

func DiffCommand(cfg *mycli.Config) error {
	environment := "production"
	if len(cfg.CLI.Args) > 0 {
		environment = cfg.CLI.Args[0]
	}
	AddGlobalConfigFiles(cfg, environment)

	dir, err := NewDir(".", cfg, environment)
	if err != nil {
		return err
	}

	mods := tengo.StatementModifiers{
		NextAutoInc: tengo.NextAutoIncIfIncreased,
	}

	for t := range dir.Targets(false, false) {
		if t.Err != nil {
			fmt.Printf("Skipping %s: %s\n", t.Dir, t.Err)
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
		for _, tableDiff := range diff.TableDiffs {
			stmt := tableDiff.Statement(mods)
			if stmt != "" {
				fmt.Printf("%s;\n", stmt)
			}
		}
		fmt.Println()
	}

	return nil
}
