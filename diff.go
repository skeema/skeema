package main

import (
	"fmt"

	log "github.com/Sirupsen/logrus"
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
"production".

An exit code of 0 will be returned if no differences were found, 1 if some
differences were found, or 2+ if an error occurred.`

	cmd := mycli.NewCommand("diff", summary, desc, DiffHandler)
	cmd.AddOption(mycli.BoolOption("verify", 0, true, "Test all generated ALTER statements on temporary schema to verify correctness"))
	cmd.AddOption(mycli.BoolOption("allow-drop-table", 0, false, "In output, include a DROP TABLE for any table without a corresponding *.sql file"))
	cmd.AddOption(mycli.BoolOption("allow-drop-column", 0, false, "In output, include DROP COLUMN clauses where appropriate"))
	cmd.AddOption(mycli.StringOption("alter-wrapper", 'x', "", "Output ALTER TABLEs as shell commands rather than just raw DDL; see manual for template vars"))
	cmd.AddArg("environment", "production", false)
	CommandSuite.AddSubCommand(cmd)
}

// DiffHandler is the handler method for `skeema diff`
func DiffHandler(cfg *mycli.Config) error {
	AddGlobalConfigFiles(cfg)
	dir, err := NewDir(".", cfg)
	if err != nil {
		return err
	}

	var errCount, diffCount, unsupportedCount int // total counts, across all targets
	mods := tengo.StatementModifiers{
		NextAutoInc: tengo.NextAutoIncIfIncreased,
	}

	for t := range dir.Targets(false, false) {
		if hasErrors, firstErr := t.HasErrors(); hasErrors {
			log.Errorf("Skipping %s:", t.Dir)
			log.Errorf("    %s\n", firstErr)
			errCount++
			continue
		}

		log.Infof("Generating diff of %s %s vs %s/*.sql", t.Instance, t.SchemaFromDir.Name, t.Dir)
		diff, err := tengo.NewSchemaDiff(t.SchemaFromInstance, t.SchemaFromDir)
		if err != nil {
			return err
		}
		if t.SchemaFromInstance == nil {
			// TODO: support CREATE DATABASE schema-level options
			fmt.Printf("-- instance: %s\n", t.Instance)
			fmt.Printf("%s;\n", t.SchemaFromDir.CreateStatement())
		}
		if cfg.GetBool("verify") && len(diff.TableDiffs) > 0 {
			if err := t.verifyDiff(diff); err != nil {
				return err
			}
		}

		mods.AllowDropTable = t.Dir.Config.GetBool("allow-drop-table")
		mods.AllowDropColumn = t.Dir.Config.GetBool("allow-drop-column")
		var targetStmtCount int
		for _, tableDiff := range diff.TableDiffs {
			ddl := NewDDLStatement(tableDiff, mods, t)
			if ddl == nil {
				// skip blank DDL (which may happen due to NextAutoInc modifier)
				continue
			}
			if targetStmtCount++; targetStmtCount == 1 {
				fmt.Printf("-- instance: %s\n", t.Instance)
				fmt.Printf("USE %s;\n", tengo.EscapeIdentifier(t.SchemaFromDir.Name))
			}
			diffCount++
			if ddl.Err != nil {
				log.Errorf("%s. The following DDL statement will be skipped. See --help for more information.", ddl.Err)
				errCount++
			}
			fmt.Println(ddl.String())
		}
		for _, table := range diff.UnsupportedTables {
			log.Warnf("Skipping table %s: unable to generate ALTER TABLE due to use of unsupported features", table.Name)
			unsupportedCount++
			targetStmtCount++
		}
		if targetStmtCount == 0 {
			log.Info("No differences found\n")
		} else {
			fmt.Println()
		}
	}

	if errCount+unsupportedCount == 0 {
		if diffCount > 0 {
			return NewExitValue(CodeDifferencesFound, "")
		}
		return nil
	}
	var plural, reason string
	code := CodeFatalError
	if errCount+unsupportedCount > 1 {
		plural = "s"
	}
	if errCount == 0 {
		code = CodePartialError
		reason = "unsupported feature"
	} else if unsupportedCount == 0 {
		reason = "error"
	} else {
		reason = "unsupported features or error"
	}
	return NewExitValue(code, "Skipped %d operation%s due to %s%s", errCount+unsupportedCount, plural, reason, plural)
}
