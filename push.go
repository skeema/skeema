package main

import (
	"fmt"

	log "github.com/Sirupsen/logrus"
	"github.com/skeema/mycli"
	"github.com/skeema/tengo"
)

func init() {
	summary := "Alter tables on DBs to reflect the filesystem representation"
	desc := `Modifies the schemas on database instance(s) to match the corresponding
filesystem representation of them. This essentially performs the same diff logic
as ` + "`" + `skeema diff` + "`" + `, but then actually runs the generated DDL. You should generally
run ` + "`" + `skeema diff` + "`" + ` first to see what changes will be applied.

You may optionally pass an environment name as a CLI option. This will affect
which section of .skeema config files is used for processing. For example,
running ` + "`" + `skeema push staging` + "`" + ` will apply config directives from the
[staging] section of config files, as well as any sectionless directives at the
top of the file. If no environment name is supplied, the default is
"production".`

	cmd := mycli.NewCommand("push", summary, desc, PushHandler)
	cmd.AddOption(mycli.BoolOption("verify", 0, true, "Test all generated ALTER statements on temporary schema to verify correctness"))
	cmd.AddOption(mycli.BoolOption("allow-drop-table", 0, false, "Permit dropping any table that has no corresponding *.sql file"))
	cmd.AddOption(mycli.BoolOption("allow-drop-column", 0, false, "Permit dropping columns that are no longer present in *.sql file"))
	cmd.AddOption(mycli.StringOption("alter-wrapper", 'x', "", "External bin to shell out to for ALTER TABLE; see manual for template vars"))
	cmd.AddArg("environment", "production", false)
	CommandSuite.AddSubCommand(cmd)
}

// PushHandler is the handler method for `skeema push`
func PushHandler(cfg *mycli.Config) error {
	AddGlobalConfigFiles(cfg)
	dir, err := NewDir(".", cfg)
	if err != nil {
		return err
	}

	var errCount, unsupportedCount int // total counts, across all targets
	mods := tengo.StatementModifiers{
		NextAutoInc: tengo.NextAutoIncIfIncreased,
	}

	// TODO: once sharding / service discovery lookup is supported, this should
	// use multiple worker goroutines all pulling instances off the channel
	for t := range dir.Targets(true, true) {
		if hasErrors, firstErr := t.HasErrors(); hasErrors {
			log.Errorf("Skipping %s:", t.Dir)
			log.Errorf("    %s", firstErr)
			fmt.Println()
			t.Done()
			errCount++
			continue
		}

		log.Infof("Pushing changes from %s/*.sql to %s %s", t.Dir, t.Instance, t.SchemaFromDir.Name)
		diff, err := tengo.NewSchemaDiff(t.SchemaFromInstance, t.SchemaFromDir)
		if err != nil {
			t.Done()
			return err
		}
		if t.SchemaFromInstance == nil {
			// TODO: support CREATE DATABASE schema-level options
			var err error
			t.SchemaFromInstance, err = t.Instance.CreateSchema(t.SchemaFromDir.Name)
			if err != nil {
				t.Done()
				return fmt.Errorf("Error creating schema %s on %s: %s", t.SchemaFromDir.Name, t.Instance, err)
			}
			fmt.Printf("-- instance: %s\n", t.Instance)
			fmt.Printf("%s;\n", t.SchemaFromDir.CreateStatement())
		}

		if t.Dir.Config.GetBool("verify") && len(diff.TableDiffs) > 0 {
			if err := t.verifyDiff(diff); err != nil {
				t.Done()
				return err
			}
		}

		mods.AllowDropTable = t.Dir.Config.GetBool("allow-drop-table")
		mods.AllowDropColumn = t.Dir.Config.GetBool("allow-drop-column")
		var targetStmtCount int
		for n, tableDiff := range diff.TableDiffs {
			ddl := NewDDLStatement(tableDiff, mods, t)
			if ddl == nil {
				// skip blank DDL (which may happen due to NextAutoInc modifier)
				continue
			}
			if targetStmtCount++; targetStmtCount == 1 {
				fmt.Printf("-- instance: %s\n", t.Instance)
				fmt.Printf("USE %s;\n", tengo.EscapeIdentifier(t.SchemaFromDir.Name))
			}
			if ddl.Err != nil {
				log.Errorf("%s. The following DDL statement will be skipped. See --help for more information.", ddl.Err)
				errCount++
			}
			fmt.Println(ddl.String())
			if ddl.Err == nil && ddl.Execute() != nil {
				log.Errorf("Error running above statement on %s: %s", t.Instance, ddl.Err)
				skipCount := len(diff.TableDiffs) - n
				if skipCount > 1 {
					log.Warnf("Skipping %d additional statements on %s %s", skipCount-1, t.Instance, t.SchemaFromDir.Name)
				}
				errCount += skipCount
				break
			}
		}
		for _, table := range diff.UnsupportedTables {
			targetStmtCount++
			unsupportedCount++
			log.Warnf("Skipping table %s: unable to generate ALTER TABLE due to use of unsupported features", table.Name)
		}

		if targetStmtCount == 0 {
			log.Info("(nothing to do)")
		}
		fmt.Println()
		t.Done()
	}

	if errCount+unsupportedCount == 0 {
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
