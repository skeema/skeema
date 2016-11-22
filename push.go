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
	cmd.AddOption(mycli.BoolOption("dry-run", 0, false, "Output DDL but don't run it; equivalent to `skeema diff`"))
	cmd.AddOption(mycli.StringOption("alter-wrapper", 'x', "", "External bin to shell out to for ALTER TABLE; see manual for template vars"))
	cmd.AddOption(mycli.StringOption("alter-lock", 0, "", `Apply a LOCK clause to all ALTER TABLEs (valid values: "NONE", "SHARED", "EXCLUSIVE")`))
	cmd.AddOption(mycli.StringOption("alter-algorithm", 0, "", `Apply an ALGORITHM clause to all ALTER TABLEs (valid values: "INPLACE", "COPY")`))
	cmd.AddOption(mycli.StringOption("ddl-wrapper", 'X', "", "Like --alter-wrapper, but applies to all DDL types (CREATE, DROP, ALTER)"))
	cmd.AddArg("environment", "production", false)
	CommandSuite.AddSubCommand(cmd)
	clonePushOptionsToDiff()
}

// PushHandler is the handler method for `skeema push`
func PushHandler(cfg *mycli.Config) error {
	AddGlobalConfigFiles(cfg)
	dir, err := NewDir(".", cfg)
	if err != nil {
		return err
	}

	var errCount, diffCount, unsupportedCount int // total counts, across all targets
	mods := tengo.StatementModifiers{
		NextAutoInc: tengo.NextAutoIncIfIncreased,
	}
	dryRun := cfg.GetBool("dry-run")

	// TODO: once sharding / service discovery lookup is supported, this should
	// use multiple worker goroutines all pulling instances off the channel,
	// unless dry-run option is true
	for t := range dir.Targets(true, true) {
		if hasErrors, firstErr := t.HasErrors(); hasErrors {
			log.Errorf("Skipping %s:", t.Dir)
			log.Errorf("    %s\n", firstErr)
			t.Done()
			errCount++
			continue
		}

		if dryRun {
			log.Infof("Generating diff of %s %s vs %s/*.sql", t.Instance, t.SchemaFromDir.Name, t.Dir)
		} else {
			log.Infof("Pushing changes from %s/*.sql to %s %s", t.Dir, t.Instance, t.SchemaFromDir.Name)
		}
		for _, warning := range t.SQLFileWarnings {
			log.Debug(warning)
		}

		diff, err := tengo.NewSchemaDiff(t.SchemaFromInstance, t.SchemaFromDir)
		if err != nil {
			t.Done()
			return err
		}
		if t.SchemaFromInstance == nil {
			// TODO: support CREATE DATABASE schema-level options
			fmt.Printf("-- instance: %s\n", t.Instance)
			fmt.Printf("%s;\n", t.SchemaFromDir.CreateStatement())
			if !dryRun {
				var err error
				t.SchemaFromInstance, err = t.Instance.CreateSchema(t.SchemaFromDir.Name)
				if err != nil {
					t.Done()
					return fmt.Errorf("Error creating schema %s on %s: %s", t.SchemaFromDir.Name, t.Instance, err)
				}
			}
		}

		if t.Dir.Config.GetBool("verify") && len(diff.TableDiffs) > 0 {
			if err := t.verifyDiff(diff); err != nil {
				t.Done()
				return err
			}
		}

		// Set configuration-dependent statement modifiers here inside the Target
		// loop, since the config for these may var per dir!
		mods.AllowDropTable = t.Dir.Config.GetBool("allow-drop-table")
		mods.AllowDropColumn = t.Dir.Config.GetBool("allow-drop-column")
		mods.AlgorithmClause, err = t.Dir.Config.GetEnum("alter-algorithm", "INPLACE", "COPY", "DEFAULT")
		if err != nil {
			return err
		}
		mods.LockClause, err = t.Dir.Config.GetEnum("alter-lock", "NONE", "SHARED", "EXCLUSIVE", "DEFAULT")
		if err != nil {
			return err
		}

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
			diffCount++
			if ddl.Err != nil {
				log.Errorf("%s. The following DDL statement will be skipped. See --help for more information.", ddl.Err)
				errCount++
			}
			fmt.Println(ddl.String())
			if !dryRun && ddl.Err == nil && ddl.Execute() != nil {
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
			unsupportedCount++
			targetStmtCount++
			if t.Dir.Config.GetBool("debug") {
				log.Warnf("Skipping table %s: unable to generate ALTER TABLE due to use of unsupported features", table.Name)
				t.logUnsupportedTableDiff(table.Name)
			} else {
				log.Warnf("Skipping table %s: unable to generate ALTER TABLE due to use of unsupported features. Use --debug for more information.", table.Name)
			}
		}

		if targetStmtCount == 0 {
			if dryRun {
				log.Info("No differences found\n")
			} else {
				log.Info("(nothing to do)\n")
			}
		} else {
			fmt.Println()
		}
		t.Done()
	}

	if errCount+unsupportedCount == 0 {
		if dryRun && diffCount > 0 {
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
