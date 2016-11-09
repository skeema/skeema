package main

import (
	"fmt"
	"log"

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

	var errCount int
	mods := tengo.StatementModifiers{
		NextAutoInc: tengo.NextAutoIncIfIncreased,
	}

	// TODO: once sharding / service discovery lookup is supported, this should
	// use multiple worker goroutines all pulling instances off the channel
	for t := range dir.Targets(true, true) {
		if hasErrors, firstErr := t.HasErrors(); hasErrors {
			fmt.Printf("\nSkipping %s:\n    %s\n", t.Dir, firstErr)
			t.Done()
			errCount++
			continue
		}

		fmt.Printf("\nPushing changes from %s/*.sql to %s %s...\n", t.Dir, t.Instance, t.SchemaFromDir.Name)
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
			fmt.Printf("%s;\n", t.SchemaFromDir.CreateStatement())
		} else if len(diff.TableDiffs) == 0 {
			fmt.Println("(nothing to do)")
			t.Done()
			continue
		}

		if t.Dir.Config.GetBool("verify") && len(diff.TableDiffs) > 0 {
			if err := t.verifyDiff(diff); err != nil {
				t.Done()
				return err
			}
		}

		mods.AllowDropTable = t.Dir.Config.GetBool("allow-drop-table")
		mods.AllowDropColumn = t.Dir.Config.GetBool("allow-drop-column")
		var statementCounter int
		for n, tableDiff := range diff.TableDiffs {
			ddl := NewDDLStatement(tableDiff, mods, t)
			if ddl == nil {
				continue
			}
			statementCounter++
			fmt.Printf(ddl.String())
			if ddl.Err == nil {
				if err := ddl.Execute(); err != nil {
					log.Printf("Error running above statement on %s: %s", t.Instance, err)
					skipCount := len(diff.TableDiffs) - n
					if skipCount > 1 {
						log.Printf("Skipping %d additional statements on %s %s", skipCount-1, t.Instance, t.SchemaFromDir.Name)
					}
					errCount += skipCount
					break
				}
			}
		}

		// If we had diffs but they were all no-ops due to StatementModifiers,
		// still display message about no actions taken
		if statementCounter == 0 {
			fmt.Println("(nothing to do)")
		}

		t.Done()
	}

	if errCount == 0 {
		return nil
	}
	var plural string
	if errCount > 1 {
		plural = "s"
	}
	return NewExitValue(CodePartialError, "Skipped %d operation%s due to error%s", errCount, plural, plural)
}
