package main

import (
	"fmt"

	"github.com/skeema/mycli"
	"github.com/skeema/tengo"
)

func init() {
	summary := "Update the filesystem representation of schemas and tables"
	desc := `Updates the existing filesystem representation of the schemas and tables on a DB
instance. Use this command when changes have been applied to the database
without using skeema, and the filesystem representation needs to be updated to
reflect those changes.

You may optionally pass an environment name as a CLI option. This will affect
which section of .skeema config files is used for processing. For example,
running ` + "`" + `skeema pull production` + "`" + ` will apply config directives from the
[production] section of config files, as well as any sectionless directives
at the top of the file. If no environment name is supplied, only the sectionless
directives alone will be applied.`

	cmd := mycli.NewCommand("pull", summary, desc, PullCommand, 0, 1, "environment")
	cmd.AddOption(mycli.BoolOption("include-auto-inc", 0, false, "Include starting auto-inc values in new table files, and update in existing files"))
	cmd.AddOption(mycli.BoolOption("normalize", 0, true, "Reformat *.sql files to match SHOW CREATE TABLE"))
	CommandSuite.AddSubCommand(cmd)
}

func PullCommand(cfg *mycli.Config) error {
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

		// If schema doesn't exist on instance, remove the corresponding dir
		if t.SchemaFromInstance == nil {
			if err := t.Dir.Delete(); err != nil {
				return fmt.Errorf("Unable to delete directory %s: %s", t.Dir, err)
			}
			fmt.Printf("    Deleted directory %s -- schema no longer exists\n", t.Dir)
			continue
		}

		diff, err := tengo.NewSchemaDiff(t.SchemaFromDir, t.SchemaFromInstance)
		if err != nil {
			return err
		}

		// pull command updates next auto-increment value for existing table always
		// if requested, or only if previously present in file otherwise
		mods := tengo.StatementModifiers{}
		if t.Dir.Config.GetBool("include-auto-inc") {
			mods.NextAutoInc = tengo.NextAutoIncAlways
		} else {
			mods.NextAutoInc = tengo.NextAutoIncIfAlready
		}

		for _, td := range diff.TableDiffs {
			switch td := td.(type) {
			case tengo.CreateTable:
				sf := SQLFile{
					Dir:      t.Dir,
					FileName: fmt.Sprintf("%s.sql", td.Table.Name),
					Contents: td.Statement(mods),
				}
				if length, err := sf.Write(); err != nil {
					return fmt.Errorf("Unable to write to %s: %s", sf.Path(), err)
				} else {
					fmt.Printf("    Wrote %s (%d bytes) -- new table\n", sf.Path(), length)
				}
			case tengo.DropTable:
				table := td.Table
				sf := SQLFile{
					Dir:      t.Dir,
					FileName: fmt.Sprintf("%s.sql", table.Name),
				}
				if err := sf.Delete(); err != nil {
					return fmt.Errorf("Unable to delete %s: %s", sf.Path(), err)
				}
				fmt.Printf("    Deleted %s -- table no longer exists\n", sf.Path())
			case tengo.AlterTable:
				// skip if mods caused the diff to be a no-op
				if td.Statement(mods) == "" {
					continue
				}
				table := td.Table
				createStmt, err := t.Instance.ShowCreateTable(t.SchemaFromInstance, table)
				if err != nil {
					return err
				}
				sf := SQLFile{
					Dir:      t.Dir,
					FileName: fmt.Sprintf("%s.sql", table.Name),
					Contents: createStmt,
				}
				if length, err := sf.Write(); err != nil {
					return fmt.Errorf("Unable to write to %s: %s", sf.Path(), err)
				} else {
					fmt.Printf("    Wrote %s (%d bytes) -- updated file to reflect table alterations\n", sf.Path(), length)
				}
			case tengo.RenameTable:
				panic(fmt.Errorf("Table renames not yet supported!"))
			default:
				panic(fmt.Errorf("Unsupported diff type %T\n", td))
			}
		}

		if dir.Config.GetBool("normalize") {
			for _, table := range diff.SameTables {
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
	}

	return findNewSchemas(dir)
}

func findNewSchemas(dir *Dir) error {
	subdirs, err := dir.Subdirs()
	if err != nil {
		return err
	}

	if dir.HasHost() && !dir.HasSchema() {
		subdirHasSchema := make(map[string]bool)
		for _, subdir := range subdirs {
			if subdir.HasSchema() {
				// TODO support expansion of multiple schema names per dir
				subdirHasSchema[subdir.Config.Get("schema")] = true
			}
		}

		// Compare dirs to schemas, UNLESS subdirs exist but don't actually map to schemas directly
		if len(subdirHasSchema) > 0 || len(subdirs) == 0 {
			inst, err := dir.FirstInstance()
			if err != nil {
				return err
			} else if inst == nil {
				return fmt.Errorf("Unable to obtain instance for %s", dir)
			}
			schemas, err := inst.Schemas()
			if err != nil {
				return err
			}
			for _, s := range schemas {
				if !subdirHasSchema[s.Name] {
					// use same logic from init command
					if err := PopulateSchemaDir(s, dir, true); err != nil {
						return err
					}
				}
			}
		}
	}

	for _, subdir := range subdirs {
		err := findNewSchemas(subdir)
		if err != nil {
			return err
		}
	}

	return nil
}
