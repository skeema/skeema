package main

import (
	"fmt"
	"os"

	log "github.com/Sirupsen/logrus"
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
running ` + "`" + `skeema pull staging` + "`" + ` will apply config directives from the
[staging] section of config files, as well as any sectionless directives at the
top of the file. If no environment name is supplied, the default is
"production".`

	cmd := mycli.NewCommand("pull", summary, desc, PullHandler)
	cmd.AddOption(mycli.BoolOption("include-auto-inc", 0, false, "Include starting auto-inc values in new table files, and update in existing files"))
	cmd.AddOption(mycli.BoolOption("normalize", 0, true, "Reformat *.sql files to match SHOW CREATE TABLE"))
	cmd.AddArg("environment", "production", false)
	CommandSuite.AddSubCommand(cmd)
}

// PullHandler is the handler method for `skeema pull`
func PullHandler(cfg *mycli.Config) error {
	AddGlobalConfigFiles(cfg)
	dir, err := NewDir(".", cfg)
	if err != nil {
		return err
	}

	var errCount int

	for _, t := range dir.Targets() {
		if t.Err != nil { // we only skip on fatal errors (t.Err), not SQL file errors (t.SQLFileErrors or t.HasError())
			log.Errorf("Skipping %s:", t.Dir)
			log.Errorf("    %s\n", t.Err)
			errCount++
			continue
		}

		log.Infof("Updating %s", t.Dir)

		// If schema doesn't exist on instance, remove the corresponding dir
		if t.SchemaFromInstance == nil {
			if err := t.Dir.Delete(); err != nil {
				return fmt.Errorf("Unable to delete directory %s: %s", t.Dir, err)
			}
			log.Infof("Deleted directory %s -- schema no longer exists\n", t.Dir)
			continue
		}

		diff, err := tengo.NewSchemaDiff(t.SchemaFromDir, t.SchemaFromInstance)
		if err != nil {
			return err
		}

		// We're permissive of drops here since we don't ever actually execute the
		// generated statement! We just examine its type.
		mods := tengo.StatementModifiers{
			AllowDropTable:  true,
			AllowDropColumn: true,
		}
		// pull command updates next auto-increment value for existing table always
		// if requested, or only if previously present in file otherwise
		if t.Dir.Config.GetBool("include-auto-inc") {
			mods.NextAutoInc = tengo.NextAutoIncAlways
		} else {
			mods.NextAutoInc = tengo.NextAutoIncIfAlready
		}

		for _, td := range diff.TableDiffs {
			stmt, err := td.Statement(mods)
			if err != nil {
				return err
			}
			switch td := td.(type) {
			case tengo.CreateTable:
				sf := SQLFile{
					Dir:      t.Dir,
					FileName: fmt.Sprintf("%s.sql", td.Table.Name),
					Contents: stmt,
				}
				if length, err := sf.Write(); err != nil {
					return fmt.Errorf("Unable to write to %s: %s", sf.Path(), err)
				} else if _, hadErr := t.SQLFileErrors[sf.Path()]; hadErr {
					// SQL files with syntax errors will result in tengo.CreateTable since
					// the temp schema will be missing the table, however we can detect this
					// scenario by looking in the Target's SQLFileErrors
					log.Infof("Wrote %s (%d bytes) -- updated file to replace invalid SQL", sf.Path(), length)
				} else {
					log.Infof("Wrote %s (%d bytes) -- new table", sf.Path(), length)
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
				log.Infof("Deleted %s -- table no longer exists", sf.Path())
			case tengo.AlterTable:
				// skip if mods caused the diff to be a no-op
				if stmt == "" {
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
				var length int
				if length, err = sf.Write(); err != nil {
					return fmt.Errorf("Unable to write to %s: %s", sf.Path(), err)
				}
				log.Infof("Wrote %s (%d bytes) -- updated file to reflect table alterations", sf.Path(), length)
			case tengo.RenameTable:
				return fmt.Errorf("Table renames not yet supported")
			default:
				return fmt.Errorf("Unsupported diff type %T", td)
			}
		}

		// Tables that use features not supported by tengo diff still need files
		// updated. Handle same as AlterTable case, since created/dropped tables don't
		// ever end up in UnsupportedTables since they don't do a diff operation.
		for _, table := range diff.UnsupportedTables {
			sf := SQLFile{
				Dir:      t.Dir,
				FileName: fmt.Sprintf("%s.sql", table.Name),
				Contents: table.CreateStatement(),
			}
			var length int
			if length, err = sf.Write(); err != nil {
				return fmt.Errorf("Unable to write to %s: %s", sf.Path(), err)
			}
			log.Infof("Wrote %s (%d bytes) -- updated file to reflect table alterations", sf.Path(), length)
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
				for _, warning := range sf.Warnings {
					log.Debug(warning)
				}
				if table.CreateStatement() != sf.Contents {
					sf.Contents = table.CreateStatement()
					var length int
					if length, err = sf.Write(); err != nil {
						return fmt.Errorf("Unable to write to %s: %s", sf.Path(), err)
					}
					log.Infof("Wrote %s (%d bytes) -- updated file to normalize format", sf.Path(), length)
				}
			}
		}

		os.Stderr.WriteString("\n")
	}

	if err := findNewSchemas(dir); err != nil {
		return err
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
		if subdir.BaseName()[0] != '.' {
			err := findNewSchemas(subdir)
			if err != nil {
				return err
			}
		}
	}

	return nil
}
