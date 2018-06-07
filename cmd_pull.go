package main

import (
	"fmt"
	"os"

	log "github.com/sirupsen/logrus"
	"github.com/skeema/mybase"
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

	cmd := mybase.NewCommand("pull", summary, desc, PullHandler)
	cmd.AddOption(mybase.BoolOption("include-auto-inc", 0, false, "Include starting auto-inc values in new table files, and update in existing files"))
	cmd.AddOption(mybase.BoolOption("normalize", 0, true, "Reformat *.sql files to match SHOW CREATE TABLE"))
	cmd.AddArg("environment", "production", false)
	CommandSuite.AddSubCommand(cmd)
}

// PullHandler is the handler method for `skeema pull`
func PullHandler(cfg *mybase.Config) error {
	AddGlobalConfigFiles(cfg)
	dir, err := NewDir(".", cfg)
	if err != nil {
		return err
	}

	var errCount int

	for _, t := range dir.Targets() {
		if t.Err != nil {
			log.Errorf("Skipping %s:", t.Dir)
			log.Errorf("    %s\n", t.Err)
			errCount++
			continue
		}

		log.Infof("Updating %s to reflect %s %s", t.Dir, t.Instance, t.SchemaFromDir.Name)

		// If schema doesn't exist on instance, remove the corresponding dir
		if t.SchemaFromInstance == nil {
			if err := t.Dir.Delete(); err != nil {
				return fmt.Errorf("Unable to delete directory %s: %s", t.Dir, err)
			}
			log.Infof("Deleted directory %s -- schema no longer exists\n", t.Dir)
			continue
		}

		diff := tengo.NewSchemaDiff(t.SchemaFromDir, t.SchemaFromInstance)

		// Handle changes in schema's default character set and/or collation by
		// persisting changes to the dir's option file. File operation errors here
		// are just surfaced as warnings.
		if diff.SchemaDDL != "" {
			instCharSet, instCollation, err := t.Instance.DefaultCharSetAndCollation()
			if err != nil {
				return err
			}
			optionFile, err := t.Dir.OptionFile()
			if err != nil {
				log.Warnf("Unable to update character set and/or collation for %s/.skeema: %s", t.Dir, err)
			} else if optionFile == nil {
				log.Warnf("Unable to update character set and/or collation for %s/.skeema: cannot read file", t.Dir)
			} else {
				if instCharSet != t.SchemaFromInstance.CharSet {
					optionFile.SetOptionValue("", "default-character-set", t.SchemaFromInstance.CharSet)
				} else {
					optionFile.UnsetOptionValue("", "default-character-set")
				}
				if instCollation != t.SchemaFromInstance.Collation {
					optionFile.SetOptionValue("", "default-collation", t.SchemaFromInstance.Collation)
				} else {
					optionFile.UnsetOptionValue("", "default-collation")
				}
				if err = optionFile.Write(true); err != nil {
					log.Warnf("Unable to update character set and/or collation for %s: %s", optionFile.Path(), err)
				} else {
					log.Infof("Wrote %s -- updated schema-level default-character-set and default-collation", optionFile.Path())
				}
			}
		}

		// We're permissive of unsafe operations here since we don't ever actually
		// execute the generated statement! We just examine its type.
		mods := tengo.StatementModifiers{
			AllowUnsafe: true,
		}
		// pull command updates next auto-increment value for existing table always
		// if requested, or only if previously present in file otherwise
		if t.Dir.Config.GetBool("include-auto-inc") {
			mods.NextAutoInc = tengo.NextAutoIncAlways
		} else {
			mods.NextAutoInc = tengo.NextAutoIncIfAlready
		}
		mods.IgnoreTable, err = t.Dir.Config.GetRegexp("ignore-table")
		if err != nil {
			return err
		}

		for _, td := range diff.TableDiffs {
			stmt, stmtErr := td.Statement(mods)
			// Errors are fatal, except for UnsupportedDiffError which we can safely
			// ignore (since pull doesn't actually run ALTERs; it just needs to know
			// which tables were altered)
			if stmtErr != nil && !tengo.IsUnsupportedDiff(stmtErr) {
				return stmtErr
			}
			// skip if mods caused the diff to be a no-op; if it's an ALTER, treat it
			// as an unchanged table so that --normalize logic still runs
			if stmt == "" && stmtErr == nil {
				if td.Type == tengo.TableDiffAlter {
					diff.SameTables = append(diff.SameTables, td.To)
				}
				continue
			}

			// For DROP TABLE, we're deleting corresponding table file; vs other
			// types we're updating/rewriting the file.
			if td.Type == tengo.TableDiffDrop {
				sf := SQLFile{
					Dir:      t.Dir,
					FileName: fmt.Sprintf("%s.sql", td.From.Name),
				}
				if err := sf.Delete(); err != nil {
					return fmt.Errorf("Unable to delete %s: %s", sf.Path(), err)
				}
				log.Infof("Deleted %s -- table no longer exists", sf.Path())
				continue
			}

			var reason string
			sf := SQLFile{
				Dir:      t.Dir,
				FileName: fmt.Sprintf("%s.sql", td.To.Name),
				Contents: stmt,
			}

			// For ALTER TABLE, we don't care about the ALTER statement, but we do
			// need to get the corresponding CREATE TABLE and process auto-inc properly
			if td.Type == tengo.TableDiffAlter {
				sf.Contents = td.To.CreateStatement
				if td.To.HasAutoIncrement() && !t.Dir.Config.GetBool("include-auto-inc") && td.From.NextAutoIncrement <= 1 {
					sf.Contents, _ = tengo.ParseCreateAutoInc(sf.Contents)
				}
				reason = "updated file to reflect table alterations"
				if tengo.IsUnsupportedDiff(stmtErr) {
					log.Warnf("Table %s uses unsupported features", td.To.Name)
					DebugLogUnsupportedDiff(stmtErr.(*tengo.UnsupportedDiffError))
				}
			} else if _, hadErr := t.SQLFileErrors[sf.Path()]; hadErr {
				// SQL files with syntax errors will result in TableDiffCreate since the
				// temp schema will be missing the table
				reason = "updated file to replace invalid SQL"
			} else {
				reason = "new table"
			}

			length, err := sf.Write()
			if err != nil {
				return fmt.Errorf("Unable to write to %s: %s", sf.Path(), err)
			}
			log.Infof("Wrote %s (%d bytes) -- %s", sf.Path(), length, reason)
		}

		if dir.Config.GetBool("normalize") {
			for _, table := range diff.SameTables {
				if mods.IgnoreTable != nil && mods.IgnoreTable.MatchString(table.Name) {
					continue
				}
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
				newContents := table.CreateStatement
				if table.HasAutoIncrement() && !t.Dir.Config.GetBool("include-auto-inc") && t.SchemaFromDir.Table(table.Name).NextAutoIncrement <= 1 {
					newContents, _ = tengo.ParseCreateAutoInc(newContents)
				}
				if sf.Contents != newContents {
					sf.Contents = newContents
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
		instance, err := dir.FirstInstance()
		if err != nil {
			return err
		}
		instCharSet, instCollation, err := instance.DefaultCharSetAndCollation()
		if err != nil {
			return err
		}

		subdirHasSchema := make(map[string]bool)
		for _, subdir := range subdirs {
			// We only want to evaluate subdirs that explicitly define the schema option
			// in that subdir's .skeema file, vs inheriting it from a parent dir.
			if !subdir.HasSchema() {
				continue
			}

			// If a subdir's schema is set to "*", it maps to all schemas on the
			// instance, so no sense in trying to detect "new" schemas
			if subdir.Config.Get("schema") == "*" {
				return nil
			}

			schemaNames, err := subdir.SchemaNames(instance)
			if err != nil {
				return err
			}
			for _, name := range schemaNames {
				subdirHasSchema[name] = true
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
			schemaNames, err := inst.SchemaNames()
			if err != nil {
				return err
			}
			for _, name := range schemaNames {
				if !subdirHasSchema[name] {
					s, err := inst.Schema(name)
					if err != nil {
						return err
					}
					// use same logic from init command
					if err := PopulateSchemaDir(s, dir, true, instCharSet != s.CharSet, instCollation != s.Collation); err != nil {
						return err
					}
				}
			}

			// If we did a schema-to-subdir comparison, no need to continue recursion
			// even if there are additional levels of subdirs
			return nil
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
