package main

import (
	"fmt"
	"os"
	"regexp"

	log "github.com/Sirupsen/logrus"
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
	cmd.AddOption(mybase.StringOption("ignore-schema", 0, "", "Ignore schemas that match regex"))
	cmd.AddOption(mybase.StringOption("ignore-table", 0, "", "Ignore tables that match regex"))
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

		diff, err := tengo.NewSchemaDiff(t.SchemaFromDir, t.SchemaFromInstance)
		if err != nil {
			return err
		}

		// Handle changes in schema's default character set and/or collation by
		// persisting changes to the dir's option file. Errors here are just surfaced
		// as warnings.
		if diff.SchemaDDL != "" {
			optionFile, err := t.Dir.OptionFile()
			if err != nil {
				log.Warnf("Unable to update character set and/or collation for %s/.skeema: %s", t.Dir, err)
			} else if optionFile == nil {
				log.Warnf("Unable to update character set and/or collation for %s/.skeema: cannot read file", t.Dir)
			} else {
				if overridesCharSet, overridesCollation, err := t.SchemaFromInstance.OverridesServerCharSet(); err == nil {
					if overridesCharSet {
						optionFile.SetOptionValue("", "default-character-set", t.SchemaFromInstance.CharSet)
					} else {
						optionFile.UnsetOptionValue("", "default-character-set")
					}
					if overridesCollation {
						optionFile.SetOptionValue("", "default-collation", t.SchemaFromInstance.Collation)
					} else {
						optionFile.UnsetOptionValue("", "default-collation")
					}
					if err = optionFile.Write(true); err != nil {
						log.Warnf("Unable to update character set and/or collation for %s: %s", optionFile.Path(), err)
					} else {
						log.Infof("Wrote %s -- updated schema-level default-character-set and default-collation", optionFile.Path())
					}
				} else {
					log.Warnf("Unable to update character set and/or collation for %s: %s", optionFile.Path(), err)
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
		ignoreTableRegex := t.Dir.Config.Get("ignore-table")
		re, err := regexp.Compile(ignoreTableRegex)
		if err != nil {
			return fmt.Errorf("Invalid regular expression on ignore-table: %s; %s", ignoreTableRegex, err)
		}
		for _, td := range diff.TableDiffs {
			tableName := ""
			switch td := td.(type) {
			case tengo.CreateTable:
				tableName = td.Table.Name
			case tengo.DropTable:
				tableName = td.Table.Name
			case tengo.AlterTable:
				tableName = td.Table.Name
			default:
				return fmt.Errorf("Unsupported diff type %T", td)
			}
			if ignoreTableRegex != "" && re.MatchString(tableName) {
				log.Debugf("Skipping table %s because ignore-table matched %s", tableName, ignoreTableRegex)
				continue
			}
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
			createStmt := table.CreateStatement()
			if table.HasAutoIncrement() && !t.Dir.Config.GetBool("include-auto-inc") {
				createStmt, _ = tengo.ParseCreateAutoInc(createStmt)
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
			log.Infof("Wrote %s (%d bytes) -- updated file to reflect (unsupported) table alterations", sf.Path(), length)
			if t.Dir.Config.GetBool("debug") {
				log.Warnf("Table %s: table uses unsupported features", table.Name)
				t.logUnsupportedTableDiff(table.Name)
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
		instance, err := dir.FirstInstance()
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
