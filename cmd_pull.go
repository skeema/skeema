package main

import (
	"database/sql"
	"fmt"
	"os"
	"regexp"

	log "github.com/sirupsen/logrus"
	"github.com/skeema/mybase"
	"github.com/skeema/skeema/dumper"
	"github.com/skeema/skeema/fs"
	"github.com/skeema/skeema/workspace"
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
	cmd.AddOption(mybase.BoolOption("format", 0, true, "Reformat SQL statements to match canonical SHOW CREATE"))
	cmd.AddOption(mybase.BoolOption("normalize", 0, true, "(deprecated alias for format)").Hidden())
	cmd.AddOption(mybase.BoolOption("new-schemas", 0, true, "Detect any new schemas and populate new dirs for them"))
	cmd.AddArg("environment", "production", false)
	CommandSuite.AddSubCommand(cmd)
}

// PullHandler is the handler method for `skeema pull`
func PullHandler(cfg *mybase.Config) error {
	dir, err := fs.ParseDir(".", cfg)
	if err != nil {
		return err
	}

	var skipCount int
	if _, skipCount, err = pullWalker(dir, 5); err != nil {
		return err
	}
	if skipCount == 0 {
		return nil
	}
	var plural string
	if skipCount > 1 {
		plural = "s"
	}
	return NewExitValue(CodePartialError, "Skipped %d operation%s due to error%s", skipCount, plural, plural)
}

// pullWalker processes dir, and recursively calls itself on any subdirs. An
// error is only returned if something fatal occurs. skipCount reflects the
// number of non-fatal failed operations that were skipped for dir and its
// subdirectories.
func pullWalker(dir *fs.Dir, maxDepth int) (handledSchemaNames []string, skipCount int, err error) {
	var instance *tengo.Instance
	if dir.Config.Changed("host") {
		instance, err = dir.FirstInstance()
		if err != nil {
			log.Warnf("Skipping %s: %s", dir, err)
			return nil, 1, nil
		}
	}

	if instance != nil && dir.HasSchema() {
		for _, logicalSchema := range dir.LogicalSchemas {
			// TODO: support pull for case where multiple explicitly-named schemas per
			// dir. For example, ability to convert a multi-schema single-file mysqldump
			// into Skeema's usual multi-dir layout.
			if logicalSchema.Name != "" {
				handledSchemaNames = append(handledSchemaNames, logicalSchema.Name)
				log.Warnf("Ignoring schema %s from directory %s -- multiple schemas per dir not supported yet", logicalSchema.Name, dir)
				continue
			}

			var schemaNames []string
			if schemaNames, err = dir.SchemaNames(instance); err != nil {
				return nil, skipCount, fmt.Errorf("%s: Unable to fetch schema names mapped by this dir: %s", dir, err)
			}
			if len(schemaNames) == 0 {
				log.Warnf("Ignoring directory %s -- did not map to any schema names for environment \"%s\"\n", dir, dir.Config.Get("environment"))
				continue
			}
			handledSchemaNames = append(handledSchemaNames, schemaNames...)
			instSchema, err := instance.Schema(schemaNames[0])
			if err == sql.ErrNoRows {
				log.Infof("Deleted directory %s -- schema %s no longer exists\n", dir, handledSchemaNames[0])
				// Explicitly return here to prevent later attempt at subdir traversal
				return nil, skipCount, dir.Delete()
			} else if err != nil {
				return nil, skipCount, fmt.Errorf("%s: Unable to fetch schema %s from %s: %s", dir, handledSchemaNames[0], instance, err)
			}
			if err = pullSchemaDir(dir, instance, instSchema, logicalSchema); err != nil {
				return nil, skipCount, err
			}
		}
	}

	if subdirs, badCount, err := dir.Subdirs(); err != nil {
		log.Errorf("Cannot list subdirs of %s: %s", dir, err)
		skipCount++
	} else if len(subdirs) > 0 && maxDepth <= 0 {
		log.Warnf("Not walking subdirs of %s: max depth reached", dir)
		skipCount += len(subdirs)
	} else {
		skipCount += badCount
		allSubSchemaNames := make([]string, 0)
		for _, sub := range subdirs {
			subSchemaNames, subSkipCount, walkErr := pullWalker(sub, maxDepth-1)
			skipCount += subSkipCount
			if walkErr != nil {
				return nil, skipCount, walkErr
			}
			allSubSchemaNames = append(allSubSchemaNames, subSchemaNames...)
		}
		if instance != nil && !dir.Config.Changed("schema") {
			updateFlavor(dir, instance)
			if dir.Config.GetBool("new-schemas") && badCount == 0 {
				err = findNewSchemas(dir, instance, allSubSchemaNames)
			}
			return nil, skipCount, err
		}
	}
	return handledSchemaNames, skipCount, nil
}

// pullSchemaDir performs appropriate pull logic on a dir that maps to one or
// more schemas. Typically these are leaf dirs.
func pullSchemaDir(dir *fs.Dir, instance *tengo.Instance, instSchema *tengo.Schema, logicalSchema *fs.LogicalSchema) error {
	log.Infof("Updating %s to reflect %s %s", dir, instance, instSchema.Name)

	// Handle changes in schema's default character set and/or collation by
	// persisting changes to the dir's option file.
	if dir.Config.Get("default-character-set") != instSchema.CharSet || dir.Config.Get("default-collation") != instSchema.Collation {
		dir.OptionFile.SetOptionValue("", "default-character-set", instSchema.CharSet)
		dir.OptionFile.SetOptionValue("", "default-collation", instSchema.Collation)
		if err := dir.OptionFile.Write(true); err != nil {
			return fmt.Errorf("Unable to update character set and collation for %s: %s", dir.OptionFile.Path(), err)
		}
		log.Infof("Wrote %s -- updated schema-level default-character-set and default-collation", dir.OptionFile.Path())
	}

	dumpOpts := dumper.Options{
		IncludeAutoInc: dir.Config.GetBool("include-auto-inc"),
	}
	var err error
	if dumpOpts.IgnoreTable, err = dir.Config.GetRegexp("ignore-table"); err != nil {
		return NewExitValue(CodeBadConfig, err.Error())
	}

	// When --skip-format is in use, we only want to update objects that have
	// actual functional modifications, NOT just cosmetic/formatting differences.
	// To make this distinction, we need to actually execute the *.sql files in a
	// Workspace and run a diff against it.
	if !dir.Config.GetBool("format") || !dir.Config.GetBool("normalize") {
		mods := statementModifiersForPull(dir.Config, instance, dumpOpts.IgnoreTable)
		opts, err := workspace.OptionsForDir(dir, instance)
		if err != nil {
			return NewExitValue(CodeBadConfig, err.Error())
		}
		inDiff, err := objectsInDiff(logicalSchema, instSchema, opts, mods)
		if err != nil {
			return err
		}
		dumpOpts.OnlyKeys(inDiff)
	}

	_, err = dumper.DumpSchema(instSchema, dir, dumpOpts)
	os.Stderr.WriteString("\n")
	return err
}

func statementModifiersForPull(config *mybase.Config, instance *tengo.Instance, ignoreTable *regexp.Regexp) tengo.StatementModifiers {
	// We're permissive of unsafe operations here since we don't ever actually
	// execute the generated statement! We just examine its type.
	mods := tengo.StatementModifiers{
		AllowUnsafe: true,
	}
	// pull command updates next auto-increment value for existing table always
	// if requested, or only if previously present in file otherwise
	if config.GetBool("include-auto-inc") {
		mods.NextAutoInc = tengo.NextAutoIncAlways
	} else {
		mods.NextAutoInc = tengo.NextAutoIncIfAlready
	}
	mods.IgnoreTable = ignoreTable
	instFlavor, confFlavor := instance.Flavor(), tengo.NewFlavor(config.Get("flavor"))
	if !instFlavor.Known() && confFlavor.Known() {
		mods.Flavor = confFlavor
	} else {
		mods.Flavor = instFlavor
	}
	return mods
}

// objectsInDiff returns a map whose keys are tengo.ObjectKeys of objects that
// have modifications in instSchema that aren't reflected in their filesystem
// representation yet. This also includes objects whose filesystem Statement has
// a SQL syntax error. The return value does not include tables whose
// differences are cosmetic / formatting-related, or are otherwise ignored by
// mods.
func objectsInDiff(logicalSchema *fs.LogicalSchema, instSchema *tengo.Schema, opts workspace.Options, mods tengo.StatementModifiers) ([]tengo.ObjectKey, error) {
	wsSchema, err := workspace.ExecLogicalSchema(logicalSchema, opts)
	if err != nil {
		return nil, fmt.Errorf("Error introspecting filesystem version of schema %s: %s", instSchema.Name, err)
	}

	// Run a diff, and create a map to track objects in the diff
	diff := tengo.NewSchemaDiff(wsSchema.Schema, instSchema)
	inDiff := make([]tengo.ObjectKey, 0)
	for _, od := range diff.ObjectDiffs() {
		odStatement, odStatementErr := od.Statement(mods)
		// Errors are fatal, except for UnsupportedDiffError which we can safely
		// ignore (since pull doesn't actually run ALTERs; it just needs to know
		// what was altered)
		if odStatementErr != nil && !tengo.IsUnsupportedDiff(odStatementErr) {
			return nil, odStatementErr
		}
		// mods may cause the diff to be a no-op; only include it in result if this
		// isn't the case
		if odStatement != "" {
			inDiff = append(inDiff, od.ObjectKey())
		}
	}

	// Treat objects with syntax errors as modified, since it isn't possible for
	// the filesystem definition to match the live definition in this case.
	inDiff = append(inDiff, wsSchema.FailedKeys()...)

	return inDiff, nil
}

// updateFlavor updates the dir's .skeema option file if the instance's current
// flavor does not match what's in the file. However, it leaves the value in the
// file alone if it's specified and we're unable to detect the instance's
// vendor, as this gives operators the ability to manually override an
// undetectable flavor.
func updateFlavor(dir *fs.Dir, instance *tengo.Instance) {
	instFlavor := instance.Flavor()
	if !instFlavor.Known() || instFlavor.String() == dir.Config.Get("flavor") {
		return
	}
	dir.OptionFile.SetOptionValue(dir.Config.Get("environment"), "flavor", instFlavor.String())
	if err := dir.OptionFile.Write(true); err != nil {
		log.Warnf("Unable to update flavor in %s: %s", dir.OptionFile.Path(), err)
	} else {
		log.Infof("Wrote %s -- updated flavor to %s", dir.OptionFile.Path(), instFlavor.String())
	}
}

func findNewSchemas(dir *fs.Dir, instance *tengo.Instance, seenNames []string) error {
	subdirHasSchema := make(map[string]bool)
	for _, name := range seenNames {
		subdirHasSchema[name] = true
	}

	schemaNames, err := instance.SchemaNames()
	if err != nil {
		return err
	}
	for _, name := range schemaNames {
		// If no existing subdir maps to the schema, we need to create and populate new dir
		if !subdirHasSchema[name] {
			s, err := instance.Schema(name)
			if err != nil {
				return err
			}
			// use same logic from init command
			if err := PopulateSchemaDir(s, dir, true); err != nil {
				return err
			}
		}
	}

	return nil
}
