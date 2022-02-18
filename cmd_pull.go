package main

import (
	"database/sql"
	"fmt"
	"os"
	"regexp"

	log "github.com/sirupsen/logrus"
	"github.com/skeema/mybase"
	"github.com/skeema/skeema/internal/dumper"
	"github.com/skeema/skeema/internal/fs"
	"github.com/skeema/skeema/internal/tengo"
	"github.com/skeema/skeema/internal/workspace"
)

func init() {
	summary := "Update the filesystem representation of schemas"
	desc := "Updates the existing filesystem representation of the schemas on a DB " +
		"instance. Use this command when changes have been applied to the database " +
		"manually or outside of Skeema, in order to make the filesystem representation " +
		"reflect those changes.\n\n" +
		"You may optionally pass an environment name as a CLI arg. This will affect " +
		"which section of .skeema config files is used for processing. For example, " +
		"running `skeema pull staging` will apply config directives from the " +
		"[staging] section of config files, as well as any sectionless directives at the " +
		"top of the file. If no environment name is supplied, the default is " +
		"\"production\"."

	cmd := mybase.NewCommand("pull", summary, desc, PullHandler)
	cmd.AddOption(mybase.BoolOption("include-auto-inc", 0, false, "Include starting auto-inc values in new table files, and update in existing files"))
	cmd.AddOption(mybase.BoolOption("format", 0, true, "Reformat SQL statements to match canonical SHOW CREATE"))
	cmd.AddOption(mybase.BoolOption("normalize", 0, true, "(deprecated alias for format)").Hidden())
	cmd.AddOption(mybase.BoolOption("new-schemas", 0, true, "Detect any new schemas and populate new dirs for them"))
	cmd.AddOption(mybase.BoolOption("update-partitioning", 0, false, "Update PARTITION BY clauses in existing table files"))
	cmd.AddOption(mybase.BoolOption("strip-partitioning", 0, false, "Omit PARTITION BY clause when writing partitioned tables to filesystem"))
	workspace.AddCommandOptions(cmd)
	cmd.AddArg("environment", "production", false)
	CommandSuite.AddSubCommand(cmd)
}

// PullHandler is the handler method for `skeema pull`
func PullHandler(cfg *mybase.Config) error {
	dir, err := fs.ParseDir(".", cfg)
	if err != nil {
		return err
	}

	// pullWalker returns the "worst" (highest) exit code it encounters. We care
	// about the exit code, but not the error message, since any error will already
	// have been logged. (Multiple errors may have been encountered along the way,
	// and it's simpler to log them when they occur, rather than needlessly
	// collecting them.)
	err = pullWalker(dir, 5)
	return NewExitValue(ExitCode(err), "")
}

func pullWalker(dir *fs.Dir, maxDepth int) error {
	if dir.ParseError != nil {
		log.Warnf("Skipping %s: %s", dir, dir.ParseError)
		return NewExitValue(CodeBadConfig, "")
	}

	var instance *tengo.Instance
	var err error
	if dir.Config.Changed("host") {
		instance, err = dir.FirstInstance()
		if err != nil {
			log.Warnf("Skipping %s: %s", dir, err)
			return NewExitValue(CodePartialError, "")
		}
	}

	// dir defines a schema in .skeema, and/or has *.sql files
	if dir.HasSchema() {
		// If we cannot pull due to lack of an instance, make it clear to the user
		if instance == nil {
			log.Errorf("Skipping %s: No host defined for environment %q", dir, dir.Config.Get("environment"))
			return NewExitValue(CodeBadConfig, "")
		}

		// Otherwise, we're in a "flat" dir that defines both host and schema: update
		// the flavor if needed and process the pull operation on *.sql files, but no
		// need to look for new schemas with this layout
		updateFlavor(dir, instance)
		updateGenerator(dir)
		_, err := pullSchemaDir(dir, instance) // already logs err (if non-nil)
		return err
	}

	subdirs, err := dir.Subdirs()
	if err != nil {
		log.Errorf("Cannot list subdirs of %s: %s", dir, err)
		return NewExitValue(CodePartialError, "")
	} else if len(subdirs) > 0 && maxDepth <= 0 {
		log.Errorf("Not walking subdirs of %s: max depth reached", dir)
		return NewExitValue(CodePartialError, "")
	}

	allSchemaNames := []string{}
	for _, sub := range subdirs {
		// If dir does not define host, simply recurse into subdirs.
		if instance == nil {
			subErr := pullWalker(sub, maxDepth-1)
			err = HighestExitCode(err, subErr)
			continue
		}

		// Otherwise, dir defines host but not schema. Treat subdirs as schema dirs,
		// and use the combined list of handled schemas to figure out whether any
		// new schema dirs need to be created (if requested).
		subSchemaNames, subErr := pullSchemaDir(sub, instance) // already logs subErr (if non-nil)
		err = HighestExitCode(err, subErr)
		allSchemaNames = append(allSchemaNames, subSchemaNames...)
	}

	if instance != nil {
		updateFlavor(dir, instance)
		updateGenerator(dir)
		if dir.Config.GetBool("new-schemas") && err == nil {
			if err = findNewSchemas(dir, instance, allSchemaNames); err != nil {
				log.Warnf("Unable to populate new schemas from %s: %s", dir, err)
				return NewExitValue(CodePartialError, "")
			}
		}
	}
	return err
}

// pullSchemaDir updates all logical schemas in dir to reflect the actual
// definitions found in instance. A slice of handled schema names is returned,
// along with any error encountered.
func pullSchemaDir(dir *fs.Dir, instance *tengo.Instance) (schemaNames []string, err error) {
	if dir.ParseError != nil {
		log.Warnf("Skipping %s: %s", dir, dir.ParseError)
		return nil, NewExitValue(CodePartialError, "")
	}
	if len(dir.LogicalSchemas) > 0 {
		// TODO: support multiple logical schemas per dir
		logicalSchema := dir.LogicalSchemas[0]
		schemaNames, err = pullLogicalSchema(dir, instance, logicalSchema)
	}
	if err != nil {
		log.Warnf("Skipping %s: %s\n", dir, err)
		if _, ok := err.(*ExitValue); !ok {
			err = NewExitValue(CodePartialError, "")
		}
	}
	return
}

// pullLogicalSchema performs appropriate pull logic on a dir that maps to one or
// more schemas. A slice of handled schema names is returned, along with any
// error encountered.
func pullLogicalSchema(dir *fs.Dir, instance *tengo.Instance, logicalSchema *fs.LogicalSchema) (schemaNames []string, err error) {
	if logicalSchema.Name != "" {
		schemaNames = []string{logicalSchema.Name}
	} else if schemaNames, err = dir.SchemaNames(instance); err != nil {
		return nil, fmt.Errorf("unable to fetch schema names mapped by this dir: %s", err)
	}
	if len(schemaNames) == 0 {
		log.Warnf("Ignoring directory %s -- did not map to any schema names for environment %q\n", dir, dir.Config.Get("environment"))
		return
	}
	instSchema, err := instance.Schema(schemaNames[0])
	if err == sql.ErrNoRows {
		log.Infof("Deleted directory %s -- schema %s no longer exists\n", dir, schemaNames[0])
		return nil, dir.Delete()
	} else if err != nil {
		return nil, fmt.Errorf("Unable to fetch schema %s from %s: %s", schemaNames[0], instance, err)
	}

	log.Infof("Updating %s to reflect %s %s", dir, instance, instSchema.Name)

	// Handle changes in schema's default character set and/or collation by
	// persisting changes to the dir's option file.
	if err := updateCharSetCollation(dir, instSchema); err != nil {
		return nil, err
	}

	dumpOpts := dumper.Options{
		IncludeAutoInc: dir.Config.GetBool("include-auto-inc"),
	}
	if dumpOpts.IgnoreTable, err = dir.Config.GetRegexp("ignore-table"); err != nil {
		return nil, NewExitValue(CodeBadConfig, err.Error())
	}
	if !dir.Config.GetBool("update-partitioning") {
		if dir.Config.GetBool("strip-partitioning") {
			// Undocumented due to potential confusion, but supported just like in init
			dumpOpts.Partitioning = tengo.PartitioningRemove
		} else {
			// Without --update-partitioning, retain whatever partitioning clause (or
			// lack of clause) was already present in the *.sql files for existing tables.
			dumpOpts.Partitioning = tengo.PartitioningKeep
		}
	}

	// When --skip-format is in use, we only want to update objects that have
	// actual functional modifications, NOT just cosmetic/formatting differences.
	// To make this distinction, we need to actually execute the *.sql files in a
	// Workspace and run a diff against it.
	if !dir.Config.GetBool("format") || !dir.Config.GetBool("normalize") {
		mods := statementModifiersForPull(dir.Config, instance, dumpOpts.IgnoreTable)
		opts, err := workspace.OptionsForDir(dir, instance)
		if err != nil {
			return nil, NewExitValue(CodeBadConfig, err.Error())
		}
		inDiff, err := objectsInDiff(logicalSchema, instSchema, opts, mods)
		if err != nil {
			return nil, err
		}
		dumpOpts.OnlyKeys(inDiff)
	}

	_, err = dumper.DumpSchema(instSchema, dir, dumpOpts)
	if err == nil {
		os.Stderr.WriteString("\n")
	}
	return
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
	instFlavor, confFlavor := instance.Flavor(), tengo.ParseFlavor(config.Get("flavor"))
	if !instFlavor.Known() && confFlavor.Known() {
		mods.Flavor = confFlavor
	} else {
		mods.Flavor = instFlavor
	}
	// Unless user specifically wants to update partitioning clauses, apply a
	// statement modifier to make some partitioning-related AlterClause types
	// return an empty statement, to exclude them from being rewritten if their
	// only differences are partitioning-related.
	if !config.GetBool("update-partitioning") {
		mods.Partitioning = tengo.PartitioningKeep
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
	if !instFlavor.Known() || instFlavor.Family().String() == dir.Config.Get("flavor") {
		return
	}
	dir.OptionFile.SetOptionValue(dir.Config.Get("environment"), "flavor", instFlavor.Family().String())
	if err := dir.OptionFile.Write(true); err != nil {
		log.Warnf("Unable to update flavor in %s: %s", dir.OptionFile.Path(), err)
	} else {
		log.Infof("Wrote %s -- updated flavor to %s", dir.OptionFile.Path(), instFlavor.Family().String())
	}
}

func updateGenerator(dir *fs.Dir) {
	currentGenerator := generatorString() // see cmd_init.go
	if dir.Config.Get("generator") == currentGenerator {
		return
	}
	dir.OptionFile.SetOptionValue("", "generator", currentGenerator)
	// ignoring errors; failure not terribly important
	if err := dir.OptionFile.Write(true); err == nil {
		log.Infof("Wrote %s -- updated generator to %s", dir.OptionFile.Path(), currentGenerator)
	}
}

// updateCharSetCollation updates the dir's .skeema option file if the schema's
// current default charset or collation does not match what's in the file.
func updateCharSetCollation(dir *fs.Dir, instSchema *tengo.Schema) error {
	if dir.Config.Get("default-character-set") != instSchema.CharSet || dir.Config.Get("default-collation") != instSchema.Collation {
		dir.OptionFile.SetOptionValue("", "default-character-set", instSchema.CharSet)
		dir.OptionFile.SetOptionValue("", "default-collation", instSchema.Collation)
		if err := dir.OptionFile.Write(true); err != nil {
			return fmt.Errorf("Unable to update character set and collation for %s: %s", dir.OptionFile.Path(), err)
		}
		log.Infof("Wrote %s -- updated schema-level default-character-set and default-collation", dir.OptionFile.Path())
	}
	return nil
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
