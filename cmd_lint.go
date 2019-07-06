package main

import (
	"fmt"

	log "github.com/sirupsen/logrus"
	"github.com/skeema/mybase"
	"github.com/skeema/skeema/dumper"
	"github.com/skeema/skeema/fs"
	"github.com/skeema/skeema/linter"
	"github.com/skeema/skeema/workspace"
	"github.com/skeema/tengo"
)

func init() {
	summary := "Verify table files and reformat them in a standardized way"
	desc := `Reformats the filesystem representation of tables to match the format of SHOW
CREATE TABLE. Verifies that all table files contain valid SQL in their CREATE
TABLE statements.

This command relies on accessing database instances to test the SQL DDL. All DDL
will be run against a temporary schema, with no impact on the real schema.

You may optionally pass an environment name as a CLI option. This will affect
which section of .skeema config files is used for obtaining a database instance
to test the SQL DDL against. For example, running ` + "`" + `skeema lint staging` + "`" + ` will
apply config directives from the [staging] section of config files, as well as
any sectionless directives at the top of the file. If no environment name is
supplied, the default is "production".

An exit code of 0 will be returned if no errors or warnings were emitted and all
files were already formatted properly; 1 if any warnings were emitted and/or
some files were reformatted; or 2+ if any errors were emitted for any reason.`

	cmd := mybase.NewCommand("lint", summary, desc, LintHandler)
	linter.AddCommandOptions(cmd)
	cmd.AddArg("environment", "production", false)
	CommandSuite.AddSubCommand(cmd)
}

// LintHandler is the handler method for `skeema lint`
func LintHandler(cfg *mybase.Config) error {
	dir, err := fs.ParseDir(".", cfg)
	if err != nil {
		return err
	}

	result := lintWalker(dir, 5)
	switch {
	case len(result.Exceptions) > 0:
		exitCode := CodeFatalError
		for _, err := range result.Exceptions {
			if _, ok := err.(linter.ConfigError); ok {
				exitCode = CodeBadConfig
			}
		}
		return NewExitValue(exitCode, "Skipped %d operations due to fatal errors", len(result.Exceptions))
	case result.ErrorCount > 0:
		return NewExitValue(CodeFatalError, "Found %d errors", result.ErrorCount)
	case result.WarningCount > 0:
		return NewExitValue(CodeDifferencesFound, "Found %d warnings", result.WarningCount)
	case result.ReformatCount > 0:
		return NewExitValue(CodeDifferencesFound, "")
	}
	return nil
}

func lintWalker(dir *fs.Dir, maxDepth int) *linter.Result {
	log.Infof("Linting %s", dir)
	result := lintDir(dir, true)
	for _, err := range result.Exceptions {
		log.Error(fmt.Errorf("Skipping schema in %s due to error: %s", dir.RelPath(), err))
	}
	for _, annotation := range result.Annotations {
		annotation.Log()
	}
	for _, dl := range result.DebugLogs {
		log.Debug(dl)
	}

	var subdirErr error
	if subdirs, badCount, err := dir.Subdirs(); err != nil {
		subdirErr = fmt.Errorf("Cannot list subdirs of %s: %s", dir, err)
	} else if len(subdirs) > 0 && maxDepth <= 0 {
		subdirErr = fmt.Errorf("Not walking subdirs of %s: max depth reached", dir)
	} else {
		if badCount > 0 {
			subdirErr = fmt.Errorf("Ignoring %d subdirs of %s with configuration errors", badCount, dir)
		}
		for _, sub := range subdirs {
			result.Merge(lintWalker(sub, maxDepth-1))
		}
	}
	if subdirErr != nil {
		log.Error(subdirErr)
		result.Fatal(subdirErr)
	}
	return result
}

// lintDir lints all logical schemas in dir, optionally also reformatting
// SQL statements along the way. A combined result for the directory is
// returned. This function does not recurse into subdirs.
func lintDir(dir *fs.Dir, reformat bool) *linter.Result {
	opts, err := linter.OptionsForDir(dir)
	if err != nil && len(dir.LogicalSchemas) > 0 {
		return linter.BadConfigResult(dir, err)
	}

	// Get workspace options for dir. This involves connecting to the first
	// defined instance, unless configured to use local Docker.
	var inst *tengo.Instance
	if wsType, _ := dir.Config.GetEnum("workspace", "temp-schema", "docker"); wsType != "docker" || !dir.Config.Changed("flavor") {
		if inst, err = dir.FirstInstance(); err != nil {
			return linter.BadConfigResult(dir, err)
		}
	}
	wsOpts, err := workspace.OptionsForDir(dir, inst)
	if err != nil {
		return linter.BadConfigResult(dir, err)
	}

	result := &linter.Result{}
	for _, logicalSchema := range dir.LogicalSchemas {
		// Convert the logical schema from the filesystem into a real schema, using a
		// workspace
		wsSchema, err := workspace.ExecLogicalSchema(logicalSchema, wsOpts)
		if err != nil {
			result.Fatal(err)
			return result
		}
		result.AnnotateStatementErrors(wsSchema.Failures, opts)

		// Reformat statements if requested. This must be done prior to checking for
		// problems. Otherwise, the line offsets in annotations can be wrong.
		if reformat {
			dumpOpts := dumper.Options{
				IncludeAutoInc: true,
				IgnoreTable:    opts.IgnoreTable,
			}
			dumpOpts.IgnoreKeys(wsSchema.FailedKeys())
			result.ReformatCount, err = dumper.DumpSchema(wsSchema.Schema, dir, dumpOpts)
			if err != nil {
				result.Fatal(err)
			}
		}

		// Check for problems
		subresult := linter.CheckSchema(wsSchema, opts)
		result.Merge(subresult)
	}

	// Add warning annotations for unparseable statements (unless we hit an
	// exception, in which case skip it to avoid extra noise!)
	if len(result.Exceptions) == 0 {
		for _, stmt := range dir.IgnoredStatements {
			note := linter.Note{
				Summary: "Unable to parse statement",
				Message: "Ignoring unsupported or unparseable SQL statement",
			}
			result.Annotate(stmt, linter.SeverityWarning, "", note)
		}
	}

	// Make sure the problem messages have a deterministic order.
	result.SortByFile()
	return result
}
