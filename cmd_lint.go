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
	summary := "Check for problems in filesystem representation of database objects"
	desc := `Checks for problems in filesystem representation of database objects. A set of
linter rules are run against all objects. Each rule may be configured to
generate an error, a warning, or be ignored entirely. Statements that contain
invalid SQL, or otherwise return an error from the database, are always flagged
as linter errors.

By default, this command also reformats statements to their canonical form,
just like ` + "`skeema format`" + `.

This command relies on accessing database instances to test the SQL DDL in a
temporary location. See the workspace option for more information.

You may optionally pass an environment name as a CLI option. This will affect
which section of .skeema config files is used for linter configuration and
workspace selection. For example, running ` + "`" + `skeema lint staging` + "`" + ` will
apply config directives from the [staging] section of config files, as well as
any sectionless directives at the top of the file. If no environment name is
supplied, the default is "production".

An exit code of 0 will be returned if no errors or warnings were emitted and all
files were already formatted properly; 1 if any warnings were emitted and/or
some files were reformatted; or 2+ if any errors were emitted for any reason.`

	cmd := mybase.NewCommand("lint", summary, desc, LintHandler)
	linter.AddCommandOptions(cmd)
	cmd.AddOption(mybase.BoolOption("format", 0, true, "Reformat SQL statements to match canonical SHOW CREATE"))
	cmd.AddOption(mybase.BoolOption("strip-partitioning", 0, false, "Remove PARTITION BY clauses from *.sql files").Hidden())
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
		return NewExitValue(exitCode, "Skipped %s due to fatal errors",
			countAndNoun(len(result.Exceptions), "operation", "operations"),
		)
	case result.ErrorCount > 0 && result.WarningCount > 0:
		return NewExitValue(CodeFatalError, "Found %s and %s",
			countAndNoun(result.ErrorCount, "error", "errors"),
			countAndNoun(result.WarningCount, "warning", "warnings"),
		)
	case result.ErrorCount > 0:
		return NewExitValue(CodeFatalError, "Found %s",
			countAndNoun(result.ErrorCount, "error", "errors"),
		)
	case result.WarningCount > 0:
		return NewExitValue(CodePartialError, "Found %s",
			countAndNoun(result.WarningCount, "warning", "warnings"),
		)
	case result.ReformatCount > 0:
		return NewExitValue(CodeDifferencesFound, "")
	}
	return nil
}

func lintWalker(dir *fs.Dir, maxDepth int) *linter.Result {
	if dir.ParseError != nil {
		log.Error(fmt.Sprintf("Skipping directory %s due to error: %s", dir.RelPath(), dir.ParseError))
		return linter.BadConfigResult(dir, dir.ParseError)
	}
	log.Infof("Linting %s", dir)
	result := lintDir(dir)
	for _, err := range result.Exceptions {
		log.Error(fmt.Sprintf("Skipping directory %s due to error: %s", dir.RelPath(), err))
	}
	for _, annotation := range result.Annotations {
		annotation.Log()
	}
	for _, dl := range result.DebugLogs {
		log.Debug(dl)
	}

	// Don't recurse into subdirs if there was something fatally wrong
	if len(result.Exceptions) > 0 {
		return result
	}

	var subdirErr error
	if subdirs, err := dir.Subdirs(); err != nil {
		subdirErr = fmt.Errorf("Cannot list subdirs of %s: %s", dir, err)
	} else if len(subdirs) > 0 && maxDepth <= 0 {
		subdirErr = fmt.Errorf("Not walking subdirs of %s: max depth reached", dir)
	} else {
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
func lintDir(dir *fs.Dir) *linter.Result {
	opts, err := linter.OptionsForDir(dir)
	if err != nil && len(dir.LogicalSchemas) > 0 {
		return linter.BadConfigResult(dir, err)
	}

	// Get workspace options for dir. This involves connecting to the first
	// defined instance, unless configured to use local Docker.
	var wsOpts workspace.Options
	if len(dir.LogicalSchemas) > 0 {
		var inst *tengo.Instance
		if wsType, _ := dir.Config.GetEnum("workspace", "temp-schema", "docker"); wsType != "docker" || !dir.Config.Changed("flavor") {
			if inst, err = dir.FirstInstance(); err != nil {
				return linter.BadConfigResult(dir, err)
			}
		}
		if wsOpts, err = workspace.OptionsForDir(dir, inst); err != nil {
			return linter.BadConfigResult(dir, err)
		}
	}

	result := &linter.Result{}
	for n, logicalSchema := range dir.LogicalSchemas {
		// Convert the logical schema from the filesystem into a real schema, using a
		// workspace
		wsSchema, err := workspace.ExecLogicalSchema(logicalSchema, wsOpts)
		if err != nil {
			result.Fatal(err)
			continue
		}
		result.AnnotateStatementErrors(wsSchema.Failures, opts)

		// Reformat statements if requested. This must be done prior to checking for
		// problems. Otherwise, the line offsets in annotations can be wrong.
		// TODO: support format for multiple logical schemas per dir
		if dir.Config.GetBool("format") && n == 0 {
			dumpOpts := dumper.Options{
				IncludeAutoInc: true,
				IgnoreTable:    opts.IgnoreTable,
			}
			if dir.Config.GetBool("strip-partitioning") {
				dumpOpts.Partitioning = tengo.PartitioningRemove
			}
			dumpOpts.IgnoreKeys(wsSchema.FailedKeys())
			result.ReformatCount, err = dumper.DumpSchema(wsSchema.Schema, dir, dumpOpts)
			if err != nil {
				log.Errorf("Skipping format operation for %s: %s", dir, err)
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
