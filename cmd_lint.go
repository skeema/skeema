package main

import (
	"errors"
	"fmt"

	log "github.com/sirupsen/logrus"
	"github.com/skeema/mybase"
	"github.com/skeema/skeema/internal/dumper"
	"github.com/skeema/skeema/internal/fs"
	"github.com/skeema/skeema/internal/linter"
	"github.com/skeema/skeema/internal/tengo"
	"github.com/skeema/skeema/internal/util"
	"github.com/skeema/skeema/internal/workspace"
)

func init() {
	summary := "Check for problems in filesystem representation of database objects"
	desc := "Checks for problems in filesystem representation of database objects. A set of " +
		"linter rules are run against all objects. Each rule may be configured to " +
		"generate an error, a warning, or be ignored entirely. Statements that contain " +
		"invalid SQL, or otherwise return an error from the database, are always flagged " +
		"as linter errors.\n\n" +
		"By default, this command also reformats CREATE statements to their canonical form, " +
		"just like `skeema format`.\n\n" +
		"This command relies on accessing a database server to test the SQL DDL in a " +
		"temporary location. See the --workspace option for more information.\n\n" +
		"You may optionally pass an environment name as a command-line arg. This will affect " +
		"which section of .skeema config files is used for linter configuration and " +
		"workspace selection. For example, running `skeema lint staging` will " +
		"apply config directives from the [staging] section of config files, as well as " +
		"any sectionless directives at the top of the file. If no environment name is " +
		"supplied, the default is \"production\".\n\n" +
		"An exit code of 0 will be returned if no errors or warnings were emitted and all " +
		"files were already formatted properly; 1 if any warnings were emitted and/or " +
		"some files were reformatted; or 2+ if any errors were emitted for any reason."

	cmd := mybase.NewCommand("lint", summary, desc, LintHandler)
	linter.AddCommandOptions(cmd)
	cmd.AddOptions("Format",
		mybase.BoolOption("format", 0, true, "Reformat SQL statements to match canonical SHOW CREATE"),
		mybase.BoolOption("strip-partitioning", 0, false, "Remove PARTITION BY clauses from *.sql files"),
	)
	workspace.AddCommandOptions(cmd)
	cmd.AddArg("environment", "production", false)
	CommandSuite.AddSubCommand(cmd)
}

// LintHandler is the handler method for `skeema lint`
func LintHandler(cfg *mybase.Config) error {
	dir, err := fs.ParseDir(".", cfg)
	if err != nil {
		return err
	}
	if !dir.Config.Supplied("format") {
		log.Warn("Upgrade notice: the --format option, which currently defaults to true in Skeema v1, will change to default to false in Skeema v2. For more information, visit https://www.skeema.io/blog/skeema-v2-roadmap")
	}

	result := lintWalker(dir, 5)
	switch {
	case len(result.Exceptions) > 0:
		exitCode := ExitCode(HighestExitCode(result.Exceptions...))
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
		log.Error(fmt.Sprintf("Skipping directory %s due to error: %s", dir.ShortName, dir.ParseError))
		return linter.BadConfigResult(dir, dir.ParseError)
	}
	log.Infof("Linting %s", dir)
	result := lintDir(dir)
	for _, err := range result.Exceptions {
		log.Error(fmt.Sprintf("Skipping directory %s due to error: %s", dir.ShortName, err))
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
	if err != nil {
		// If there's nothing to lint here anyway, return an empty result instead
		// of surfacing the error on what may be an empty intermediate directory.
		if len(dir.LogicalSchemas) == 0 {
			return &linter.Result{}
		}
		return linter.BadConfigResult(dir, err)
	}
	opts.StripAnnotationNewlines = !util.StderrIsTerminal()

	// Get workspace options for dir. This involves connecting to the first defined
	// instance, so that any auto-detect-related settings work properly. However,
	// with workspace=docker we can ignore connection errors; we'll get reasonable
	// defaults from workspace.OptionsForDir if inst is nil as long as flavor is set.
	var wsOpts workspace.Options
	if len(dir.LogicalSchemas) > 0 {
		inst, err := dir.FirstInstance()
		if wsType, _ := dir.Config.GetEnum("workspace", "temp-schema", "docker"); wsType != "docker" || !dir.Config.Changed("flavor") {
			if err != nil {
				return linter.BadConfigResult(dir, err)
			} else if inst == nil {
				return linter.BadConfigResult(dir, fmt.Errorf("This command needs either a host (with workspace=temp-schema) or flavor (with workspace=docker), but one is not configured for environment %q", dir.Config.Get("environment")))
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
			// Since `skeema lint` is often used in Docker-based CI, add extra
			// explanatory text if the error was caused by lack of Docker CLI on PATH
			if errors.Is(err, tengo.ErrNoDockerCLI) {
				err = fmt.Errorf("%w\nSkeema v1.11+ no longer includes a built-in Docker client. If you are using workspace=docker while also running `skeema` itself in a container, be sure to put a Linux `docker` client CLI binary in the container as well.\nFor more information, see https://github.com/skeema/skeema/issues/186#issuecomment-1648483625", err)
			}
			result.Fatal(err)
			continue
		}
		log.Debugf("Workspace performance for %s using %s:\n%s", dir.ShortName, wsSchema.Info, wsSchema.Timers)
		result.AnnotateStatementErrors(wsSchema.Failures, opts)

		// Reformat statements if requested. This must be done prior to checking for
		// problems. Otherwise, the line offsets in annotations can be wrong.
		// TODO: support format for multiple logical schemas per dir
		if dir.Config.GetBool("format") && n == 0 {
			dumpOpts := dumper.Options{
				IncludeAutoInc: true,
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

	// Add warnings for any unsupported combinations of schema names, for example
	// USE commands or dbname prefixes in CREATEs in a dir that also configures
	// schema name in .skeema
	result.AnnotateMixedSchemaNames(dir, opts)

	// Add warning annotations for unparseable statements (unless we hit an
	// exception, in which case skip it to avoid extra noise!)
	if len(result.Exceptions) == 0 {
		for _, stmt := range dir.UnparsedStatements {
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

func countAndNoun(n int, singular, plural string) string {
	if n == 1 {
		return fmt.Sprintf("1 %s", singular)
	} else if n == 0 {
		return fmt.Sprintf("no %s", plural)
	}
	return fmt.Sprintf("%d %s", n, plural)
}
