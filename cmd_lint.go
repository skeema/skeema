package main

import (
	"fmt"

	log "github.com/sirupsen/logrus"
	"github.com/skeema/mybase"
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
	case len(result.Errors) > 0:
		return NewExitValue(CodeFatalError, "Found %d errors", len(result.Errors))
	case len(result.Warnings) > 0:
		return NewExitValue(CodeDifferencesFound, "Found %d warnings", len(result.Warnings))
	case len(result.FormatNotices) > 0:
		return NewExitValue(CodeDifferencesFound, "")
	}
	return nil
}

func lintWalker(dir *fs.Dir, maxDepth int) (result *linter.Result) {
	log.Infof("Linting %s", dir)

	// Connect to first defined instance, unless configured to use local Docker
	var inst *tengo.Instance
	if wsType, _ := dir.Config.GetEnum("workspace", "temp-schema", "docker"); wsType != "docker" || !dir.Config.Changed("flavor") {
		var err error
		if inst, err = dir.FirstInstance(); err != nil {
			result = linter.BadConfigResult(err)
		}
	}
	opts, err := workspace.OptionsForDir(dir, inst)
	if err != nil {
		result = linter.BadConfigResult(err)
	}

	if result == nil {
		result = linter.LintDir(dir, opts)
	}
	for _, err := range result.Exceptions {
		log.Error(fmt.Errorf("Skipping schema in %s due to error: %s", dir.RelPath(), err))
	}
	for _, annotation := range result.Errors {
		log.Error(annotation.MessageWithLocation())
	}
	for _, annotation := range result.Warnings {
		log.Warning(annotation.MessageWithLocation())
	}
	for _, annotation := range result.FormatNotices {
		annotation.Statement.Text = annotation.Message
		length, err := annotation.Statement.FromFile.Rewrite()
		if err != nil {
			writeErr := fmt.Errorf("Unable to write to %s: %s", annotation.Statement.File, err)
			log.Error(writeErr.Error())
			result.Exceptions = append(result.Exceptions, writeErr)
		} else {
			log.Infof("Wrote %s (%d bytes) -- updated file to normalize format", annotation.Statement.File, length)
		}
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
		result.Exceptions = append(result.Exceptions, subdirErr)
	}
	return result
}
