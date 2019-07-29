package main

import (
	"strings"

	log "github.com/sirupsen/logrus"
	"github.com/skeema/mybase"
	"github.com/skeema/skeema/dumper"
	"github.com/skeema/skeema/fs"
	"github.com/skeema/skeema/workspace"
	"github.com/skeema/tengo"
)

func init() {
	summary := "Normalize format of filesystem representation of database objects"
	desc := `Reformats the filesystem representation of database objects to match the canonical
format shown in SHOW CREATE.

This command relies on accessing database instances to test the SQL DDL in a
temporary location. See the workspace option for more information.

You may optionally pass an environment name as a CLI option. This will affect
which section of .skeema config files is used for workspace selection. For
example, running ` + "`" + `skeema format staging` + "`" + ` will
apply config directives from the [staging] section of config files, as well as
any sectionless directives at the top of the file. If no environment name is
supplied, the default is "production".

An exit code of 0 will be returned if all files were already formatted properly;
1 if some files were not already in the correct format; or 2+ if any errors
occurred.`

	cmd := mybase.NewCommand("format", summary, desc, FormatHandler)
	cmd.AddOption(mybase.BoolOption("write", 0, true, "Update files to correct format"))
	cmd.AddArg("environment", "production", false)
	CommandSuite.AddSubCommand(cmd)
}

// FormatHandler is the handler method for `skeema format`
func FormatHandler(cfg *mybase.Config) error {
	dir, err := fs.ParseDir(".", cfg)
	if err != nil {
		return err
	}
	return formatWalker(dir, 5)
}

func formatWalker(dir *fs.Dir, maxDepth int) error {
	result := formatDir(dir)
	if subdirs, badCount, err := dir.Subdirs(); err != nil {
		log.Errorf("Cannot list subdirs of %s: %s", dir, err)
		result = HighestExitValue(result, err)
	} else if len(subdirs) > 0 && maxDepth <= 0 {
		log.Errorf("Not walking subdirs of %s: max depth reached", dir)
		result = HighestExitValue(result, NewExitValue(CodePartialError, ""))
	} else {
		if badCount > 0 {
			log.Errorf("Ignoring %d subdirs of %s with configuration errors", badCount, dir)
			result = HighestExitValue(result, NewExitValue(CodeBadConfig, ""))
		}
		for _, sub := range subdirs {
			result = HighestExitValue(result, formatWalker(sub, maxDepth-1))
		}
	}
	return result
}

// formatDir reformats SQL statements in all logical schemas in dir. This
// function does not recurse into subdirs.
func formatDir(dir *fs.Dir) error {
	ignoreTable, err := dir.Config.GetRegexp("ignore-table")
	if err != nil {
		return NewExitValue(CodeBadConfig, err.Error())
	}

	// Get workspace options for dir. This involves connecting to the first
	// defined instance, unless configured to use local Docker.
	var inst *tengo.Instance
	if wsType, _ := dir.Config.GetEnum("workspace", "temp-schema", "docker"); wsType != "docker" || !dir.Config.Changed("flavor") {
		if inst, err = dir.FirstInstance(); err != nil {
			return NewExitValue(CodeBadConfig, err.Error())
		}
	}
	wsOpts, err := workspace.OptionsForDir(dir, inst)
	if err != nil {
		return NewExitValue(CodeBadConfig, err.Error())
	}

	var totalReformatCount int
	var failures bool
	for _, logicalSchema := range dir.LogicalSchemas {
		wsSchema, err := workspace.ExecLogicalSchema(logicalSchema, wsOpts)
		if err != nil {
			return NewExitValue(CodeFatalError, err.Error())
		}
		for _, stmtErr := range wsSchema.Failures {
			failures = true
			message := strings.Replace(stmtErr.Err.Error(), "Error executing DDL in workspace: ", "", 1)
			log.Errorf("%s: %s", stmtErr.Location(), message)
		}

		dumpOpts := dumper.Options{
			IncludeAutoInc: true,
			IgnoreTable:    ignoreTable,
			CountOnly:      !dir.Config.GetBool("write"),
		}
		dumpOpts.IgnoreKeys(wsSchema.FailedKeys())
		reformatCount, err := dumper.DumpSchema(wsSchema.Schema, dir, dumpOpts)
		if err != nil {
			return NewExitValue(CodeFatalError, err.Error())
		}
		totalReformatCount += reformatCount
	}
	for _, stmt := range dir.IgnoredStatements {
		log.Debug("%s: unable to parse statement", stmt.Location())
	}
	if failures || totalReformatCount > 0 {
		return NewExitValue(CodeDifferencesFound, "")
	}
	return nil
}
