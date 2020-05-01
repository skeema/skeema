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
	cmd.AddOption(mybase.BoolOption("strip-partitioning", 0, false, "Remove PARTITION BY clauses from *.sql files"))
	cmd.AddArg("environment", "production", false)
	CommandSuite.AddSubCommand(cmd)
}

// FormatHandler is the handler method for `skeema format`
func FormatHandler(cfg *mybase.Config) error {
	dir, err := fs.ParseDir(".", cfg)
	if err != nil {
		return err
	}

	// formatWalker returns the "worst" (highest) exit code it encounters. We care
	// about the exit code, but not the error message, since any error will already
	// have been logged. (Multiple errors may have been encountered along the way,
	// and it's simpler to log them when they occur, rather than needlessly
	// collecting them.)
	err = formatWalker(dir, 5)
	return NewExitValue(ExitCode(err), "")
}

func formatWalker(dir *fs.Dir, maxDepth int) error {
	if dir.ParseError != nil {
		log.Warnf("Skipping %s: %s", dir.Path, dir.ParseError)
		return NewExitValue(CodeBadConfig, "")
	}

	if dir.Config.GetBool("write") {
		log.Infof("Reformatting %s", dir)
	} else {
		log.Infof("Checking format of %s", dir)
	}
	result := formatDir(dir)
	if ExitCode(result) > CodeDifferencesFound {
		log.Errorf("Skipping %s: %s", dir, result)
		return result // don't walk subdirs if something fatal happened here
	}

	subdirs, err := dir.Subdirs()
	if err != nil {
		log.Errorf("Cannot list subdirs of %s: %s", dir, err)
		return err
	} else if len(subdirs) > 0 && maxDepth <= 0 {
		log.Errorf("Not walking subdirs of %s: max depth reached", dir)
		return result
	}
	for _, sub := range subdirs {
		err := formatWalker(sub, maxDepth-1)
		if ExitCode(err) > ExitCode(result) {
			result = err
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
	var wsOpts workspace.Options
	if len(dir.LogicalSchemas) > 0 {
		var inst *tengo.Instance
		if wsType, _ := dir.Config.GetEnum("workspace", "temp-schema", "docker"); wsType != "docker" || !dir.Config.Changed("flavor") {
			if inst, err = dir.FirstInstance(); err != nil {
				return NewExitValue(CodeBadConfig, err.Error())
			}
		}
		if wsOpts, err = workspace.OptionsForDir(dir, inst); err != nil {
			return NewExitValue(CodeBadConfig, err.Error())
		}
	}

	var totalReformatCount int
	for _, logicalSchema := range dir.LogicalSchemas {
		wsSchema, err := workspace.ExecLogicalSchema(logicalSchema, wsOpts)
		if err != nil {
			return err
		}
		for _, stmtErr := range wsSchema.Failures {
			message := strings.Replace(stmtErr.Err.Error(), "Error executing DDL in workspace: ", "", 1)
			log.Errorf("%s: %s", stmtErr.Location(), message)
			totalReformatCount++
		}

		dumpOpts := dumper.Options{
			IncludeAutoInc: true,
			IgnoreTable:    ignoreTable,
			CountOnly:      !dir.Config.GetBool("write"),
		}
		if dir.Config.GetBool("strip-partitioning") {
			dumpOpts.Partitioning = tengo.PartitioningRemove
		}
		dumpOpts.IgnoreKeys(wsSchema.FailedKeys())
		reformatCount, err := dumper.DumpSchema(wsSchema.Schema, dir, dumpOpts)
		if err != nil {
			return err
		}
		totalReformatCount += reformatCount
	}
	for _, stmt := range dir.IgnoredStatements {
		log.Debugf("%s: unable to parse statement", stmt.Location())
	}
	if totalReformatCount > 0 {
		return NewExitValue(CodeDifferencesFound, "")
	}
	return nil
}
