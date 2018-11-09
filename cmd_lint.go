package main

import (
	"fmt"
	"os"

	log "github.com/sirupsen/logrus"
	"github.com/skeema/mybase"
	"github.com/skeema/skeema/fs"
	"github.com/skeema/skeema/workspace"
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

An exit code of 0 will be returned if all files were already formatted properly,
1 if some files were reformatted but all SQL was valid, or 2+ if at least one
file had SQL syntax errors or some other error occurred.`

	cmd := mybase.NewCommand("lint", summary, desc, LintHandler)
	cmd.AddArg("environment", "production", false)
	CommandSuite.AddSubCommand(cmd)
}

// LintHandler is the handler method for `skeema lint`
func LintHandler(cfg *mybase.Config) error {
	dir, err := fs.ParseDir(".", cfg)
	if err != nil {
		return err
	}

	lc := &lintCounters{}
	if err = lintWalker(dir, lc, 5); err != nil {
		return err
	}
	return lc.exitValue()
}

type lintCounters struct {
	errCount      int
	sqlErrCount   int
	reformatCount int
}

func (lc *lintCounters) exitValue() error {
	var plural string
	if lc.errCount > 1 || (lc.errCount == 0 && lc.sqlErrCount > 1) {
		plural = "s"
	}
	switch {
	case lc.errCount > 0:
		return NewExitValue(CodeFatalError, "Skipped %d operation%s due to error%s", lc.errCount, plural, plural)
	case lc.sqlErrCount > 0:
		return NewExitValue(CodeFatalError, "Found syntax error%s in %d SQL file%s", plural, lc.sqlErrCount, plural)
	case lc.reformatCount > 0:
		return NewExitValue(CodeDifferencesFound, "")
	}
	return nil
}

func lintWalker(dir *fs.Dir, lc *lintCounters, maxDepth int) error {
	log.Infof("Linting %s", dir)
	if len(dir.IgnoredStatements) > 0 {
		log.Warnf("Ignoring %d non-CREATE TABLE statements found in this directory's *.sql files", len(dir.IgnoredStatements))
	}

	ignoreTable, err := dir.Config.GetRegexp("ignore-table")
	if err != nil {
		return NewExitValue(CodeBadConfig, err.Error())
	}

	inst, err := dir.FirstInstance()
	if err != nil {
		return err
	}
	opts, err := workspace.OptionsForDir(dir, inst)
	if err != nil {
		return NewExitValue(CodeBadConfig, err.Error())
	}

	for _, logicalSchema := range dir.LogicalSchemas {
		schema, statementErrors, err := workspace.ExecLogicalSchema(logicalSchema, opts)
		if err != nil {
			log.Warnf("Skipping schema %s in %s due to error: %s", logicalSchema.Name, dir.Path, err)
			lc.errCount++
			continue
		}
		for _, stmtErr := range statementErrors {
			if ignoreTable != nil && ignoreTable.MatchString(stmtErr.TableName) {
				log.Debugf("Skipping table %s because ignore-table='%s'", stmtErr.TableName, ignoreTable)
				continue
			}
			log.Error(stmtErr.Error())
			lc.sqlErrCount++
		}
		for _, table := range schema.Tables {
			if ignoreTable != nil && ignoreTable.MatchString(table.Name) {
				log.Debugf("Skipping table %s because ignore-table='%s'", table.Name, ignoreTable)
				continue
			}
			body, suffix := logicalSchema.CreateTables[table.Name].SplitTextBody()
			if table.CreateStatement != body {
				logicalSchema.CreateTables[table.Name].Text = fmt.Sprintf("%s%s", table.CreateStatement, suffix)
				length, err := logicalSchema.CreateTables[table.Name].FromFile.Rewrite()
				if err != nil {
					return fmt.Errorf("Unable to write to %s: %s", logicalSchema.CreateTables[table.Name].File, err)
				}
				log.Infof("Wrote %s (%d bytes) -- updated file to normalize format", logicalSchema.CreateTables[table.Name].File, length)
				lc.reformatCount++
			}
		}
		os.Stderr.WriteString("\n")
	}

	if subdirs, badCount, err := dir.Subdirs(); err != nil {
		log.Errorf("Cannot list subdirs of %s: %s", dir, err)
		lc.errCount++
	} else if len(subdirs) > 0 && maxDepth <= 0 {
		log.Warnf("Not walking subdirs of %s: max depth reached", dir)
		lc.errCount += len(subdirs)
	} else {
		lc.errCount += badCount
		for _, sub := range subdirs {
			if walkErr := lintWalker(sub, lc, maxDepth-1); walkErr != nil {
				return walkErr
			}
		}
	}
	return nil
}
