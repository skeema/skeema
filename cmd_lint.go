package main

import (
	"fmt"
	"os"

	log "github.com/sirupsen/logrus"
	"github.com/skeema/mybase"
	"github.com/skeema/skeema/fs"
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
		log.Warnf("Ignoring %d unsupported or unparseable statements found in this directory's *.sql files", len(dir.IgnoredStatements))
	}

	ignoreTable, err := dir.Config.GetRegexp("ignore-table")
	if err != nil {
		return NewExitValue(CodeBadConfig, err.Error())
	}

	// Connect to first defined instance, unless configured to use local Docker
	var inst *tengo.Instance
	if wsType, _ := dir.Config.GetEnum("workspace", "temp-schema", "docker"); wsType != "docker" || !dir.Config.Changed("flavor") {
		if inst, err = dir.FirstInstance(); err != nil {
			return err
		}
	}

	opts, err := workspace.OptionsForDir(dir, inst)
	if err != nil {
		return NewExitValue(CodeBadConfig, err.Error())
	}

	for _, logicalSchema := range dir.LogicalSchemas {
		// ignore-schema is handled relatively simplistically here: skip dir entirely
		// if any literal schema name matches the pattern, but don't bother
		// interpretting schema=`shellout` or schema=*, which require an instance.
		ignoreSchema, err := dir.Config.GetRegexp("ignore-schema")
		if err != nil {
			return NewExitValue(CodeBadConfig, err.Error())
		} else if ignoreSchema != nil {
			var foundIgnoredName bool
			for _, schemaName := range dir.Config.GetSlice("schema", ',', true) {
				if ignoreSchema.MatchString(schemaName) {
					foundIgnoredName = true
				}
			}
			if foundIgnoredName {
				log.Warnf("Skipping schema in %s because ignore-schema='%s'", dir.Path, ignoreSchema)
				break
			}
		}

		// Convert the logical schema from the filesystem into a real schema, using a
		// workspace
		schema, statementErrors, err := workspace.ExecLogicalSchema(logicalSchema, opts)
		if err != nil {
			log.Errorf("Skipping schema in %s due to error: %s", dir.Path, err)
			lc.errCount++
			continue
		}

		// Log and count each errored statement, unless the statement was a CREATE
		// TABLE and the table name matches ignore-table
		for _, stmtErr := range statementErrors {
			if stmtErr.ObjectType == tengo.ObjectTypeTable && ignoreTable != nil && ignoreTable.MatchString(stmtErr.ObjectName) {
				log.Debugf("Skipping %s because ignore-table='%s'", stmtErr.ObjectKey(), ignoreTable)
				continue
			}
			log.Error(stmtErr.Error())
			lc.sqlErrCount++
		}

		// Compare each canonical CREATE in the real schema to each CREATE statement
		// from the filesystem. In cases where they differ, rewrite the file using
		// the canonical version from the DB.
		for key, instCreateText := range schema.ObjectDefinitions() {
			if key.Type == tengo.ObjectTypeTable && ignoreTable != nil && ignoreTable.MatchString(key.Name) {
				log.Debugf("Skipping %s because ignore-table='%s'", key, ignoreTable)
				continue
			}
			fsStmt := logicalSchema.Creates[key]
			fsBody, fsSuffix := fsStmt.SplitTextBody()
			if instCreateText != fsBody {
				fsStmt.Text = fmt.Sprintf("%s%s", instCreateText, fsSuffix)
				length, err := fsStmt.FromFile.Rewrite()
				if err != nil {
					return fmt.Errorf("Unable to write to %s: %s", fsStmt.File, err)
				}
				log.Infof("Wrote %s (%d bytes) -- updated file to normalize format", fsStmt.File, length)
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
