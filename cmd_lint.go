package main

import (
	"fmt"
	"os"
	"regexp"

	log "github.com/Sirupsen/logrus"
	"github.com/skeema/mybase"
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
	AddGlobalConfigFiles(cfg)
	dir, err := NewDir(".", cfg)
	if err != nil {
		return err
	}

	var errCount, sqlErrCount, reformatCount int
	for _, t := range dir.Targets() {
		if t.Err != nil {
			log.Errorf("Skipping %s:", t.Dir)
			log.Errorf("    %s\n", t.Err)
			errCount++
			continue
		}

		log.Infof("Linting %s", t.Dir)

		for _, sf := range t.SQLFileErrors {
			log.Error(sf.Error)
			sqlErrCount++
		}

		ignoreTable := t.Dir.Config.Get("ignore-table")
		re, err := regexp.Compile(ignoreTable)
		if err != nil {
			return fmt.Errorf("Invalid regular expression on ignore-table: %s; %s", ignoreTable, err)
		}
		tables, _ := t.SchemaFromDir.Tables() // can ignore error since table list already guaranteed to be cached
		for _, table := range tables {
			if ignoreTable != "" && re.MatchString(table.Name) {
				log.Warnf("Skipping table %s because ignore-table matched %s", table.Name, ignoreTable)
				continue
			}
			sf := SQLFile{
				Dir:      t.Dir,
				FileName: fmt.Sprintf("%s.sql", table.Name),
			}
			if _, err := sf.Read(); err != nil {
				return err
			}
			for _, warning := range sf.Warnings {
				log.Debug(warning)
			}
			if table.CreateStatement() != sf.Contents {
				sf.Contents = table.CreateStatement()
				var length int
				if length, err = sf.Write(); err != nil {
					return fmt.Errorf("Unable to write to %s: %s", sf.Path(), err)
				}
				log.Infof("Wrote %s (%d bytes) -- updated file to normalize format", sf.Path(), length)
				reformatCount++
			}
		}
		os.Stderr.WriteString("\n")
	}

	var plural string
	if errCount > 1 || (errCount == 0 && sqlErrCount > 1) {
		plural = "s"
	}
	switch {
	case errCount > 0:
		return NewExitValue(CodeFatalError, "Skipped %d operation%s due to error%s", errCount, plural, plural)
	case sqlErrCount > 0:
		return NewExitValue(CodeFatalError, "Found syntax error%s in %d SQL file%s", plural, sqlErrCount, plural)
	case reformatCount > 0:
		return NewExitValue(CodeDifferencesFound, "")
	default:
		return nil
	}
}
