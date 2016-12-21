package main

import (
	"fmt"
	"sync"

	log "github.com/Sirupsen/logrus"
	"github.com/skeema/mycli"
	"github.com/skeema/tengo"
)

func init() {
	summary := "Alter tables on DBs to reflect the filesystem representation"
	desc := `Modifies the schemas on database instance(s) to match the corresponding
filesystem representation of them. This essentially performs the same diff logic
as ` + "`" + `skeema diff` + "`" + `, but then actually runs the generated DDL. You should generally
run ` + "`" + `skeema diff` + "`" + ` first to see what changes will be applied.

You may optionally pass an environment name as a CLI option. This will affect
which section of .skeema config files is used for processing. For example,
running ` + "`" + `skeema push staging` + "`" + ` will apply config directives from the
[staging] section of config files, as well as any sectionless directives at the
top of the file. If no environment name is supplied, the default is
"production".`

	cmd := mycli.NewCommand("push", summary, desc, PushHandler)
	cmd.AddOption(mycli.BoolOption("verify", 0, true, "Test all generated ALTER statements on temp schema to verify correctness"))
	cmd.AddOption(mycli.BoolOption("allow-drop-table", 0, false, "Permit dropping any table that has no corresponding *.sql file"))
	cmd.AddOption(mycli.BoolOption("allow-drop-column", 0, false, "Permit dropping columns that are no longer present in *.sql file"))
	cmd.AddOption(mycli.BoolOption("dry-run", 0, false, "Output DDL but don't run it; equivalent to `skeema diff`"))
	cmd.AddOption(mycli.BoolOption("first-only", 0, false, "For dirs mapping to multiple instances or schemas, just run against the first"))
	cmd.AddOption(mycli.BoolOption("all", 0, false, "<overridden by diff command>").Hidden())
	cmd.AddOption(mycli.StringOption("alter-wrapper", 'x', "", "External bin to shell out to for ALTER TABLE; see manual for template vars"))
	cmd.AddOption(mycli.StringOption("alter-wrapper-min-size", 0, "0", "Ignore --alter-wrapper for tables smaller than this size in bytes"))
	cmd.AddOption(mycli.StringOption("alter-lock", 0, "", `Apply a LOCK clause to all ALTER TABLEs (valid values: "NONE", "SHARED", "EXCLUSIVE")`))
	cmd.AddOption(mycli.StringOption("alter-algorithm", 0, "", `Apply an ALGORITHM clause to all ALTER TABLEs (valid values: "INPLACE", "COPY")`))
	cmd.AddOption(mycli.StringOption("ddl-wrapper", 'X', "", "Like --alter-wrapper, but applies to all DDL types (CREATE, DROP, ALTER)"))
	cmd.AddOption(mycli.StringOption("allow-below-size", 0, "0", "For tables under this size, act as if --allow-drop-table --allow-drop-column"))
	cmd.AddOption(mycli.StringOption("concurrent-instances", 'c', "1", "Perform operations on this number of instances concurrently"))
	cmd.AddArg("environment", "production", false)
	CommandSuite.AddSubCommand(cmd)
	clonePushOptionsToDiff()
}

// sharedPushState stores and manages state shared between multiple push workers
type sharedPushState struct {
	targetGroups       <-chan TargetGroup
	dryRun             bool
	errCount           int
	diffCount          int
	unsupportedCount   int
	lastStdoutInstance string
	lastStdoutSchema   string
	fatalError         error
	*sync.WaitGroup
	*sync.Mutex // protects counters as well as STDOUT output and tracking vars
}

// PushHandler is the handler method for `skeema push`
func PushHandler(cfg *mycli.Config) error {
	AddGlobalConfigFiles(cfg)
	dir, err := NewDir(".", cfg)
	if err != nil {
		return err
	}

	workerCount, err := dir.Config.GetInt("concurrent-instances")
	if err == nil && workerCount < 1 {
		err = fmt.Errorf("concurrent-instances cannot be less than 1")
	}
	if err != nil {
		return err
	}

	// `skeema push` and `skeema diff` have different defaults / option names for
	// controlling whether or not to only take the first instance and first schema
	// for dirs that normally multiple to multiple of one or both.
	// that map to multiple instances and/or schemas, e.g. in a sharded setup):
	// `skeema push` defaults to taking all, but can be overridden via --first-only.
	// `skeema diff` defaults to taking the first only, but can be overridden via --all.
	var firstOnly bool
	if cfg.CLI.Command.Name == "diff" {
		firstOnly = !cfg.GetBool("all")
	} else {
		firstOnly = cfg.GetBool("first-only")
	}

	// The 2nd param of dir.TargetGroups indicates that SQLFile errors are to be
	// treated as fatal. This is required for push and diff. Otherwise, a file with
	// invalid CREATE TABLE SQL would lead to a table being missing in the temp
	// schema, which would confuse the logic that diffs schemas.
	sps := &sharedPushState{
		targetGroups: dir.TargetGroups(firstOnly, true),
		dryRun:       cfg.GetBool("dry-run"),
		Mutex:        new(sync.Mutex),
		WaitGroup:    new(sync.WaitGroup),
	}

	for n := 0; n < workerCount; n++ {
		sps.Add(1) // increment the waitgroup
		go pushWorker(sps)
	}

	sps.Wait()
	if sps.fatalError != nil {
		return sps.fatalError
	}

	if sps.errCount+sps.unsupportedCount == 0 {
		if sps.dryRun && sps.diffCount > 0 {
			return NewExitValue(CodeDifferencesFound, "")
		}
		return nil
	}
	var plural, reason string
	code := CodeFatalError
	if sps.errCount+sps.unsupportedCount > 1 {
		plural = "s"
	}
	if sps.errCount == 0 {
		code = CodePartialError
		reason = "unsupported feature"
	} else if sps.unsupportedCount == 0 {
		reason = "error"
	} else {
		reason = "unsupported features or error"
	}
	return NewExitValue(code, "Skipped %d operation%s due to %s%s", sps.errCount+sps.unsupportedCount, plural, reason, plural)
}

func pushWorker(sps *sharedPushState) {
	defer sps.Done()

	mods := tengo.StatementModifiers{
		NextAutoInc: tengo.NextAutoIncIfIncreased,
	}

	for tg := range sps.targetGroups { // consume a TargetGroup from the channel
		for _, t := range tg { // iterate over each Target in the TargetGroup
			if sps.fatalError != nil {
				return
			}
			if t.Err != nil {
				if t.Instance == nil {
					log.Errorf("Skipping %s: %s\n", t.Dir, t.Err)
				} else if t.SchemaFromDir == nil {
					log.Errorf("Skipping %s for %s: %s\n", t.Instance, t.Dir, t.Err)
				} else {
					log.Errorf("Skipping %s %s for %s: %s\n", t.Instance, t.SchemaFromDir.Name, t.Dir, t.Err)
				}
				sps.incrementErrCount(1)
				continue
			}

			// Get schema name from t.SchemaFromDir, NOT t.SchemaFromInstance, since
			// t.SchemaFromInstance will be nil if the schema doesn't exist yet
			schemaName := t.SchemaFromDir.Name

			if sps.dryRun {
				log.Infof("Generating diff of %s %s vs %s/*.sql", t.Instance, schemaName, t.Dir)
			} else {
				log.Infof("Pushing changes from %s/*.sql to %s %s", t.Dir, t.Instance, schemaName)
			}
			for _, warning := range t.SQLFileWarnings {
				log.Debug(warning)
			}

			diff, err := tengo.NewSchemaDiff(t.SchemaFromInstance, t.SchemaFromDir)
			if err != nil {
				sps.setFatalError(err)
				return
			}
			if t.SchemaFromInstance == nil {
				// TODO: support CREATE DATABASE schema-level options
				sps.syncPrintf(t.Instance, "", "%s;\n", t.SchemaFromDir.CreateStatement())
				if !sps.dryRun {
					var err error
					t.SchemaFromInstance, err = t.Instance.CreateSchema(schemaName)
					if err != nil {
						sps.setFatalError(fmt.Errorf("Error creating schema %s on %s: %s", schemaName, t.Instance, err))
						return
					}
				}
			}

			if t.Dir.Config.GetBool("verify") && len(diff.TableDiffs) > 0 {
				if err := t.verifyDiff(diff); err != nil {
					sps.setFatalError(err)
					return
				}
			}

			// Set configuration-dependent statement modifiers here inside the Target
			// loop, since the config for these may var per dir!
			mods.AllowDropTable = t.Dir.Config.GetBool("allow-drop-table")
			mods.AllowDropColumn = t.Dir.Config.GetBool("allow-drop-column")
			mods.AlgorithmClause, err = t.Dir.Config.GetEnum("alter-algorithm", "INPLACE", "COPY", "DEFAULT")
			if err != nil {
				sps.setFatalError(err)
				return
			}
			mods.LockClause, err = t.Dir.Config.GetEnum("alter-lock", "NONE", "SHARED", "EXCLUSIVE", "DEFAULT")
			if err != nil {
				sps.setFatalError(err)
				return
			}

			var targetStmtCount int
			for n, tableDiff := range diff.TableDiffs {
				ddl := NewDDLStatement(tableDiff, mods, t)
				if ddl == nil {
					// skip blank DDL (which may happen due to NextAutoInc modifier)
					continue
				}
				targetStmtCount++
				sps.incrementDiffCount()
				if ddl.Err != nil {
					log.Errorf("%s. The affected DDL statement will be skipped. See --help for more information.", ddl.Err)
					sps.incrementErrCount(1)
				}
				sps.syncPrintf(t.Instance, schemaName, "%s\n", ddl.String())
				if !sps.dryRun && ddl.Err == nil && ddl.Execute() != nil {
					log.Errorf("Error running DDL on %s %s: %s", t.Instance, schemaName, ddl.Err)
					skipCount := len(diff.TableDiffs) - n
					if skipCount > 1 {
						log.Warnf("Due to previous error, skipping %d additional statements on %s %s", skipCount-1, t.Instance, schemaName)
					}
					sps.incrementErrCount(skipCount)
					break
				}
			}
			for _, table := range diff.UnsupportedTables {
				sps.incrementUnsupportedCount()
				targetStmtCount++
				if t.Dir.Config.GetBool("debug") {
					log.Warnf("Skipping table %s: unable to generate ALTER TABLE due to use of unsupported features", table.Name)
					t.logUnsupportedTableDiff(table.Name)
				} else {
					log.Warnf("Skipping table %s: unable to generate ALTER TABLE due to use of unsupported features. Use --debug for more information.", table.Name)
				}
			}

			if targetStmtCount == 0 {
				log.Infof("%s %s: No differences found\n", t.Instance, schemaName)
			} else {
				var verb string
				if sps.dryRun {
					verb = "diff"
				} else {
					verb = "push"
				}
				log.Infof("%s %s: %s complete\n", t.Instance, schemaName, verb)
			}
		}
	}
}

func (sps *sharedPushState) incrementErrCount(n int) {
	sps.Lock()
	sps.errCount += n
	sps.Unlock()
}

func (sps *sharedPushState) incrementDiffCount() {
	sps.Lock()
	sps.diffCount++
	sps.Unlock()
}

func (sps *sharedPushState) incrementUnsupportedCount() {
	sps.Lock()
	sps.unsupportedCount++
	sps.Unlock()
}

func (sps *sharedPushState) setFatalError(err error) {
	sps.Lock()
	if sps.fatalError == nil {
		sps.fatalError = err
	}
	sps.Unlock()
}

// syncPrintf prevents interleaving of STDOUT output from multiple workers.
// It also adds instance and schema lines before output if the previous STDOUT
// was for a different instance or schema.
// TODO: buffer output from external commands and also prevent interleaving there
func (sps *sharedPushState) syncPrintf(instance *tengo.Instance, schemaName string, format string, a ...interface{}) {
	sps.Lock()
	if instance.String() != sps.lastStdoutInstance || schemaName != sps.lastStdoutSchema {
		fmt.Printf("-- instance: %s\n", instance)
		if schemaName != "" {
			fmt.Printf("USE %s;\n", tengo.EscapeIdentifier(schemaName))
		}
		sps.lastStdoutInstance = instance.String()
		sps.lastStdoutSchema = schemaName
	}
	fmt.Printf(format, a...)
	sps.Unlock()
}
