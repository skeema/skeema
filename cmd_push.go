package main

import (
	"fmt"
	"strings"
	"sync"

	log "github.com/sirupsen/logrus"
	"github.com/skeema/mybase"
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

	cmd := mybase.NewCommand("push", summary, desc, PushHandler)
	cmd.AddOption(mybase.BoolOption("verify", 0, true, "Test all generated ALTER statements on temp schema to verify correctness"))
	cmd.AddOption(mybase.BoolOption("allow-unsafe", 0, false, "Permit running ALTER or DROP operations that are potentially destructive"))
	cmd.AddOption(mybase.BoolOption("dry-run", 0, false, "Output DDL but don't run it; equivalent to `skeema diff`"))
	cmd.AddOption(mybase.BoolOption("first-only", '1', false, "For dirs mapping to multiple instances or schemas, just run against the first per dir"))
	cmd.AddOption(mybase.BoolOption("exact-match", 0, false, "Follow *.sql table definitions exactly, even for differences with no functional impact"))
	cmd.AddOption(mybase.BoolOption("foreign-key-checks", 0, false, "Force the server to check referential integrity of any new foreign key"))
	cmd.AddOption(mybase.BoolOption("brief", 'q', false, "<overridden by diff command>").Hidden())
	cmd.AddOption(mybase.StringOption("alter-wrapper", 'x', "", "External bin to shell out to for ALTER TABLE; see manual for template vars"))
	cmd.AddOption(mybase.StringOption("alter-wrapper-min-size", 0, "0", "Ignore --alter-wrapper for tables smaller than this size in bytes"))
	cmd.AddOption(mybase.StringOption("alter-lock", 0, "", `Apply a LOCK clause to all ALTER TABLEs (valid values: "NONE", "SHARED", "EXCLUSIVE")`))
	cmd.AddOption(mybase.StringOption("alter-algorithm", 0, "", `Apply an ALGORITHM clause to all ALTER TABLEs (valid values: "INPLACE", "COPY")`))
	cmd.AddOption(mybase.StringOption("ddl-wrapper", 'X', "", "Like --alter-wrapper, but applies to all DDL types (CREATE, DROP, ALTER)"))
	cmd.AddOption(mybase.StringOption("safe-below-size", 0, "0", "Always permit destructive operations for tables below this size in bytes"))
	cmd.AddOption(mybase.StringOption("concurrent-instances", 'c', "1", "Perform operations on this number of instances concurrently"))
	cmd.AddArg("environment", "production", false)
	CommandSuite.AddSubCommand(cmd)
	clonePushOptionsToDiff()
}

// sharedPushState stores and manages state shared between multiple push workers
type sharedPushState struct {
	targetGroups       <-chan TargetGroup
	dryRun             bool
	briefOutput        bool
	errCount           int
	diffCount          int
	unsupportedCount   int
	lastStdoutInstance string
	lastStdoutSchema   string
	seenInstance       map[string]bool
	fatalError         error
	*sync.WaitGroup
	*sync.Mutex // protects counters as well as STDOUT output and tracking vars
}

// PushHandler is the handler method for `skeema push`
func PushHandler(cfg *mybase.Config) error {
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
		return NewExitValue(CodeBadConfig, err.Error())
	}

	// The 2nd param of dir.TargetGroups indicates that SQLFile errors are to be
	// treated as fatal. This is required for push and diff. Otherwise, a file with
	// invalid CREATE TABLE SQL would lead to a table being missing in the temp
	// schema, which would confuse the logic that diffs schemas.
	sps := &sharedPushState{
		targetGroups: dir.TargetGroups(cfg.GetBool("first-only"), true),
		dryRun:       cfg.GetBool("dry-run"),
		briefOutput:  cfg.GetBool("brief") && cfg.GetBool("dry-run"),
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

	for tg := range sps.targetGroups { // consume a TargetGroup from the channel
		for _, t := range tg { // iterate over each Target in the TargetGroup
			if sps.fatalError != nil {
				return
			}
			// Skip this target if there's a fatal error with the dir or instance, not
			// specific to one schema
			if t.Err != nil && t.SchemaFromDir == nil {
				if t.Instance == nil {
					log.Errorf("Skipping %s: %s\n", t.Dir, t.Err)
				} else {
					log.Errorf("Skipping %s for %s: %s\n", t.Instance, t.Dir, t.Err)
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

			if t.Err != nil {
				log.Errorf("Skipping %s %s: %s\n", t.Instance, schemaName, t.Err)
				sps.incrementErrCount(1)
				continue
			}
			for _, warning := range t.SQLFileWarnings {
				log.Debug(warning)
			}

			diff := tengo.NewSchemaDiff(t.SchemaFromInstance, t.SchemaFromDir)
			var targetStmtCount int

			if diff.SchemaDDL != "" {
				sps.syncPrintf(t.Instance, "", "%s;\n", diff.SchemaDDL)
				targetStmtCount++
				if !sps.dryRun {
					if strings.HasPrefix(diff.SchemaDDL, "CREATE DATABASE") && t.SchemaFromInstance == nil {
						_, err := t.Instance.CreateSchema(schemaName, t.SchemaFromDir.CharSet, t.SchemaFromDir.Collation)
						if err != nil {
							sps.setFatalError(fmt.Errorf("Error creating schema %s on %s: %s", schemaName, t.Instance, err))
							return
						}
					} else if strings.HasPrefix(diff.SchemaDDL, "ALTER DATABASE") {
						err := t.Instance.AlterSchema(t.SchemaFromInstance.Name, t.SchemaFromDir.CharSet, t.SchemaFromDir.Collation)
						if err != nil {
							sps.setFatalError(fmt.Errorf("Unable to alter defaults for schema %s on %s: %s", t.SchemaFromInstance.Name, t.Instance, err))
							return
						}
					} else {
						sps.setFatalError(fmt.Errorf("Refusing to run unexpectedly-generated schema-level DDL: %s", diff.SchemaDDL))
						return
					}
				}
			}

			if t.Dir.Config.GetBool("verify") && len(diff.TableDiffs) > 0 && !sps.briefOutput {
				if err := t.verifyDiff(diff); err != nil {
					sps.setFatalError(err)
					return
				}
			}

			// Obtain StatementModifiers based on the dir's config
			mods, err := t.Dir.StatementModifiers(sps.briefOutput)
			if err != nil {
				sps.setFatalError(NewExitValue(CodeBadConfig, err.Error()))
				return
			}
			mods.Flavor = t.Instance.Flavor()

			// Build DDLStatements for each TableDiff, handling pre-execution errors
			// accordingly
			ddls := make([]*DDLStatement, 0, len(diff.TableDiffs))
			for _, tableDiff := range diff.TableDiffs {
				ddl := NewDDLStatement(tableDiff, mods, t)
				if ddl.IsNoop() {
					continue
				}
				targetStmtCount++ // counter for operations on this specific target
				if ddl.Err == nil {
					if sps.dryRun {
						sps.syncPrintf(t.Instance, schemaName, "%s\n", ddl.String())
						sps.incrementDiffCount()
					} else {
						ddls = append(ddls, ddl)
					}
				} else if err, ok := ddl.Err.(*tengo.UnsupportedDiffError); ok {
					sps.incrementUnsupportedCount()
					log.Warnf("Skipping table %s: unable to generate ALTER TABLE due to use of unsupported features. Use --debug for more information.", err.Name)
					DebugLogUnsupportedDiff(err)
				} else {
					sps.incrementErrCount(1)
					if tengo.IsForbiddenDiff(ddl.Err) {
						log.Errorf("Destructive statement %s is considered unsafe. Use --allow-unsafe or --safe-below-size to permit this operation; see --help for more information.", ddl.String())
					} else {
						log.Errorf("A fatal error occurred with pre-processing a DDL statement: %s.", ddl.Err)
					}
					t.Err = fmt.Errorf("Skipping %s %s due to error", t.Instance, schemaName)
					break
				}
			}

			// Execute DDL
			for _, ddl := range ddls {
				if t.Err != nil {
					break
				}
				sps.syncPrintf(t.Instance, schemaName, "%s\n", ddl.String())
				if ddl.Execute() != nil {
					log.Errorf("Error running DDL on %s %s: %s", t.Instance, schemaName, ddl.Err)
					t.Err = fmt.Errorf("Aborting early on %s %s due to error", t.Instance, schemaName)
					sps.incrementErrCount(1)
				}
			}

			if t.Err != nil {
				log.Errorf("%s\n", t.Err)
			} else if targetStmtCount == 0 {
				log.Infof("%s %s: No differences found\n", t.Instance, schemaName)
			} else {
				verb := "push"
				if sps.dryRun {
					verb = "diff"
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
	defer sps.Unlock()

	if sps.briefOutput {
		if sps.seenInstance == nil {
			sps.seenInstance = make(map[string]bool)
		}
		if _, already := sps.seenInstance[instance.String()]; !already {
			fmt.Printf("%s\n", instance)
			sps.seenInstance[instance.String()] = true
		}
		return
	}
	if instance.String() != sps.lastStdoutInstance || schemaName != sps.lastStdoutSchema {
		fmt.Printf("-- instance: %s\n", instance)
		if schemaName != "" {
			fmt.Printf("USE %s;\n", tengo.EscapeIdentifier(schemaName))
		}
		sps.lastStdoutInstance = instance.String()
		sps.lastStdoutSchema = schemaName
	}
	fmt.Printf(format, a...)
}
