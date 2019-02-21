// Package applier handles execution of generating diffs between schemas, and
// appropriate application of the generated DDL.
package applier

import (
	"context"
	"strings"

	log "github.com/sirupsen/logrus"
	"github.com/skeema/skeema/fs"
	"github.com/skeema/tengo"
)

// Result stores the overall result of all operations the worker has completed.
type Result struct {
	Differences      bool
	SkipCount        int
	UnsupportedCount int
}

// Worker reads TargetGroups from the input channel and performs the appropriate
// diff/push operation on each target per TargetGroup. When there are no more
// TargetGroups to read, it writes its aggregate Result to the output channel.
// If a fatal error occurs, it will be returned immediately; Worker is meant to
// be called via an errgroup (see golang.org/x/sync/errgroup).
func Worker(ctx context.Context, targetGroups <-chan TargetGroup, results chan<- Result, printer *Printer) error {
	var result Result
	for tg := range targetGroups {
	TargetsInGroup:
		for _, t := range tg { // iterate over each Target in the TargetGroup
			// Get schema name from t.SchemaFromDir, NOT t.SchemaFromInstance, since
			// t.SchemaFromInstance will be nil if the schema doesn't exist yet
			schemaName := t.SchemaFromDir.Name
			dryRun := t.Dir.Config.GetBool("dry-run")
			brief := dryRun && t.Dir.Config.GetBool("brief")

			if dryRun {
				log.Infof("Generating diff of %s %s vs %s/*.sql", t.Instance, schemaName, t.Dir)
			} else {
				log.Infof("Pushing changes from %s/*.sql to %s %s", t.Dir, t.Instance, schemaName)
			}

			diff := tengo.NewSchemaDiff(t.SchemaFromInstance, t.SchemaFromDir)
			var targetStmtCount int

			if t.Dir.Config.GetBool("verify") && len(diff.TableDiffs) > 0 && !brief {
				if err := VerifyDiff(diff, t); err != nil {
					return err
				}
			}

			// Obtain StatementModifiers based on the dir's config
			mods, err := StatementModifiersForDir(t.Dir)
			if err != nil {
				return ConfigError(err.Error())
			}
			if configFlavor := tengo.NewFlavor(t.Dir.Config.Get("flavor")); configFlavor != tengo.FlavorUnknown {
				mods.Flavor = configFlavor
			} else {
				mods.Flavor = t.Instance.Flavor()
			}

			// Build DDLStatements for each ObjectDiff, handling pre-execution errors
			// accordingly
			objDiffs := diff.ObjectDiffs()
			ddls := make([]*DDLStatement, 0, len(objDiffs))
			for _, objDiff := range objDiffs {
				ddl, err := NewDDLStatement(objDiff, mods, t)
				if ddl == nil && err == nil {
					continue // Skip entirely if mods made the statement a noop
				}
				targetStmtCount++
				result.Differences = true
				if err == nil {
					ddls = append(ddls, ddl)
				} else if unsupportedErr, ok := err.(*tengo.UnsupportedDiffError); ok {
					result.UnsupportedCount++
					log.Warnf("Skipping %s: unable to generate DDL due to use of unsupported features. Use --debug for more information.", unsupportedErr.ObjectKey)
					DebugLogUnsupportedDiff(unsupportedErr)
				} else {
					result.SkipCount += len(objDiffs)
					log.Errorf(err.Error())
					if len(objDiffs) > 1 {
						log.Warnf("Skipping %d additional operations for %s %s due to previous error", len(objDiffs)-1, t.Instance, schemaName)
					}
					continue TargetsInGroup
				}
			}

			// Print DDL; if not dry-run, execute it
			for i, ddl := range ddls {
				printer.printDDL(ddl)
				if !dryRun {
					if err := ddl.Execute(); err != nil {
						log.Errorf("Error running DDL on %s %s: %s", t.Instance, schemaName, err)
						skipped := len(ddls) - i
						result.SkipCount += skipped
						if skipped > 1 {
							log.Warnf("Skipping %d remaining operations for %s %s due to previous error", skipped-1, t.Instance, schemaName)
						}
						break
					}
				}
			}

			if targetStmtCount == 0 {
				log.Infof("%s %s: No differences found\n", t.Instance, schemaName)
			} else {
				verb := "push"
				if dryRun {
					verb = "diff"
				}
				log.Infof("%s %s: %s complete\n", t.Instance, schemaName, verb)
			}

			// Exit early if context cancelled
			select {
			case <-ctx.Done():
				return nil
			default:
			}
		}
	}
	results <- result
	return nil
}

// SumResults adds up the supplied results to return a single combined result.
func SumResults(results []Result) Result {
	var total Result
	for _, r := range results {
		total.Differences = total.Differences || r.Differences
		total.SkipCount += r.SkipCount
		total.UnsupportedCount += r.UnsupportedCount
	}
	return total
}

// StatementModifiersForDir returns a set of DDL modifiers, based on the
// directory's configuration.
func StatementModifiersForDir(dir *fs.Dir) (mods tengo.StatementModifiers, err error) {
	mods.NextAutoInc = tengo.NextAutoIncIfIncreased
	forceAllowUnsafe := dir.Config.GetBool("brief") && dir.Config.GetBool("dry-run")
	mods.AllowUnsafe = forceAllowUnsafe || dir.Config.GetBool("allow-unsafe")
	if dir.Config.GetBool("exact-match") {
		mods.StrictIndexOrder = true
		mods.StrictForeignKeyNaming = true
	}
	if mods.AlgorithmClause, err = dir.Config.GetEnum("alter-algorithm", "INPLACE", "COPY", "INSTANT", "DEFAULT"); err != nil {
		return
	}
	if mods.LockClause, err = dir.Config.GetEnum("alter-lock", "NONE", "SHARED", "EXCLUSIVE", "DEFAULT"); err != nil {
		return
	}
	if mods.IgnoreTable, err = dir.Config.GetRegexp("ignore-table"); err != nil {
		return
	}
	return
}

// DebugLogUnsupportedDiff logs (at Debug level) the reason why an object is
// unsupported for diff/alter operations.
func DebugLogUnsupportedDiff(err *tengo.UnsupportedDiffError) {
	for _, line := range strings.Split(err.ExtendedError(), "\n") {
		if len(line) > 0 {
			log.Debug(line)
		}
	}
}

// ConfigError represents a configuration problem encountered at runtime.
type ConfigError string

// Error satisfies the builtin error interface.
func (ce ConfigError) Error() string {
	return string(ce)
}
