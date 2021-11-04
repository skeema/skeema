// Package applier handles execution of generating diffs between schemas, and
// appropriate application of the generated DDL.
package applier

import (
	"context"
	"fmt"
	"strings"

	log "github.com/sirupsen/logrus"
	"github.com/skeema/skeema/internal/fs"
	"github.com/skeema/skeema/internal/linter"
	"github.com/skeema/skeema/internal/tengo"
)

// Result stores the overall result of all operations the worker has completed.
type Result struct {
	Differences      bool
	SkipCount        int
	UnsupportedCount int
}

// Summary returns a string reflecting the contents of the result.
func (r Result) Summary() string {
	if r.SkipCount+r.UnsupportedCount == 0 {
		return ""
	}
	var plural, reason string
	if r.SkipCount+r.UnsupportedCount > 1 {
		plural = "s"
	}
	if r.SkipCount == 0 {
		reason = "unsupported feature"
	} else if r.UnsupportedCount == 0 {
		reason = "problem"
	} else {
		reason = "problems or unsupported feature"
	}
	return fmt.Sprintf("Skipped %d operation%s due to %s%s", r.SkipCount+r.UnsupportedCount, plural, reason, plural)
}

// Worker reads TargetGroups from the input channel and performs the appropriate
// diff/push operation on each target per TargetGroup. When there are no more
// TargetGroups to read, it writes its aggregate Result to the output channel.
// If a fatal error occurs, it will be returned immediately; Worker is meant to
// be called via an errgroup (see golang.org/x/sync/errgroup).
func Worker(ctx context.Context, targetGroups <-chan TargetGroup, results chan<- Result, printer *Printer) error {
	for tg := range targetGroups {
		for _, t := range tg {
			result, err := applyTarget(t, printer)
			if err != nil {
				return err
			}
			results <- result

			// Exit early if context cancelled
			select {
			case <-ctx.Done():
				return nil
			default:
			}
		}
	}
	return nil
}

func applyTarget(t *Target, printer *Printer) (Result, error) {
	var result Result

	schemaFromInstance, err := t.SchemaFromInstance()
	if err != nil {
		result.SkipCount++
		log.Errorf("Skipping %s schema %s for %s: %s\n", t.Instance, t.SchemaName, t.Dir, err)
		return result, err
	}

	t.logApplyStart()
	schemaFromDir := t.SchemaFromDir()

	// Obtain StatementModifiers based on the dir's config
	mods, err := StatementModifiersForDir(t.Dir)
	if err != nil {
		return result, ConfigError(err.Error())
	}
	mods.Flavor = t.Instance.Flavor()
	if mods.Partitioning == tengo.PartitioningRemove {
		// With partitioning=remove, forcibly treat all filesystem definitions as if
		// they didn't have a partitioning clause. This is designed to aid in the
		// use-case of not running any partition management in a dev environment; if
		// a table somehow manages to be partitioned there anyway by mistake, we
		// intentionally want to de-partition it.
		stripPartitionClauses(schemaFromDir.Tables, mods.Flavor)
	}

	diff := tengo.NewSchemaDiff(schemaFromInstance, schemaFromDir)
	if err := VerifyDiff(diff, t); err != nil {
		return result, err
	}

	// Build DDLStatements for each ObjectDiff, handling pre-execution errors
	// accordingly. Also track ObjectKeys for modified objects, for subsequent
	// use in linting.
	objDiffs := diff.ObjectDiffs()
	ddls := make([]*DDLStatement, 0, len(objDiffs))
	keys := make([]tengo.ObjectKey, 0, len(objDiffs))
	for _, objDiff := range objDiffs {
		ddl, err := NewDDLStatement(objDiff, mods, t)
		if ddl == nil && err == nil {
			continue // Skip entirely if mods made the statement a noop
		}
		result.Differences = true
		if err == nil {
			ddls = append(ddls, ddl)
			keys = append(keys, objDiff.ObjectKey())
		} else if unsupportedErr, ok := err.(*tengo.UnsupportedDiffError); ok {
			result.UnsupportedCount++
			log.Warnf("Skipping %s: Skeema does not support generating a diff of this table. Use --debug to see which properties of this table are not supported.", unsupportedErr.ObjectKey)
			if td, ok := objDiff.(*tengo.TableDiff); ok && td.From != nil && td.From.Engine != "InnoDB" {
				log.Warnf("This table's storage engine is %s. Skeema is primarily designed to operate on InnoDB tables. Diff support for other engines is less complete.", td.From.Engine)
			}
			DebugLogUnsupportedDiff(unsupportedErr)
		} else {
			result.SkipCount += len(objDiffs)
			log.Errorf(err.Error())
			if len(objDiffs) > 1 {
				log.Warnf("Skipping %d additional operations for %s %s due to previous error\n", len(objDiffs)-1, t.Instance, t.SchemaName)
			}
			return result, nil
		}
	}

	// Lint any modified objects; output the result; skip target if any
	// annotations are at the error level
	if t.Dir.Config.GetBool("lint") {
		lintOpts, err := linter.OptionsForDir(t.Dir)
		if err != nil {
			return result, ConfigError(err.Error())
		}
		lintOpts.OnlyKeys(keys)
		lintResult := linter.CheckSchema(t.DesiredSchema, lintOpts)
		lintResult.SortByFile()
		for _, annotation := range lintResult.Annotations {
			annotation.Log()
		}
		if lintResult.ErrorCount > 0 {
			result.SkipCount += len(objDiffs)
			log.Warnf("Skipping %s %s due to %s\n", t.Instance, t.SchemaName, countAndNoun(lintResult.ErrorCount, "linter error"))
			return result, nil
		}
	}

	// Print DDL; if not dry-run, execute it; final logging; return result
	result.SkipCount += t.processDDL(ddls, printer)
	t.logApplyEnd(result)
	return result, nil
}

func stripPartitionClauses(tables []*tengo.Table, flavor tengo.Flavor) {
	for _, table := range tables {
		if table.Partitioning != nil {
			table.CreateStatement = table.UnpartitionedCreateStatement(flavor)
			table.Partitioning = nil
		}
	}
}

// supply 1 noun if pluralization is just adding an s, or 2 nouns if using
// another word entirely
func countAndNoun(n int, nouns ...string) string {
	if n == 1 {
		return fmt.Sprintf("1 %s", nouns[0])
	}
	var plural string
	if len(nouns) == 1 {
		plural = fmt.Sprintf("%ss", nouns[0])
	} else {
		plural = nouns[1]
	}
	return fmt.Sprintf("%d %s", n, plural)
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
	mods.CompareMetadata = dir.Config.GetBool("compare-metadata")
	mods.VirtualColValidation = dir.Config.GetBool("alter-validate-virtual")
	if dir.Config.GetBool("exact-match") {
		mods.StrictIndexOrder = true
		mods.StrictCheckOrder = true // only affects MariaDB
		mods.StrictForeignKeyNaming = true
	}
	if mods.AlgorithmClause, err = dir.Config.GetEnum("alter-algorithm", "inplace", "copy", "instant", "default"); err != nil {
		return
	}
	if mods.LockClause, err = dir.Config.GetEnum("alter-lock", "none", "shared", "exclusive", "default"); err != nil {
		return
	}
	if mods.IgnoreTable, err = dir.Config.GetRegexp("ignore-table"); err != nil {
		return
	}
	var partitioning string
	if partitioning, err = dir.Config.GetEnum("partitioning", "keep", "remove", "modify"); err != nil {
		return
	}
	partMap := map[string]tengo.PartitioningMode{
		"keep":   tengo.PartitioningKeep,
		"remove": tengo.PartitioningRemove,
		"modify": tengo.PartitioningPermissive,
	}
	mods.Partitioning = partMap[partitioning]
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
