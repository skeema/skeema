// Package applier obtains diffs between the fs and db versions of a schema,
// and can handle execution of the generated SQL.
package applier

import (
	"fmt"
	"os"
	"strings"

	log "github.com/sirupsen/logrus"
	"github.com/skeema/skeema/internal/fs"
	"github.com/skeema/skeema/internal/linter"
	"github.com/skeema/skeema/internal/tengo"
	"github.com/skeema/skeema/internal/util"
)

// ClientState provides information on where and how a SQL statement would be
// executed. It is intended for use in display purposes.
type ClientState struct {
	InstanceName string
	SchemaName   string
	Delimiter    string
	// Eventually may include additional state such as session vars
}

// PlannedStatement represents a SQL statement that is targeted for a specific
// database instance and schema name.
type PlannedStatement interface {
	Execute() error
	Statement() string
	ClientState() ClientState
}

// Result stores the result of applying an individual target, or a combined
// summary of multiple targets.
type Result struct {
	Differences      bool
	SkipCount        int
	UnsupportedCount int
}

// Merge modifies the receiver to include the sub-totals from the supplied arg.
func (r *Result) Merge(other Result) {
	r.Differences = r.Differences || other.Differences
	r.SkipCount += other.SkipCount
	r.UnsupportedCount += other.UnsupportedCount
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

// ApplyTarget generates the diff for the supplied target, prints the resulting
// SQL, and executes the SQL if this isn't a dry-run.
func ApplyTarget(t *Target, printer Printer) (Result, error) {
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
	if vopts, err := VerifierOptionsForTarget(t); err != nil {
		return result, err
	} else if err := VerifyDiff(diff, vopts); err != nil {
		return result, err
	}

	// Build PlannedStatement for each ObjectDiff, handling pre-execution errors
	// accordingly. Also track ObjectKeys for modified objects, for subsequent
	// use in linting.
	objDiffs := diff.ObjectDiffs()
	stmts := make([]PlannedStatement, 0, len(objDiffs))
	keys := make([]tengo.ObjectKey, 0, len(objDiffs))
	var unsafeTables, unsafeNonTables, stderrTerminalWidth int
	var fatalErr error
	for _, objDiff := range objDiffs {
		key := objDiff.ObjectKey()
		ddl, err := NewDDLStatement(objDiff, mods, t)
		if tengo.IsUnsupportedDiff(err) {
			var nonInnoWarning string
			if td, ok := objDiff.(*tengo.TableDiff); ok && td.From != nil && td.From.Engine != "InnoDB" {
				nonInnoWarning = " This table's storage engine is " + td.From.Engine + ", but Skeema is designed to operate primarily on InnoDB tables."
			}
			log.Warnf("Skipping %s: Skeema does not support generating a diff of this table.%s Use --debug to see which properties of this table are not supported.", key, nonInnoWarning)
			log.Debug(err.Error())
			result.UnsupportedCount++
			result.Differences = true
			continue
		}
		if ddl != nil {
			stmts = append(stmts, ddl)
			keys = append(keys, key)
			result.Differences = true
		}
		if tengo.IsUnsafeDiff(err) {
			if unsafeTables+unsafeNonTables == 0 {
				// Attempt to fetch terminal width if this is first unsafe stmt error.
				// If stderr isn't a terminal, this is fine, no wrapping occurs.
				stderrTerminalWidth, _ = util.TerminalWidth(int(os.Stderr.Fd()))
			}
			log.Error(err.Error() + " Generated SQL statement:\n# " + util.WrapStringWithPadding(ddl.stmt, stderrTerminalWidth-29, "# "))
			if key.Type == tengo.ObjectTypeTable {
				unsafeTables++
			} else {
				unsafeNonTables++
			}
		} else if err != nil {
			fatalErr = err
		}
	}

	if fatalErr != nil {
		result.SkipCount += len(stmts)
		return result, fatalErr
	}

	// Lint any modified objects, and log any linter annotations
	var lintResult *linter.Result
	if t.Dir.Config.GetBool("lint") {
		lintOpts, err := linter.OptionsForDir(t.Dir)
		if err != nil {
			return result, ConfigError(err.Error())
		}
		lintOpts.OnlyKeys(keys)
		lintOpts.StripAnnotationNewlines = !util.StderrIsTerminal()
		lintResult = linter.CheckSchema(t.DesiredSchema, lintOpts)
		lintResult.SortByFile()
		for _, annotation := range lintResult.Annotations {
			annotation.Log()
		}
	}

	// Exit early if we had an unsafe statements and/or linter errors
	var fatalProblems []string
	var solution string
	if unsafeTables+unsafeNonTables > 0 {
		var onlyTables string
		if unsafeNonTables == 0 {
			onlyTables = "or --safe-below-size "
		}
		solution = ". Use --allow-unsafe " + onlyTables + "to permit this operation. Refer to the Safety Options section of --help."
		fatalProblems = append(fatalProblems, countAndNoun(unsafeTables+unsafeNonTables, "unsafe statement"))
	}
	if lintResult != nil && lintResult.ErrorCount > 0 {
		solution = "" // Remove message about allow-unsafe if there are also linter errors
		fatalProblems = append(fatalProblems, countAndNoun(lintResult.ErrorCount, "linter error"))
	}
	if len(fatalProblems) > 0 {
		result.SkipCount += len(stmts)
		log.Warnf("Skipping %s %s due to %s%s\n", t.Instance, t.SchemaName, strings.Join(fatalProblems, " and "), solution)
		return result, nil
	}

	// Print SQL; if not dry-run, execute it; final logging; return result
	result.SkipCount += t.processSQL(stmts, printer)
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

// StatementModifiersForDir returns a set of DDL modifiers, based on the
// directory's configuration.
func StatementModifiersForDir(dir *fs.Dir) (mods tengo.StatementModifiers, err error) {
	mods.NextAutoInc = tengo.NextAutoIncIfIncreased
	mods.AllowUnsafe = dir.Config.GetBool("allow-unsafe")
	mods.CompareMetadata = dir.Config.GetBool("compare-metadata")
	mods.VirtualColValidation = dir.Config.GetBool("alter-validate-virtual")
	mods.LaxColumnOrder = dir.Config.GetBool("lax-column-order")
	mods.LaxComments = dir.Config.GetBool("lax-comments")
	if dir.Config.GetBool("exact-match") {
		mods.StrictIndexOrder = true
		mods.StrictCheckOrder = true // only affects MariaDB
		mods.StrictForeignKeyNaming = true
		mods.StrictColumnDefinition = true // only affects MySQL 8
	}
	if mods.AlgorithmClause, err = dir.Config.GetEnum("alter-algorithm", "inplace", "copy", "instant", "nocopy", "default"); err != nil {
		return
	}
	if mods.LockClause, err = dir.Config.GetEnum("alter-lock", "none", "shared", "exclusive", "default"); err != nil {
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

// ConfigError represents a configuration problem encountered at runtime.
type ConfigError string

// Error satisfies the builtin error interface.
func (ce ConfigError) Error() string {
	return string(ce)
}

// ExitCode returns 78 for ConfigError, corresponding to EX_CONFIG in BSD's
// SYSEXITS(3) manpage.
func (ce ConfigError) ExitCode() int {
	return 78
}
