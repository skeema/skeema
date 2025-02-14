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

// UnsafeStatement represents a SQL statement that is destructive, lossy, or
// otherwise operationally risky to execute. If such statements are present and
// not specially approved, the apply process is prevented.
type UnsafeStatement struct {
	Key       tengo.ObjectKey
	Statement string
	Reason    string
}

// Plan represents a set of ordered statements that, if executed, will bring a
// specific Target to the desired state. Unlike a raw tengo.SchemaDiff, the
// statements in a Plan are ordered, and have been pre-processed (handling
// various error conditions and removing diffs that are ignored/no-ops based on
// the configuration) and are ordered in a specific way.
type Plan struct {
	Target      *Target
	Statements  []PlannedStatement
	DiffKeys    []tengo.ObjectKey          // objects with non-blank supported schema differences
	Unsupported map[tengo.ObjectKey]string // map of object key => details on why unsupported
	Unsafe      []UnsafeStatement
}

// Run prints each statement in the plan, and also executes them if the Target's
// configuration indicates that this is not a dry-run.
func (plan *Plan) Run(printer Printer) (skipCount int) {
	dryRun := plan.Target.Dir.Config.GetBool("dry-run")
	for i, stmt := range plan.Statements {
		printer.Print(stmt)
		if !dryRun {
			if err := stmt.Execute(); err != nil {
				log.Errorf("Error running SQL statement on %s: %s\nFull SQL statement: %s%s", plan.Target, err, stmt.Statement(), stmt.ClientState().Delimiter)
				skipCount = len(plan.Statements) - i
				if skipCount > 1 {
					log.Warnf("Skipping %d additional operations for %s due to previous error", skipCount-1, plan.Target)
				}
				return skipCount
			}
		}
	}
	if printerFinisher, ok := printer.(Finisher); ok && len(plan.Statements) > 0 {
		printerFinisher.Finish(plan.Target)
	}
	return 0
}

// LintModifiedObjects lints all objects affected by DDL in the plan.
func (plan *Plan) LintModifiedObjects() (*linter.Result, error) {
	lintOpts, err := linter.OptionsForDir(plan.Target.Dir)
	if err != nil {
		return nil, err
	}
	lintOpts.OnlyKeys(plan.DiffKeys)
	lintOpts.StripAnnotationNewlines = !util.StderrIsTerminal()
	lintResult := linter.CheckSchema(plan.Target.DesiredSchema, lintOpts)
	lintResult.SortByFile()
	return lintResult, nil
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

// Error returns an error with a message indicating the number of problems
// and/or unsupported features reflected in the result. If there were no
// problems or unsupported features, the returned value is nil.
func (r Result) Error() error {
	if r.SkipCount+r.UnsupportedCount == 0 {
		return nil
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
	return fmt.Errorf("Skipped %d operation%s due to %s%s", r.SkipCount+r.UnsupportedCount, plural, reason, plural)
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
	schemaFromDir := t.SchemaFromDir()

	if t.Dir.Config.GetBool("dry-run") {
		log.Infof("Generating diff of %s vs %s%c*.sql", t, t.Dir, os.PathSeparator)
	} else {
		log.Infof("Pushing changes from %s%c*.sql to %s", t.Dir, os.PathSeparator, t)
	}
	if len(t.Dir.UnparsedStatements) > 0 {
		log.Warnf("Ignoring %d unsupported or unparseable statements found in this directory's *.sql files; run `skeema lint` for more info", len(t.Dir.UnparsedStatements))
	}

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
		schemaFromDir.StripTablePartitioning(mods.Flavor)
	}

	diff := tengo.NewSchemaDiff(schemaFromInstance, schemaFromDir)
	plan, err := CreatePlanForTarget(t, diff, mods)
	result.UnsupportedCount = len(plan.Unsupported)
	result.Differences = (len(plan.DiffKeys) + len(plan.Unsupported)) > 0
	if err != nil {
		result.SkipCount += len(plan.Statements)
		return result, err
	}
	for key, details := range plan.Unsupported {
		var nonInnoWarning string
		if table := schemaFromInstance.Table(key.Name); key.Type == tengo.ObjectTypeTable && table != nil && table.Engine != "InnoDB" {
			nonInnoWarning = " This table's storage engine is " + table.Engine + ", but Skeema is designed to operate primarily on InnoDB tables."
		}
		log.Warnf("Skipping %s: Skeema does not support generating a diff of this table.%s Use --debug to see which properties of this table are not supported.", key, nonInnoWarning)
		log.Debug(details)
	}

	// Log errors for unsafe statements, and start to build summary error message
	var fatalProblems []string
	var solutionMessage string
	if len(plan.Unsafe) > 0 {
		onlyTablesMessage := "or --safe-below-size "
		stderrTerminalWidth, _ := util.TerminalWidth(int(os.Stderr.Fd())) // safe to ignore error; if STDERR not tty, no line-wrapping is used
		for _, unsafe := range plan.Unsafe {
			log.Error(unsafe.Reason + " Generated SQL statement:\n# " + util.WrapStringWithPadding(unsafe.Statement, stderrTerminalWidth-29, "# "))
			if unsafe.Key.Type != tengo.ObjectTypeTable {
				onlyTablesMessage = "" // remove message about --safe-below-size, doesn't work on non-tables
			}
		}
		fatalProblems = append(fatalProblems, countAndNoun(len(plan.Unsafe), "unsafe statement"))
		solutionMessage = ". Use --allow-unsafe " + onlyTablesMessage + "to permit this operation. Refer to the Safety Options section of --help."
	}

	// Lint any modified objects, log any linter annotations, and add to summary
	// error message
	if t.Dir.Config.GetBool("lint") {
		lintResult, err := plan.LintModifiedObjects()
		if err != nil {
			return result, ConfigError(err.Error())
		}
		for _, annotation := range lintResult.Annotations {
			annotation.Log()
		}
		if lintResult.ErrorCount > 0 {
			solutionMessage = "" // Remove message about allow-unsafe if there are also linter errors
			fatalProblems = append(fatalProblems, countAndNoun(lintResult.ErrorCount, "linter error"))
		}
	}

	// Return early if we had any unsafe statements and/or linter errors
	if len(fatalProblems) > 0 {
		result.SkipCount += len(plan.Statements)
		log.Warnf("Skipping %s due to %s%s\n", t, strings.Join(fatalProblems, " and "), solutionMessage)
		return result, nil
	}

	// Apply plan (print if dry-run, or execute if not); final logging; return result
	result.SkipCount += plan.Run(printer)
	if !result.Differences {
		log.Infof("%s: No differences found\n", t)
	} else if t.Dir.Config.GetBool("dry-run") {
		log.Infof("%s: diff complete\n", t)
	} else {
		log.Infof("%s: push complete\n", t)
	}
	return result, nil
}

// CreatePlanForTarget converts a raw *tengo.SchemaDiff into a concrete Plan,
// which takes into account the specific Target and its configuration. If a
// fatal error occurs, we still attempt to create the rest of the Plan so that
// the caller can measure how many statements had to be skipped.
func CreatePlanForTarget(t *Target, diff *tengo.SchemaDiff, mods tengo.StatementModifiers) (*Plan, error) {
	var fatalErr error

	// First pass over the full schema diff: Filter out no-ops based on mods (e.g.
	// auto_inc discrepancies); determine what ALTER TABLEs need verification.
	// We verify the correctness of ALTER TABLEs in a workspace in these cases:
	// * If --verify is enabled (as it is by default), we verify any table with at
	//   least one ALTER TABLE that wasn't blank due to mods
	// * If Statement returns a tengo.UnsupportedDiffError, we want to see if any
	//   supported part of the diff could be generated (using stricter mods), and
	//   verify it if so. If it passes verification (meaning, the generated diff
	//   actually fully converts the table from the original to the desired state)
	//   then we can mark it as supported and handle it like any other diff.
	verifyAllAlterTables := t.Dir.Config.GetBool("verify")
	allObjDiffs := diff.ObjectDiffs()
	objDiffs := make([]tengo.ObjectDiff, 0, len(allObjDiffs))
	allAlterTables := make([]*tengo.TableDiff, 0)
	verifyKeys := make(map[tengo.ObjectKey]bool)
	for _, objDiff := range allObjDiffs {
		// Filter out cases where stmt is blank and err is nil. That return combo
		// indicates a no-op difference, i.e. ignored based on the options supplied.
		stmt, err := objDiff.Statement(mods)
		if stmt != "" || err != nil {
			objDiffs = append(objDiffs, objDiff)
		}

		// Extra bookkeeping for ALTER TABLE verification:
		// Track all ALTER TABLEs (even if no-ops) in an ordered slice, as well as
		// tracking object keys needing verification as per the two cases described in
		// bullets above. This is necessary since some types of diffs are split into
		// multiple separate ALTER TABLE on the same table, for edge cases with adding
		// foreign keys or adding fulltext indexes. If *any* of the diffs for a given
		// table require verification, we need all those ALTERs for that table, even
		// if some were no-ops when applying statement mods. (Verification uses a
		// different set of mods, so these might not remain no-ops for verification.)
		// Maintaining the original diff order among the ALTER TABLEs is also required
		// for ensuring foreign keys get added after any new parent-table indexes they
		// depend on.
		if td, ok := objDiff.(*tengo.TableDiff); ok && td.DiffType() == tengo.DiffTypeAlter {
			allAlterTables = append(allAlterTables, td)
			if (stmt != "" && verifyAllAlterTables) || tengo.IsUnsupportedDiff(err) {
				verifyKeys[objDiff.ObjectKey()] = true
			}
		}
	}

	// Run verification on ALTER TABLEs if needed
	if len(verifyKeys) > 0 {
		var toVerify []*tengo.TableDiff
		for _, td := range allAlterTables {
			if verifyKeys[td.ObjectKey()] {
				toVerify = append(toVerify, td)
			}
		}
		if vopts, err := VerifierOptionsForTarget(t); err != nil {
			fatalErr = err
		} else if err := VerifyDiff(toVerify, vopts); err != nil {
			fatalErr = err
		}
	}

	plan := &Plan{
		Target:      t,
		Statements:  make([]PlannedStatement, 0, len(objDiffs)),
		DiffKeys:    make([]tengo.ObjectKey, 0, len(objDiffs)),
		Unsupported: make(map[tengo.ObjectKey]string),
	}

	// Second pass over diffs: build plan
	for _, objDiff := range objDiffs {
		key := objDiff.ObjectKey()
		ddl, err := NewDDLStatement(objDiff, mods, t)
		if tengo.IsUnsupportedDiff(err) {
			plan.Unsupported[key] = err.Error()
			continue
		}
		if ddl != nil {
			plan.Statements = append(plan.Statements, ddl)
			plan.DiffKeys = append(plan.DiffKeys, key)
			if tengo.IsUnsafeDiff(err) {
				plan.Unsafe = append(plan.Unsafe, UnsafeStatement{
					Key:       key,
					Statement: ddl.stmt,
					Reason:    err.Error(),
				})
			}
		}
		if err != nil && fatalErr == nil && !tengo.IsUnsafeDiff(err) {
			// Track first non-unsupported, non-unsafe error for use in this function's return value
			fatalErr = err
		}
	}

	return plan, fatalErr
}

// supply 1 noun if pluralized form just adds an s; otherwise supply singular
// and plural nouns separately
func countAndNoun(n int, nouns ...string) string {
	if n == 1 { // use singular form from nouns[0]
		return "1 " + nouns[0]
	} else if len(nouns) == 1 { // pluralize by adding 's' to nouns[0]
		return fmt.Sprintf("%d %ss", n, nouns[0])
	} else { // use plural form from nouns[1]
		return fmt.Sprintf("%d %s", n, nouns[1])
	}
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
		mods.StrictCheckConstraints = true
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
