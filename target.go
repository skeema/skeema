package main

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/pmezard/go-difflib/difflib"
	"github.com/skeema/tengo"
)

// MaxNonSkeemaDirs indicates an upper-bound for how many seemingly non-Skeema-
// related directories in one dir tree we can encounter before halting
// recursive dir descent early
const MaxNonSkeemaDirs = 1000

// Target represents a unit of operation. For commands that operate recursively
// on a directory tree, one or more Targets are generated for each leaf
// directory -- the cartesian product of (instances this dir maps to) x (schemas
// that this dir maps to on each instance).
type Target struct {
	Instance           *tengo.Instance
	SchemaFromInstance *tengo.Schema
	SchemaFromDir      *tengo.Schema
	Dir                *Dir
	Err                error
	SQLFileErrors      map[string]*SQLFile // map of string path to *SQLFile that contains an error
	SQLFileWarnings    []error             // slice of all warnings for Target.Dir (no need to organize by file or path)
}

// TargetGroup represents a group of Targets that all have the same Instance.
type TargetGroup []*Target

// TargetGroupMap stores multiple TargetGroups, properly arranged by Instance.
type TargetGroupMap map[string]TargetGroup

// NewTargetGroupMap returns a new TargetGroupMap
func NewTargetGroupMap() TargetGroupMap {
	return make(TargetGroupMap)
}

// Add stores a Target in the appropriate TargetGroup.
func (tgm TargetGroupMap) Add(t *Target) {
	key := t.Instance.String()
	tgm[key] = append(tgm[key], t)
}

// AddDirError records a special Target value which indicates there was a
// fatal problem with a directory, not specific to one instance.
func (tgm TargetGroupMap) AddDirError(dir *Dir, err error) {
	t := &Target{
		Dir: dir,
		Err: err,
	}
	tgm["errors"] = append(tgm["errors"], t)
}

// AddInstanceError is a convenience method for encoding a Target value which
// hit a fatal problem on one specific instance and dir.
func (tgm TargetGroupMap) AddInstanceError(instance *tengo.Instance, dir *Dir, err error) {
	t := &Target{
		Dir:      dir,
		Err:      err,
		Instance: instance,
	}
	tgm.Add(t)
}

// generateTargetsForDir examines dir's configuration, figures out what Target
// or Targets the dir maps to, indexes them in targetsByInstance, and then
// recursively descends through dir's subdirectories to do the same.
//
// If firstOnly is true, any directory that normally maps to multiple instances
// and/or schemas will only use of the first of each. If fatalSQLFileErrors is
// true, any file with an invalid CREATE TABLE will cause a single instanceless
// error Target to be used for the directory.
//
// The return values indicate the count of dirs (this dir + all subdirs) that
// did or did not (respectively) define a host+schema for at least one
// environment.
func generateTargetsForDir(dir *Dir, targetsByInstance TargetGroupMap, firstOnly, fatalSQLFileErrors bool) (skeemaDirs, otherDirs int) {
	// Generate targets if this dir's .skeema file defines a schema (for current
	// environment section), and the dir's config hierarchy defines a host
	// somewhere (here, or a parent dir)
	if dir.Config.Changed("host") && dir.HasSchema() {
		var instances []*tengo.Instance
		var instancesErr error

		if firstOnly {
			var onlyInstance *tengo.Instance
			onlyInstance, instancesErr = dir.FirstInstance()
			if onlyInstance == nil && instancesErr == nil {
				instancesErr = fmt.Errorf("No instance defined for %s", dir)
			}
			if instancesErr == nil {
				// dir.FirstInstance already checks for connectivity, so no need to redo that here
				instances = []*tengo.Instance{onlyInstance}
			}
		} else {
			var rawInstances []*tengo.Instance
			rawInstances, instancesErr = dir.Instances()
			// dir.Instances doesn't pre-check for connectivity problems, so do that now
			for _, inst := range rawInstances {
				if ok, err := inst.CanConnect(); !ok {
					targetsByInstance.AddInstanceError(inst, dir, err)
				} else {
					instances = append(instances, inst)
				}
			}
		}

		// This class of error means the config was invalid (i.e. some option had a gibberish value)
		if instancesErr != nil {
			targetsByInstance.AddDirError(dir, instancesErr)
		}

		// Obtain a "template" Target based on the dir's configuration and *.sql
		// contents. This is used later for creating instance- and schema-specific
		// Targets.
		var template Target
		if len(instances) > 0 {
			template = dir.TargetTemplate(instances[0])

			if template.Err == nil && fatalSQLFileErrors && len(template.SQLFileErrors) > 0 {
				for _, sf := range template.SQLFileErrors {
					template.Err = sf.Error
					break // only need one element of the map, doesn't matter which one
				}
			}

			// If something went wrong obtaining the temp schema, record the error
			// (without the instance, so it's clear that the entire dir is being skipped)
			// and don't generate any instance-specific Targets for this dir.
			if template.Err != nil {
				targetsByInstance.AddDirError(dir, template.Err)
				instances = instances[:0]
			}
		}

		for _, inst := range instances {
			schemaNames, err := dir.SchemaNames(inst)
			if err != nil {
				targetsByInstance.AddInstanceError(inst, dir, err)
				continue
			}
			schemasByName, err := inst.SchemasByName()
			if err != nil {
				targetsByInstance.AddInstanceError(inst, dir, err)
				continue
			}
			if len(schemaNames) > 1 && firstOnly {
				schemaNames = schemaNames[0:1]
			}
			for _, schemaName := range schemaNames {
				// Copy the template into a new Target. Using inst, set its Instance and
				// SchemaFromInstance accordingly. Set its SchemaFromDir to a copy of the
				// template's, so that we can "correct" its name without affecting other
				// targets.
				t := template
				t.Instance = inst
				t.SchemaFromDir, _ = t.SchemaFromDir.CachedCopy() // error not possible so safe to ignore
				t.SchemaFromDir.Name = schemaName
				t.SchemaFromInstance = schemasByName[schemaName] // this may be nil if schema doesn't exist yet; callers handle that
				targetsByInstance.Add(&t)
			}
		}
		skeemaDirs++
	} else if !dir.Config.Changed("host") && dir.HasSchema() {
		// If we have a schema defined but no host, display a warning
		log.Warnf("Skipping %s: no host defined for environment \"%s\"\n", dir, dir.section)
		skeemaDirs++ // still counts as a skeema-relevant dir though
	} else if f, err := dir.OptionFile(); err == nil && f.SomeSectionHasOption("schema") {
		// If we don't have a schema defined, but we would if some other environment
		// had been selected, display a warning
		log.Warnf("Skipping %s: no schema defined for environment \"%s\"\n", dir, dir.section)
		skeemaDirs++ // still counts as a skeema-relevant dir though
	} else {
		otherDirs++ // no combination of host+schema defined here, for any environment
	}

	subdirs, err := dir.Subdirs()
	if err != nil {
		targetsByInstance.AddDirError(dir, err)
	} else {
		for _, subdir := range subdirs {
			// Don't iterate into hidden dirs, since version control software may store
			// files in there with names matching real things we care about (*.sql,
			// .skeema, etc)
			if subdir.BaseName()[0] == '.' {
				continue
			}

			// Recurse into the subdir, halting early if we've encountered too many
			// irrelevant subdirs, possibly indicating that skeema was invoked in the
			// wrong directory tree
			skeemaSubdirs, otherSubdirs := generateTargetsForDir(subdir, targetsByInstance, firstOnly, fatalSQLFileErrors)
			skeemaDirs += skeemaSubdirs
			otherDirs += otherSubdirs
			if otherDirs >= MaxNonSkeemaDirs && skeemaDirs == 0 {
				return
			}
		}
	}
	return
}

// verifyDiff verifies the result of all AlterTable values found in
// diff.TableDiffs, confirming that applying the corresponding ALTER would
// bring a table from the version in SchemaFromInstance to the version in
// SchemaFromDir.
func (t *Target) verifyDiff(diff *tengo.SchemaDiff) (err error) {
	// Populate the temp schema with a copy of the tables from SchemaFromInstance,
	// the "before" state of the tables
	tempSchemaName := t.Dir.Config.Get("temp-schema")

	// TODO: want to skip binlogging for all temp schema actions, if super priv available
	var tx *sql.Tx
	if tx, err = t.lockTempSchema(30 * time.Second); err != nil {
		return fmt.Errorf("verifyDiff: %s", err)
	}
	defer func() {
		unlockErr := t.unlockTempSchema(tx)
		if unlockErr != nil && err == nil {
			err = fmt.Errorf("verifyDiff: %s", unlockErr)
		}
	}()

	tempSchema, err := t.Instance.Schema(tempSchemaName)
	if err != nil {
		return err
	}
	if tempSchema != nil {
		// Attempt to drop any tables already present in tempSchema, but fail if
		// any of them actually have 1 or more rows
		if err := t.Instance.DropTablesInSchema(tempSchema, true); err != nil {
			return fmt.Errorf("verifyDiff: cannot drop existing tables for %s on %s: %s", t.Dir, t.Instance, err)
		}
	} else {
		tempSchema, err = t.Instance.CreateSchema(tempSchemaName)
		if err != nil {
			return fmt.Errorf("verifyDiff: cannot create temporary schema for %s on %s: %s", t.Dir, t.Instance, err)
		}
	}
	if err = t.Instance.CloneSchema(t.SchemaFromInstance, tempSchema); err != nil {
		return err
	}

	db, err := t.Instance.Connect(tempSchemaName, "")
	if err != nil {
		return fmt.Errorf("verifyDiff: cannot connect to %s: %s", t.Instance, err)
	}
	mods := tengo.StatementModifiers{
		NextAutoInc: tengo.NextAutoIncIgnore,
	}
	alteredTableNames := make([]string, 0)

	// Iterate over the TableDiffs in the SchemaDiff. For any that are an ALTER,
	// run it against the table in the temp schema, and see if the table now matches
	// the version in the toTables map.
	for _, tableDiff := range diff.TableDiffs {
		alter, ok := tableDiff.(tengo.AlterTable)
		if !ok {
			continue
		}
		stmt, _ := tableDiff.Statement(mods) // fine to ignore errors for verifying DDL against temporary schema
		if stmt == "" {
			continue
		}
		if _, err = db.Exec(stmt); err != nil {
			return err
		}
		alteredTableNames = append(alteredTableNames, alter.Table.Name)
	}
	postAlterTables, err := tempSchema.TablesByName()
	if err != nil {
		return err
	}
	expectTables, _ := t.SchemaFromDir.TablesByName() // can ignore error since we know table list already cached

	for _, name := range alteredTableNames {
		// We have to compare CREATE TABLE statements without their next auto-inc
		// values, since divergence there may be expected depending on settings
		expected, _ := tengo.ParseCreateAutoInc(expectTables[name].CreateStatement())
		actual, _ := tengo.ParseCreateAutoInc(postAlterTables[name].CreateStatement())
		if expected != actual {
			return fmt.Errorf("verifyDiff: Failure on table %s\nEXPECTED POST-ALTER:\n%s\n\nACTUAL POST-ALTER:\n%s\n\nRun command again with --skip-verify if this discrepancy is safe to ignore", name, expected, actual)
		}
	}

	// Clean up the temp schema
	if t.Dir.Config.GetBool("reuse-temp-schema") {
		if err = t.Instance.DropTablesInSchema(tempSchema, true); err != nil {
			return fmt.Errorf("verifyDiff: cannot drop tables in temporary schema for %s on %s: %s", t.Dir, t.Instance, err)
		}
	} else {
		if err = t.Instance.DropSchema(tempSchema, true); err != nil {
			return fmt.Errorf("verifyDiff: cannot drop temporary schema for %s on %s: %s", t.Dir, t.Instance, err)
		}
	}

	return nil
}

// logUnsupportedTableDiff provides debug logging to identify why a table is
// considered unsupported. It is "best effort" and simply returns early if it
// encounters any errors.
func (t *Target) logUnsupportedTableDiff(name string) {
	table, err := t.SchemaFromDir.Table(name)
	if err != nil {
		return
	}

	// If the table from the dir is supported (or doesn't exist), obtain the
	// table from the instance instead.
	if table == nil || !table.UnsupportedDDL {
		table, err = t.SchemaFromInstance.Table(name)
		if table == nil || err != nil || !table.UnsupportedDDL {
			return
		}
	}

	diff := difflib.UnifiedDiff{
		A:        difflib.SplitLines(table.GeneratedCreateStatement()),
		B:        difflib.SplitLines(table.CreateStatement()),
		FromFile: "Skeema-expected",
		ToFile:   "MySQL-actual",
		Context:  0,
	}
	diffText, err := difflib.GetUnifiedDiffString(diff)
	if err == nil {
		for _, line := range strings.Split(diffText, "\n") {
			if len(line) > 0 {
				log.Debug(line)
			}
		}
	}
}

func (t *Target) lockTempSchema(maxWait time.Duration) (*sql.Tx, error) {
	db, err := t.Instance.Connect("", "")
	if err != nil {
		return nil, err
	}
	tx, err := db.Begin()
	if err != nil {
		return nil, err
	}

	var getLockResult int
	lockName := fmt.Sprintf("skeema.%s", t.Dir.Config.Get("temp-schema"))
	start := time.Now()

	for time.Since(start) < maxWait {
		// Only using a timeout of 1 sec on each query to avoid potential issues with
		// query killers, spurious slow query logging, etc
		err := tx.QueryRow("SELECT GET_LOCK(?, 1)", lockName).Scan(&getLockResult)
		if err == nil && getLockResult == 1 {
			return tx, nil
		}
	}
	return nil, errors.New("Unable to acquire lock")
}

func (t *Target) unlockTempSchema(tx *sql.Tx) error {
	lockName := fmt.Sprintf("skeema.%s", t.Dir.Config.Get("temp-schema"))
	var releaseLockResult int
	err := tx.QueryRow("SELECT RELEASE_LOCK(?)", lockName).Scan(&releaseLockResult)
	if err != nil || releaseLockResult != 1 {
		return errors.New("Failed to release lock, or connection holding lock already dropped")
	}
	return tx.Rollback()
}
