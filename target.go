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

func generateTargetsForDir(dir *Dir, targets chan Target, expandInstances, expandSchemas bool) {
	// Generate targets if this dir's .skeema file defines a schema (for current
	// environment section), and the dir's config hierarchy defines a host
	// somewhere (here, or a parent dir)
	if dir.Config.Changed("host") && dir.HasSchema() {
		var dirSchema *tengo.Schema

		// TODO: support multiple instances / service discovery lookup per dir if
		// expandInstances is true
		instances := make([]*tengo.Instance, 0, 1)
		onlyInstance, err := dir.FirstInstance()
		if onlyInstance == nil && err == nil {
			err = fmt.Errorf("No instance defined for %s", dir)
		}
		if err == nil {
			instances = append(instances, onlyInstance)
		} else {
			targets <- Target{
				Dir: dir,
				Err: err,
			}
		}

		for _, inst := range instances {
			// TODO: support multiple schemas / service discovery lookup per instance if
			// expandSchemas is true
			for _, schemaName := range []string{dir.Config.Get("schema")} {
				t := Target{
					Instance: inst,
					Dir:      dir,
				}
				if dirSchema == nil {
					t.obtainSchemaFromDir()
					dirSchema = t.SchemaFromDir
				} else {
					// Can re-use the same value even if expanding instances and/or schemas,
					// since the same dir (and therefore same dir schema) is used for all
					t.SchemaFromDir = dirSchema
				}
				if t.Err == nil {
					t.SchemaFromDir.Name = schemaName // "fix" temp schema name to match correct corresponding schema
					t.SchemaFromInstance, t.Err = inst.Schema(schemaName)
				}
				targets <- t
			}
		}
	} else if !dir.Config.Changed("host") && dir.HasSchema() {
		// If we have a schema defined but no host, display a warning
		log.Warnf("Skipping %s: no host defined for environment \"%s\"\n", dir, dir.section)
	} else if f, err := dir.OptionFile(); err == nil && f.SomeSectionHasOption("schema") {
		// If we don't have a schema defined, but we would if some other environment
		// had been selected, display a warning
		log.Warnf("Skipping %s: no schema defined for environment \"%s\"\n", dir, dir.section)
	}

	subdirs, err := dir.Subdirs()
	if err != nil {
		targets <- Target{
			Dir: dir,
			Err: err,
		}
	} else {
		for _, subdir := range subdirs {
			// Don't iterate into hidden dirs, since version control software may store
			// files in there with names matching real things we care about (*.sql,
			// .skeema, etc)
			if subdir.BaseName()[0] != '.' {
				generateTargetsForDir(subdir, targets, expandInstances, expandSchemas)
			}
		}
	}
}

// Done is currently a no-op. Once Skeema supports expandInstances (looking up
// multiple instances for one dir, via service discovery or shelling to an
// external bin), Target generation will be threadsafe and support limiting the
// number of goroutines working on an instance at a time. Callers doing so will
// need to call Done() on a target once they are finished with it, so that the
// concurrent user count for the instance can be decremented properly.
func (t *Target) Done() {
}

// HasErrors returns true if the Target encountered a fatal error OR any errors
// in individual *.SQL files while performing temp schema operations. It also
// returns the first error.
func (t *Target) HasErrors() (bool, error) {
	if t.Err != nil {
		return true, t.Err
	}
	if len(t.SQLFileErrors) > 0 {
		for _, sf := range t.SQLFileErrors {
			return true, sf.Error
		}
	}
	return false, nil
}

func (t *Target) obtainSchemaFromDir() {
	tempSchemaName := t.Dir.Config.Get("temp-schema")
	sqlFiles, err := t.Dir.SQLFiles()
	if err != nil {
		t.Err = fmt.Errorf("obtainSchemaFromDir: unable to list SQL files in %s: %s", t.Dir, err)
		return
	}

	// TODO: want to skip binlogging for all temp schema actions, if super priv available
	var tx *sql.Tx
	if tx, err = t.lockTempSchema(30 * time.Second); err != nil {
		t.Err = fmt.Errorf("obtainSchemaFromDir: %s", err)
		return
	}
	defer func() {
		unlockErr := t.unlockTempSchema(tx)
		if unlockErr != nil && t.Err == nil {
			t.Err = fmt.Errorf("obtainSchemaFromDir: %s", unlockErr)
		}
	}()

	tempSchema, err := t.Instance.Schema(tempSchemaName)
	if err != nil {
		t.Err = fmt.Errorf("obtainSchemaFromDir: unable to obtain temp schema for %s on %s: %s", t.Dir, t.Instance, err)
		return
	}
	if tempSchema != nil {
		// Attempt to drop any tables already present in tempSchema, but fail if
		// any of them actually have 1 or more rows
		if err := t.Instance.DropTablesInSchema(tempSchema, true); err != nil {
			t.Err = fmt.Errorf("obtainSchemaFromDir: cannot drop existing tables for %s on %s: %s", t.Dir, t.Instance, err)
			return
		}
	} else {
		tempSchema, err = t.Instance.CreateSchema(tempSchemaName)
		if err != nil {
			t.Err = fmt.Errorf("obtainSchemaFromDir: cannot create temporary schema for %s on %s: %s", t.Dir, t.Instance, err)
			return
		}
	}

	db, err := t.Instance.Connect(tempSchemaName, "")
	if err != nil {
		t.Err = fmt.Errorf("obtainSchemaFromDir: cannot connect to %s: %s", t.Instance, err)
		return
	}
	if t.SQLFileErrors == nil {
		t.SQLFileErrors = make(map[string]*SQLFile)
	}
	if t.SQLFileWarnings == nil {
		t.SQLFileWarnings = make([]error, 0)
	}
	for _, sf := range sqlFiles {
		if sf.Error != nil {
			t.SQLFileErrors[sf.Path()] = sf
			continue
		}
		for _, warning := range sf.Warnings {
			t.SQLFileWarnings = append(t.SQLFileWarnings, warning)
		}
		_, err := db.Exec(sf.Contents)
		if err != nil {
			if tengo.IsSyntaxError(err) {
				sf.Error = fmt.Errorf("%s: SQL syntax error: %s", sf.Path(), err)
			} else {
				sf.Error = fmt.Errorf("%s: Error executing DDL: %s", sf.Path(), err)
			}
			t.SQLFileErrors[sf.Path()] = sf
		}
	}

	if t.SchemaFromDir, err = tempSchema.CachedCopy(); err != nil {
		t.Err = fmt.Errorf("obtainSchemaFromDir: unable to clone temporary schema for %s on %s: %s", t.Dir, t.Instance, err)
	}

	if t.Dir.Config.GetBool("reuse-temp-schema") {
		if err := t.Instance.DropTablesInSchema(tempSchema, true); err != nil {
			t.Err = fmt.Errorf("obtainSchemaFromDir: cannot drop tables in temporary schema for %s on %s: %s", t.Dir, t.Instance, err)
		}
	} else {
		if err := t.Instance.DropSchema(tempSchema, true); err != nil {
			t.Err = fmt.Errorf("obtainSchemaFromDir: cannot drop temporary schema for %s on %s: %s", t.Dir, t.Instance, err)
		}
	}
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
