package workspace

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/jmoiron/sqlx"
	"github.com/skeema/skeema/internal/tengo"
)

// TempSchema is a Workspace that exists as a schema that is created on another
// database instance. The schema is cleaned up when done interacting with the
// workspace.
type TempSchema struct {
	schemaName  string
	keepSchema  bool
	concurrency int
	skipBinlog  bool
	inst        *tengo.Instance
	releaseLock releaseFunc
	mdlTimeout  int // metadata lock wait timeout, in seconds; 0 for session default
}

// NewTempSchema creates a temporary schema on the supplied instance and returns
// it.
func NewTempSchema(opts Options) (_ *TempSchema, retErr error) {
	if opts.Instance == nil {
		return nil, errors.New("No instance defined in options")
	}

	// NewTempSchema names its error return so that a deferred func can check if
	// an error occurred, but otherwise intentionally does not use named return
	// variables, and instead declares new local vars for all other usage. This is
	// to avoid mistakes with variable shadowing, nil pointer panics, etc which are
	// common when dealing with named returns and deferred anonymous functions.
	var err error
	ts := &TempSchema{
		schemaName:  opts.SchemaName,
		keepSchema:  opts.CleanupAction == CleanupActionNone,
		inst:        opts.Instance,
		concurrency: opts.Concurrency,
		skipBinlog:  opts.SkipBinlog,
	}

	lockName := fmt.Sprintf("skeema.%s", ts.schemaName)
	if ts.releaseLock, err = getLock(ts.inst, lockName, opts.LockTimeout); err != nil {
		return nil, fmt.Errorf("Unable to lock temp-schema workspace on %s: %s\n"+
			"Usually this means another copy of Skeema is already holding the lock and operating on this database server. If you are certain that your operation will not conflict, try supplying a different name for --temp-schema on the command-line.",
			ts.inst, err)
	}

	// If NewTempSchema errors, don't continue to hold the lock
	defer func() {
		if retErr != nil {
			ts.releaseLock()
		}
	}()

	// MySQL 8 extends foreign key metadata locks to the "parent" side of the FK,
	// which means the TempSchema may not be fully isolated from non-workspace
	// workloads and their own usage of metadata locks. As a result, we must force
	// a low lock_wait_timeout on any TempSchema DDL in MySQL 8.
	if ts.inst.Flavor().Min(tengo.FlavorMySQL80) {
		wantLockWait := 5
		if strings.HasSuffix(os.Args[0], ".test") || strings.HasSuffix(os.Args[0], ".test.exe") {
			wantLockWait = 2 // use lower value in test suites so MDL-related tests aren't super slow
		}
		if ts.inst.LockWaitTimeout() > wantLockWait {
			ts.mdlTimeout = wantLockWait
		}
	}

	createOpts := tengo.SchemaCreationOptions{
		DefaultCharSet:   opts.DefaultCharacterSet,
		DefaultCollation: opts.DefaultCollation,
		SkipBinlog:       opts.SkipBinlog,
	}
	if has, err := ts.inst.HasSchema(ts.schemaName); err != nil {
		return nil, fmt.Errorf("Unable to check for existence of temp schema on %s: %s", ts.inst, err)
	} else if has {
		// Attempt to drop any tables already present in tempSchema, but fail if
		// any of them actually have 1 or more rows
		dropOpts := ts.bulkDropOptions()
		if err := ts.inst.DropTablesInSchema(ts.schemaName, dropOpts); err != nil {
			return nil, fmt.Errorf("Cannot drop existing temp schema tables on %s: %s", ts.inst, err)
		}
		if err := ts.inst.DropRoutinesInSchema(ts.schemaName, dropOpts); err != nil {
			return nil, fmt.Errorf("Cannot drop existing temp schema routines on %s: %s", ts.inst, err)
		}
		if err := ts.inst.AlterSchema(ts.schemaName, createOpts); err != nil {
			return nil, fmt.Errorf("Cannot alter existing temp schema charset and collation on %s: %s", ts.inst, err)
		}
	} else if _, err := ts.inst.CreateSchema(ts.schemaName, createOpts); err != nil {
		return nil, fmt.Errorf("Cannot create temporary schema on %s: %s", ts.inst, err)
	}
	return ts, nil
}

func (ts *TempSchema) bulkDropOptions() tengo.BulkDropOptions {
	return tengo.BulkDropOptions{
		MaxConcurrency:  ts.concurrency,
		OnlyIfEmpty:     true,
		SkipBinlog:      ts.skipBinlog,
		PartitionsFirst: true,
		LockWaitTimeout: ts.mdlTimeout,
	}
}

// ConnectionPool returns a connection pool (*sqlx.DB) to the temporary
// workspace schema, using the supplied connection params (which may be blank).
func (ts *TempSchema) ConnectionPool(params string) (*sqlx.DB, error) {
	if ts.mdlTimeout > 0 && !strings.Contains(params, "lock_wait_timeout") {
		params = strings.TrimLeft(fmt.Sprintf("%s&lock_wait_timeout=%d", params, ts.mdlTimeout), "&")
	}
	return ts.inst.CachedConnectionPool(ts.schemaName, params)
}

// IntrospectSchema introspects and returns the temporary workspace schema.
func (ts *TempSchema) IntrospectSchema() (*tengo.Schema, error) {
	return ts.inst.Schema(ts.schemaName)
}

// Cleanup either drops the temporary schema (if not using reuse-temp-schema)
// or just drops all tables in the schema (if using reuse-temp-schema). If any
// tables have any rows in the temp schema, the cleanup aborts and an error is
// returned.
func (ts *TempSchema) Cleanup(schema *tengo.Schema) error {
	if ts.releaseLock == nil {
		return errors.New("Cleanup() called multiple times on same TempSchema")
	}
	defer func() {
		ts.releaseLock()
		ts.releaseLock = nil
	}()

	dropOpts := ts.bulkDropOptions()
	dropOpts.Schema = schema // may be nil, not a problem

	if ts.keepSchema {
		if err := ts.inst.DropTablesInSchema(ts.schemaName, dropOpts); err != nil {
			return fmt.Errorf("Cannot drop tables in temporary schema on %s: %s", ts.inst, err)
		}
		if err := ts.inst.DropRoutinesInSchema(ts.schemaName, dropOpts); err != nil {
			return fmt.Errorf("Cannot drop routines in temporary schema on %s: %s", ts.inst, err)
		}
	} else if err := ts.inst.DropSchema(ts.schemaName, dropOpts); err != nil {
		return fmt.Errorf("Cannot drop temporary schema on %s: %s", ts.inst, err)
	}
	return nil
}
