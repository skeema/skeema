package workspace

import (
	"errors"
	"fmt"

	"github.com/jmoiron/sqlx"
	"github.com/skeema/tengo"
)

// TempSchema is a Workspace that exists as a schema that is created on another
// database instance. The schema is cleaned up when done interacting with the
// workspace.
type TempSchema struct {
	schemaName  string
	keepSchema  bool
	inst        *tengo.Instance
	releaseLock releaseFunc
}

// NewTempSchema creates a temporary schema on the supplied instance and returns
// it.
func NewTempSchema(opts Options) (ts *TempSchema, err error) {
	if opts.Instance == nil {
		return nil, errors.New("No instance defined in options")
	}
	ts = &TempSchema{
		schemaName: opts.SchemaName,
		keepSchema: opts.CleanupAction == CleanupActionNone,
		inst:       opts.Instance,
	}

	lockName := fmt.Sprintf("skeema.%s", ts.schemaName)
	if ts.releaseLock, err = getLock(ts.inst, lockName, opts.LockWaitTimeout); err != nil {
		return nil, fmt.Errorf("Unable to lock temporary schema on %s: %s", ts.inst, err)
	}
	// If NewTempSchema errors, don't continue to hold the lock
	defer func() {
		if err != nil {
			ts.releaseLock()
			ts = nil
		}
	}()

	if has, err := ts.inst.HasSchema(ts.schemaName); err != nil {
		return nil, fmt.Errorf("Unable to check for existence of temp schema on %s: %s", ts.inst, err)
	} else if has {
		// Attempt to drop any tables already present in tempSchema, but fail if
		// any of them actually have 1 or more rows
		if err := ts.inst.DropTablesInSchema(ts.schemaName, true); err != nil {
			return ts, fmt.Errorf("Cannot drop existing temp schema tables on %s: %s", ts.inst, err)
		}
	} else {
		_, err = ts.inst.CreateSchema(ts.schemaName, opts.DefaultCharacterSet, opts.DefaultCollation)
		if err != nil {
			return ts, fmt.Errorf("Cannot create temporary schema on %s: %s", ts.inst, err)
		}
	}
	return ts, nil
}

// ConnectionPool returns a connection pool (*sqlx.DB) to the temporary
// workspace schema, using the supplied connection params (which may be blank).
func (ts *TempSchema) ConnectionPool(params string) (*sqlx.DB, error) {
	return ts.inst.Connect(ts.schemaName, params)
}

// IntrospectSchema introspects and returns the temporary workspace schema.
func (ts *TempSchema) IntrospectSchema() (*tengo.Schema, error) {
	return ts.inst.Schema(ts.schemaName)
}

// Cleanup either drops the temporary schema (if not using reuse-temp-schema)
// or just drops all tables in the schema (if using reuse-temp-schema). If any
// tables have any rows in the temp schema, the cleanup aborts and an error is
// returned.
func (ts *TempSchema) Cleanup() error {
	if ts.releaseLock == nil {
		return errors.New("Cleanup() called multiple times on same TempSchema")
	}
	defer func() {
		ts.releaseLock()
		ts.releaseLock = nil
	}()

	if ts.keepSchema {
		if err := ts.inst.DropTablesInSchema(ts.schemaName, true); err != nil {
			return fmt.Errorf("Cannot drop tables in temporary schema on %s: %s", ts.inst, err)
		}
	} else if err := ts.inst.DropSchema(ts.schemaName, true); err != nil {
		return fmt.Errorf("Cannot drop temporary schema on %s: %s", ts.inst, err)
	}
	return nil
}
