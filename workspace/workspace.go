package workspace

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
	log "github.com/sirupsen/logrus"
	"github.com/skeema/skeema/fs"
	"github.com/skeema/tengo"
)

// Workspace represents a "scratch space" for DDL operations and schema
// introspection.
type Workspace interface {
	// ConnectionPool returns a *sqlx.DB representing a connection pool for
	// interacting with the workspace. The pool should already be using the
	// correct default database for interacting with the workspace schema.
	ConnectionPool(params string) (*sqlx.DB, error)

	// IntrospectSchema returns a *tengo.Schema representing the current state
	// of the workspace schema.
	IntrospectSchema() (*tengo.Schema, error)

	// Cleanup cleans up the workspace, leaving it in a state where it could be
	// re-used/re-initialized as needed. Repeated calls to Cleanup() may error.
	Cleanup() error
}

// Type represents a type of workspace provider to use.
type Type int

// Constants enumerating different types of workspaces
const (
	TypeTempSchema  Type = iota
	TypeLocalDocker      // not implemented yet
)

// Options represent different parameters controlling the workspace that is
// used. Some options are specific to a Type.
type Options struct {
	Type                Type
	Instance            *tengo.Instance // only TypeTempSchema
	Flavor              tengo.Flavor    // only TypeLocalDocker
	SchemaName          string
	KeepSchema          bool // only TypeTempSchema
	DefaultCharacterSet string
	DefaultCollation    string
	LockWaitTimeout     time.Duration
}

// TableError represents a problem that occurred when attempting to create a
// table in a workspace.
type TableError struct {
	TableName string
	Err       error
}

// Error satisfies the builtin error interface.
func (te TableError) Error() string {
	return te.Err.Error()
}

// MaterializeIdealSchema converts an IdealSchema to a tengo.Schema. It obtains
// a Workspace, executes the creation DDL contained in an IdealSchema there,
// introspects it into a *tengo.Schema, cleans up the Workspace, and then
// returns the introspected schema. SQL errors (tables that could not be
// created) are non-fatal, and are returned in the second return value. The
// third return value represents fatal errors only.
func MaterializeIdealSchema(idealSchema *fs.IdealSchema, opts Options) (schema *tengo.Schema, tableErrors []TableError, fatalErr error) {
	statements := make([]string, 0, len(idealSchema.CreateTables))
	tableNames := make([]string, 0, len(idealSchema.CreateTables))
	for name, stmt := range idealSchema.CreateTables {
		tableNames = append(tableNames, name)
		statements = append(statements, stmt.Text)
	}

	var stmtErrors []error
	schema, stmtErrors, fatalErr = statementsToSchemaWithErrs(statements, opts, 10)
	if fatalErr != nil {
		return
	}
	schema.Name = idealSchema.Name
	for n, err := range stmtErrors {
		if err != nil {
			tableErrors = append(tableErrors, TableError{
				TableName: tableNames[n],
				Err:       err,
			})
		}
	}
	return
}

// StatementsToSchema serially executes the supplied statements in a temporary
// workspace, and then returns the introspected schema. Errors are not tracked
// on a per-statement basis; the returned error value will be nil only if all
// statements succeeded. If multiple statements returned an error, the return
// value here will be from the first such erroring statement.
func StatementsToSchema(statements []string, opts Options) (*tengo.Schema, error) {
	schema, stmtErrors, err := statementsToSchemaWithErrs(statements, opts, 1)
	if err != nil {
		return nil, err
	}
	for _, err := range stmtErrors {
		if err != nil {
			return nil, err
		}
	}
	return schema, nil
}

func statementsToSchemaWithErrs(statements []string, opts Options, concurrency int) (schema *tengo.Schema, stmtErrors []error, err error) {
	var ws Workspace

	switch opts.Type {
	case TypeTempSchema:
		ws, err = NewTempSchema(opts)
	default:
		ws, err = nil, fmt.Errorf("Unsupported workspace type %v", opts.Type)
	}
	if err != nil {
		return
	}
	defer func() {
		if cleanupErr := ws.Cleanup(); err == nil {
			err = cleanupErr
		}
	}()

	db, err := ws.ConnectionPool("")
	if err != nil {
		err = fmt.Errorf("Cannot connect to workspace: %s", err)
		return
	}

	stmtErrors = make([]error, len(statements))
	if concurrency <= 1 {
		for n, stmt := range statements {
			stmtErrors[n] = execStatement(db, stmt)
		}
	} else {
		defer db.SetMaxOpenConns(0)
		db.SetMaxOpenConns(concurrency)
		var wg sync.WaitGroup
		for n, stmt := range statements {
			wg.Add(1)
			go func(n int, stmt string) {
				defer wg.Done()
				stmtErrors[n] = execStatement(db, stmt)
			}(n, stmt)
		}
		wg.Wait()
	}

	schema, err = ws.IntrospectSchema()
	return
}

func execStatement(db *sqlx.DB, stmt string) error {
	_, err := db.Exec(stmt)
	if tengo.IsSyntaxError(err) {
		return fmt.Errorf("SQL syntax error: %s", err)
	} else if err != nil {
		return fmt.Errorf("Error executing DDL: %s", err)
	}
	return nil
}

// releaseFunc is a function to release a lock obtained by getLock
type releaseFunc func()

func getLock(instance *tengo.Instance, lockName string, maxWait time.Duration) (releaseFunc, error) {
	db, err := instance.Connect("", "")
	if err != nil {
		return nil, err
	}
	lockConn, err := db.Conn(context.Background())
	if err != nil {
		return nil, err
	}

	done := make(chan struct{})
	release := func() {
		close(done)
	}
	connMaintainer := func() {
		var result int
		defer lockConn.Close()
		for {
			select {
			case <-done:
				err := lockConn.QueryRowContext(context.Background(), "SELECT RELEASE_LOCK(?)", lockName).Scan(&result)
				if err != nil || result != 1 {
					log.Warnf("%s: Failed to release lock, or lock released early due to connection being dropped: %s [%d]", instance, err, result)
				}
				return
			case <-time.After(750 * time.Millisecond):
				err := lockConn.QueryRowContext(context.Background(), "SELECT 1").Scan(&result)
				if err != nil {
					log.Warnf("%s: Lock released early due to connection being dropped: %s", instance, err)
					return
				}
			}
		}
	}

	var getLockResult int
	start := time.Now()
	for time.Since(start) < maxWait {
		// Only using a timeout of 1 sec on each query to avoid potential issues with
		// query killers, spurious slow query logging, etc
		err := lockConn.QueryRowContext(context.Background(), "SELECT GET_LOCK(?, 1)", lockName).Scan(&getLockResult)
		if err == nil && getLockResult == 1 {
			// Launch a goroutine to keep the connection active, and release the lock
			// once the ReleaseFunc is called
			go connMaintainer()
			return release, nil
		}
	}
	return nil, errors.New("Unable to acquire lock")
}
