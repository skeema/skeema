// Package workspace provides functions for interacting with a temporary MySQL
// schema. It manages creating a schema on a desired location (either an
// existing MySQL instance, or a dynamically-controlled Docker instance),
// running SQL DDL or DML, introspecting the resulting schema, and cleaning
// up the schema when it is no longer needed. Callers primarily interact with
// workspaces using the ExecLogicalSchema function.
package workspace

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	log "github.com/sirupsen/logrus"
	"github.com/skeema/skeema/internal/fs"
	"github.com/skeema/skeema/internal/tengo"
)

// Workspace represents a "scratch space" for DDL operations and schema
// introspection.
type Workspace interface {
	// ConnectionPool returns a *sqlx.DB representing a connection pool for
	// interacting with the workspace. The pool should already be using the
	// correct default database for interacting with the workspace schema.
	ConnectionPool(params string) (*sqlx.DB, error)

	// IntrospectSchema returns a struct containing the current state of the
	// workspace schema, along with other metadata about the workspace's
	// execution environment.
	IntrospectSchema() (IntrospectionResult, error)

	// Cleanup cleans up the workspace, leaving it in a state where it could be
	// re-used/re-initialized as needed. Repeated calls to Cleanup() may error.
	// The arg may be nil, and/or the implementation may ignore the arg; it is
	// supplied to optionally improve performance where relevant.
	Cleanup(schema *tengo.Schema) error
}

// IntrospectionResult bundles a tengo.Schema with metadata from the workspace's
// underlying tengo.Instance.
type IntrospectionResult struct {
	Schema  *tengo.Schema
	Flavor  tengo.Flavor
	SQLMode string
	Info    string // human-readable text describing workspace config
}

// New returns a pointer to a ready-to-use Workspace, using the configuration
// specified in opts.
func New(opts Options) (Workspace, error) {
	switch opts.Type {
	case TypeTempSchema:
		return NewTempSchema(opts)
	case TypeLocalDocker:
		return NewLocalDocker(opts)
	}
	return nil, fmt.Errorf("Unsupported workspace type %v", opts.Type)
}

// ShutdownFunc is a function that manages final cleanup of a Workspace upon
// completion of a request or process. It may optionally use args, passed
// through by Shutdown(), to determine whether or not a Workspace needs to be
// cleaned up. It should return true if cleanup occurred (meaning that the
// ShutdownFunc should be de-registered from future calls to Shutdown()), or
// false otherwise.
type ShutdownFunc func(...interface{}) bool

var shutdownFuncs []ShutdownFunc
var shutdownFuncMutex sync.Mutex

// Shutdown performs any necessary cleanup operations prior to the program
// exiting. For example, if containers need to be stopped or destroyed, it is
// most efficient to do so at program exit, rather than needlessly doing so
// for each workspace invocation.
// It is recommended that programs importing this package call Shutdown as a
// deferred function in main().
func Shutdown(args ...interface{}) {
	shutdownFuncMutex.Lock()
	defer shutdownFuncMutex.Unlock()
	retainedFuncs := make([]ShutdownFunc, 0, len(shutdownFuncs))
	for _, f := range shutdownFuncs {
		if deregister := f(args...); !deregister {
			retainedFuncs = append(retainedFuncs, f)
		}
	}
	shutdownFuncs = retainedFuncs
}

// RegisterShutdownFunc registers a function to be executed by Shutdown.
// Structs satisfying the Workspace interface may optionally use this function
// to track actions to perform at shutdown time, such as stopping or destroying
// containers.
func RegisterShutdownFunc(f ShutdownFunc) {
	shutdownFuncMutex.Lock()
	defer shutdownFuncMutex.Unlock()
	shutdownFuncs = append(shutdownFuncs, f)
}

// StatementError represents a problem that occurred when executing a specific
// tengo.Statement in a Workspace.
type StatementError struct {
	*tengo.Statement
	Err error
}

// Error satisfies the builtin error interface.
func (se *StatementError) Error() string {
	loc := se.Location()
	if loc == "" {
		return fmt.Sprintf("%s [Full SQL: %s]", se.Err.Error(), se.Body())
	}
	return fmt.Sprintf("%s: %s", loc, se.Err.Error())
}

func (se *StatementError) String() string {
	return se.Error()
}

// ErrorNumber returns the server error code corresponding to Err if it is a
// driver-provided error. Otherwise, it returns 0.
func (se *StatementError) ErrorNumber() uint16 {
	var merr *mysql.MySQLError
	if errors.As(se.Err, &merr) {
		return merr.Number
	}
	return 0
}

// Timers describes execution time for different stages of ExecLogicalSchema's
// operation.
type Timers struct {
	Init       time.Duration // Create or start container (if applicable), create database
	Populate   time.Duration // Create objects (tables, procs, etc)
	Introspect time.Duration // Introspect objects
	Cleanup    time.Duration // Drop tables (separately for breaks in lock time), drop database
}

func (timers Timers) Total() time.Duration {
	// Pre-round the individual durations so that the numbers in String() add up
	ms := timers.Init.Milliseconds() + timers.Populate.Milliseconds() + timers.Introspect.Milliseconds() + timers.Cleanup.Milliseconds()
	return time.Duration(ms) * time.Millisecond
}

func (timers Timers) String() string {
	return fmt.Sprintf(
		"Init=%dms Populate=%dms Introspect=%dms Cleanup=%dms -> Total=%s",
		timers.Init.Milliseconds(),
		timers.Populate.Milliseconds(),
		timers.Introspect.Milliseconds(),
		timers.Cleanup.Milliseconds(),
		timers.Total().Round(time.Millisecond),
	)
}

// Schema captures the result of executing the SQL from an fs.LogicalSchema
// in a workspace, and then introspecting the resulting schema. It wraps the
// introspected tengo.Schema, alongside the original fs.LogicalSchema, the
// Flavor of the workspace used, and any SQL errors that occurred.
type Schema struct {
	*tengo.Schema
	LogicalSchema *fs.LogicalSchema
	Flavor        tengo.Flavor
	Failures      []*StatementError
	Info          string // human-readable info on execution environment used for workspace
	Timers        Timers // performance timing for each stage of workspace execution
}

// FailedKeys returns a slice of tengo.ObjectKey values corresponding to
// statements that had SQL errors when executed.
func (wsSchema *Schema) FailedKeys() (result []tengo.ObjectKey) {
	for _, statementError := range wsSchema.Failures {
		result = append(result, statementError.ObjectKey())
	}
	return result
}

// ExecLogicalSchema converts a LogicalSchema into a workspace.Schema. It
// obtains a Workspace, executes the creation DDL contained in a LogicalSchema
// there, introspects it into a *tengo.Schema, cleans up the Workspace, and then
// returns a value containing the introspected schema and any SQL errors (e.g.
// tables that could not be created). Such individual statement errors are not
// fatal and are not included in the error return value. The error return value
// only represents fatal errors that prevented the entire process.
// Note that if opts.NameCaseMode > tengo.NameCaseAsIs, logicalSchema may be
// modified in-place to force some identifiers to lowercase.
func ExecLogicalSchema(logicalSchema *fs.LogicalSchema, opts Options) (_ *Schema, retErr error) {
	if logicalSchema.CharSet != "" {
		opts.DefaultCharacterSet = logicalSchema.CharSet
	}
	if logicalSchema.Collation != "" {
		opts.DefaultCollation = logicalSchema.Collation
	}
	if opts.NameCaseMode > tengo.NameCaseAsIs {
		if err := logicalSchema.LowerCaseNames(opts.NameCaseMode); err != nil {
			return nil, err
		}
	}

	timerStart := time.Now()
	ws, err := New(opts)
	if err != nil {
		return nil, err
	}

	// ExecLogicalSchema names its error return so that a deferred func can check
	// if an error occurred, but otherwise intentionally does not use named return
	// variables, and instead declares new local vars for all other usage. This is
	// to avoid mistakes with variable shadowing, nil pointer panics, etc which are
	// common when dealing with named returns and deferred anonymous functions.
	wsSchema := &Schema{
		LogicalSchema: logicalSchema,
		Failures:      []*StatementError{},
	}
	defer func() {
		cleanupTimerStart := time.Now()
		cleanupErr := ws.Cleanup(wsSchema.Schema)
		wsSchema.Timers.Cleanup = time.Since(cleanupTimerStart)
		// We only care about a cleanup error if the original returned error was nil
		if retErr == nil && cleanupErr != nil {
			retErr = cleanupErr
		}
	}()

	params := "foreign_key_checks=0"
	if opts.SkipBinlog {
		params += "&sql_log_bin=0"
	}
	db, err := ws.ConnectionPool(params)
	if err != nil {
		return nil, fmt.Errorf("Cannot connect to workspace: %w", err)
	}
	wsSchema.Timers.Init = time.Since(timerStart)

	// Run CREATEs in parallel, bounded by opts.Concurrency
	timerStart = time.Now()
	creates := make(chan *tengo.Statement, opts.Concurrency)
	errs := make(chan error, opts.Concurrency)
	go func() {
		for _, stmt := range logicalSchema.Creates {
			creates <- stmt
		}
		close(creates)
	}()
	for n := 0; n < len(logicalSchema.Creates) && n < opts.Concurrency; n++ {
		go func() {
			for stmt := range creates {
				_, err := db.Exec(stmt.Body())
				if err != nil {
					err = wrapFailure(stmt, err)
				}
				errs <- err
			}
		}()
	}

	// Examine statement errors. If any deadlocks occurred, retry them
	// sequentially, since some deadlocks are expected from concurrent CREATEs in
	// MySQL 8+ if FKs are present. Ditto with metadata lock wait timeouts.
	// Also retry errors from CREATE TABLE...LIKE being run out-of-order (only once
	// though; nested chains of CREATE TABLE...LIKE are unsupported)
	sequentialStatements := []*tengo.Statement{}
	for n := 0; n < len(logicalSchema.Creates); n++ {
		if err := <-errs; err != nil {
			stmterr := err.(*StatementError)
			if tengo.IsLockConflictError(stmterr.Err) || tengo.IsObjectNotFoundError(stmterr.Err) {
				sequentialStatements = append(sequentialStatements, stmterr.Statement)
			} else {
				wsSchema.Failures = append(wsSchema.Failures, stmterr)
			}
		}
	}
	close(errs)

	// Run ALTERs sequentially, since foreign key manipulations don't play
	// nice with concurrency.
	sequentialStatements = append(sequentialStatements, logicalSchema.Alters...)

	for _, statement := range sequentialStatements {
		if _, err := db.Exec(statement.Body()); err != nil {
			wsSchema.Failures = append(wsSchema.Failures, wrapFailure(statement, err))
		}
	}
	wsSchema.Timers.Populate = time.Since(timerStart)

	timerStart = time.Now()
	result, err := ws.IntrospectSchema()
	wsSchema.Schema = result.Schema
	wsSchema.Flavor = result.Flavor
	wsSchema.Info = result.Info
	wsSchema.Timers.Introspect = time.Since(timerStart)

	return wsSchema, err
}

func wrapFailure(statement *tengo.Statement, err error) *StatementError {
	stmtErr := &StatementError{
		Statement: statement,
	}
	if tengo.IsSyntaxError(err) {
		stmtErr.Err = fmt.Errorf("SQL syntax error: %w", err)
	} else {
		stmtErr.Err = fmt.Errorf("Error executing DDL in workspace: %w", err)
	}
	return stmtErr
}

// releaseFunc is a function to release a lock obtained by getLock
type releaseFunc func()

func getLock(instance *tengo.Instance, lockName string, maxWait time.Duration) (releaseFunc, error) {
	db, err := instance.CachedConnectionPool("", "")
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
					log.Warnf("%s: Failed to release workspace lock, or lock released early due to connection being dropped: %s [%d]", instance, err, result)
				}
				return
			case <-time.After(750 * time.Millisecond):
				err := lockConn.QueryRowContext(context.Background(), "SELECT 1").Scan(&result)
				if err != nil {
					log.Warnf("%s: Workspace lock released early due to connection being dropped: %s", instance, err)
					return
				}
			}
		}
	}

	var getLockResult, attempts int
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
		if attempts++; attempts == 3 {
			log.Warnf("Obtaining a workspace lock on %s is taking longer than expected. Some other Skeema process or thread may be holding the lock already. This operation will be re-attempted for up to %s total.", instance, maxWait)
		}
	}
	return nil, errors.New("Unable to acquire lock before timeout")
}
