// Package workspace provides functions for interacting with a temporary MySQL
// schema. It manages creating a schema on a desired location (either an
// existing MySQL instance, or a dynamically-controlled Docker instance),
// running SQL DDL or DML, introspecting the resulting schema, and cleaning
// up the schema when it is no longer needed.
package workspace

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/VividCortex/mysqlerr"
	"github.com/jmoiron/sqlx"
	"github.com/nozzle/throttler"
	log "github.com/sirupsen/logrus"
	"github.com/skeema/mybase"
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

	// IntrospectSchema returns a *tengo.Schema representing the current state
	// of the workspace schema.
	IntrospectSchema() (*tengo.Schema, error)

	// Cleanup cleans up the workspace, leaving it in a state where it could be
	// re-used/re-initialized as needed. Repeated calls to Cleanup() may error.
	// The arg may be nil, and/or the implementation may ignore the arg; it is
	// supplied to optionally improve performance where relevant.
	Cleanup(schema *tengo.Schema) error
}

// Type represents a kind of workspace to use.
type Type int

// Constants enumerating different types of workspaces
const (
	TypeTempSchema  Type = iota // A temporary schema on a real pre-supplied Instance
	TypeLocalDocker             // A schema on an ephemeral Docker container on localhost
	TypePrefab                  // A pre-supplied Workspace, possibly from another package
)

// CleanupAction represents how to clean up a workspace.
type CleanupAction int

// Constants enumerating different cleanup actions. These may affect the
// behavior of Workspace.Cleanup() and/or Shutdown().
const (
	// CleanupActionNone means to perform no special cleanup
	CleanupActionNone CleanupAction = iota

	// CleanupActionDrop means to drop the schema in Workspace.Cleanup(). Only
	// used with TypeTempSchema.
	CleanupActionDrop

	// CleanupActionStop means to stop the MySQL instance container in Shutdown().
	// Only used with TypeLocalDocker.
	CleanupActionStop

	// CleanupActionDestroy means to destroy the MySQL instance container in
	// Shutdown(). Only used with TypeLocalDocker.
	CleanupActionDestroy
)

// Options represent different parameters controlling the workspace that is
// used. Some options are specific to a Type.
type Options struct {
	Type                Type
	CleanupAction       CleanupAction
	Instance            *tengo.Instance // only TypeTempSchema
	Flavor              tengo.Flavor    // only TypeLocalDocker
	ContainerName       string          // only TypeLocalDocker
	SchemaName          string
	DefaultCharacterSet string
	DefaultCollation    string
	DefaultConnParams   string    // only TypeLocalDocker
	RootPassword        string    // only TypeLocalDocker
	PrefabWorkspace     Workspace // only TypePrefab
	LockWaitTimeout     time.Duration
	Concurrency         int
	SkipBinlog          bool
}

// New returns a pointer to a ready-to-use Workspace, using the configuration
// specified in opts.
func New(opts Options) (Workspace, error) {
	switch opts.Type {
	case TypeTempSchema:
		return NewTempSchema(opts)
	case TypeLocalDocker:
		return NewLocalDocker(opts)
	case TypePrefab:
		return opts.PrefabWorkspace, nil
	}
	return nil, fmt.Errorf("Unsupported workspace type %v", opts.Type)
}

// OptionsForDir returns Options based on the configuration in an fs.Dir.
// A non-nil instance should be supplied, unless the caller already knows the
// workspace won't be temp-schema based.
// This method relies on option definitions from AddCommandOptions(), as well
// as the "flavor" option from util.AddGlobalOptions().
func OptionsForDir(dir *fs.Dir, instance *tengo.Instance) (Options, error) {
	requestedType, err := dir.Config.GetEnum("workspace", "temp-schema", "docker")
	if err != nil {
		return Options{}, err
	}
	opts := Options{
		CleanupAction:   CleanupActionNone,
		SchemaName:      dir.Config.Get("temp-schema"),
		LockWaitTimeout: 30 * time.Second,
		Concurrency:     10,
	}
	if requestedType == "docker" {
		opts.Type = TypeLocalDocker
		opts.Flavor = tengo.NewFlavor(dir.Config.Get("flavor"))
		opts.SkipBinlog = true
		if !opts.Flavor.Known() && instance != nil {
			opts.Flavor = instance.Flavor().Family()
		}
		opts.ContainerName = "skeema-" + tengo.ContainerNameForImage(opts.Flavor.String())
		if cleanup, err := dir.Config.GetEnum("docker-cleanup", "none", "stop", "destroy"); err != nil {
			return Options{}, err
		} else if cleanup == "stop" {
			opts.CleanupAction = CleanupActionStop
		} else if cleanup == "destroy" {
			opts.CleanupAction = CleanupActionDestroy
		}
		if opts.DefaultConnParams, err = dir.InstanceDefaultParams(); err != nil {
			return Options{}, err
		}
	} else {
		opts.Type = TypeTempSchema
		opts.Instance = instance
		if !dir.Config.GetBool("reuse-temp-schema") {
			opts.CleanupAction = CleanupActionDrop
		}
		if concurrency, err := dir.Config.GetInt("temp-schema-threads"); err != nil {
			return Options{}, err
		} else if concurrency < 1 {
			return Options{}, errors.New("temp-schema-threads cannot be less than 1")
		} else {
			opts.Concurrency = concurrency
		}
		binlogEnum, err := dir.Config.GetEnum("temp-schema-binlog", "on", "off", "auto")
		if err != nil {
			return Options{}, err
		}
		opts.SkipBinlog = (binlogEnum == "off" || (binlogEnum == "auto" && instance != nil && instance.CanSkipBinlog()))

		// Note: no support for opts.DefaultConnParams for temp-schema because the
		// supplied instance already has default params
	}
	return opts, nil
}

// AddCommandOptions adds workspace-related option definitions to the supplied
// mybase.Command.
func AddCommandOptions(cmd *mybase.Command) {
	cmd.AddOptions("workspace",
		mybase.StringOption("temp-schema", 't', "_skeema_tmp", "Name of temporary schema for intermediate operations, created and dropped each run"),
		mybase.StringOption("temp-schema-binlog", 0, "auto", `Controls whether temp schema DDL operations are replicated (valid values: "on", "off", "auto")`),
		mybase.StringOption("temp-schema-threads", 0, "5", "Max number of concurrent CREATE/DROP with workspace=temp-schema"),
		mybase.StringOption("workspace", 'w', "temp-schema", `Specifies where to run intermediate operations (valid values: "temp-schema", "docker")`),
		mybase.StringOption("docker-cleanup", 0, "none", `With --workspace=docker, specifies how to clean up containers (valid values: "none", "stop", "destroy")`),
		mybase.BoolOption("reuse-temp-schema", 0, false, "Do not drop temp-schema when done").Hidden(), // DEPRECATED -- hidden for this reason
	)
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
// fs.Statement in a Workspace.
type StatementError struct {
	*fs.Statement
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

// Schema captures the result of executing the SQL from an fs.LogicalSchema
// in a workspace, and then introspecting the resulting schema. It wraps the
// introspected tengo.Schema alongside the original fs.LogicalSchema and any
// SQL errors that occurred.
type Schema struct {
	*tengo.Schema
	LogicalSchema *fs.LogicalSchema
	Failures      []*StatementError
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
func ExecLogicalSchema(logicalSchema *fs.LogicalSchema, opts Options) (wsSchema *Schema, fatalErr error) {
	if logicalSchema.CharSet != "" {
		opts.DefaultCharacterSet = logicalSchema.CharSet
	}
	if logicalSchema.Collation != "" {
		opts.DefaultCollation = logicalSchema.Collation
	}
	var ws Workspace
	ws, fatalErr = New(opts)
	if fatalErr != nil {
		return
	}
	wsSchema = &Schema{
		LogicalSchema: logicalSchema,
		Failures:      []*StatementError{},
	}

	defer func() {
		if cleanupErr := ws.Cleanup(wsSchema.Schema); fatalErr == nil {
			fatalErr = cleanupErr
		}
	}()

	// Run CREATEs in parallel
	th := throttler.New(opts.Concurrency, len(logicalSchema.Creates))
	for _, stmt := range logicalSchema.Creates {
		db, err := ws.ConnectionPool(paramsForStatement(stmt, opts))
		if err != nil {
			fatalErr = fmt.Errorf("Cannot connect to workspace: %s", err)
			return
		}
		go func(db *sqlx.DB, statement *fs.Statement) {
			_, err := db.Exec(statement.Body())
			if err != nil {
				err = wrapFailure(statement, err)
			}
			th.Done(err)
		}(db, stmt)
		th.Throttle()
	}

	// Examine statement errors. If any deadlocks occurred, retry them
	// sequentially, since some deadlocks are expected from concurrent CREATEs in
	// MySQL 8+ if FKs are present.
	sequentialStatements := []*fs.Statement{}
	for _, err := range th.Errs() {
		stmterr := err.(*StatementError)
		if tengo.IsDatabaseError(stmterr.Err, mysqlerr.ER_LOCK_DEADLOCK) {
			sequentialStatements = append(sequentialStatements, stmterr.Statement)
		} else {
			wsSchema.Failures = append(wsSchema.Failures, stmterr)
		}
	}

	// Run ALTERs sequentially, since foreign key manipulations don't play
	// nice with concurrency.
	sequentialStatements = append(sequentialStatements, logicalSchema.Alters...)

	for _, statement := range sequentialStatements {
		db, connErr := ws.ConnectionPool(paramsForStatement(statement, opts))
		if connErr != nil {
			fatalErr = fmt.Errorf("Cannot connect to workspace: %s", connErr)
			return
		}
		if _, err := db.Exec(statement.Body()); err != nil {
			wsSchema.Failures = append(wsSchema.Failures, wrapFailure(statement, err))
		}
	}

	wsSchema.Schema, fatalErr = ws.IntrospectSchema()
	return
}

// paramsForStatement returns the session settings for executing the supplied
// statement in a workspace.
func paramsForStatement(statement *fs.Statement, opts Options) string {
	var params []string

	// Disable binlogging if requested
	if opts.SkipBinlog {
		params = append(params, "sql_log_bin=0")
	}

	// Disable FK checks when operating on tables, since otherwise DDL would
	// need to be ordered to resolve dependencies, and circular references would
	// be highly problematic
	if statement.ObjectType == tengo.ObjectTypeTable {
		params = append(params, "foreign_key_checks=0")
	}

	return strings.Join(params, "&")
}

func wrapFailure(statement *fs.Statement, err error) *StatementError {
	stmtErr := &StatementError{
		Statement: statement,
	}
	if tengo.IsSyntaxError(err) {
		stmtErr.Err = fmt.Errorf("SQL syntax error: %s", err)
	} else if tengo.IsDatabaseError(err, mysqlerr.ER_LOCK_DEADLOCK) {
		stmtErr.Err = err // Need to maintain original type
	} else {
		stmtErr.Err = fmt.Errorf("Error executing DDL in workspace: %s", err)
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
