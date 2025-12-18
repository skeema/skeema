package tengo

import (
	"context"
	"database/sql"
	"fmt"
	"maps"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"golang.org/x/sync/errgroup"
)

// Instance represents a single database server running on a specific host or address.
type Instance struct {
	BaseDSN         string // DSN ending in trailing slash; i.e. no schema name or params
	Driver          string
	User            string
	Password        string
	Host            string
	Port            int
	SocketPath      string
	defaultParams   map[string]string
	connectionPool  map[string]*sqlx.DB // key is in format "schema?params"
	m               *sync.Mutex         // protects unexported fields for concurrent operations
	flavor          Flavor
	grants          []string
	latency         time.Duration // round-trip latency measured in hydrateVars using trivial query
	waitTimeout     int
	lockWaitTimeout int
	maxUserConns    int
	lowerCaseNames  int
	sqlMode         []string
	ahiEnabled      bool // true if innodb_adaptive_hash_index is enabled
	valid           bool // true if any conn has ever successfully been made yet
}

// NewInstance returns a pointer to a new Instance corresponding to the
// supplied driver and dsn. Currently only "mysql" driver is supported.
// dsn should be formatted according to driver specifications. If it contains
// a schema name, it will be ignored. If it contains any params, they will be
// applied as default params to all connections (in addition to whatever is
// supplied in Connect).
func NewInstance(driver, dsn string) (*Instance, error) {
	if driver != "mysql" {
		return nil, fmt.Errorf("Unsupported driver \"%s\"", driver)
	}

	base := baseDSN(dsn)
	params := paramMap(dsn)
	parsedConfig, err := mysql.ParseDSN(dsn)
	if err != nil {
		return nil, err
	}

	instance := &Instance{
		BaseDSN:        base,
		Driver:         driver,
		User:           parsedConfig.User,
		Password:       parsedConfig.Passwd,
		defaultParams:  params,
		connectionPool: make(map[string]*sqlx.DB),
		flavor:         FlavorUnknown,
		m:              new(sync.Mutex),
	}

	switch parsedConfig.Net {
	case "unix":
		instance.Host = "localhost"
		instance.SocketPath = parsedConfig.Addr
	default:
		instance.Host, instance.Port, err = SplitHostOptionalPort(parsedConfig.Addr)
		if err != nil {
			return nil, err
		}
	}

	return instance, nil
}

// String for an instance returns a "host:port" string (or "localhost:/path/to/socket"
// if using UNIX domain socket)
func (instance *Instance) String() string {
	if instance.SocketPath != "" {
		return instance.Host + ":" + instance.SocketPath
	} else if instance.Port == 0 {
		return instance.Host
	} else {
		return instance.Host + ":" + strconv.Itoa(instance.Port)
	}
}

// BuildParamString returns a DB connection parameter string, which first takes
// the instance's default params and then applies overrides on top.
// The arg should be a URL query string formatted value, for example
// "foo=bar&fizz=buzz" to apply foo=bar and fizz=buzz on top of any instance
// default parameters.
func (instance *Instance) BuildParamString(params string) string {
	v := url.Values{}
	for defName, defValue := range instance.defaultParams {
		v.Set(defName, defValue)
	}
	overrides, _ := url.ParseQuery(params)
	for name := range overrides {
		v.Set(name, overrides.Get(name))
	}

	// If a readTimeout is being set, make the lock wait timeouts be less than the
	// readTimeout. This ensures queries/statements will hit lock wait timeouts
	// *before* hitting the readTimeout, aiding in troubleshooting lock-related
	// problems and preventing dangling queries after a readTimeout.
	if v.Has("readTimeout") {
		if readTimeout, _ := time.ParseDuration(v.Get("readTimeout")); readTimeout > 0 {
			readTimeoutSec := int(readTimeout / time.Second)
			if readTimeoutSec < 2 {
				// Override an overly-low readTimeout, to ensure we can set lock timeouts to
				// 1 second under the readTimeout and still end up with a positive number
				v.Set("readTimeout", "2s")
				v.Set("lock_wait_timeout", "1")
				v.Set("innodb_lock_wait_timeout", "1")
			} else {
				for _, varname := range []string{"lock_wait_timeout", "innodb_lock_wait_timeout"} {
					if !v.Has(varname) {
						// Params didn't set this var, and we don't know what value the server is
						// using yet. Set var to the lesser of (server value, readTimeout-1)
						v.Set(varname, fmt.Sprintf("LEAST(%d,CONVERT(@@%s,SIGNED))", readTimeoutSec-1, varname))
					} else if requested, _ := strconv.Atoi(v.Get(varname)); requested >= readTimeoutSec {
						// Param did set the value, to an int literal equal or above the
						// readTimeout: override to be below the readTimeout instead
						v.Set(varname, strconv.Itoa(readTimeoutSec-1))
					}
				}
			}
		}
	}

	return v.Encode()
}

// ConnectionPool returns a new sqlx.DB for this instance's host/port/user/pass
// with the supplied default schema and params string. A connection attempt is
// made, and an error will be returned if connection fails.
// defaultSchema may be "" if it is not relevant.
// params should be supplied in format "foo=bar&fizz=buzz" with URL escaping
// already applied. Do not include a prefix of "?". params will be merged with
// instance.defaultParams, with params supplied here taking precedence.
// The connection pool's max size, max conn lifetime, and max idle time are all
// tuned automatically to intelligent defaults based on auto-discovered limits.
func (instance *Instance) ConnectionPool(defaultSchema, params string) (*sqlx.DB, error) {
	fullParams := instance.BuildParamString(params)
	return instance.rawConnectionPool(defaultSchema, fullParams, false)
}

// CachedConnectionPool operates like ConnectionPool, except it caches
// connection pools for reuse. When multiple requests are made for the same
// combination of defaultSchema and params, a pre-existing connection pool will
// be returned. See ConnectionPool for usage of the args for this method.
func (instance *Instance) CachedConnectionPool(defaultSchema, params string) (*sqlx.DB, error) {
	fullParams := instance.BuildParamString(params)
	key := defaultSchema + "?" + fullParams

	instance.m.Lock()
	defer instance.m.Unlock()
	if pool, ok := instance.connectionPool[key]; ok {
		return pool, nil
	}
	db, err := instance.rawConnectionPool(defaultSchema, fullParams, true)
	if err == nil {
		instance.connectionPool[key] = db
	}
	return db, err
}

func (instance *Instance) maxConnsPerPool() int {
	return max(2, instance.maxUserConns-10)
}

func (instance *Instance) rawConnectionPool(defaultSchema, fullParams string, alreadyLocked bool) (*sqlx.DB, error) {
	fullDSN := instance.BaseDSN + defaultSchema + "?" + fullParams
	db, err := sqlx.Connect(instance.Driver, fullDSN)
	if err != nil {
		return nil, err
	}
	if !instance.valid {
		err = instance.hydrateVars(db, !alreadyLocked)
		if err != nil {
			return nil, err
		}
	}

	// Set max concurrent connections, ensuring it is less than any limit set on
	// the database side either globally or for this user. This does not completely
	// eliminate max-conn problems, because each Instance can have many separate
	// connection pools, but it may help.
	db.SetMaxOpenConns(instance.maxConnsPerPool())

	// Set max conn reuse lifetime to 1 minute, and set max idle time based on
	// the session wait_timeout or 10s max.
	db.SetConnMaxLifetime(time.Minute)
	if instance.waitTimeout <= 10 {
		db.SetConnMaxIdleTime((time.Duration(instance.waitTimeout) * time.Second) - (250 * time.Millisecond))
	} else {
		db.SetConnMaxIdleTime(10 * time.Second)
	}

	return db.Unsafe(), nil
}

// CanConnect returns true if the Instance can currently be connected to, using
// its configured User and Password. If a new connection cannot be made, the
// return value will be false, along with an error expressing the reason.
func (instance *Instance) CanConnect() (bool, error) {
	// Important: if this logic ever changes, be sure to un-comment-out the second
	// half of TengoIntegrationSuite.TestInstanceCanConnect() in instance_test.go.
	// That logic was commented out since this logic changes so rarely, and the
	// test setup there is especially disruptive when using tmpfs containers.
	if !instance.valid {
		// We haven't ever successfully connected yet, so we can safely call Valid()
		// without getting a false-positive from a previous cached conn pool; and if
		// successful, we can then re-use this newly cached pool for other code paths
		return instance.Valid()
	}

	// Use a new connection pool and intentionally avoid the pool cache
	db, err := instance.ConnectionPool("", "")
	if db != nil {
		// close immediately since we bypassed cache and therefore the pool can't be
		// reused elsewhere
		db.Close()
	}
	return err == nil, err
}

// Valid returns true if a successful connection can be made to the Instance,
// or if a successful connection has already been made previously. This method
// only returns false if no previous successful connection was ever made, and a
// new attempt to establish one fails.
func (instance *Instance) Valid() (bool, error) {
	if instance == nil {
		return false, nil
	} else if instance.valid {
		return true, nil
	}
	// CachedConnectionPool establishes one conn in the pool; if
	// successful, this also calls hydrateVars which then sets valid to true
	_, err := instance.CachedConnectionPool("", "")
	return err == nil, err
}

// CloseAll closes all of instance's cached connection pools. This can be
// useful for graceful shutdown, to avoid aborted-connection counters/logging
// in some versions of MySQL.
func (instance *Instance) CloseAll() {
	instance.m.Lock()
	for key, db := range instance.connectionPool {
		db.Close()
		delete(instance.connectionPool, key)
	}
	instance.valid = false // force future conns to re-hydrate vars
	instance.m.Unlock()
}

// Flavor returns this instance's flavor value, representing the database
// distribution/fork/vendor as well as major and minor version. If this is
// unable to be determined or an error occurs, FlavorUnknown will be returned.
func (instance *Instance) Flavor() Flavor {
	// Attempt to hydrate flavor, unless it was already done OR explicitly forced
	// via ForceFlavor. (This call pattern differs slightly from other hydrated
	// fields, since other fields don't have a notion of forcing an override value.)
	if instance.flavor == FlavorUnknown {
		instance.Valid()
	}
	return instance.flavor
}

// SetFlavor attempts to set this instance's flavor value. If the instance's
// flavor has already been hydrated successfully, the value is not changed and
// an error is returned.
func (instance *Instance) SetFlavor(flavor Flavor) error {
	if instance.flavor.Known() {
		return fmt.Errorf("SetFlavor: instance %s already detected as flavor %s", instance, instance.flavor)
	}
	instance.ForceFlavor(flavor)
	return nil
}

// ForceFlavor overrides this instance's flavor value. Only tests should call
// this method directly; all other callers should use SetFlavor instead and
// check the error return value.
func (instance *Instance) ForceFlavor(flavor Flavor) {
	instance.flavor = flavor
}

// NameCaseMode represents different values of the lower_case_table_names
// read-only global server variable.
type NameCaseMode int

// Constants representing valid NameCaseMode values
const (
	NameCaseUnknown     NameCaseMode = -1
	NameCaseAsIs        NameCaseMode = 0
	NameCaseLower       NameCaseMode = 1
	NameCaseInsensitive NameCaseMode = 2
)

// NameCaseMode returns a value reflecting this instance's lower_case_table_names,
// normally a value between 0 and 2 if successfully queryable.
func (instance *Instance) NameCaseMode() NameCaseMode {
	if ok, _ := instance.Valid(); !ok {
		return NameCaseUnknown
	}
	return NameCaseMode(instance.lowerCaseNames)
}

// LockWaitTimeout returns the default session lock_wait_timeout for connections
// to this instance, or 0 if it could not be queried.
func (instance *Instance) LockWaitTimeout() int {
	if ok, _ := instance.Valid(); !ok {
		return 0
	}
	return instance.lockWaitTimeout
}

// SQLMode returns the full session-level sql_mode string for connections
// using default parameters, or a blank string if it could not be queried.
func (instance *Instance) SQLMode() string {
	if ok, _ := instance.Valid(); !ok {
		return ""
	}
	return strings.Join(instance.sqlMode, ",")
}

// BaseLatency returns the round-trip latency that was measured for a trivial
// query e.g. `SELECT 1`. A return value of 0 indicates an error occurred
// when communicating with the database.
func (instance *Instance) BaseLatency() time.Duration {
	if ok, _ := instance.Valid(); !ok {
		return 0
	}
	return instance.latency
}

// AdaptiveHashIndexEnabled returns true if the InnoDB adaptive hash index is
// enabled on the instance. If the instance could not be introspected, false is
// returned.
func (instance *Instance) AdaptiveHashIndexEnabled() bool {
	if ok, _ := instance.Valid(); !ok {
		return false
	}
	return instance.ahiEnabled
}

// hydrateVars populates several non-exported Instance fields by querying
// various global and session variables.
func (instance *Instance) hydrateVars(db *sqlx.DB, lock bool) (err error) {
	if lock {
		instance.m.Lock()
		defer instance.m.Unlock()
		if instance.valid {
			return nil
		}
	}

	query := `SELECT @@global.version_comment, @@global.version, @@session.sql_mode,
		@@session.wait_timeout, @@session.lock_wait_timeout,
		@@session.max_user_connections, @@global.max_connections,
		@@global.lower_case_table_names, @@global.innodb_adaptive_hash_index`
	ctx := context.Background()

	// We use a Conn here so that we can measure query time without it including
	// connection time. The query time is then tracked as the server's baseline
	// round-trip network latency. Since it's only a single sample, it's not
	// a fully accurate depiction of the latency, but it provides a rough ballpark
	// number.
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	start := time.Now()
	row := conn.QueryRowContext(ctx, query)
	instance.latency = time.Since(start)
	var versionComment, version, sqlMode string
	var maxUserConns, maxConns int
	err = row.Scan(&versionComment, &version, &sqlMode,
		&instance.waitTimeout, &instance.lockWaitTimeout,
		&maxUserConns, &maxConns,
		&instance.lowerCaseNames, &instance.ahiEnabled)
	if err != nil {
		return err
	}
	instance.valid = true
	if instance.flavor == FlavorUnknown { // Only set flavor if it wasn't already forced to some value
		instance.flavor = IdentifyFlavor(version, versionComment)
	}
	instance.sqlMode = strings.Split(sqlMode, ",")
	if maxUserConns > 0 {
		instance.maxUserConns = maxUserConns
	} else {
		instance.maxUserConns = maxConns
	}
	return nil
}

// Regular expression defining privileges that allow use of setting session
// variable sql_log_bin. Note that SESSION_VARIABLES_ADMIN and
// SYSTEM_VARIABLES_ADMIN are from MySQL 8.0+. Meanwhile BINLOG ADMIN is from
// MariaDB 10.5+ as per https://jira.mariadb.org/browse/MDEV-21957; note the
// space in the name (not to be confused with BINLOG_ADMIN with an underscore,
// which is a MySQL 8.0 privilege which does NOT control sql_log_bin!)
// Meanwhile MariaDB 11.0 weakens SUPER to no longer confer fine-grained privs.
var (
	reSkipBinlog         = regexp.MustCompile(`(?:ALL PRIVILEGES ON \*\.\*|SUPER|SESSION_VARIABLES_ADMIN|SYSTEM_VARIABLES_ADMIN|BINLOG ADMIN)[,\s]`)
	reSkipBinlogMaria110 = regexp.MustCompile(`(?:ALL PRIVILEGES ON \*\.\*|BINLOG ADMIN)[,\s]`)
)

// CanSkipBinlog returns true if instance.User has privileges necessary to
// set sql_log_bin=0. If an error occurs in checking grants, this method returns
// false as a safe fallback.
func (instance *Instance) CanSkipBinlog() bool {
	var re *regexp.Regexp
	if instance.Flavor().MinMariaDB(11, 0) {
		re = reSkipBinlogMaria110
	} else {
		re = reSkipBinlog
	}
	return instance.checkGrantsRegexp(re)
}

func (instance *Instance) checkGrantsRegexp(re *regexp.Regexp) bool {
	if instance.grants == nil {
		instance.hydrateGrants()
	}
	for _, grant := range instance.grants {
		if re.MatchString(grant) {
			return true
		}
	}
	return false
}

func (instance *Instance) hydrateGrants() {
	db, err := instance.CachedConnectionPool("", "")
	if err != nil {
		return
	}
	instance.m.Lock()
	defer instance.m.Unlock()
	if instance.grants != nil {
		// If the Lock call above blocked, and meanwhile another goroutine already
		// populated the grants, no need to query redundantly now
		return
	}
	var allGrants []string
	rows, err := db.Query("SHOW GRANTS")
	if err != nil {
		// Errors are not surfaced here; instead we simply don't hydrate the grants
		return
	}
	defer rows.Close()
	for rows.Next() {
		var grantValue string
		if err := rows.Scan(&grantValue); err != nil {
			return
		}
		allGrants = append(allGrants, grantValue)
	}
	if rows.Err() == nil {
		instance.grants = allGrants
	}
}

// SchemaNames returns a slice of all schema name strings on the instance
// visible to the user. System schemas are excluded.
func (instance *Instance) SchemaNames() (result []string, err error) {
	db, err := instance.CachedConnectionPool("", "")
	if err != nil {
		return nil, err
	}
	query := `
		SELECT schema_name
		FROM   information_schema.schemata
		WHERE  schema_name NOT IN ('information_schema', 'performance_schema', 'mysql', 'test', 'sys')`
	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, err
		}
		result = append(result, s)
	}
	return result, rows.Err()
}

// Schemas returns a slice of schemas on the instance visible to the user. If
// called with no args, all non-system schemas will be returned. Or pass one or
// more schema names as args to filter the result to just those schemas.
// Note that the ordering of the resulting slice is not guaranteed.
func (instance *Instance) Schemas(onlyNames ...string) ([]*Schema, error) {
	opts := IntrospectionOptions{SchemaNames: onlyNames}
	return IntrospectSchemas(context.Background(), instance, opts)
}

// SchemasByName returns a map of schema name string to *Schema.  If
// called with no args, all non-system schemas will be returned. Or pass one or
// more schema names as args to filter the result to just those schemas.
func (instance *Instance) SchemasByName(onlyNames ...string) (map[string]*Schema, error) {
	schemas, err := instance.Schemas(onlyNames...)
	if err != nil {
		return nil, err
	}
	result := make(map[string]*Schema, len(schemas))
	for _, s := range schemas {
		result[s.Name] = s
	}
	return result, nil
}

// Schema returns a single schema by name. If the schema does not exist, nil
// will be returned along with a sql.ErrNoRows error.
func (instance *Instance) Schema(name string) (*Schema, error) {
	schemas, err := instance.Schemas(name)
	if err != nil {
		return nil, err
	} else if len(schemas) == 0 {
		return nil, sql.ErrNoRows
	}
	return schemas[0], nil
}

// HasSchema returns true if this instance has a schema with the supplied name
// visible to the user, or false otherwise. An error result will only be
// returned if a connection or query failed entirely and we weren't able to
// determine whether the schema exists.
func (instance *Instance) HasSchema(name string) (bool, error) {
	db, err := instance.CachedConnectionPool("", "")
	if err != nil {
		return false, err
	}
	var exists int
	query := `
		SELECT 1
		FROM   information_schema.schemata
		WHERE  schema_name = ?`
	err = db.QueryRow(query, name).Scan(&exists)
	if err == nil {
		return true, nil
	} else if err == sql.ErrNoRows {
		return false, nil
	} else {
		return false, err
	}
}

// introspectionParams returns a params string which ensures safe session
// variables for use with SHOW CREATE as well as queries on information_schema
func (instance *Instance) introspectionParams() string {
	v := url.Values{}
	v.Set("sql_quote_show_create", "1")

	flavor := instance.Flavor()

	// In MySQL 8, ensure we get up-to-date values for table sizes as well as next
	// auto_increment value
	if flavor.MinMySQL(8) {
		v.Set("information_schema_stats_expiry", "0")
	}

	// In MySQL, we need a binary collation in order for SHOW CREATE TABLE to
	// correctly return 4-byte chars in generated column expressions (5.7+), column
	// default expressions (8.0+), check constraint clauses (8.0+), and
	// functional index expressions (8.0+). Note that this isn't a silver bullet:
	// * Non-expression default value literals still don't show 4-byte chars
	//   correctly, regardless of collation, in any flavor
	// * information_schema does not return 4-byte chars properly, regardless of
	//   collation; we must reparse from SHOW CREATE TABLE
	// * In MariaDB, SHOW CREATE TABLE does not return 4-byte chars correctly
	//   regardless of collation
	if flavor.MinMySQL(5, 7) {
		v.Set("collation", "binary")
	}

	// Remove any problematic sql_mode values
	keepModes := filterSQLMode(instance.sqlMode, IntrospectionBadSQLModes)
	if len(keepModes) != len(instance.sqlMode) {
		v.Set("sql_mode", "'"+strings.Join(keepModes, ",")+"'")
	}

	return v.Encode()
}

// ShowCreateTable returns a string with a CREATE TABLE statement, representing
// how the instance views the specified table as having been created.
func (instance *Instance) ShowCreateTable(schema, table string) (string, error) {
	db, err := instance.CachedConnectionPool("", instance.introspectionParams())
	if err != nil {
		return "", err
	}
	return showCreateTable(context.Background(), db, schema, table)
}

// TableSize returns an estimate of the table's size on-disk, based on data in
// information_schema. As a special case, if the table has no rows, the returned
// value will be 0 even though empty InnoDB tables typically take up 16KB. If
// the table or schema does not exist on this instance, an error is returned.
// Use of innodb_stats_persistent negatively impacts the result accuracy; see
// https://bugs.mysql.com/bug.php?id=75428.
func (instance *Instance) TableSize(schema, table string) (int64, error) {
	var result int64
	db, err := instance.CachedConnectionPool("", instance.introspectionParams())
	if err != nil {
		return 0, err
	}
	query := fmt.Sprintf(`
		SELECT  (data_length + index_length) *
		        EXISTS (SELECT 1 FROM %s.%s LIMIT 1)
		FROM    information_schema.tables
		WHERE   table_schema = ? and table_name = ?`,
		EscapeIdentifier(schema), EscapeIdentifier(table))
	err = db.QueryRow(query, schema, table).Scan(&result)
	return result, err
}

// FindNonEmptyTables examines the supplied list of table names, and filters out
// any that have no rows. The returned slice consists of the names of the
// supplied tables that had at least one row.
func (instance *Instance) FindNonEmptyTables(schema string, tables []string) (nonEmptyTables []string, err error) {
	db, err := instance.CachedConnectionPool("", "")
	if err != nil {
		return nil, err
	}
	return findNonEmptyTables(db, schema, tables)
}

func findNonEmptyTables(db *sqlx.DB, schema string, tables []string) (nonEmptyTables []string, err error) {
	if len(tables) == 0 {
		return
	}

	// Query for existence of rows in chunks of up to 4 tables per query, times up
	// to 4 concurrent goroutines.
	hasRowsChan := make(chan string)
	g, ctx := errgroup.WithContext(context.Background())
	g.SetLimit(4)
	// Since g.SetLimit is used, g.Go can block, so it must be called from a
	// separate goroutine. Meanwhile we need the main goroutine to proceed to its
	// loop of reading from the channel, so that writes to the channel won't block.
	go func() {
		for chunk := range slices.Chunk(tables, 4) {
			g.Go(func() error {
				var subqueries []string
				for _, name := range chunk {
					subqueries = append(subqueries, fmt.Sprintf("(SELECT '%s' FROM %s.%s LIMIT 1)", EscapeValueForCreateTable(name), EscapeIdentifier(schema), EscapeIdentifier(name)))
				}
				query := strings.Join(subqueries, "UNION ALL")
				rows, err := db.QueryContext(ctx, query)
				if err != nil {
					return err
				}
				defer rows.Close()
				var name string
				for rows.Next() {
					if err := rows.Scan(&name); err != nil {
						return err
					} else {
						hasRowsChan <- name
					}
				}
				return rows.Err()
			})
		}
		g.Wait()
		close(hasRowsChan)
	}()
	for name := range hasRowsChan {
		nonEmptyTables = append(nonEmptyTables, name)
	}
	if err = g.Wait(); err != nil {
		return nil, err
	}
	return nonEmptyTables, nil
}

// SchemaCreationOptions specifies schema-level metadata when creating or
// altering a database.
type SchemaCreationOptions struct {
	DefaultCharSet   string
	DefaultCollation string
	SkipBinlog       bool
}

func (opts SchemaCreationOptions) params() string {
	if opts.SkipBinlog {
		return "sql_log_bin=0"
	}
	return ""
}

// CreateSchema creates a new database schema with the supplied name, and
// optionally the supplied default CharSet and Collation. (Leave these fields
// blank to use server defaults.)
func (instance *Instance) CreateSchema(name string, opts SchemaCreationOptions) (*Schema, error) {
	db, err := instance.CachedConnectionPool("", opts.params())
	if err != nil {
		return nil, err
	}
	// Technically the server defaults would be used anyway if these are left
	// blank, but we need the returned Schema value to reflect the correct values,
	// and we can avoid re-querying this way
	if opts.DefaultCharSet == "" || opts.DefaultCollation == "" {
		defCharSet, defCollation, err := instance.DefaultCharSetAndCollation()
		if err != nil {
			return nil, err
		}
		if opts.DefaultCharSet == "" {
			opts.DefaultCharSet = defCharSet
		}
		if opts.DefaultCollation == "" {
			opts.DefaultCollation = defCollation
		}
	}
	schema := &Schema{
		Name:      name,
		CharSet:   opts.DefaultCharSet,
		Collation: opts.DefaultCollation,
		Tables:    []*Table{},
	}
	_, err = db.Exec(schema.CreateStatement())
	if err != nil {
		return nil, err
	}
	return schema, nil
}

// DropSchema first drops all tables in the schema, and then drops the database
// schema itself. If opts.OnlyIfEmpty==true, returns an error if any of the
// tables have any rows. If opts.OneShot==true, the initial table drops are
// skipped; however this can result in locks being held for a long time.
func (instance *Instance) DropSchema(schema string, opts BulkDropOptions) error {
	if !opts.OneShot {
		// Drop the tables first. Note that DropTablesInSchema performs table-
		// emptiness checks for opts.OnlyIfEmpty itself, so no need to do that here.
		err := instance.DropTablesInSchema(schema, opts)
		if err != nil {
			return err
		}
	} else if opts.OnlyIfEmpty {
		// Not dropping the tables first, but still want to confirm they're empty
		db, err := instance.CachedConnectionPool("", opts.params())
		if err != nil {
			return fmt.Errorf("Error obtaining connection pool for checking table emptiness before dropping schema %s: %w", EscapeIdentifier(schema), err)
		}
		var tableMap map[string][]string
		if opts.Schema != nil {
			tableMap = opts.Schema.tablesToPartitions()
		} else {
			tableMap, err = tablesToPartitions(db, schema, instance.Flavor())
			if err != nil {
				return err
			}
		}
		names := slices.Collect(maps.Keys(tableMap))
		if nonEmpty, err := findNonEmptyTables(db, schema, names); err != nil {
			return err
		} else if len(nonEmpty) > 0 {
			return fmt.Errorf("non-empty tables present: %s", strings.Join(nonEmpty, ", "))
		}
	}

	// Now proceed to drop the schema
	db, err := instance.CachedConnectionPool("", opts.params())
	if err != nil {
		return fmt.Errorf("Error obtaining connection pool for dropping schema %s: %w", EscapeIdentifier(schema), err)
	}
	err = dropSchema(db, schema)
	if IsLockConflictError(err) {
		// we do 1 retry upon seeing a metadata locking conflict, consistent with
		// logic in DropTablesInSchema
		err = dropSchema(db, schema)
	}
	return err
}

// dropSchema executes a DROP DATABASE on the supplied database name. This isn't
// exported directly because it is unsafe to drop things in this manner in
// production, as it holds locks for a long time if many tables are present.
func dropSchema(db *sqlx.DB, schema string) error {
	_, err := db.Exec("DROP DATABASE " + EscapeIdentifier(schema))
	if err != nil {
		return fmt.Errorf("Error dropping database %s: %w", EscapeIdentifier(schema), err)
	}
	return nil
}

// AlterSchema changes the character set and/or collation of the supplied schema
// on instance. Supply an empty string for opts.DefaultCharSet to only change
// the collation, or supply an empty string for opts.DefaultCollation to use the
// default collation of opts.DefaultCharSet. (Supplying an empty string for both
// is also allowed, but is a no-op.)
func (instance *Instance) AlterSchema(schema string, opts SchemaCreationOptions) error {
	s, err := instance.Schema(schema)
	if err != nil {
		return err
	}
	statement := s.AlterStatement(opts.DefaultCharSet, opts.DefaultCollation)
	if statement == "" {
		return nil
	}
	db, err := instance.CachedConnectionPool("", opts.params())
	if err != nil {
		return err
	}
	if _, err = db.Exec(statement); err != nil {
		return err
	}
	return nil
}

// BulkDropOptions controls how objects are dropped in bulk.
type BulkDropOptions struct {
	OnlyIfEmpty     bool    // If true, when dropping tables, error if any have rows
	ChunkSize       int     // Objects to drop per statement
	OneShot         bool    // If true, drop everything at once (only affects DropSchema; overrides ChunkSize)
	SkipBinlog      bool    // If true, use session sql_log_bin=0 (requires superuser)
	PartitionsFirst bool    // If true, drop RANGE/LIST partitioned tables one partition at a time
	LockWaitTimeout int     // If greater than 0, limit how long to wait for metadata locks (in seconds)
	Schema          *Schema // If non-nil, obtain object lists from Schema instead of running I_S queries
}

func (opts BulkDropOptions) params() string {
	values := []string{"foreign_key_checks=0"}
	if opts.SkipBinlog {
		values = append(values, "sql_log_bin=0")
	}
	if opts.LockWaitTimeout > 0 {
		values = append(values, "lock_wait_timeout="+strconv.Itoa(opts.LockWaitTimeout))
	}
	return strings.Join(values, "&")
}

// DropTablesInSchema drops all tables in a schema. If opts.OnlyIfEmpty==true,
// returns an error if any of the tables have any rows.
func (instance *Instance) DropTablesInSchema(schema string, opts BulkDropOptions) error {
	db, err := instance.CachedConnectionPool("", opts.params())
	if err != nil {
		return fmt.Errorf("Error obtaining connection pool for dropping tables in schema %s: %w", EscapeIdentifier(schema), err)
	}

	// Obtain table and partition names
	var tableMap map[string][]string
	if opts.Schema != nil {
		tableMap = opts.Schema.tablesToPartitions()
	} else {
		tableMap, err = tablesToPartitions(db, schema, instance.Flavor())
		if err != nil {
			return err
		}
	}
	if len(tableMap) == 0 {
		return nil
	}
	names := slices.Collect(maps.Keys(tableMap))

	// If requested, confirm tables are empty
	if opts.OnlyIfEmpty {
		if nonEmpty, err := findNonEmptyTables(db, schema, names); err != nil {
			return err
		} else if len(nonEmpty) > 0 {
			return fmt.Errorf("non-empty tables present: %s", strings.Join(nonEmpty, ", "))
		}
	}

	// If requested, for each partitioned table, drop all partitions but 1
	if opts.PartitionsFirst {
		for name, partitions := range tableMap {
			if len(partitions) > 1 {
				for chunk := range slices.Chunk(partitions[0:len(partitions)-1], max(opts.ChunkSize, 1)) {
					escapedPartitionNames := make([]string, len(chunk))
					for n := range chunk {
						escapedPartitionNames[n] = EscapeIdentifier(chunk[n])
					}
					alter := "ALTER TABLE " + EscapeIdentifier(schema) + "." + EscapeIdentifier(name) + " DROP PARTITION " + strings.Join(escapedPartitionNames, ", ")
					if _, err := db.Exec(alter); err != nil {
						return fmt.Errorf("Error dropping partitions via %s: %w", alter, err)
					}
				}
			}
		}
	}

	// We don't ever run DROP TABLEs concurrently, as this can deadlock in recent
	// MySQL and MariaDB if foreign keys are present; and dropping tables too
	// rapidly can be stall-prone in older MySQL and MariaDB, especially if AHI is
	// enabled and/or the buffer pool is large. But we do permit dropping multiple
	// tables per statement to reduce the number of round-trips. If any statements
	// fail, we retry each table from the chunk individually.
	retries := []string{}
	for chunk := range slices.Chunk(names, max(opts.ChunkSize, 1)) {
		escapedNames := make([]string, len(chunk))
		for n := range chunk {
			escapedNames[n] = EscapeIdentifier(schema) + "." + EscapeIdentifier(chunk[n])
		}
		_, err := db.Exec("DROP TABLE " + strings.Join(escapedNames, ", "))
		if err != nil {
			// If foreign keys are being used, DROP TABLE can encounter lock wait
			// timeouts in various situations in some flavors. We retry any error
			// at the end, without chunking.
			retries = append(retries, escapedNames...)
		}
	}
	for _, escapedName := range retries {
		if _, err := db.Exec("DROP TABLE " + escapedName); err != nil {
			return fmt.Errorf("Error dropping table %s.%s even after retry: %w", EscapeIdentifier(schema), escapedName, err)
		}
	}
	return nil
}

// DropRoutinesInSchema drops all stored procedures and functions in a schema.
func (instance *Instance) DropRoutinesInSchema(schema string, opts BulkDropOptions) error {
	params := opts.params()
	chunkSize := max(opts.ChunkSize, 1)
	if chunkSize > 1 {
		// DROP PROCEDURE and DROP FUNCTION do not allow multiple objects to be
		// specified at once, so instead we use multiStatements to send multiple
		// DDLs per round-trip
		params += "&multiStatements=true"
	}
	db, err := instance.CachedConnectionPool("", params)
	if err != nil && chunkSize > 1 {
		// Try again without chunking, in case there's a proxy which doesn't support
		// multiStatements or is configured to forbid it
		chunkSize = 1
		db, err = instance.CachedConnectionPool("", opts.params())
	}
	if err != nil {
		return err
	}

	// If schema was provided in opts, obtain routine names from there. Otherwise,
	// query names and types directly, since this is much faster than going through
	// instance.Schema() which performs full introspection of the schema.
	type nameAndType struct {
		Name string
		Type string
	}
	var routineInfo []nameAndType
	if opts.Schema != nil {
		routineInfo = make([]nameAndType, len(opts.Schema.Routines))
		for n, routine := range opts.Schema.Routines {
			routineInfo[n].Name = routine.Name
			routineInfo[n].Type = string(routine.Type)
		}
	} else {
		query := `
			SELECT routine_name AS routine_name, UPPER(routine_type) AS routine_type
			FROM   information_schema.routines
			WHERE  routine_schema = ?`
		rows, err := db.Query(query, schema)
		if err != nil {
			return err
		}
		defer rows.Close()
		var name, typ string
		for rows.Next() {
			if err := rows.Scan(&name, &typ); err != nil {
				return err
			}
			routineInfo = append(routineInfo, nameAndType{Name: name, Type: typ})
		}
		if err := rows.Err(); err != nil {
			return err
		}
	}
	if len(routineInfo) == 0 {
		return nil
	}

	for chunk := range slices.Chunk(routineInfo, chunkSize) {
		chunkStrings := make([]string, 0, len(chunk))
		for _, ri := range chunk {
			chunkStrings = append(chunkStrings, "DROP "+ri.Type+" IF EXISTS "+EscapeIdentifier(schema)+"."+EscapeIdentifier(ri.Name))
		}
		if chunkSize > 1 {
			query := strings.Join(chunkStrings, ";") + ";"
			if _, err := db.Exec(query); err != nil {
				// Try each statement in this chunk again individually, and disable chunking moving forwards
				chunkSize = 1
			}
		}
		if chunkSize == 1 {
			for _, query := range chunkStrings {
				if _, err := db.Exec(query); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// tablesToPartitions returns a map whose keys are all tables in the schema
// (whether partitioned or not), and values are either nil (if unpartitioned or
// partitioned in a way that doesn't support DROP PARTITION) or a slice of
// partition names (if using RANGE or LIST partitioning). Views are excluded
// from the result.
func tablesToPartitions(db *sqlx.DB, schema string, flavor Flavor) (map[string][]string, error) {
	// information_schema.partitions contains all tables (not just partitioned)
	// and excludes views (which we don't want here anyway) in non-MySQL8+ flavors
	query := `
		SELECT   SQL_BUFFER_RESULT
		         table_name, partition_name, partition_method, subpartition_method,
		         partition_ordinal_position, data_length
		FROM     information_schema.partitions
		WHERE    table_schema = ?
		ORDER BY table_name, partition_ordinal_position`
	rows, err := db.Query(query, schema)
	if err != nil {
		return nil, fmt.Errorf("Error querying information_schema.partitions: %w", err)
	}
	defer rows.Close()

	var checkViews bool
	partitions := make(map[string][]string)
	for rows.Next() {
		var tableName string
		var partitionName, method, subMethod sql.NullString
		var position sql.NullInt64
		var dataLength int64
		err := rows.Scan(&tableName, &partitionName, &method, &subMethod, &position, &dataLength)
		if err != nil {
			return nil, fmt.Errorf("Error querying information_schema.partitions: %w", err)
		}
		if !position.Valid || position.Int64 == 1 {
			partitions[tableName] = nil
		}
		if method.Valid && !subMethod.Valid &&
			(strings.HasPrefix(method.String, "RANGE") || strings.HasPrefix(method.String, "LIST")) {
			partitions[tableName] = append(partitions[tableName], partitionName.String)
		}
		// In MySQL 8, views are present here with a data_length of 0. Although InnoDB
		// tables likely always have nonzero data_length, non-InnoDB tables do show up
		// here with data_length of 0, so this alone isn't sufficient to identify
		// specific views. However, if all rows have nonzero data_length, we know
		// there are no views and can skip the query.
		if dataLength == 0 {
			checkViews = true
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("Error querying information_schema.partitions: %w", err)
	}

	// MySQL 8's new data dictionary actually includes views in
	// information_schema.partitions, so remove them explicitly.
	if checkViews && flavor.MinMySQL(8) {
		query := `SELECT table_name FROM information_schema.views WHERE table_schema = ?`
		rows, err := db.Query(query, schema)
		if err != nil {
			return nil, fmt.Errorf("Error querying information_schema.views: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var viewName string
			if err := rows.Scan(&viewName); err != nil {
				return nil, fmt.Errorf("Error querying information_schema.views: %w", err)
			}
			delete(partitions, viewName)
		}
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("Error querying information_schema.views: %w", err)
		}
	}

	return partitions, nil
}

// DefaultCharSetAndCollation returns the instance's default character set and
// collation.
func (instance *Instance) DefaultCharSetAndCollation() (serverCharSet, serverCollation string, err error) {
	db, err := instance.CachedConnectionPool("", "")
	if err != nil {
		return
	}
	err = db.QueryRow("SELECT @@global.character_set_server, @@global.collation_server").Scan(&serverCharSet, &serverCollation)
	return
}

// ServerProcess describes the status of a connection on an Instance.
type ServerProcess struct {
	ID      int64
	User    string
	Schema  string
	Command string
	Time    float64
	State   string
	Info    string
}

// ProcessList returns the current list of connections on instance. This is
// primarily intended for debugging and test output at this time, and may be
// disruptive on live production database servers, especially on MySQL 8.0+.
func (instance *Instance) ProcessList() (plist []ServerProcess, err error) {
	db, err := instance.CachedConnectionPool("", "")
	if err != nil {
		return nil, err
	}
	var query string
	if instance.Flavor().IsMariaDB() {
		query = "SELECT id, user, db, command, time_ms, state, info FROM information_schema.processlist"
	} else {
		// This is deprecated and lock-heavy; however, the modern alternative requires
		// performance_schema, which we disable in ephemeral containers
		query = "SHOW PROCESSLIST"
	}
	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var dests []any
	var sp ServerProcess
	var schema, state, info sql.NullString
	var timeSec int64
	var timeMsec float64
	if colNames, err := rows.Columns(); err != nil {
		return nil, err
	} else {
		dests = make([]any, len(colNames))
		for n, colName := range colNames {
			switch strings.ToLower(colName) {
			case "id":
				dests[n] = &sp.ID
			case "user":
				dests[n] = &sp.User
			case "db":
				dests[n] = &schema
			case "command":
				dests[n] = &sp.Command
			case "time":
				dests[n] = &timeSec
			case "time_ms":
				dests[n] = &timeMsec
			case "state":
				dests[n] = &state
			case "info":
				dests[n] = &info
			default: // ignore any other cols by scanning into RawBytes
				var d sql.RawBytes
				dests[n] = &d
			}
		}
	}
	for rows.Next() {
		sp = ServerProcess{}
		if err := rows.Scan(dests...); err != nil {
			return nil, err
		}
		sp.Schema = schema.String
		sp.State = state.String
		sp.Info = info.String
		if timeMsec > 0.0 { // Only in MariaDB
			sp.Time = timeMsec / 1000.0
		} else {
			sp.Time = float64(timeSec)
		}
		plist = append(plist, sp)
	}
	return plist, rows.Err()
}
