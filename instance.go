package tengo

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/VividCortex/mysqlerr"
	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"golang.org/x/sync/errgroup"
)

// Instance represents a single database server running on a specific host or address.
type Instance struct {
	BaseDSN        string // DSN ending in trailing slash; i.e. no schema name or params
	Driver         string
	User           string
	Password       string
	Host           string
	Port           int
	SocketPath     string
	defaultParams  map[string]string
	connectionPool map[string]*sqlx.DB // key is in format "schema?params"
	*sync.RWMutex                      // protects connectionPool for concurrent operations
	flavor         Flavor
	version        [3]int
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
		RWMutex:        new(sync.RWMutex),
	}

	switch parsedConfig.Net {
	case "unix":
		instance.Host = "localhost"
		instance.SocketPath = parsedConfig.Addr
	case "cloudsql":
		instance.Host = parsedConfig.Addr
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
		return fmt.Sprintf("%s:%s", instance.Host, instance.SocketPath)
	} else if instance.Port == 0 {
		return instance.Host
	} else {
		return fmt.Sprintf("%s:%d", instance.Host, instance.Port)
	}
}

// HostAndOptionalPort is like String(), but omits the port if default
func (instance *Instance) HostAndOptionalPort() string {
	if instance.Port == 3306 || instance.SocketPath != "" {
		return instance.Host
	}
	return instance.String()
}

func (instance *Instance) buildParamString(params string) string {
	v := url.Values{}
	for defName, defValue := range instance.defaultParams {
		v.Set(defName, defValue)
	}
	overrides, _ := url.ParseQuery(params)
	for name := range overrides {
		v.Set(name, overrides.Get(name))
	}
	return v.Encode()
}

// Connect returns a connection pool (sql.DB) for this instance's host/port/
// user/pass with the supplied default schema and params string. If a connection
// pool already exists for this combination, it will be returned; otherwise, one
// will be initialized and a connection attempt is made to confirm access.
// defaultSchema may be "" if it is not relevant.
// params should be supplied in format "foo=bar&fizz=buzz" with URL escaping
// already applied. Do not include a prefix of "?". params will be merged with
// instance.defaultParams, with params supplied here taking precedence.
// To avoid problems with unexpected disconnection, the connection pool will
// automatically have a max conn lifetime of at most 30sec, or less if a lower
// session-level wait_timeout was set in params or instance.defaultParams.
func (instance *Instance) Connect(defaultSchema string, params string) (*sqlx.DB, error) {
	fullParams := instance.buildParamString(params)
	key := fmt.Sprintf("%s?%s", defaultSchema, fullParams)

	instance.RLock()
	pool, ok := instance.connectionPool[key]
	instance.RUnlock()

	if ok {
		return pool, nil
	}

	fullDSN := instance.BaseDSN + key
	db, err := sqlx.Connect(instance.Driver, fullDSN)
	if err != nil {
		return nil, err
	}

	// Determine max conn lifetime, ensuring it is less than wait_timeout. If
	// wait_timeout wasn't supplied explicitly in params, query it from the server.
	// Then set conn lifetime to a value less than wait_timeout, but no less than
	// 900ms and no more than 30s.
	maxLifetime := 30 * time.Second
	parsedParams, _ := url.ParseQuery(fullParams)
	waitTimeout, _ := strconv.Atoi(parsedParams.Get("wait_timeout"))
	if waitTimeout == 0 {
		// Ignoring errors here, since this will keep maxLifetime at 30s sane default
		db.QueryRow("SELECT @@wait_timeout").Scan(&waitTimeout)
	}
	if waitTimeout > 1 && waitTimeout <= 30 {
		maxLifetime = time.Duration(waitTimeout-1) * time.Second
	} else if waitTimeout == 1 {
		maxLifetime = 900 * time.Millisecond
	}
	db.SetConnMaxLifetime(maxLifetime)

	instance.Lock()
	defer instance.Unlock()
	instance.connectionPool[key] = db.Unsafe()
	return instance.connectionPool[key], nil
}

// CanConnect verifies that the Instance can be connected to
func (instance *Instance) CanConnect() (bool, error) {
	var err error
	instance.Lock()

	// To ensure we're initializing a new connection, if a conn pool already exists
	// with the setup we want (no default db, only defaultParams for args), force
	// it to close idle connections and then make an explicit Conn. Otherwise, go
	// through Instance.Connect, which also verifies connectivity by making a new
	// pool.
	key := fmt.Sprintf("?%s", instance.buildParamString(""))
	if db, ok := instance.connectionPool[key]; ok {
		db.SetMaxIdleConns(0)
		var conn *sql.Conn
		conn, err = db.Conn(context.Background())
		if conn != nil {
			conn.Close()
		}
		db.SetMaxIdleConns(2) // default in database/sql, current as of Go 1.11
		instance.Unlock()
	} else {
		instance.Unlock()
		_, err = instance.Connect("", "")
	}

	return err == nil, err
}

// CloseAll closes all of instance's connection pools. This can be useful for
// graceful shutdown, to avoid aborted-connection counters/logging in some
// versions of MySQL.
func (instance *Instance) CloseAll() {
	instance.Lock()
	for key, db := range instance.connectionPool {
		db.Close()
		delete(instance.connectionPool, key)
	}
	instance.Unlock()
}

// Flavor returns this instance's flavor value, representing the database
// distribution/fork/vendor as well as major and minor version. If this is
// unable to be determined or an error occurs, FlavorUnknown will be returned.
func (instance *Instance) Flavor() Flavor {
	if instance.flavor == FlavorUnknown {
		instance.hydrateFlavorAndVersion()
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
	instance.version = [3]int{flavor.Major, flavor.Minor, 0}
}

// Version returns three ints representing the database's major, minor, and
// patch version, respectively. If this is unable to be determined, all 0's
// will be returned.
func (instance *Instance) Version() (int, int, int) {
	if instance.version[0] == 0 {
		instance.hydrateFlavorAndVersion()
	}
	return instance.version[0], instance.version[1], instance.version[2]
}

func (instance *Instance) hydrateFlavorAndVersion() {
	db, err := instance.Connect("", "")
	if err != nil {
		return
	}
	var versionComment, versionString string
	if err = db.QueryRow("SELECT @@global.version_comment, @@global.version").Scan(&versionComment, &versionString); err != nil {
		return
	}
	instance.version = ParseVersion(versionString)
	instance.flavor = ParseFlavor(versionString, versionComment)
}

// SchemaNames returns a slice of all schema name strings on the instance
// visible to the user. System schemas are excluded.
func (instance *Instance) SchemaNames() ([]string, error) {
	db, err := instance.Connect("information_schema", "")
	if err != nil {
		return nil, err
	}
	var result []string
	query := `
		SELECT schema_name
		FROM   schemata
		WHERE  schema_name NOT IN ('information_schema', 'performance_schema', 'mysql', 'test', 'sys')`
	if err := db.Select(&result, query); err != nil {
		return nil, err
	}
	return result, nil
}

// Schemas returns a slice of schemas on the instance visible to the user. If
// called with no args, all non-system schemas will be returned. Or pass one or
// more schema names as args to filter the result to just those schemas.
// Note that the ordering of the resulting slice is not guaranteed.
func (instance *Instance) Schemas(onlyNames ...string) ([]*Schema, error) {
	db, err := instance.Connect("information_schema", "")
	if err != nil {
		return nil, err
	}
	var rawSchemas []struct {
		Name      string `db:"schema_name"`
		CharSet   string `db:"default_character_set_name"`
		Collation string `db:"default_collation_name"`
	}

	var args []interface{}
	var query string

	// Note on these queries: MySQL 8.0 changes information_schema column names to
	// come back from queries in all caps, so we need to explicitly use AS clauses
	// in order to get them back as lowercase and have sqlx Select() work
	if len(onlyNames) == 0 {
		query = `
			SELECT schema_name AS schema_name, default_character_set_name AS default_character_set_name,
			       default_collation_name AS default_collation_name
			FROM   schemata
			WHERE  schema_name NOT IN ('information_schema', 'performance_schema', 'mysql', 'test', 'sys')`
	} else {
		query = `
			SELECT schema_name AS schema_name, default_character_set_name AS default_character_set_name,
			       default_collation_name AS default_collation_name
			FROM   schemata
			WHERE  schema_name IN (?)`
		query, args, err = sqlx.In(query, onlyNames)
	}
	if err := db.Select(&rawSchemas, query, args...); err != nil {
		return nil, err
	}

	schemas := make([]*Schema, len(rawSchemas))
	for n, rawSchema := range rawSchemas {
		schemas[n] = &Schema{
			Name:      rawSchema.Name,
			CharSet:   rawSchema.CharSet,
			Collation: rawSchema.Collation,
		}
		if schemas[n].Tables, err = instance.querySchemaTables(rawSchema.Name); err != nil {
			return nil, err
		}
		if schemas[n].Routines, err = instance.querySchemaRoutines(rawSchema.Name); err != nil {
			return nil, err
		}
	}
	return schemas, nil
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
	db, err := instance.Connect("information_schema", "")
	if err != nil {
		return false, err
	}
	var exists int
	query := `
		SELECT 1
		FROM   schemata
		WHERE  schema_name = ?`
	err = db.Get(&exists, query, name)
	if err == nil {
		return true, nil
	} else if err == sql.ErrNoRows {
		return false, nil
	} else {
		return false, err
	}
}

// ShowCreateTable returns a string with a CREATE TABLE statement, representing
// how the instance views the specified table as having been created.
func (instance *Instance) ShowCreateTable(schema, table string) (string, error) {
	db, err := instance.Connect(schema, "")
	if err != nil {
		return "", err
	}
	return showCreateTable(db, table)
}

func showCreateTable(db *sqlx.DB, table string) (string, error) {
	var createRows []struct {
		TableName       string `db:"Table"`
		CreateStatement string `db:"Create Table"`
	}
	query := fmt.Sprintf("SHOW CREATE TABLE %s", EscapeIdentifier(table))
	if err := db.Select(&createRows, query); err != nil {
		return "", err
	}
	if len(createRows) != 1 {
		return "", sql.ErrNoRows
	}
	return createRows[0].CreateStatement, nil
}

// TableSize returns an estimate of the table's size on-disk, based on data in
// information_schema. If the table or schema does not exist on this instance,
// the error will be sql.ErrNoRows.
// Please note that use of innodb_stats_persistent may negatively impact the
// accuracy. For example, see https://bugs.mysql.com/bug.php?id=75428.
func (instance *Instance) TableSize(schema, table string) (int64, error) {
	var result int64
	db, err := instance.Connect("information_schema", "")
	if err != nil {
		return 0, err
	}
	err = db.Get(&result, `
		SELECT  data_length + index_length + data_free
		FROM    tables
		WHERE   table_schema = ? and table_name = ?`,
		schema, table)
	return result, err
}

// TableHasRows returns true if the table has at least one row. If an error
// occurs in querying, also returns true (along with the error) since a false
// positive is generally less dangerous in this case than a false negative.
func (instance *Instance) TableHasRows(schema, table string) (bool, error) {
	db, err := instance.Connect(schema, "")
	if err != nil {
		return true, err
	}
	return tableHasRows(db, table)
}

func tableHasRows(db *sqlx.DB, table string) (bool, error) {
	var result []int
	query := fmt.Sprintf("SELECT 1 FROM %s LIMIT 1", EscapeIdentifier(table))
	if err := db.Select(&result, query); err != nil {
		return true, err
	}
	return len(result) != 0, nil
}

// CreateSchema creates a new database schema with the supplied name, and
// optionally the supplied default charSet and collation. (Leave charSet and
// collation blank to use server defaults.)
func (instance *Instance) CreateSchema(name, charSet, collation string) (*Schema, error) {
	db, err := instance.Connect("", "")
	if err != nil {
		return nil, err
	}
	// Technically the server defaults would be used anyway if these are left
	// blank, but we need the returned Schema value to reflect the correct values,
	// and we can avoid re-querying this way
	if charSet == "" || collation == "" {
		defCharSet, defCollation, err := instance.DefaultCharSetAndCollation()
		if err != nil {
			return nil, err
		}
		if charSet == "" {
			charSet = defCharSet
		}
		if collation == "" {
			collation = defCollation
		}
	}
	schema := &Schema{
		Name:      name,
		CharSet:   charSet,
		Collation: collation,
		Tables:    []*Table{},
	}
	_, err = db.Exec(schema.CreateStatement())
	if err != nil {
		return nil, err
	}
	return schema, nil
}

// DropSchema first drops all tables in the schema, and then drops the database
// schema itself. If onlyIfEmpty==true, returns an error if any of the tables
// have any rows.
func (instance *Instance) DropSchema(schema string, onlyIfEmpty bool) error {
	err := instance.DropTablesInSchema(schema, onlyIfEmpty)
	if err != nil {
		return err
	}

	// No need to actually obtain the fully hydrated schema value; we already know
	// it has no tables after the call above, and the schema's name alone is
	// sufficient to call Schema.DropStatement() to generate the necessary SQL
	s := &Schema{
		Name: schema,
	}
	db, err := instance.Connect("", "")
	if err != nil {
		return err
	}
	_, err = db.Exec(s.DropStatement())
	if err != nil {
		return err
	}

	prefix := fmt.Sprintf("%s?", schema)
	instance.Lock()
	for key, connPool := range instance.connectionPool {
		if strings.HasPrefix(key, prefix) {
			connPool.Close()
			delete(instance.connectionPool, key)
		}
	}
	instance.Unlock()
	return nil
}

// AlterSchema changes the character set and/or collation of the supplied schema
// on instance. Supply an empty string for newCharSet to only change the
// collation, or supply an empty string for newCollation to use the default
// collation of newCharSet. (Supplying an empty string for both is also allowed,
// but is a no-op.)
func (instance *Instance) AlterSchema(schema, newCharSet, newCollation string) error {
	s, err := instance.Schema(schema)
	if err != nil {
		return err
	}
	statement := s.AlterStatement(newCharSet, newCollation)
	if statement == "" {
		return nil
	}
	db, err := instance.Connect("", "")
	if err != nil {
		return err
	}
	if _, err = db.Exec(statement); err != nil {
		return err
	}
	return nil
}

// DropTablesInSchema drops all tables in a schema. If onlyIfEmpty==true,
// returns an error if any of the tables have any rows.
func (instance *Instance) DropTablesInSchema(schema string, onlyIfEmpty bool) error {
	db, err := instance.Connect(schema, "foreign_key_checks=0")
	if err != nil {
		return err
	}

	// Obtain table names directly; faster than going through instance.Schema(schema)
	// since we don't need other info besides the names
	var names []string
	query := `
		SELECT table_name
		FROM   information_schema.tables
		WHERE  table_schema = ?
		AND    table_type = 'BASE TABLE'`
	if err := db.Select(&names, query, schema); err != nil {
		return err
	} else if len(names) == 0 {
		return nil
	}

	var g errgroup.Group
	defer db.SetMaxOpenConns(0)

	if onlyIfEmpty {
		db.SetMaxOpenConns(15)
		for _, name := range names {
			name := name
			g.Go(func() error {
				hasRows, err := tableHasRows(db, name)
				if err == nil && hasRows {
					err = fmt.Errorf("DropTablesInSchema: table %s.%s has at least one row", EscapeIdentifier(schema), EscapeIdentifier(name))
				}
				return err
			})
		}
		if err := g.Wait(); err != nil {
			return err
		}
	}

	db.SetMaxOpenConns(10)
	retries := make(chan string, len(names))
	for _, name := range names {
		name := name
		g.Go(func() error {
			_, err := db.Exec(fmt.Sprintf("DROP TABLE %s", EscapeIdentifier(name)))
			// With the new data dictionary added in MySQL 8.0, attempting to
			// concurrently drop two tables that have a foreign key constraint between
			// them can deadlock.
			if IsDatabaseError(err, mysqlerr.ER_LOCK_DEADLOCK) {
				retries <- name
				err = nil
			}
			return err
		})
	}
	err = g.Wait()
	close(retries)
	for name := range retries {
		if _, err := db.Exec(fmt.Sprintf("DROP TABLE %s", EscapeIdentifier(name))); err != nil {
			return err
		}
	}
	return err
}

// DefaultCharSetAndCollation returns the instance's default character set and
// collation
func (instance *Instance) DefaultCharSetAndCollation() (serverCharSet, serverCollation string, err error) {
	db, err := instance.Connect("information_schema", "")
	if err != nil {
		return
	}
	err = db.QueryRow("SELECT @@global.character_set_server, @@global.collation_server").Scan(&serverCharSet, &serverCollation)
	return
}

// StrictModeCompliant returns true if all tables in the supplied schemas,
// if re-created on instance, would comply with innodb_strict_mode and a
// sql_mode including STRICT_TRANS_TABLES,NO_ZERO_DATE.
// This method does not currently detect invalid-but-nonzero dates in default
// values, although it may in the future.
func (instance *Instance) StrictModeCompliant(schemas []*Schema) (bool, error) {
	var hasFilePerTable, hasBarracuda, alreadyPopulated bool
	getFormatVars := func() (fpt, barracuda bool, err error) {
		if alreadyPopulated {
			return hasFilePerTable, hasBarracuda, nil
		}
		db, err := instance.Connect("", "")
		if err != nil {
			return false, false, err
		}
		var ifpt, iff string
		if instance.Flavor().HasInnoFileFormat() {
			err = db.QueryRow("SELECT @@global.innodb_file_per_table, @@global.innodb_file_format").Scan(&ifpt, &iff)
			hasBarracuda = (strings.ToLower(iff) == "barracuda")
		} else {
			err = db.QueryRow("SELECT @@global.innodb_file_per_table").Scan(&ifpt)
			hasBarracuda = true
		}
		hasFilePerTable = (ifpt == "1")
		alreadyPopulated = (err == nil)
		return hasFilePerTable, hasBarracuda, err
	}

	for _, s := range schemas {
		for _, t := range s.Tables {
			for _, c := range t.Columns {
				if strings.HasPrefix(c.TypeInDB, "timestamp") || strings.HasPrefix(c.TypeInDB, "date") {
					if strings.HasPrefix(c.Default.Value, "0000-00-00") {
						return false, nil
					}
				}
			}
			if format := t.RowFormatClause(); format != "" {
				needFilePerTable, needBarracuda := instance.Flavor().InnoRowFormatReqs(format)
				if needFilePerTable || needBarracuda {
					haveFilePerTable, haveBarracuda, err := getFormatVars()
					if err != nil {
						return false, err
					}
					if (needFilePerTable && !haveFilePerTable) || (needBarracuda && !haveBarracuda) {
						return false, nil
					}
				}
			}
		}
	}
	return true, nil
}

var reExtraOnUpdate = regexp.MustCompile(`(?i)\bon update (current_timestamp(?:\(\d*\))?)`)

func (instance *Instance) querySchemaTables(schema string) ([]*Table, error) {
	db, err := instance.Connect("information_schema", "")
	if err != nil {
		return nil, err
	}

	// Obtain flavor and version info. MariaDB changed how default values are
	// represented in information_schema in 10.2+.
	flavor := instance.Flavor()

	// Note on these queries: MySQL 8.0 changes information_schema column names to
	// come back from queries in all caps, so we need to explicitly use AS clauses
	// in order to get them back as lowercase and have sqlx Select() work

	// Obtain the tables in the schema
	var rawTables []struct {
		Name               string         `db:"table_name"`
		Type               string         `db:"table_type"`
		Engine             sql.NullString `db:"engine"`
		AutoIncrement      sql.NullInt64  `db:"auto_increment"`
		TableCollation     sql.NullString `db:"table_collation"`
		CreateOptions      sql.NullString `db:"create_options"`
		Comment            string         `db:"table_comment"`
		CharSet            string         `db:"character_set_name"`
		CollationIsDefault string         `db:"is_default"`
	}
	query := `
		SELECT t.table_name AS table_name, t.table_type AS table_type, t.engine AS engine,
		       t.auto_increment AS auto_increment, t.table_collation AS table_collation,
		       UPPER(t.create_options) AS create_options, t.table_comment AS table_comment,
		       c.character_set_name AS character_set_name, c.is_default AS is_default
		FROM   tables t
		JOIN   collations c ON t.table_collation = c.collation_name
		WHERE  t.table_schema = ?
		AND    t.table_type = 'BASE TABLE'`
	if err := db.Select(&rawTables, query, schema); err != nil {
		return nil, fmt.Errorf("Error querying information_schema.tables for schema %s: %s", schema, err)
	}
	if len(rawTables) == 0 {
		return []*Table{}, nil
	}
	tables := make([]*Table, len(rawTables))
	for n, rawTable := range rawTables {
		tables[n] = &Table{
			Name:               rawTable.Name,
			Engine:             rawTable.Engine.String,
			CharSet:            rawTable.CharSet,
			Collation:          rawTable.TableCollation.String,
			CollationIsDefault: rawTable.CollationIsDefault != "",
			Comment:            rawTable.Comment,
		}
		if rawTable.AutoIncrement.Valid {
			tables[n].NextAutoIncrement = uint64(rawTable.AutoIncrement.Int64)
		}
		if rawTable.CreateOptions.Valid && rawTable.CreateOptions.String != "" && rawTable.CreateOptions.String != "PARTITIONED" {
			// information_schema.tables.create_options annoyingly contains "partitioned"
			// if the table is partitioned, despite this not being present as-is in the
			// table table definition. All other create_options are present verbatim.
			// Currently in mysql-server/sql/sql_show.cc, it's always at the *end* of
			// create_options... but just to code defensively we handle any location.
			if strings.HasPrefix(rawTable.CreateOptions.String, "PARTITIONED ") {
				tables[n].CreateOptions = strings.Replace(rawTable.CreateOptions.String, "PARTITIONED ", "", 1)
			} else {
				tables[n].CreateOptions = strings.Replace(rawTable.CreateOptions.String, " PARTITIONED", "", 1)
			}
		}
	}

	// Obtain the columns in all tables in the schema
	var rawColumns []struct {
		Name               string         `db:"column_name"`
		TableName          string         `db:"table_name"`
		Type               string         `db:"column_type"`
		IsNullable         string         `db:"is_nullable"`
		Default            sql.NullString `db:"column_default"`
		Extra              string         `db:"extra"`
		Comment            string         `db:"column_comment"`
		CharSet            sql.NullString `db:"character_set_name"`
		Collation          sql.NullString `db:"collation_name"`
		CollationIsDefault sql.NullString `db:"is_default"`
	}
	query = `
		SELECT    c.table_name AS table_name, c.column_name AS column_name,
		          c.column_type AS column_type, c.is_nullable AS is_nullable,
		          c.column_default AS column_default, c.extra AS extra,
		          c.column_comment AS column_comment,
		          c.character_set_name AS character_set_name,
		          c.collation_name AS collation_name, co.is_default AS is_default
		FROM      columns c
		LEFT JOIN collations co ON co.collation_name = c.collation_name
		WHERE     c.table_schema = ?
		ORDER BY  c.table_name, c.ordinal_position`
	if err := db.Select(&rawColumns, query, schema); err != nil {
		return nil, fmt.Errorf("Error querying information_schema.columns for schema %s: %s", schema, err)
	}
	columnsByTableName := make(map[string][]*Column)
	columnsByTableAndName := make(map[string]*Column)
	for _, rawColumn := range rawColumns {
		col := &Column{
			Name:          rawColumn.Name,
			TypeInDB:      rawColumn.Type,
			Nullable:      strings.ToUpper(rawColumn.IsNullable) == "YES",
			AutoIncrement: strings.Contains(rawColumn.Extra, "auto_increment"),
			Comment:       rawColumn.Comment,
		}
		if !rawColumn.Default.Valid {
			col.Default = ColumnDefaultNull
		} else if flavor.VendorMinVersion(VendorMariaDB, 10, 2) {
			if rawColumn.Default.String[0] == '\'' {
				col.Default = ColumnDefaultValue(strings.Trim(rawColumn.Default.String, "'"))
			} else if rawColumn.Default.String == "NULL" {
				col.Default = ColumnDefaultNull
			} else {
				col.Default = ColumnDefaultExpression(rawColumn.Default.String)
			}
		} else if strings.HasPrefix(rawColumn.Default.String, "CURRENT_TIMESTAMP") && (strings.HasPrefix(rawColumn.Type, "timestamp") || strings.HasPrefix(rawColumn.Type, "datetime")) {
			col.Default = ColumnDefaultExpression(rawColumn.Default.String)
		} else if strings.HasPrefix(rawColumn.Type, "bit") && strings.HasPrefix(rawColumn.Default.String, "b'") {
			col.Default = ColumnDefaultExpression(rawColumn.Default.String)
		} else if strings.Contains(rawColumn.Extra, "DEFAULT_GENERATED") && strings.HasPrefix(rawColumn.Default.String, "(") {
			// MySQL/Percona 8.0.13+ added default expressions, which are single-paren-
			// wrapped in information_schema, but double-paren-wrapped in SHOW CREATE
			col.Default = ColumnDefaultExpression(fmt.Sprintf("(%s)", rawColumn.Default.String))
		} else {
			col.Default = ColumnDefaultValue(rawColumn.Default.String)
		}
		if matches := reExtraOnUpdate.FindStringSubmatch(rawColumn.Extra); matches != nil {
			col.OnUpdate = matches[1]
			// Some flavors omit fractional precision from ON UPDATE in
			// information_schema only, despite it being present everywhere else
			if openParen := strings.IndexByte(rawColumn.Type, '('); openParen > -1 && !strings.Contains(col.OnUpdate, "(") {
				col.OnUpdate = fmt.Sprintf("%s%s", col.OnUpdate, rawColumn.Type[openParen:])
			}
		}
		if rawColumn.Collation.Valid { // only text-based column types have a notion of charset and collation
			col.CharSet = rawColumn.CharSet.String
			col.Collation = rawColumn.Collation.String
			col.CollationIsDefault = (rawColumn.CollationIsDefault.String != "")
		}
		if columnsByTableName[rawColumn.TableName] == nil {
			columnsByTableName[rawColumn.TableName] = make([]*Column, 0)
		}
		columnsByTableName[rawColumn.TableName] = append(columnsByTableName[rawColumn.TableName], col)
		fullNameStr := fmt.Sprintf("%s.%s.%s", schema, rawColumn.TableName, rawColumn.Name)
		columnsByTableAndName[fullNameStr] = col
	}
	for n, t := range tables {
		// Put the columns into the table
		tables[n].Columns = columnsByTableName[t.Name]

		// Avoid issues from data dictionary weirdly caching a NULL next auto-inc
		if t.NextAutoIncrement == 0 && t.HasAutoIncrement() {
			tables[n].NextAutoIncrement = 1
		}
	}

	// Obtain the indexes of all tables in the schema. Since multi-column indexes
	// have multiple rows in the result set, we do two passes over the result: one
	// to figure out which indexes exist, and one to stitch together the col info.
	// We cannot use an ORDER BY on this query, since only the unsorted result
	// matches the same order of secondary indexes as the CREATE TABLE statement.
	var rawIndexes []struct {
		Name       string         `db:"index_name"`
		TableName  string         `db:"table_name"`
		NonUnique  uint8          `db:"non_unique"`
		SeqInIndex uint8          `db:"seq_in_index"`
		ColumnName string         `db:"column_name"`
		SubPart    sql.NullInt64  `db:"sub_part"`
		Comment    sql.NullString `db:"index_comment"`
	}
	query = `
		SELECT   index_name AS index_name, table_name AS table_name,
		         non_unique AS non_unique, seq_in_index AS seq_in_index,
		         column_name AS column_name, sub_part AS sub_part,
		         index_comment AS index_comment
		FROM     statistics
		WHERE    table_schema = ?`
	if err := db.Select(&rawIndexes, query, schema); err != nil {
		return nil, fmt.Errorf("Error querying information_schema.statistics for schema %s: %s", schema, err)
	}
	primaryKeyByTableName := make(map[string]*Index)
	secondaryIndexesByTableName := make(map[string][]*Index)
	indexesByTableAndName := make(map[string]*Index)
	for _, rawIndex := range rawIndexes {
		if rawIndex.SeqInIndex > 1 {
			continue
		}
		index := &Index{
			Name:     rawIndex.Name,
			Unique:   rawIndex.NonUnique == 0,
			Columns:  make([]*Column, 0),
			SubParts: make([]uint16, 0),
			Comment:  rawIndex.Comment.String,
		}
		if strings.ToUpper(index.Name) == "PRIMARY" {
			primaryKeyByTableName[rawIndex.TableName] = index
			index.PrimaryKey = true
		} else {
			if secondaryIndexesByTableName[rawIndex.TableName] == nil {
				secondaryIndexesByTableName[rawIndex.TableName] = make([]*Index, 0)
			}
			secondaryIndexesByTableName[rawIndex.TableName] = append(secondaryIndexesByTableName[rawIndex.TableName], index)
		}
		fullNameStr := fmt.Sprintf("%s.%s.%s", schema, rawIndex.TableName, rawIndex.Name)
		indexesByTableAndName[fullNameStr] = index
	}
	for _, rawIndex := range rawIndexes {
		fullIndexNameStr := fmt.Sprintf("%s.%s.%s", schema, rawIndex.TableName, rawIndex.Name)
		index, ok := indexesByTableAndName[fullIndexNameStr]
		if !ok {
			panic(fmt.Errorf("Cannot find index %s", fullIndexNameStr))
		}
		fullColNameStr := fmt.Sprintf("%s.%s.%s", schema, rawIndex.TableName, rawIndex.ColumnName)
		col, ok := columnsByTableAndName[fullColNameStr]
		if !ok {
			panic(fmt.Errorf("Cannot find indexed column %s for index %s", fullColNameStr, fullIndexNameStr))
		}
		for len(index.Columns) < int(rawIndex.SeqInIndex) {
			index.Columns = append(index.Columns, new(Column))
			index.SubParts = append(index.SubParts, 0)
		}
		index.Columns[rawIndex.SeqInIndex-1] = col
		if rawIndex.SubPart.Valid {
			index.SubParts[rawIndex.SeqInIndex-1] = uint16(rawIndex.SubPart.Int64)
		}
	}
	for _, t := range tables {
		t.PrimaryKey = primaryKeyByTableName[t.Name]
		t.SecondaryIndexes = secondaryIndexesByTableName[t.Name]
	}

	// Obtain the foreign keys of the tables in the schema
	var rawForeignKeys []struct {
		Name                 string `db:"constraint_name"`
		TableName            string `db:"table_name"`
		UpdateRule           string `db:"update_rule"`
		DeleteRule           string `db:"delete_rule"`
		ReferencedTableName  string `db:"referenced_table_name"`
		ReferencedSchemaName string `db:"referenced_schema"`
		ReferencedColumnName string `db:"referenced_column_name"`
		ColumnLookupKey      string `db:"col_lookup_key"`
	}
	query = `
		SELECT   rc.constraint_name AS constraint_name, rc.table_name AS table_name,
		         rc.update_rule AS update_rule, rc.delete_rule AS delete_rule,
		         rc.referenced_table_name AS referenced_table_name,
		         IF(rc.constraint_schema=rc.unique_constraint_schema, '', rc.unique_constraint_schema) AS referenced_schema,
		         kcu.referenced_column_name AS referenced_column_name,
		         CONCAT(kcu.constraint_schema, '.', kcu.table_name, '.', kcu.column_name) AS col_lookup_key
		FROM     referential_constraints rc
		JOIN     key_column_usage kcu ON kcu.constraint_name = rc.constraint_name AND
		                                 kcu.table_schema = ? AND
		                                 kcu.referenced_column_name IS NOT NULL
		WHERE    rc.constraint_schema = ?
		ORDER BY BINARY rc.constraint_name, kcu.ordinal_position`
	if err := db.Select(&rawForeignKeys, query, schema, schema); err != nil {
		return nil, fmt.Errorf("Error querying foreign key constraints for schema %s: %s", schema, err)
	}
	foreignKeysByTableName := make(map[string][]*ForeignKey)
	foreignKeysByName := make(map[string]*ForeignKey)
	for _, rawForeignKey := range rawForeignKeys {
		col := columnsByTableAndName[rawForeignKey.ColumnLookupKey]
		if fk, already := foreignKeysByName[rawForeignKey.Name]; already {
			fk.Columns = append(fk.Columns, col)
			fk.ReferencedColumnNames = append(fk.ReferencedColumnNames, rawForeignKey.ReferencedColumnName)
		} else {
			foreignKey := &ForeignKey{
				Name:                  rawForeignKey.Name,
				ReferencedSchemaName:  rawForeignKey.ReferencedSchemaName,
				ReferencedTableName:   rawForeignKey.ReferencedTableName,
				UpdateRule:            rawForeignKey.UpdateRule,
				DeleteRule:            rawForeignKey.DeleteRule,
				Columns:               []*Column{col},
				ReferencedColumnNames: []string{rawForeignKey.ReferencedColumnName},
			}
			foreignKeysByName[rawForeignKey.Name] = foreignKey
			foreignKeysByTableName[rawForeignKey.TableName] = append(foreignKeysByTableName[rawForeignKey.TableName], foreignKey)
		}
	}
	for _, t := range tables {
		t.ForeignKeys = foreignKeysByTableName[t.Name]
	}

	// Obtain actual SHOW CREATE TABLE output and store in each table. Since
	// there's no way in MySQL to bulk fetch this for multiple tables at once,
	// use multiple goroutines to make this faster.
	db, err = instance.Connect(schema, "")
	if err != nil {
		return nil, err
	}
	defer db.SetMaxOpenConns(0)
	db.SetMaxOpenConns(10)
	var g errgroup.Group
	for _, t := range tables {
		t := t
		g.Go(func() (err error) {
			if t.CreateStatement, err = showCreateTable(db, t.Name); err != nil {
				return fmt.Errorf("Error executing SHOW CREATE TABLE for %s.%s: %s", EscapeIdentifier(schema), EscapeIdentifier(t.Name), err)
			}
			if t.Engine == "InnoDB" {
				t.CreateStatement = NormalizeCreateOptions(t.CreateStatement)
			}
			// Index order is unpredictable with new MySQL 8 data dictionary, so reorder
			// indexes based on parsing SHOW CREATE TABLE if needed
			if flavor.HasDataDictionary() && len(t.SecondaryIndexes) > 1 {
				fixIndexOrder(t)
			}
			// Foreign keys order is unpredictable in MySQL before 5.6, so reorder
			// foreign keys based on parsing SHOW CREATE TABLE if needed
			if !flavor.SortedForeignKeys() && len(t.ForeignKeys) > 1 {
				fixForeignKeyOrder(t)
			}
			// Compare what we expect the create DDL to be, to determine if we support
			// diffing for the table. Ignore next-auto-increment differences in this
			// comparison, since the value may have changed between our previous
			// information_schema introspection and our current SHOW CREATE TABLE call!
			actual, _ := ParseCreateAutoInc(t.CreateStatement)
			expected, _ := ParseCreateAutoInc(t.GeneratedCreateStatement(flavor))
			if actual != expected {
				t.UnsupportedDDL = true
			}
			return nil
		})
	}
	return tables, g.Wait()
}

var reIndexLine = regexp.MustCompile("^\\s+(?:UNIQUE )?KEY `((?:[^`]|``)+)` \\(`")

func fixIndexOrder(t *Table) {
	byName := t.SecondaryIndexesByName()
	t.SecondaryIndexes = make([]*Index, len(byName))
	var cur int
	for _, line := range strings.Split(t.CreateStatement, "\n") {
		matches := reIndexLine.FindStringSubmatch(line)
		if matches == nil {
			continue
		}
		t.SecondaryIndexes[cur] = byName[matches[1]]
		cur++
	}
}

var reForeignKeyLine = regexp.MustCompile("^\\s+CONSTRAINT `((?:[^`]|``)+)` FOREIGN KEY")

func fixForeignKeyOrder(t *Table) {
	byName := t.foreignKeysByName()
	t.ForeignKeys = make([]*ForeignKey, len(byName))
	var cur int
	for _, line := range strings.Split(t.CreateStatement, "\n") {
		matches := reForeignKeyLine.FindStringSubmatch(line)
		if matches == nil {
			continue
		}
		t.ForeignKeys[cur] = byName[matches[1]]
		cur++
	}
}

func (instance *Instance) querySchemaRoutines(schema string) ([]*Routine, error) {
	db, err := instance.Connect("information_schema", "")
	if err != nil {
		return nil, err
	}

	// Obtain the routines in the schema
	// We completely exclude routines that the user can call, but not examine --
	// e.g. user has EXECUTE priv but missing other vital privs. In this case
	// routine_definition will be NULL.
	// Note on this query: MySQL 8.0 changes information_schema column names to
	// come back from queries in all caps, so we need to explicitly use AS clauses
	// in order to get them back as lowercase and have sqlx Select() work
	var rawRoutines []struct {
		Name              string         `db:"routine_name"`
		Type              string         `db:"routine_type"`
		Body              sql.NullString `db:"routine_definition"`
		IsDeterministic   string         `db:"is_deterministic"`
		SQLDataAccess     string         `db:"sql_data_access"`
		SecurityType      string         `db:"security_type"`
		SQLMode           string         `db:"sql_mode"`
		Comment           string         `db:"routine_comment"`
		Definer           string         `db:"definer"`
		DatabaseCollation string         `db:"database_collation"`
	}
	query := `
		SELECT r.routine_name AS routine_name, UPPER(r.routine_type) AS routine_type,
		       r.routine_definition AS routine_definition,
		       UPPER(r.is_deterministic) AS is_deterministic,
		       UPPER(r.sql_data_access) AS sql_data_access,
		       UPPER(r.security_type) AS security_type,
		       r.sql_mode AS sql_mode, r.routine_comment AS routine_comment,
		       r.definer AS definer, r.database_collation AS database_collation
		FROM   routines r
		WHERE  r.routine_schema = ? AND routine_definition IS NOT NULL`
	if err := db.Select(&rawRoutines, query, schema); err != nil {
		return nil, fmt.Errorf("Error querying information_schema.routines for schema %s: %s", schema, err)
	}
	if len(rawRoutines) == 0 {
		return []*Routine{}, nil
	}
	routines := make([]*Routine, len(rawRoutines))
	dict := make(map[ObjectKey]*Routine, len(rawRoutines))
	for n, rawRoutine := range rawRoutines {
		routines[n] = &Routine{
			Name:              rawRoutine.Name,
			Type:              ObjectType(strings.ToLower(rawRoutine.Type)),
			Body:              rawRoutine.Body.String, // This contains incorrect formatting conversions; overwritten later
			Definer:           rawRoutine.Definer,
			DatabaseCollation: rawRoutine.DatabaseCollation,
			Comment:           rawRoutine.Comment,
			Deterministic:     rawRoutine.IsDeterministic == "YES",
			SQLDataAccess:     rawRoutine.SQLDataAccess,
			SecurityType:      rawRoutine.SecurityType,
			SQLMode:           rawRoutine.SQLMode,
		}
		if routines[n].Type != ObjectTypeProc && routines[n].Type != ObjectTypeFunc {
			return nil, fmt.Errorf("Unsupported routine type %s found in %s.%s", rawRoutine.Type, schema, rawRoutine.Name)
		}
		key := ObjectKey{Type: routines[n].Type, Name: routines[n].Name}
		dict[key] = routines[n]
	}

	// Obtain param string, return type string, and full create statement:
	// We can't rely only on information_schema, since it doesn't have the param
	// string formatted in the same way as the original CREATE, nor does
	// routines.body handle strings/charsets correctly for re-runnable SQL.
	// In flavors without the new data dictionary, we first try querying mysql.proc
	// to bulk-fetch sufficient info to rebuild the CREATE without needing to run
	// a SHOW CREATE per routine.
	// If mysql.proc doesn't exist or that query fails, we then run a SHOW CREATE
	// per routine, using multiple goroutines for performance reasons.
	db, err = instance.Connect(schema, "")
	if err != nil {
		return nil, err
	}
	if !instance.Flavor().HasDataDictionary() {
		var rawRoutineMeta []struct {
			Name      string `db:"name"`
			Type      string `db:"type"`
			Body      string `db:"body"`
			ParamList string `db:"param_list"`
			Returns   string `db:"returns"`
		}
		query := `
			SELECT name, type, body, param_list, returns
			FROM   mysql.proc
			WHERE  db = ?`
		// Errors here are non-fatal. No need to even check; slice will be empty which is fine
		db.Select(&rawRoutineMeta, query, schema)
		for _, meta := range rawRoutineMeta {
			key := ObjectKey{Type: ObjectType(strings.ToLower(meta.Type)), Name: meta.Name}
			if routine, ok := dict[key]; ok {
				routine.ParamString = meta.ParamList
				routine.ReturnDataType = meta.Returns
				routine.Body = strings.Replace(meta.Body, "\r\n", "\n", -1)
				routine.CreateStatement = routine.Definition(instance.Flavor())
			}
		}
	}
	defer db.SetMaxOpenConns(0)
	db.SetMaxOpenConns(10)
	var g errgroup.Group
	for _, r := range routines {
		if r.CreateStatement != "" {
			continue // already hydrated from mysql.proc query above
		}
		r := r
		g.Go(func() (err error) {
			if r.CreateStatement, err = showCreateRoutine(db, r.Name, r.Type); err != nil {
				return fmt.Errorf("Error executing SHOW CREATE %s for %s.%s: %s", r.Type.Caps(), EscapeIdentifier(schema), EscapeIdentifier(r.Name), err)
			}
			r.CreateStatement = strings.Replace(r.CreateStatement, "\r\n", "\n", -1)
			var returnsClause string
			if r.Type == ObjectTypeFunc {
				returnsClause = " RETURNS ([^\n]+)"
			}
			reTemplate := fmt.Sprintf("^CREATE[^\n]* %s %s\\(([^\n]*)\\)%s\n", r.Type.Caps(), EscapeIdentifier(r.Name), returnsClause)
			var reCreateRoutine = regexp.MustCompile(reTemplate)
			matches := reCreateRoutine.FindStringSubmatch(r.CreateStatement)
			if matches == nil {
				return fmt.Errorf("Failed to parse SHOW CREATE %s %s.%s: %s", r.Type.Caps(), EscapeIdentifier(schema), EscapeIdentifier(r.Name), r.CreateStatement)
			}
			r.ParamString = matches[1]
			if r.Type == ObjectTypeFunc {
				r.ReturnDataType = matches[2]
			}
			// Attempt to replace r.Body with one that doesn't have character conversion problems
			if header := r.head(instance.Flavor()); strings.HasPrefix(r.CreateStatement, header) {
				r.Body = r.CreateStatement[len(header):]
			}
			return nil
		})
	}
	return routines, g.Wait()
}

func showCreateRoutine(db *sqlx.DB, routine string, ot ObjectType) (create string, err error) {
	query := fmt.Sprintf("SHOW CREATE %s %s", ot.Caps(), EscapeIdentifier(routine))
	if ot == ObjectTypeProc {
		var createRows []struct {
			CreateStatement sql.NullString `db:"Create Procedure"`
		}
		err = db.Select(&createRows, query)
		if (err == nil && len(createRows) != 1) || IsDatabaseError(err, mysqlerr.ER_SP_DOES_NOT_EXIST) {
			err = sql.ErrNoRows
		} else if err == nil {
			create = createRows[0].CreateStatement.String
		}
	} else if ot == ObjectTypeFunc {
		var createRows []struct {
			CreateStatement sql.NullString `db:"Create Function"`
		}
		err = db.Select(&createRows, query)
		if (err == nil && len(createRows) != 1) || IsDatabaseError(err, mysqlerr.ER_SP_DOES_NOT_EXIST) {
			err = sql.ErrNoRows
		} else if err == nil {
			create = createRows[0].CreateStatement.String
		}
	} else {
		err = fmt.Errorf("Object type %s is not a routine", ot)
	}
	return
}
