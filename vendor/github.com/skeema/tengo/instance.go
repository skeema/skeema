package tengo

import (
	"database/sql"
	"fmt"
	"net/url"
	"reflect"
	"strings"
	"sync"

	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
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
	schemas        []*Schema
	connectionPool map[string]*sqlx.DB // key is in format "schema?params" or just "schema" if no params
	*sync.RWMutex                      // protects internal state
}

var allInstances struct {
	sync.Mutex
	byDSN map[string]*Instance
}

func init() {
	allInstances.byDSN = make(map[string]*Instance)
}

// NewInstance returns a pointer to a new Instance corresponding to the
// supplied driver and dsn. Currently only "mysql" driver is supported.
// dsn should be formatted according to driver specifications. If it contains
// a schema name, it will be ignored. If it contains any params, they will be
// applied as default params to all connections (in addition to whatever is
// supplied in Connect).
// If an Instance with the supplied dsn has already been created previously,
// it will be returned instead of a new Instance being created. This
// deduplication is necessary in order for Instance's internal caching to work
// properly.
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

	// See if an instance with the supplied dsn already exists. Note that we forbid
	// creating a duplicate instance that has a different user, pass, or default
	// params; having multiple Instances that refer to the same underlying DB
	// server instance would break caching logic.
	// TODO: permit changing the username, password, and/or params of an existing
	// instance through another set of methods
	allInstances.Lock()
	defer allInstances.Unlock()
	instance, already := allInstances.byDSN[base]
	if already {
		if instance.User != parsedConfig.User {
			return nil, fmt.Errorf("Instance already exists, but with different username")
		} else if instance.Password != parsedConfig.Passwd {
			return nil, fmt.Errorf("Instance already exists, but with different password")
		} else if !reflect.DeepEqual(instance.defaultParams, params) {
			return nil, fmt.Errorf("Instance already exists, but with different default params")
		}
		return instance, nil
	}

	instance = &Instance{
		BaseDSN:        base,
		Driver:         driver,
		User:           parsedConfig.User,
		Password:       parsedConfig.Passwd,
		defaultParams:  params,
		connectionPool: make(map[string]*sqlx.DB),
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

	allInstances.byDSN[base] = instance
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
func (instance *Instance) Connect(defaultSchema string, params string) (*sqlx.DB, error) {
	key := fmt.Sprintf("%s?%s", defaultSchema, instance.buildParamString(params))

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

	instance.Lock()
	defer instance.Unlock()
	instance.connectionPool[key] = db.Unsafe()
	return instance.connectionPool[key], nil
}

// CanConnect verifies that the Instance can be connected to
func (instance *Instance) CanConnect() (bool, error) {
	instance.Lock()
	delete(instance.connectionPool, "?") // ensure we're initializing a new conn pool for schemalass, paramless use
	instance.Unlock()

	_, err := instance.Connect("", "")
	return err == nil, err
}

// Schemas returns a slice of all schemas on the instance visible to the user.
func (instance *Instance) Schemas() ([]*Schema, error) {
	instance.RLock()
	ret := instance.schemas
	instance.RUnlock()
	if ret != nil {
		return ret, nil
	}

	db, err := instance.Connect("information_schema", "")
	if err != nil {
		return nil, err
	}

	instance.Lock()
	defer instance.Unlock()

	var rawSchemas []struct {
		Name      string `db:"schema_name"`
		CharSet   string `db:"default_character_set_name"`
		Collation string `db:"default_collation_name"`
	}
	query := `
		SELECT schema_name, default_character_set_name, default_collation_name
		FROM   schemata
		WHERE  schema_name NOT IN ('information_schema', 'performance_schema', 'mysql', 'test', 'sys')`
	if err := db.Select(&rawSchemas, query); err != nil {
		return nil, err
	}

	instance.schemas = make([]*Schema, len(rawSchemas))
	for n, rawSchema := range rawSchemas {
		instance.schemas[n] = &Schema{
			Name:      rawSchema.Name,
			CharSet:   rawSchema.CharSet,
			Collation: rawSchema.Collation,
			instance:  instance,
		}
	}
	return instance.schemas, nil
}

// SchemasByName returns a map of schema name string to *Schema, for all schemas
// that exist on the instance.
func (instance *Instance) SchemasByName() (map[string]*Schema, error) {
	schemas, err := instance.Schemas()
	if err != nil {
		return nil, err
	}
	result := make(map[string]*Schema, len(schemas))
	for _, s := range schemas {
		result[s.Name] = s
	}
	return result, nil
}

// Schema returns a single schema by name.
func (instance *Instance) Schema(name string) (*Schema, error) {
	byName, err := instance.SchemasByName()
	if err != nil {
		return nil, err
	}
	return byName[name], nil
}

// HasSchema returns true if this instance has a schema with the supplied name
// visible to the user, or false otherwise.
func (instance *Instance) HasSchema(name string) bool {
	s, _ := instance.Schema(name)
	return s != nil
}

// ShowCreateTable returns a string with a CREATE TABLE statement, representing
// how the instance views the specified table as having been created.
func (instance *Instance) ShowCreateTable(schema *Schema, table *Table) (string, error) {
	db, err := instance.Connect(schema.Name, "")
	if err != nil {
		return "", err
	}

	var createRows []struct {
		TableName       string `db:"Table"`
		CreateStatement string `db:"Create Table"`
	}
	query := fmt.Sprintf("SHOW CREATE TABLE %s", EscapeIdentifier(table.Name))
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
func (instance *Instance) TableSize(schema *Schema, table *Table) (int64, error) {
	var result int64
	db, err := instance.Connect("information_schema", "")
	if err != nil {
		return 0, err
	}
	err = db.Get(&result, `
		SELECT  data_length + index_length + data_free
		FROM    tables
		WHERE   table_schema = ? and table_name = ?`,
		schema.Name, table.Name)
	return result, err
}

// TableHasRows returns true if the table has at least one row. If an error
// occurs in querying, also returns true (along with the error) since a false
// positive is generally less dangerous in this case than a false negative.
func (instance *Instance) TableHasRows(schema *Schema, table *Table) (bool, error) {
	db, err := instance.Connect(schema.Name, "")
	if err != nil {
		return true, err
	}
	var result []int
	query := fmt.Sprintf("SELECT 1 FROM %s LIMIT 1", EscapeIdentifier(table.Name))
	if err := db.Select(&result, query); err != nil {
		return true, err
	}
	return len(result) != 0, nil
}

func (instance *Instance) purgeSchemaCache() {
	instance.Lock()
	instance.schemas = nil
	instance.Unlock()
}

// CreateSchema creates a new database schema with the supplied name, and
// optionally the supplied default charSet and collation. (Leave charSet and
// collation blank to use server defaults.)
func (instance *Instance) CreateSchema(name, charSet, collation string) (*Schema, error) {
	db, err := instance.Connect("", "")
	if err != nil {
		return nil, err
	}
	schema := Schema{
		Name:      name,
		CharSet:   charSet,
		Collation: collation,
	}
	_, err = db.Exec(schema.CreateStatement())
	if err != nil {
		return nil, err
	}

	// Purge schema cache; next call to Schema will repopulate
	instance.purgeSchemaCache()
	return instance.Schema(name)
}

// DropSchema first drops all tables in the schema, and then drops the database
// schema itself. If onlyIfEmpty==true, returns an error if any of the tables
// have any rows.
func (instance *Instance) DropSchema(schema *Schema, onlyIfEmpty bool) error {
	err := instance.DropTablesInSchema(schema, onlyIfEmpty)
	if err != nil {
		return err
	}

	db, err := instance.Connect(schema.Name, "")
	if err != nil {
		return err
	}
	_, err = db.Exec(schema.DropStatement())
	if err != nil {
		return err
	}

	prefix := fmt.Sprintf("%s?", schema.Name)
	instance.Lock()
	for key, connPool := range instance.connectionPool {
		if strings.HasPrefix(key, prefix) {
			connPool.Close()
			delete(instance.connectionPool, key)
		}
	}
	instance.Unlock()

	// Purge schema cache; next call to Schema will repopulate
	instance.purgeSchemaCache()
	return nil
}

// AlterSchema changes the character set and/or collation of the supplied schema
// on instance. Supply an empty string for newCharSet to only change the
// collation, or supply an empty string for newCollation to use the default
// collation of newCharSet. (Supplying an empty string for both is also allowed,
// but is a no-op.)
func (instance *Instance) AlterSchema(schema *Schema, newCharSet, newCollation string) error {
	db, err := instance.Connect(schema.Name, "")
	if err != nil {
		return err
	}
	statement := schema.AlterStatement(newCharSet, newCollation)
	if statement == "" {
		return nil
	}
	if _, err = db.Exec(statement); err != nil {
		return err
	}

	// Purge schema cache, so that the call to Schema will repopulate with new
	// charset and collation. (We can't just set them directly without querying
	// since default-collation-for-charset info is handled by the database.)
	instance.purgeSchemaCache()
	alteredSchema, err := instance.Schema(schema.Name)
	if err == nil {
		*schema = *alteredSchema
	}
	return err
}

// DropTablesInSchema drops all tables in a schema. If onlyIfEmpty==true,
// returns an error if any of the tables have any rows.
func (instance *Instance) DropTablesInSchema(schema *Schema, onlyIfEmpty bool) error {
	db, err := instance.Connect(schema.Name, "foreign_key_checks=0")
	if err != nil {
		return err
	}
	tables, err := schema.Tables()
	if err != nil {
		return err
	}

	if onlyIfEmpty {
		for _, t := range tables {
			hasRows, err := instance.TableHasRows(schema, t)
			if err != nil {
				return err
			}
			if hasRows {
				return fmt.Errorf("DropTablesInSchema: table %s.%s has at least one row", EscapeIdentifier(schema.Name), EscapeIdentifier(t.Name))
			}
		}
	}

	for _, t := range tables {
		_, err := db.Exec(t.DropStatement())
		if err != nil {
			return err
		}
	}

	schema.PurgeTableCache()
	return nil
}

// CloneSchema copies all tables (just definitions, not data) from src to dest.
// Ideally dest should be an empty schema, or at least be pre-verified for not
// having existing tables with conflicting names, but this is the caller's
// responsibility to confirm.
func (instance *Instance) CloneSchema(src, dest *Schema) error {
	db, err := instance.Connect(dest.Name, "foreign_key_checks=0")
	if err != nil {
		return err
	}
	tables, err := src.Tables()
	if err != nil {
		return err
	}
	for _, t := range tables {
		_, err := db.Exec(t.CreateStatement())
		if err != nil {
			return err
		}
	}
	dest.PurgeTableCache()
	return nil
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

// baseDSN returns a DSN with the database (schema) name and params stripped.
// Currently only supports MySQL, via go-sql-driver/mysql's DSN format.
func baseDSN(dsn string) string {
	tokens := strings.SplitAfter(dsn, "/")
	return strings.Join(tokens[0:len(tokens)-1], "")
}

// paramMap builds a map representing all params in the DSN.
// This does not rely on mysql.ParseDSN because that handles some vars
// separately; i.e. mysql.Config's params field does NOT include all
// params that are passed in!
func paramMap(dsn string) map[string]string {
	parts := strings.Split(dsn, "?")
	if len(parts) == 1 {
		return make(map[string]string)
	}
	params := parts[len(parts)-1]
	values, _ := url.ParseQuery(params)

	// Convert values, which is map[string][]string, to single-valued map[string]string
	// i.e. if a param is present multiple times, we only keep the first value
	result := make(map[string]string, len(values))
	for key := range values {
		result[key] = values.Get(key)
	}
	return result
}
