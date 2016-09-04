package tengo

import (
	"database/sql"
	"fmt"
	"net/url"
	"strings"

	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

type Instance struct {
	BaseDSN        string // DSN ending in trailing slash; i.e. no schema name or params
	Driver         string
	User           string
	Password       string
	Host           string
	Port           int
	SocketPath     string
	DefaultParams  map[string]string
	schemas        []*Schema
	connectionPool map[string]*sqlx.DB // key is in format "schema?params" or just "schema" if no params
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
	parsedConfig, err := mysql.ParseDSN(dsn)
	if err != nil {
		return nil, err
	}

	instance := &Instance{
		BaseDSN:        baseDSN(dsn),
		Driver:         driver,
		User:           parsedConfig.User,
		Password:       parsedConfig.Passwd,
		DefaultParams:  paramMap(dsn),
		connectionPool: make(map[string]*sqlx.DB),
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
func (instance Instance) String() string {
	if instance.SocketPath != "" {
		return fmt.Sprintf("%s:%s", instance.Host, instance.SocketPath)
	} else if instance.Port == 0 {
		return instance.Host
	} else {
		return fmt.Sprintf("%s:%d", instance.Host, instance.Port)
	}
}

// HostAndOptionalPort is like String(), but omits the port if default
func (instance Instance) HostAndOptionalPort() string {
	if instance.Port == 3306 || instance.SocketPath != "" {
		return instance.Host
	} else {
		return instance.String()
	}
}

func (instance *Instance) buildParamString(params string) string {
	v := url.Values{}
	for defName, defValue := range instance.DefaultParams {
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
// instance.DefaultParams, with params supplied here taking precedence.
func (instance *Instance) Connect(defaultSchema string, params string) (*sqlx.DB, error) {
	key := fmt.Sprintf("%s?%s", defaultSchema, instance.buildParamString(params))
	if instance.connectionPool[key] == nil {
		fullDSN := instance.BaseDSN + key
		db, err := sqlx.Connect(instance.Driver, fullDSN)
		if err != nil {
			return nil, err
		}
		instance.connectionPool[key] = db.Unsafe()
	}
	return instance.connectionPool[key], nil
}

// CanConnect verifies that the Instance can be connected to
func (instance *Instance) CanConnect() (bool, error) {
	delete(instance.connectionPool, "?") // ensure we're initializing a new conn pool for schemalass, paramless use
	_, err := instance.Connect("", "")
	return err == nil, err
}

func (instance *Instance) Schemas() ([]*Schema, error) {
	if instance.schemas != nil {
		return instance.schemas, nil
	}

	db, err := instance.Connect("information_schema", "")
	if err != nil {
		return nil, err
	}

	var rawSchemas []struct {
		Name             string `db:"schema_name"`
		DefaultCharSet   string `db:"default_character_set_name"`
		DefaultCollation string `db:"default_collation_name"`
	}
	query := `
		SELECT schema_name, default_character_set_name, default_collation_name
		FROM   schemata
		WHERE  schema_name NOT IN ('information_schema', 'performance_schema', 'mysql', 'test')`
	if err := db.Select(&rawSchemas, query); err != nil {
		return nil, err
	}

	instance.schemas = make([]*Schema, len(rawSchemas))
	for n, rawSchema := range rawSchemas {
		instance.schemas[n] = &Schema{
			Name:             rawSchema.Name,
			DefaultCharSet:   rawSchema.DefaultCharSet,
			DefaultCollation: rawSchema.DefaultCollation,
			instance:         instance,
		}
	}
	return instance.schemas, nil
}

func (instance *Instance) Schema(name string) (*Schema, error) {
	schemas, err := instance.Schemas()
	if err != nil {
		return nil, err
	}
	for _, s := range schemas {
		if s.Name == name {
			return s, nil
		}
	}
	return nil, nil
}

func (instance *Instance) HasSchema(name string) bool {
	s, _ := instance.Schema(name)
	return s != nil
}

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
// the error will be sql.ErrNoRows
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

func (instance *Instance) CreateSchema(name string) (*Schema, error) {
	db, err := instance.Connect("", "")
	if err != nil {
		return nil, err
	}
	// TODO: support DEFAULT CHARACTER SET and DEFAULT COLLATE
	schema := Schema{Name: name}
	_, err = db.Exec(schema.CreateStatement())
	if err != nil {
		return nil, err
	}

	// Purge schema cache; next call to Schema will repopulate
	instance.schemas = nil
	return instance.Schema(name)
}

// DropSchema first drops all tables in the schema, and then drops the database.
//  If onlyIfEmpty==true, returns an error if any of the tables have any rows.
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
	db.Close()
	delete(instance.connectionPool, schema.Name)

	// Purge schema cache before returning
	instance.schemas = nil
	return nil
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
		var result []int
		for _, t := range tables {
			query := fmt.Sprintf("SELECT 1 FROM %s LIMIT 1", EscapeIdentifier(t.Name))
			if err := db.Select(&result, query); err != nil {
				return err
			}
			if len(result) != 0 {
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
