package tengo

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

type Instance struct {
	DSN            string
	Driver         string
	User           string
	Password       string
	Host           string
	Port           int
	schemas        []*Schema
	connectionPool map[string]*sqlx.DB
}

// Returns the DSN with the trailing database (schema) name stripped
func BaseDSN(dsn string) string {
	tokens := strings.SplitAfter(dsn, "/")
	return strings.Join(tokens[0:len(tokens)-1], "")
}

func NewInstance(driver, dsn string) *Instance {
	parsedConfig, err := mysql.ParseDSN(dsn)
	if err != nil {
		return nil
	}
	port := 0
	parts := strings.SplitN(parsedConfig.Addr, ":", 2)
	if len(parts) == 2 {
		parsedConfig.Addr = parts[0]
		port, _ = strconv.Atoi(parts[1])
	}

	return &Instance{
		DSN:            BaseDSN(dsn),
		Driver:         driver,
		User:           parsedConfig.User,
		Password:       parsedConfig.Passwd,
		Host:           parsedConfig.Addr,
		Port:           port,
		connectionPool: make(map[string]*sqlx.DB),
	}
}

// String for an instance returns a "host:port" string
func (instance Instance) String() string {
	return fmt.Sprintf("%s:%d", instance.Host, instance.Port)
}

// HostAndOptionalPort is like String(), but omits the port if default
func (instance Instance) HostAndOptionalPort() string {
	if instance.Port == 3306 {
		return instance.Host
	} else {
		return instance.String()
	}
}

func (instance *Instance) Connect(defaultSchema string) (*sqlx.DB, error) {
	if instance.connectionPool[defaultSchema] == nil {
		fullDSN := fmt.Sprintf("%s%s?interpolateParams=true", instance.DSN, defaultSchema)
		db, err := sqlx.Connect(instance.Driver, fullDSN)
		if err != nil {
			return nil, err
		}
		instance.connectionPool[defaultSchema] = db.Unsafe()
	} else if err := instance.connectionPool[defaultSchema].Ping(); err != nil {
		// Bad ping on existing conn: nuke the connection pool and try again
		_ = instance.connectionPool[defaultSchema].Close() // intentionally ignoring any error here
		instance.connectionPool[defaultSchema] = nil
		return instance.Connect(defaultSchema)
	}
	return instance.connectionPool[defaultSchema], nil
}

func (instance *Instance) MustConnect(defaultSchema string) *sqlx.DB {
	db, err := instance.Connect(defaultSchema)
	if err != nil {
		panic(err)
	}
	return db
}

func (instance *Instance) CanConnect() (bool, error) {
	_, err := instance.Connect("")
	return err == nil, err
}

func (instance *Instance) Schemas() ([]*Schema, error) {
	if instance.schemas != nil {
		return instance.schemas, nil
	}

	db, err := instance.Connect("information_schema")
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
	db, err := instance.Connect(schema.Name)
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
	db, err := instance.Connect("information_schema")
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
	db, err := instance.Connect("")
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

	db, err := instance.Connect(schema.Name)
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
	// TODO: disable foreign key checks in this conn
	db, err := instance.Connect(schema.Name)
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
	// TODO: disable foreign key checks in this conn
	db, err := instance.Connect(dest.Name)
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
