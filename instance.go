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

func (instance *Instance) Connect(defaultSchema string) *sqlx.DB {
	if instance.connectionPool[defaultSchema] == nil {
		instance.connectionPool[defaultSchema] = sqlx.MustConnect(instance.Driver, instance.DSN+defaultSchema).Unsafe()
	}
	return instance.connectionPool[defaultSchema]
}

func (instance *Instance) Schemas() []*Schema {
	if instance.schemas != nil {
		return instance.schemas
	}

	db := instance.Connect("information_schema")

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
		panic(err)
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
	return instance.schemas
}

func (instance *Instance) Refresh() {
	instance.schemas = nil
	instance.Schemas()
}

func (instance *Instance) Schema(name string) *Schema {
	for _, s := range instance.Schemas() {
		if s.Name == name {
			return s
		}
	}
	return nil
}

func (instance *Instance) HasSchema(name string) bool {
	return (instance.Schema(name) != nil)
}

func (instance *Instance) ShowCreateTable(schema *Schema, table *Table) (string, error) {
	db := instance.Connect(schema.Name)

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
	db := instance.Connect("information_schema")
	err := db.Get(&result, `
		SELECT  data_length + index_length + data_free
		FROM    tables
		WHERE   table_schema = ? and table_name = ?`,
		schema.Name, table.Name)
	return result, err
}

func (instance *Instance) CreateSchema(name string) (*Schema, error) {
	db := instance.Connect("")
	// TODO: support DEFAULT CHARACTER SET and DEFAULT COLLATE
	schema := Schema{Name: name}
	_, err := db.Exec(schema.CreateStatement())
	if err != nil {
		return nil, err
	}
	instance.Refresh()
	return instance.Schema(name), nil
}

// DropSchema first drops all tables in the schema, and then drops the database.
func (instance *Instance) DropSchema(schema *Schema) error {
	db := instance.Connect(schema.Name)

	// TODO: need to handle proper ordering for foreign keys
	for _, t := range schema.Tables() {
		_, err := db.Exec(t.DropStatement())
		if err != nil {
			return err
		}
	}

	_, err := db.Exec(schema.DropStatement())
	if err != nil {
		return err
	}
	db.Close()
	delete(instance.connectionPool, schema.Name)
	instance.Refresh()
	return nil
}
