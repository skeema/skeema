package tengo

import (
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

type Instance struct {
	DSN            string
	Driver         string
	schemas        []*Schema
	connectionPool *sqlx.DB
}

func (instance *Instance) Connect() *sqlx.DB {
	if instance.connectionPool == nil {
		instance.connectionPool = sqlx.MustConnect(instance.Driver, instance.DSN).Unsafe()
	}
	return instance.connectionPool
}

func (instance *Instance) Schemas() []*Schema {
	if instance.schemas != nil {
		return instance.schemas
	}

	db := instance.Connect()

	var rawSchemas []struct {
		Name             string `db:"schema_name"`
		DefaultCharSet   string `db:"default_character_set_name"`
		DefaultCollation string `db:"default_collation_name"`
	}
	query := `
		SELECT schema_name, default_character_set_name, default_collation_name
		FROM   information_schema.schemata
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
	db := instance.Connect()

	var createRows []struct {
		TableName       string `db:"Table"`
		CreateStatement string `db:"Create Table"`
	}
	query := fmt.Sprintf("SHOW CREATE TABLE %s.%s", EscapeIdentifier(schema.Name), EscapeIdentifier(table.Name))
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
	db := instance.Connect()
	err := db.Get(&result, `
		SELECT  data_length + index_length + data_free
		FROM    information_schema.tables
		WHERE   table_schema = ? and table_name = ?`,
		schema.Name, table.Name)
	return result, err
}

func (instance *Instance) CreateSchema(name string) (*Schema, error) {
	db := instance.Connect()
	// TODO: support DEFAULT CHARACTER SET and DEFAULT COLLATE
	query := fmt.Sprintf("CREATE DATABASE %s", EscapeIdentifier(name))
	_, err := db.Exec(query)
	if err != nil {
		return nil, err
	}
	instance.Refresh()
	return instance.Schema(name), nil
}
