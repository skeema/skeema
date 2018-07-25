package tengo

import (
	"database/sql"
	"fmt"
	"net/url"
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
	connectionPool map[string]*sqlx.DB // key is in format "schema?params" or just "schema" if no params
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

// Flavor returns this instance's flavor value, representing the database
// distribution/fork/vendor as well as major and minor version. If this is
// unable to be determined or an error occurs, FlavorUnknown will be returned.
func (instance *Instance) Flavor() Flavor {
	if instance.flavor == FlavorUnknown {
		instance.hydrateFlavorAndVersion()
	}
	return instance.flavor
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
	var vendorString, versionString string
	if err = db.QueryRow("SELECT @@global.version_comment, @@global.version").Scan(&vendorString, &versionString); err != nil {
		return
	}
	instance.version = ParseVersion(versionString)
	instance.flavor = NewFlavor(vendorString, instance.version[0], instance.version[1])
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

	if len(onlyNames) == 0 {
		query = `
			SELECT schema_name, default_character_set_name, default_collation_name
			FROM   schemata
			WHERE  schema_name NOT IN ('information_schema', 'performance_schema', 'mysql', 'test', 'sys')`
	} else {
		query = `
			SELECT schema_name, default_character_set_name, default_collation_name
			FROM   schemata
			WHERE  schema_name IN (?)`
		query, args, err = sqlx.In(query, onlyNames)
	}
	if err := db.Select(&rawSchemas, query, args...); err != nil {
		return nil, err
	}

	schemas := make([]*Schema, len(rawSchemas))
	for n, rawSchema := range rawSchemas {
		tables, err := instance.querySchemaTables(rawSchema.Name)
		if err != nil {
			return nil, err
		}
		schemas[n] = &Schema{
			Name:      rawSchema.Name,
			CharSet:   rawSchema.CharSet,
			Collation: rawSchema.Collation,
			Tables:    tables,
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

	errOut := make(chan error, len(names))
	defer db.SetMaxOpenConns(0)

	if onlyIfEmpty {
		db.SetMaxOpenConns(15)
		for _, name := range names {
			go func(name string) {
				hasRows, err := tableHasRows(db, name)
				if err == nil && hasRows {
					err = fmt.Errorf("DropTablesInSchema: table %s.%s has at least one row", EscapeIdentifier(schema), EscapeIdentifier(name))
				}
				errOut <- err
			}(name)
		}
		for range names {
			if err := <-errOut; err != nil {
				return err
			}
		}
	}

	db.SetMaxOpenConns(10)
	for _, name := range names {
		go func(name string) {
			_, err := db.Exec(fmt.Sprintf("DROP TABLE %s", EscapeIdentifier(name)))
			errOut <- err
		}(name)
	}
	for range names {
		if err := <-errOut; err != nil {
			return err
		}
	}
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

func (instance *Instance) querySchemaTables(schema string) ([]*Table, error) {
	db, err := instance.Connect("information_schema", "")
	if err != nil {
		return nil, err
	}

	// Obtain flavor and version info. MariaDB changed how default values are
	// represented in information_schema in 10.2+.
	flavor := instance.Flavor()

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
		SELECT t.table_name, t.table_type, t.engine, t.auto_increment, t.table_collation,
		       UPPER(t.create_options) AS create_options, t.table_comment,
		       c.character_set_name, c.is_default
		FROM   tables t
		JOIN   collations c ON t.table_collation = c.collation_name
		WHERE  t.table_schema = ?
		AND    t.table_type = 'BASE TABLE'`
	if err := db.Select(&rawTables, query, schema); err != nil {
		return nil, fmt.Errorf("Error querying information_schema.tables: %s", err)
	}
	if len(rawTables) == 0 {
		return []*Table{}, nil
	}
	tables := make([]*Table, len(rawTables))
	for n, rawTable := range rawTables {
		tables[n] = &Table{
			Name:    rawTable.Name,
			Engine:  rawTable.Engine.String,
			CharSet: rawTable.CharSet,
			Comment: rawTable.Comment,
		}
		if rawTable.CollationIsDefault == "" && rawTable.TableCollation.Valid {
			tables[n].Collation = rawTable.TableCollation.String
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
		SELECT    c.table_name, c.column_name, c.column_type, c.is_nullable, c.column_default,
		          c.extra, c.column_comment, c.character_set_name, c.collation_name,
		          co.is_default
		FROM      columns c
		LEFT JOIN collations co ON co.collation_name = c.collation_name
		WHERE     c.table_schema = ?
		ORDER BY  c.table_name, c.ordinal_position`
	if err := db.Select(&rawColumns, query, schema); err != nil {
		return nil, fmt.Errorf("Error querying information_schema.columns: %s", err)
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
		if col.AutoIncrement {
			col.Default = ColumnDefaultForbidden
		} else if !flavor.AllowBlobDefaults() && (strings.HasSuffix(col.TypeInDB, "blob") || strings.HasSuffix(col.TypeInDB, "text")) {
			col.Default = ColumnDefaultForbidden
		} else if !rawColumn.Default.Valid {
			col.Default = ColumnDefaultNull
		} else if flavor.AllowDefaultExpression() {
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
		} else {
			col.Default = ColumnDefaultValue(rawColumn.Default.String)
		}
		if strings.HasPrefix(strings.ToLower(rawColumn.Extra), "on update ") {
			// MariaDB 10.2+ lowercases the value. Previous versions of MariaDB omit
			// the fractional second precision in information_schema but include it in
			// SHOW CREATE TABLE.
			if flavor.LowercaseOnUpdate() {
				col.OnUpdate = rawColumn.Extra[10:]
			} else if openParen := strings.IndexByte(rawColumn.Type, '('); flavor.Vendor == VendorMariaDB && openParen > -1 && !strings.Contains(strings.ToLower(rawColumn.Extra), "current_timestamp(") {
				col.OnUpdate = fmt.Sprintf("%s%s", strings.ToUpper(rawColumn.Extra[10:]), rawColumn.Type[openParen:])
			} else {
				col.OnUpdate = strings.ToUpper(rawColumn.Extra[10:])
			}
		}
		if rawColumn.Collation.Valid { // only text-based column types have a notion of charset and collation
			col.CharSet = rawColumn.CharSet.String
			if rawColumn.CollationIsDefault.String == "" {
				// SHOW CREATE TABLE only includes col's collation if it differs from col's charset's default collation
				col.Collation = rawColumn.Collation.String
			}
		}
		if columnsByTableName[rawColumn.TableName] == nil {
			columnsByTableName[rawColumn.TableName] = make([]*Column, 0)
		}
		columnsByTableName[rawColumn.TableName] = append(columnsByTableName[rawColumn.TableName], col)
		fullNameStr := fmt.Sprintf("%s.%s.%s", schema, rawColumn.TableName, rawColumn.Name)
		columnsByTableAndName[fullNameStr] = col
	}
	for n, t := range tables {
		tables[n].Columns = columnsByTableName[t.Name]
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
		SELECT   index_name, table_name, non_unique, seq_in_index, column_name,
		         sub_part, index_comment
		FROM     statistics
		WHERE    table_schema = ?`
	if err := db.Select(&rawIndexes, query, schema); err != nil {
		return nil, fmt.Errorf("Error querying information_schema.statistics: %s", err)
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
		}
		index.Columns[rawIndex.SeqInIndex-1] = col
		if rawIndex.SubPart.Valid {
			index.SubParts = append(index.SubParts, uint16(rawIndex.SubPart.Int64))
		} else {
			index.SubParts = append(index.SubParts, 0)
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
		SELECT   rc.constraint_name, rc.table_name, rc.update_rule, rc.delete_rule, rc.referenced_table_name,
		         IF(rc.constraint_schema=rc.unique_constraint_schema, '', rc.unique_constraint_schema) AS referenced_schema,
		         kcu.referenced_column_name,
		         CONCAT(kcu.constraint_schema, '.', kcu.table_name, '.', kcu.column_name) AS col_lookup_key
		FROM     referential_constraints rc
		JOIN     key_column_usage kcu ON kcu.constraint_name = rc.constraint_name AND kcu.constraint_schema = rc.constraint_schema
		WHERE    rc.constraint_schema = ?
		ORDER BY BINARY rc.constraint_name, kcu.ordinal_position`
	if err := db.Select(&rawForeignKeys, query, schema); err != nil {
		return nil, fmt.Errorf("Error querying foreign key constraints: %s", err)
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
	errOut := make(chan error, len(tables))
	for _, t := range tables {
		go func(t *Table) {
			var err error
			if t.CreateStatement, err = showCreateTable(db, t.Name); err != nil {
				errOut <- fmt.Errorf("Error executing SHOW CREATE TABLE: %s", err)
				return
			}
			if t.Engine == "InnoDB" {
				t.CreateStatement = NormalizeCreateOptions(t.CreateStatement)
			}
			// Compare what we expect the create DDL to be, to determine if we support
			// diffing for the table. Ignore next-auto-increment differences in this
			// comparison, since the value may have changed between our previous
			// information_schema introspection and our current SHOW CREATE TABLE call!
			actual, _ := ParseCreateAutoInc(t.CreateStatement)
			expected, _ := ParseCreateAutoInc(t.GeneratedCreateStatement())
			if actual != expected {
				t.UnsupportedDDL = true
			}
			errOut <- nil
		}(t)
	}
	for range tables {
		if err := <-errOut; err != nil {
			return nil, err
		}
	}

	return tables, nil
}
