package tengo

import (
	"database/sql"
	"fmt"
	"strings"
)

// Schema represents a database schema.
type Schema struct {
	Name             string
	DefaultCharSet   string
	DefaultCollation string
	tables           []*Table
	instance         *Instance
}

// TablesByName returns a mapping of table names to Table struct values, for
// all tables in the schema.
func (s *Schema) TablesByName() (map[string]*Table, error) {
	tables, err := s.Tables()
	if err != nil {
		return nil, err
	}
	result := make(map[string]*Table, len(tables))
	for _, t := range tables {
		result[t.Name] = t
	}
	return result, nil
}

// HasTable returns true if a table with the given name exists in the schema.
func (s Schema) HasTable(name string) bool {
	byName, err := s.TablesByName()
	if err != nil {
		return false
	}
	_, exists := byName[name]
	return exists
}

// Tables returns a slice of all tables in the schema.
func (s *Schema) Tables() ([]*Table, error) {
	if s == nil {
		return []*Table{}, nil
	}
	if s.tables != nil {
		return s.tables, nil
	}
	if s.instance == nil {
		return nil, fmt.Errorf("Schema.Tables: schema %s has been detached from its instance", s.Name)
	}

	db, err := s.instance.Connect("information_schema", "")
	if err != nil {
		return nil, err
	}

	// Obtain the tables in the schema
	var rawTables []struct {
		Name               string        `db:"table_name"`
		Type               string        `db:"table_type"`
		Engine             string        `db:"engine"`
		RowFormat          string        `db:"row_format"`
		AutoIncrement      sql.NullInt64 `db:"auto_increment"`
		CreateOptions      string        `db:"create_options"`
		TableCollation     string        `db:"table_collation"`
		TableComment       string        `db:"table_comment"`
		CharacterSet       string        `db:"character_set_name"`
		CollationIsDefault string        `db:"is_default"`
	}
	query := `
		SELECT    t.table_name, t.table_type, t.engine, t.row_format, t.auto_increment,
		          t.create_options, t.table_collation, t.table_comment,
		          c.character_set_name, c.is_default
		FROM      tables t
		LEFT JOIN collations c ON t.table_collation = c.collation_name
		WHERE     t.table_schema = ?
		AND       t.table_type = 'BASE TABLE'`
	if err := db.Select(&rawTables, query, s.Name); err != nil {
		return nil, err
	}

	s.tables = make([]*Table, len(rawTables))
	for n, rawTable := range rawTables {
		s.tables[n] = &Table{
			Name:         rawTable.Name,
			Engine:       rawTable.Engine,
			CharacterSet: rawTable.CharacterSet,
		}
		if rawTable.CollationIsDefault == "" {
			s.tables[n].Collation = rawTable.TableCollation
		}
		if rawTable.AutoIncrement.Valid {
			s.tables[n].NextAutoIncrement = uint64(rawTable.AutoIncrement.Int64)
		}
	}

	// Obtain the columns in all tables in the schema
	var rawColumns []struct {
		Name       string         `db:"column_name"`
		TableName  string         `db:"table_name"`
		Type       string         `db:"column_type"`
		IsNullable string         `db:"is_nullable"`
		Default    sql.NullString `db:"column_default"`
		Extra      string         `db:"extra"`
		Comment    string         `db:"column_comment"`
	}
	query = `
		SELECT   table_name, column_name, column_type, is_nullable, column_default, extra, column_comment
		FROM     columns
		WHERE    table_schema = ?
		ORDER BY table_name, ordinal_position`
	if err := db.Select(&rawColumns, query, s.Name); err != nil {
		return nil, err
	}
	columnsByTableName := make(map[string][]*Column)
	columnsByTableAndName := make(map[string]*Column)
	for _, rawColumn := range rawColumns {
		col := &Column{
			Name:          rawColumn.Name,
			TypeInDB:      rawColumn.Type,
			Nullable:      strings.ToUpper(rawColumn.IsNullable) == "YES",
			AutoIncrement: strings.Contains(rawColumn.Extra, "auto_increment"),
		}
		if !rawColumn.Default.Valid {
			col.Default = ColumnDefaultNull
		} else if rawColumn.Default.String == "CURRENT_TIMESTAMP" && (rawColumn.Type == "timestamp" || rawColumn.Type == "datetime") {
			col.Default = ColumnDefaultCurrentTimestamp
		} else {
			col.Default = ColumnDefaultValue(rawColumn.Default.String)
		}
		if strings.Contains(strings.ToLower(rawColumn.Extra), "on update") {
			col.Extra = strings.ToUpper(rawColumn.Extra)
		}
		if columnsByTableName[rawColumn.TableName] == nil {
			columnsByTableName[rawColumn.TableName] = make([]*Column, 0)
		}
		columnsByTableName[rawColumn.TableName] = append(columnsByTableName[rawColumn.TableName], col)
		fullNameStr := fmt.Sprintf("%s.%s.%s", s.Name, rawColumn.TableName, rawColumn.Name)
		columnsByTableAndName[fullNameStr] = col
	}
	for n, t := range s.tables {
		s.tables[n].Columns = columnsByTableName[t.Name]
	}

	// Obtain the indexes of all tables in the schema. Since multi-column indexes
	// have multiple rows in the result set, we do two passes over the result: one
	// to figure out which indexes exist, and one to stitch together the col info.
	// We cannot use an ORDER BY on this query, since only the unsorted result
	// matches the same order of secondary indexes as the CREATE TABLE statement.
	var rawIndexes []struct {
		Name       string        `db:"index_name"`
		TableName  string        `db:"table_name"`
		NonUnique  uint8         `db:"non_unique"`
		SeqInIndex uint8         `db:"seq_in_index"`
		ColumnName string        `db:"column_name"`
		SubPart    sql.NullInt64 `db:"sub_part"`
	}
	query = `
		SELECT   index_name, table_name, non_unique, seq_in_index, column_name, sub_part
		FROM     statistics
		WHERE    table_schema = ?`
	if err := db.Select(&rawIndexes, query, s.Name); err != nil {
		return nil, err
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
		fullNameStr := fmt.Sprintf("%s.%s.%s", s.Name, rawIndex.TableName, rawIndex.Name)
		indexesByTableAndName[fullNameStr] = index
	}
	for _, rawIndex := range rawIndexes {
		fullIndexNameStr := fmt.Sprintf("%s.%s.%s", s.Name, rawIndex.TableName, rawIndex.Name)
		index, ok := indexesByTableAndName[fullIndexNameStr]
		if !ok {
			panic(fmt.Errorf("Cannot find index %s", fullIndexNameStr))
		}
		fullColNameStr := fmt.Sprintf("%s.%s.%s", s.Name, rawIndex.TableName, rawIndex.ColumnName)
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
	for _, t := range s.tables {
		t.PrimaryKey = primaryKeyByTableName[t.Name]
		t.SecondaryIndexes = secondaryIndexesByTableName[t.Name]
	}

	// Obtain actual SHOW CREATE TABLE output and store in each table. Compare
	// with what we expect the create DDL to be, to determine if we support
	// diffing for the table.
	for _, t := range s.tables {
		t.createStatement, err = s.instance.ShowCreateTable(s, t)
		if err != nil {
			return nil, err
		}
		if t.createStatement != t.GeneratedCreateStatement() {
			t.UnsupportedDDL = true
		}
	}

	return s.tables, nil
}

// PurgeTableCache purges any previously-cached table information. This should
// be used after creating, altering, renaming, or dropping tables.
func (s *Schema) PurgeTableCache() {
	if s == nil || s.instance == nil {
		return
	}
	s.tables = nil
}

// Diff returns the set of differences between this schema and another schema.
func (s *Schema) Diff(other *Schema) (*SchemaDiff, error) {
	return NewSchemaDiff(s, other)
}

// DropStatement returns a SQL statement that, if run, would drop this schema.
func (s Schema) DropStatement() string {
	return fmt.Sprintf("DROP DATABASE %s", EscapeIdentifier(s.Name))
}

// CreateStatement returns a SQL statement that, if run, would create this
// schema.
func (s Schema) CreateStatement() string {
	// TODO: support DEFAULT CHARACTER SET and DEFAULT COLLATE
	return fmt.Sprintf("CREATE DATABASE %s", EscapeIdentifier(s.Name))
}

// CachedCopy returns a copy of the Schema object without its instance
// association. This copy may be used in diff operations even if the original
// schema it was copied from is dropped from its instance.
func (s *Schema) CachedCopy() (*Schema, error) {
	if s == nil {
		return nil, nil
	}

	// Populate cache if missing
	if s.tables == nil {
		if _, err := s.Tables(); err != nil {
			return nil, err
		}
	}

	clone := &Schema{
		Name:             s.Name,
		DefaultCharSet:   s.DefaultCharSet,
		DefaultCollation: s.DefaultCollation,
		tables:           s.tables,
	}
	return clone, nil
}
