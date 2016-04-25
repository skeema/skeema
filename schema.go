package tengo

import (
	"database/sql"
	"fmt"
	"strings"
)

type Schema struct {
	Name             string
	DefaultCharSet   string
	DefaultCollation string
	tables           []*Table
	instance         *Instance
}

func (s Schema) TablesByName() map[string]*Table {
	tables := s.Tables()
	result := make(map[string]*Table, len(tables))
	for _, t := range tables {
		result[t.Name] = t
	}
	return result
}

func (s Schema) HasTable(name string) bool {
	byName := s.TablesByName()
	_, exists := byName[name]
	return exists
}

func (s *Schema) Tables() []*Table {
	if s.tables != nil {
		return s.tables
	}

	db := s.instance.Connect()

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
		FROM      information_schema.tables t
		LEFT JOIN information_schema.collations c ON t.table_collation = c.collation_name
		WHERE     t.table_schema = ?
		AND       t.table_type = 'BASE TABLE'`
	if err := db.Select(&rawTables, query, s.Name); err != nil {
		panic(err)
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
		FROM     information_schema.columns
		WHERE    table_schema = ?
		ORDER BY table_name, ordinal_position`
	if err := db.Select(&rawColumns, query, s.Name); err != nil {
		panic(err)
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
	// to figure out which indexes exist, and one to stitch together the col info
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
		FROM     information_schema.statistics
		WHERE    table_schema = ?
		ORDER BY table_name, index_name, seq_in_index`
	if err := db.Select(&rawIndexes, query, s.Name); err != nil {
		panic(err)
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
		index.Columns = append(index.Columns, col)
		if rawIndex.SubPart.Valid {
			index.SubParts = append(index.SubParts, uint16(rawIndex.SubPart.Int64))
		} else {
			index.SubParts = append(index.SubParts, 0)
		}
	}
	for n, t := range s.tables {
		s.tables[n].PrimaryKey = primaryKeyByTableName[t.Name]
		s.tables[n].SecondaryIndexes = secondaryIndexesByTableName[t.Name]
	}

	return s.tables
}

func (s *Schema) Refresh() {
	s.tables = nil
	s.Tables()
}

func (from *Schema) Diff(to *Schema) *SchemaDiff {
	return NewSchemaDiff(from, to)
}
