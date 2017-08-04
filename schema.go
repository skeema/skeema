package tengo

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

// Schema represents a database schema.
type Schema struct {
	Name      string
	CharSet   string
	Collation string
	tables    []*Table
	instance  *Instance
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
func (s *Schema) HasTable(name string) bool {
	t, err := s.Table(name)
	return (err == nil && t != nil)
}

// Table returns a table by name.
func (s *Schema) Table(name string) (*Table, error) {
	if s == nil {
		return nil, nil
	}
	byName, err := s.TablesByName()
	if err != nil {
		return nil, err
	}
	return byName[name], nil
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

	// We use MySQL's information_schema to perform schema introspection and build
	// corresponding structs
	db, err := s.instance.Connect("information_schema", "")
	if err != nil {
		return nil, err
	}

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
	if err := db.Select(&rawTables, query, s.Name); err != nil {
		return nil, fmt.Errorf("Error querying information_schema.tables: %s", err)
	}

	s.tables = make([]*Table, len(rawTables))
	for n, rawTable := range rawTables {
		s.tables[n] = &Table{
			Name:    rawTable.Name,
			Engine:  rawTable.Engine.String,
			CharSet: rawTable.CharSet,
			Comment: rawTable.Comment,
		}
		if rawTable.CollationIsDefault == "" && rawTable.TableCollation.Valid {
			s.tables[n].Collation = rawTable.TableCollation.String
		}
		if rawTable.AutoIncrement.Valid {
			s.tables[n].NextAutoIncrement = uint64(rawTable.AutoIncrement.Int64)
		}
		if rawTable.CreateOptions.Valid && rawTable.CreateOptions.String != "" && rawTable.CreateOptions.String != "PARTITIONED" {
			// information_schema.tables.create_options annoyingly contains "partitioned"
			// if the table is partitioned, despite this not being present as-is in the
			// table table definition. All other create_options are present verbatim.
			// Currently in mysql-server/sql/sql_show.cc, it's always at the *end* of
			// create_options... but just to code defensively we handle any location.
			if strings.HasPrefix(rawTable.CreateOptions.String, "PARTITIONED ") {
				s.tables[n].CreateOptions = strings.Replace(rawTable.CreateOptions.String, "PARTITIONED ", "", 1)
			} else {
				s.tables[n].CreateOptions = strings.Replace(rawTable.CreateOptions.String, " PARTITIONED", "", 1)
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
	if err := db.Select(&rawColumns, query, s.Name); err != nil {
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
		if !rawColumn.Default.Valid {
			col.Default = ColumnDefaultNull
		} else if strings.HasPrefix(rawColumn.Default.String, "CURRENT_TIMESTAMP") && (strings.HasPrefix(rawColumn.Type, "timestamp") || strings.HasPrefix(rawColumn.Type, "datetime")) {
			col.Default = ColumnDefaultExpression(rawColumn.Default.String)
		} else {
			col.Default = ColumnDefaultValue(rawColumn.Default.String)
		}
		if strings.HasPrefix(strings.ToLower(rawColumn.Extra), "on update ") {
			// MariaDB strips fractional second precision here but includes it in SHOW
			// CREATE TABLE. MySQL includes it in both places. Here we adjust the MariaDB
			// one to look like MySQL, so that our generated DDL matches SHOW CREATE TABLE.
			if openParen := strings.IndexByte(rawColumn.Type, '('); openParen > -1 && !strings.Contains(strings.ToLower(rawColumn.Extra), "current_timestamp(") {
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
	if err := db.Select(&rawIndexes, query, s.Name); err != nil {
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

	// Get all the constraints for all the tables and place them in the table object
	var rawConstraints []struct {
		Name                 string `db:"constraint_name"`
		ColumnName           string `db:"column_name"`
		ReferencedSchemaName string `db:"referenced_schema_name"`
		ReferencedTableName  string `db:"referenced_table_name"`
		ReferencedColumnName string `db:"referenced_column_name"`
		UpdateRule           string `db:"update_rule"`
		DeleteRule           string `db:"delete_rule"`
		TableName            string `db:"table_name"`
	}

	query = `
		SELECT   kcu.constraint_name, kcu.table_name, kcu.column_name,
		         kcu.referenced_table_name, kcu.referenced_column_name,
		         kcu.referenced_table_schema AS referenced_schema_name,
		         rc.update_rule, rc.delete_rule
		FROM     key_column_usage kcu
		JOIN     referential_constraints rc ON kcu.constraint_name = rc.constraint_name
		WHERE    kcu.table_schema = ? AND rc.constraint_schema = ? AND
		         kcu.referenced_column_name IS NOT NULL`

	if err := db.Select(&rawConstraints, query, s.Name, s.Name); err != nil {
		return nil, fmt.Errorf("Error querying foreign key constraints: %s", err)
	}

	constraintsByTableName := make(map[string][]*Constraint)
	for _, rawConstraint := range rawConstraints {
		// If this is a foreign key constraint which references a column in a table of a DIFFERENT database/schema,
		// We need to include the ReferencedSchemaName in the constraint as it will be SIGNIFICANT to the
		// contraint definition.
		// If however it just references a table inside the current database/schema (s.Name), just provide "" to signal that we do not need it
		referencedSchemaName := ""
		if strings.ToLower(rawConstraint.ReferencedSchemaName) != strings.ToLower(s.Name) {
                referencedSchemaName = rawConstraint.ReferencedSchemaName
        }

		fullColNameStr := fmt.Sprintf("%s.%s.%s", s.Name, rawConstraint.TableName, rawConstraint.ColumnName)
		column := columnsByTableAndName[fullColNameStr]

		constraint := &Constraint{
			Name:                 rawConstraint.Name,
			Column:               column,
			ReferencedSchemaName: referencedSchemaName,
			ReferencedTableName:  rawConstraint.ReferencedTableName,
			ReferencedColumnName: rawConstraint.ReferencedColumnName,
			UpdateRule:           rawConstraint.UpdateRule,
			DeleteRule:           rawConstraint.DeleteRule,
		}
		constraintsByTableName[rawConstraint.TableName] = append(constraintsByTableName[rawConstraint.TableName], constraint)
	}
	for _, t := range s.tables {
		t.Constraints = constraintsByTableName[t.Name]
	}

	// Obtain actual SHOW CREATE TABLE output and store in each table. Compare
	// with what we expect the create DDL to be, to determine if we support
	// diffing for the table.
	for _, t := range s.tables {
		t.createStatement, err = s.instance.ShowCreateTable(s, t)
		if err != nil {
			return nil, fmt.Errorf("Error executing SHOW CREATE TABLE: %s", err)
		}
		if strings.ToLower(t.createStatement) != strings.ToLower(t.GeneratedCreateStatement()) {
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
func (s *Schema) DropStatement() string {
	return fmt.Sprintf("DROP DATABASE %s", EscapeIdentifier(s.Name))
}

// CreateStatement returns a SQL statement that, if run, would create this
// schema.
func (s *Schema) CreateStatement() string {
	var charSet, collate string
	if s.CharSet != "" {
		charSet = fmt.Sprintf(" CHARACTER SET %s", s.CharSet)
	}
	if s.Collation != "" {
		collate = fmt.Sprintf(" COLLATE %s", s.Collation)
	}
	return fmt.Sprintf("CREATE DATABASE %s%s%s", EscapeIdentifier(s.Name), charSet, collate)
}

// AlterStatement returns a SQL statement that, if run, would alter this
// schema's default charset and/or collation to the supplied values.
// If charSet is "" and collation isn't, only the collation will be changed.
// If collation is "" and charSet isn't, the default collation for charSet is
// used automatically.
// If both params are "", or if values equal to the schema's current charSet
// and collation are supplied, an empty string is returned.
func (s *Schema) AlterStatement(charSet, collation string) string {
	var charSetClause, collateClause string
	if s.CharSet != charSet && charSet != "" {
		charSetClause = fmt.Sprintf(" CHARACTER SET %s", charSet)
	}
	if s.Collation != collation && collation != "" {
		collateClause = fmt.Sprintf(" COLLATE %s", collation)
	}
	if charSetClause == "" && collateClause == "" {
		return ""
	}
	return fmt.Sprintf("ALTER DATABASE %s%s%s", EscapeIdentifier(s.Name), charSetClause, collateClause)
}

// OverridesServerCharSet checks if the schema's default character set and
// collation differ from its instance's server-level default character set
// and collation. The first return value will be true if the schema's charset
// differs from its instance's; the second return value will be true if the
// schema's collation differs from its instance's.
func (s *Schema) OverridesServerCharSet() (overridesCharSet bool, overridesCollation bool, err error) {
	if s == nil {
		return false, false, errors.New("Attempted to check character set and collation on a nil schema")
	}
	if s.instance == nil {
		return false, false, fmt.Errorf("Attempted to check character set and collation on schema %s which has been detached from its instance", s.Name)
	}
	if s.Collation == "" && s.CharSet == "" {
		return false, false, nil
	}

	db, err := s.instance.Connect("information_schema", "")
	if err != nil {
		return false, false, err
	}
	var serverCharSet, serverCollation string
	err = db.QueryRow("SELECT @@global.character_set_server, @@global.collation_server").Scan(&serverCharSet, &serverCollation)
	if err != nil {
		return false, false, err
	}
	if s.CharSet != "" && serverCharSet != s.CharSet {
		// Different charset also inherently means different collation
		return true, true, nil
	} else if s.Collation != "" && serverCollation != s.Collation {
		return false, true, nil
	}
	return false, false, nil
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
		Name:      s.Name,
		CharSet:   s.CharSet,
		Collation: s.Collation,
		tables:    s.tables,
	}
	return clone, nil
}
