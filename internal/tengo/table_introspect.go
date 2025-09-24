package tengo

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strings"

	"github.com/jmoiron/sqlx"
)

func init() {
	primaryIntrospectorTasks = append(primaryIntrospectorTasks, introspectTables)
	primaryIntrospectorFixups = append(primaryIntrospectorFixups, fixupTables)
}

/*
	Important note on information_schema queries in this file: MySQL 8.0 changes
	information_schema column names to come back from queries in all caps, so we
	need to explicitly use AS clauses in order to get them back as lowercase and
	have sqlx Select() work.
*/

func introspectTables(ctx context.Context, insp *introspector) error {
	flavor := insp.instance.Flavor()
	var rawTables []struct {
		Name           string         `db:"table_name"`
		Type           string         `db:"table_type"`
		Engine         sql.NullString `db:"engine"`
		TableCollation sql.NullString `db:"table_collation"`
		CreateOptions  sql.NullString `db:"create_options"`
		Comment        string         `db:"table_comment"`
	}
	query := `
		SELECT SQL_BUFFER_RESULT
		       table_name AS table_name, table_type AS table_type,
		       engine AS engine, table_collation AS table_collation,
		       create_options AS create_options, table_comment AS table_comment
		FROM   information_schema.tables
		WHERE  table_schema = ?`
	if err := insp.db.SelectContext(ctx, &rawTables, query, insp.schema.Name); err != nil {
		return fmt.Errorf("Error querying information_schema.tables for schema %s: %w", insp.schema.Name, err)
	}
	tables := make([]*Table, 0, len(rawTables))
	var havePartitions bool
	haveAltType := make(map[string]bool, 4)
	for _, rawTable := range rawTables {
		if rawTable.Type != "BASE TABLE" {
			haveAltType[rawTable.Type] = true
			continue
		}

		// Note that we no longer set Table.NextAutoIncrement here. information_schema
		// potentially has bad data, e.g. a table without an auto-inc col can still
		// have a non-NULL tables.auto_increment if the original CREATE specified one.
		// Instead the value is parsed from SHOW CREATE TABLE.
		table := &Table{
			Name:      rawTable.Name,
			Engine:    rawTable.Engine.String,
			Collation: rawTable.TableCollation.String,
			Comment:   rawTable.Comment,
		}
		if underscore := strings.IndexByte(table.Collation, '_'); underscore > 0 {
			table.CharSet = table.Collation[0:underscore]
			if flavor.AlwaysShowCollate() {
				table.ShowCollation = true
			} else if !collationIsDefault(table.Collation, table.CharSet, flavor) {
				table.ShowCollation = true
			} else if table.CharSet == "utf8mb4" && flavor.MinMySQL(8) {
				table.ShowCollation = true
			}
		} else {
			table.CharSet = "undefined"
		}
		if rawTable.CreateOptions.Valid && rawTable.CreateOptions.String != "" {
			if strings.Contains(strings.ToUpper(rawTable.CreateOptions.String), "PARTITIONED") {
				havePartitions = true
			}
			table.CreateOptions = reformatCreateOptions(rawTable.CreateOptions.String)
		}
		tables = append(tables, table)
	}

	insp.schema.Tables = tables

	subtasks := make([]introspectorTask, 0, len(haveAltType)+len(tables)+5)
	for ttype := range haveAltType {
		if task, ok := altTableTypeTasks[ttype]; ok {
			subtasks = append(subtasks, task)
		}
	}
	for _, t := range tables {
		subtasks = append(subtasks, t.introspectShowCreate)
	}
	if len(tables) > 0 {
		subtasks = append(subtasks,
			introspectColumns,
			introspectIndexes,
			introspectForeignKeys,
		)
		if flavor.HasCheckConstraints() {
			subtasks = append(subtasks, introspectChecks)
		}
		if havePartitions {
			subtasks = append(subtasks, introspectPartitioning)
		}
	}
	for _, task := range subtasks {
		insp.Go(ctx, task)
	}
	return nil
}

// fixupTables cleans up edge cases in information_schema where data must be
// obtained from SHOW CREATE TABLE instead.
func fixupTables(schema *Schema, flavor Flavor) {
	for _, t := range schema.Tables {
		if t.Partitioning != nil {
			fixPartitioningEdgeCases(t, flavor)
		}

		if t.NextAutoIncrement == 0 && t.HasAutoIncrement() {
			t.NextAutoIncrement = 1
		}

		// Index order is unpredictable with new MySQL 8 data dictionary, so reorder
		// indexes based on parsing SHOW CREATE TABLE if needed
		if flavor.MinMySQL(8) && len(t.SecondaryIndexes) > 1 {
			fixIndexOrder(t)
		}
		// Foreign keys order is unpredictable in MySQL before 5.6, so reorder
		// foreign keys based on parsing SHOW CREATE TABLE if needed
		if !flavor.SortedForeignKeys() && len(t.ForeignKeys) > 1 {
			fixForeignKeyOrder(t)
		}
		// Create options order is unpredictable with the new MySQL 8 data dictionary
		// Also need to fix some charset/collation edge cases in SHOW CREATE TABLE
		// behavior in MySQL 8
		if flavor.MinMySQL(8) {
			fixCreateOptionsOrder(t, flavor)
			fixShowCharSets(t)
		}
		// MySQL 5.7+ generated column expressions must be reparased from SHOW CREATE
		// TABLE to properly obtain any 4-byte chars. Additionally in 8.0 the I_S
		// representation has incorrect escaping and potentially different charset
		// in string literal introducers.
		if flavor.MinMySQL(5, 7) {
			fixGenerationExpr(t, flavor)
		}
		// Percona Server column compression can only be parsed from SHOW CREATE
		// TABLE. (Although it also has new I_S tables, their name differs pre-8.0
		// vs post-8.0, and cols that aren't using a COMPRESSION_DICTIONARY are not
		// even present there.)
		if flavor.IsPercona() && flavor.MinMySQL(5, 6, 33) && strings.Contains(t.CreateStatement, "COLUMN_FORMAT COMPRESSED") {
			fixPerconaColCompression(t)
		}
		// FULLTEXT indexes may have a PARSER clause, which isn't exposed in I_S
		if strings.Contains(t.CreateStatement, "WITH PARSER") {
			fixFulltextIndexParsers(t, flavor)
		}
		// Fix problems with I_S data for default expressions as well as functional
		// indexes in MySQL 8+
		if flavor.MinMySQL(8) {
			fixDefaultExpression(t, flavor)
			fixIndexExpression(t, flavor)
		}
		// Fix shortcoming in I_S data for check constraints
		if len(t.Checks) > 0 {
			fixChecks(t, flavor)
		}
		// MariaDB's VECTOR indexes can have M and/or DISTANCE attributes, which
		// aren't exposed anywhere in I_S
		if flavor.MinMariaDB(11, 7) && strings.Contains(t.CreateStatement, "VECTOR KEY") {
			fixVectorIndexes(t, flavor)
		}

		// Compare what we expect the create DDL to be, to determine if we support
		// diffing for the table. (No need to remove next AUTO_INCREMENT from this
		// comparison since the value was parsed from t.CreateStatement earlier.)
		if t.CreateStatement != t.GeneratedCreateStatement(flavor) {
			t.UnsupportedDDL = true
		}
	}
}

func (t *Table) introspectShowCreate(ctx context.Context, insp *introspector) (err error) {
	t.CreateStatement, err = showCreateTable(ctx, insp.db, insp.schema.Name, t.Name)
	if err != nil {
		return fmt.Errorf("Error executing SHOW CREATE TABLE for %s.%s: %w", EscapeIdentifier(insp.schema.Name), EscapeIdentifier(t.Name), err)
	}

	// Obtain TABLESPACE clause from SHOW CREATE TABLE, if present
	t.Tablespace = ParseCreateTablespace(t.CreateStatement)

	// Obtain next AUTO_INCREMENT value from SHOW CREATE TABLE, which avoids
	// potential problems with information_schema discrepancies
	_, t.NextAutoIncrement = ParseCreateAutoInc(t.CreateStatement)

	// Remove column and index attributes which don't affect InnoDB
	if t.Engine == "InnoDB" {
		t.CreateStatement = StripNonInnoAttributes(t.CreateStatement)
	}

	return nil
}

func showCreateTable(ctx context.Context, db *sqlx.DB, schema, table string) (string, error) {
	return showCreateObject(ctx, db, schema, ObjectTypeTable, table)
}

var reExtraOnUpdate = regexp.MustCompile(`(?i)\bon update (current_timestamp(?:\(\d*\))?)`)

func introspectColumns(ctx context.Context, insp *introspector) error {
	flavor := insp.instance.Flavor()
	stripDisplayWidth := flavor.OmitIntDisplayWidth()
	var mariaCompressedColMarker string
	if flavor.MinMariaDB(10, 3) {
		mariaCompressedColMarker = " " + flavor.compressedColumnOpenComment() + "COMPRESSED"
	}
	var rawColumns []struct {
		Name               string         `db:"column_name"`
		TableName          string         `db:"table_name"`
		Type               string         `db:"column_type"`
		IsNullable         string         `db:"is_nullable"`
		Default            sql.NullString `db:"column_default"`
		Extra              string         `db:"extra"`
		GenerationExpr     sql.NullString `db:"generation_expression"`
		Comment            string         `db:"column_comment"`
		CharSet            sql.NullString `db:"character_set_name"`
		Collation          sql.NullString `db:"collation_name"`
		SpatialReferenceID sql.NullInt64  `db:"srs_id"`
	}
	query := `
		SELECT   SQL_BUFFER_RESULT
		         table_name AS table_name, column_name AS column_name,
		         column_type AS column_type, is_nullable AS is_nullable,
		         column_default AS column_default, extra AS extra,
		         %s AS generation_expression,
		         column_comment AS column_comment,
		         character_set_name AS character_set_name,
		         collation_name AS collation_name,
		         %s AS srs_id
		FROM     information_schema.columns
		WHERE    table_schema = ?
		ORDER BY table_name, ordinal_position`
	genExpr, srid := "NULL", "NULL"
	if flavor.GeneratedColumns() {
		genExpr = "generation_expression"
	}
	if flavor.MinMySQL(8) {
		srid = "srs_id"
	}
	// Note: we could get MariaDB SRIDs from information_schema.geometry_columns.srid
	// but since MariaDB doesn't expose its REF_SYSTEM_ID attribute in SHOW CREATE
	// TABLE there's currently no point to querying them

	query = fmt.Sprintf(query, genExpr, srid)
	if err := insp.db.SelectContext(ctx, &rawColumns, query, insp.schema.Name); err != nil {
		return fmt.Errorf("Error querying information_schema.columns for schema %s: %s", insp.schema.Name, err)
	}
	columnsByTableName := make(map[string][]*Column)
	for _, rawColumn := range rawColumns {
		// MariaDB includes compression attribute in column type; remove it BEFORE
		// parsing the column type
		var mariaCompressedColumn bool
		if mariaCompressedColMarker != "" {
			rawColumn.Type, _, mariaCompressedColumn = strings.Cut(rawColumn.Type, mariaCompressedColMarker)
		}

		col := &Column{
			Name:          rawColumn.Name,
			Type:          ParseColumnType(rawColumn.Type),
			Nullable:      strings.EqualFold(rawColumn.IsNullable, "YES"),
			AutoIncrement: strings.Contains(rawColumn.Extra, "auto_increment"),
			Comment:       rawColumn.Comment,
			Invisible:     strings.Contains(rawColumn.Extra, "INVISIBLE"),
		}
		if mariaCompressedColumn {
			col.Compression = "COMPRESSED"
		}

		// If db was upgraded from a pre-8.0.19 version (but still 8.0+) to 8.0.19+,
		// I_S may still contain int display widths even though SHOW CREATE TABLE
		// omits them. Strip to avoid incorrectly flagging the table as unsupported
		// for diffs.
		if stripDisplayWidth {
			col.Type.StripDisplayWidth() // safe/no-op if already no int display width
		}
		if rawColumn.GenerationExpr.Valid {
			col.GenerationExpr = rawColumn.GenerationExpr.String
			col.Virtual = strings.Contains(rawColumn.Extra, "VIRTUAL GENERATED")
		}
		if !rawColumn.Default.Valid {
			allowNullDefault := col.Nullable && !col.AutoIncrement && col.GenerationExpr == ""
			// Only MariaDB 10.2+ allows blob/text default literals, including explicit
			// DEFAULT NULL clause.
			// Recent versions of MySQL do allow default *expressions* for these col
			// types, but 8.0.13-8.0.22 erroneously omit them from I_S, so we need to
			// catch this situation and parse from SHOW CREATE later.
			if !flavor.MinMariaDB(10, 2) && (strings.HasSuffix(col.Type.Base, "blob") || strings.HasSuffix(col.Type.Base, "text")) {
				allowNullDefault = false
				if strings.Contains(rawColumn.Extra, "DEFAULT_GENERATED") {
					col.Default = "(!!!BLOBDEFAULT!!!)"
				}
			}
			if allowNullDefault {
				col.Default = "NULL"
			}
		} else if flavor.MinMariaDB(10, 2) {
			if !col.AutoIncrement && col.GenerationExpr == "" {
				// MariaDB 10.2+ exposes defaults as expressions / quote-wrapped strings
				col.Default = rawColumn.Default.String
			}
		} else if strings.HasPrefix(rawColumn.Default.String, "CURRENT_TIMESTAMP") && (strings.HasPrefix(rawColumn.Type, "timestamp") || strings.HasPrefix(rawColumn.Type, "datetime")) {
			col.Default = rawColumn.Default.String
		} else if strings.HasPrefix(rawColumn.Type, "bit") && strings.HasPrefix(rawColumn.Default.String, "b'") {
			col.Default = rawColumn.Default.String
		} else if strings.Contains(rawColumn.Extra, "DEFAULT_GENERATED") {
			// MySQL 8.0.13+ supports default expressions, which are paren-wrapped in
			// SHOW CREATE TABLE in MySQL. However MySQL I_S data has some issues for
			// default expressions. The most common one is fixed here, and if additional
			// mismatches remain, they get corrected by fixDefaultExpression later on.
			col.Default = "(" + strings.ReplaceAll(rawColumn.Default.String, "\\'", "'") + ")"
		} else {
			col.Default = "'" + EscapeValueForCreateTable(rawColumn.Default.String) + "'"
		}
		if matches := reExtraOnUpdate.FindStringSubmatch(rawColumn.Extra); matches != nil {
			col.OnUpdate = matches[1]
			// Some flavors omit fractional precision from ON UPDATE in
			// information_schema only, despite it being present everywhere else
			if openParen := strings.IndexByte(rawColumn.Type, '('); openParen > -1 && !strings.Contains(col.OnUpdate, "(") {
				col.OnUpdate = col.OnUpdate + rawColumn.Type[openParen:]
			}
		}
		if rawColumn.Collation.Valid { // only text-based column types have a notion of charset and collation
			col.CharSet = rawColumn.CharSet.String
			col.Collation = rawColumn.Collation.String
			// note: fields ShowCharSet and ShowCollation are set by caller, since these
			// require comparing things to the table's defaults, and we don't have the
			// full table here
		}
		if rawColumn.SpatialReferenceID.Valid { // Spatial columns in MySQL 8+ can have optional SRID
			col.HasSpatialReference = true
			col.SpatialReferenceID = uint32(rawColumn.SpatialReferenceID.Int64)
		}
		if columnsByTableName[rawColumn.TableName] == nil {
			columnsByTableName[rawColumn.TableName] = make([]*Column, 0)
		}
		columnsByTableName[rawColumn.TableName] = append(columnsByTableName[rawColumn.TableName], col)
	}

	// Place the columns into the tables in insp.schema
	for _, t := range insp.schema.Tables {
		t.Columns = columnsByTableName[t.Name]
		for _, col := range t.Columns {
			// Set ShowCharSet and ShowCollation for each column as appropriate. This
			// isn't handled by queryColumnsInSchema because it requires the full table.
			if col.Collation != "" {
				// Column-level CHARACTER SET shown whenever the *collation* differs from
				// table's default
				col.ShowCharSet = (col.Collation != t.Collation)
				if flavor.AlwaysShowCollate() {
					// Since Nov 2022, MariaDB always shows a COLLATE clause if showing charset
					col.ShowCollation = col.ShowCharSet
				} else {
					// Other flavors show a COLLATE clause whenever the collation isn't the
					// default one for the charset.
					col.ShowCollation = !collationIsDefault(col.Collation, col.CharSet, flavor)
				}
				// Note: MySQL 8 has additional edge cases for both ShowCharSet and
				// ShowCollation, both of which are handled later in fixShowCharSets
			}
		}
	}
	return nil
}

func introspectIndexes(ctx context.Context, insp *introspector) error {
	flavor := insp.instance.Flavor()
	var rawIndexes []struct {
		Name       string         `db:"index_name"`
		TableName  string         `db:"table_name"`
		NonUnique  uint8          `db:"non_unique"`
		SeqInIndex uint8          `db:"seq_in_index"`
		ColumnName sql.NullString `db:"column_name"`
		SubPart    sql.NullInt64  `db:"sub_part"`
		Comment    sql.NullString `db:"index_comment"`
		Type       string         `db:"index_type"`
		Collation  sql.NullString `db:"collation"`
		Expression sql.NullString `db:"expression"`
		Visible    string         `db:"is_visible"`
	}
	query := `
		SELECT   SQL_BUFFER_RESULT
		         index_name AS index_name, table_name AS table_name,
		         non_unique AS non_unique, seq_in_index AS seq_in_index,
		         column_name AS column_name, sub_part AS sub_part,
		         index_comment AS index_comment, index_type AS index_type,
		         collation AS collation, %s AS expression, %s AS is_visible
		FROM     information_schema.statistics
		WHERE    table_schema = ?`
	exprSelect, visSelect := "NULL", "'YES'"
	if flavor.MinMySQL(8) {
		// Index expressions added in 8.0.13
		if flavor.MinMySQL(8, 0, 13) {
			exprSelect = "expression"
		}
		visSelect = "is_visible" // available in all 8.0
	} else if flavor.MinMariaDB(10, 6) {
		// MariaDB I_S uses the inverse: YES for ignored (invisible), NO for visible
		visSelect = "IF(ignored = 'YES', 'NO', 'YES')"
	}
	query = fmt.Sprintf(query, exprSelect, visSelect)
	if err := insp.db.SelectContext(ctx, &rawIndexes, query, insp.schema.Name); err != nil {
		return fmt.Errorf("Error querying information_schema.statistics for schema %s: %s", insp.schema.Name, err)
	}

	primaryKeyByTableName := make(map[string]*Index)
	secondaryIndexesByTableName := make(map[string][]*Index)

	// Since multi-column indexes have multiple rows in the result set, we do two
	// passes over the result: one to figure out which indexes exist, and one to
	// stitch together the col info. We cannot use an ORDER BY on this query, since
	// only the unsorted result matches the same order of secondary indexes as the
	// CREATE TABLE statement.
	indexesByTableAndName := make(map[string]*Index)
	for _, rawIndex := range rawIndexes {
		if rawIndex.SeqInIndex > 1 {
			continue
		}
		index := &Index{
			Name:      rawIndex.Name,
			Unique:    rawIndex.NonUnique == 0,
			Comment:   rawIndex.Comment.String,
			Type:      rawIndex.Type,
			Invisible: (rawIndex.Visible == "NO"),
		}
		if strings.EqualFold(index.Name, "PRIMARY") {
			index.PrimaryKey = true
			primaryKeyByTableName[rawIndex.TableName] = index
		} else {
			secondaryIndexesByTableName[rawIndex.TableName] = append(secondaryIndexesByTableName[rawIndex.TableName], index)
		}
		fullNameStr := rawIndex.TableName + "." + rawIndex.Name
		indexesByTableAndName[fullNameStr] = index
	}
	for _, rawIndex := range rawIndexes {
		fullIndexNameStr := rawIndex.TableName + "." + rawIndex.Name
		index, ok := indexesByTableAndName[fullIndexNameStr]
		if !ok {
			panic(fmt.Errorf("Cannot find index %s", fullIndexNameStr))
		}
		for len(index.Parts) < int(rawIndex.SeqInIndex) {
			index.Parts = append(index.Parts, IndexPart{})
		}
		part := &index.Parts[rawIndex.SeqInIndex-1]
		part.ColumnName = rawIndex.ColumnName.String
		part.Expression = rawIndex.Expression.String
		part.Descending = (rawIndex.Collation.String == "D")
		if rawIndex.Type != "SPATIAL" { // Sub-part value only used for non-SPATIAL indexes
			part.PrefixLength = uint16(rawIndex.SubPart.Int64)
		}
	}

	for _, t := range insp.schema.Tables {
		t.PrimaryKey = primaryKeyByTableName[t.Name]
		t.SecondaryIndexes = secondaryIndexesByTableName[t.Name]
	}
	return nil
}

func introspectForeignKeys(ctx context.Context, insp *introspector) error {
	var rawForeignKeys []struct {
		Name                 string `db:"constraint_name"`
		TableName            string `db:"table_name"`
		ColumnName           string `db:"column_name"`
		UpdateRule           string `db:"update_rule"`
		DeleteRule           string `db:"delete_rule"`
		ReferencedTableName  string `db:"referenced_table_name"`
		ReferencedSchemaName string `db:"referenced_schema"`
		ReferencedColumnName string `db:"referenced_column_name"`
	}
	query := `
		SELECT   SQL_BUFFER_RESULT
		         rc.constraint_name AS constraint_name, rc.table_name AS table_name,
		         kcu.column_name AS column_name,
		         rc.update_rule AS update_rule, rc.delete_rule AS delete_rule,
		         rc.referenced_table_name AS referenced_table_name,
		         IF(rc.constraint_schema=rc.unique_constraint_schema, '', rc.unique_constraint_schema) AS referenced_schema,
		         kcu.referenced_column_name AS referenced_column_name
		FROM     information_schema.referential_constraints rc
		JOIN     information_schema.key_column_usage kcu ON kcu.constraint_name = rc.constraint_name AND
		                                 kcu.table_schema = ? AND
		                                 kcu.referenced_column_name IS NOT NULL
		WHERE    rc.constraint_schema = ?
		ORDER BY BINARY rc.constraint_name, kcu.ordinal_position`
	if err := insp.db.SelectContext(ctx, &rawForeignKeys, query, insp.schema.Name, insp.schema.Name); err != nil {
		return fmt.Errorf("Error querying foreign key constraints for schema %s: %s", insp.schema.Name, err)
	}
	foreignKeysByTableName := make(map[string][]*ForeignKey)
	foreignKeysByName := make(map[string]*ForeignKey)
	for _, rawForeignKey := range rawForeignKeys {
		if fk, already := foreignKeysByName[rawForeignKey.Name]; already {
			fk.ColumnNames = append(fk.ColumnNames, rawForeignKey.ColumnName)
			fk.ReferencedColumnNames = append(fk.ReferencedColumnNames, rawForeignKey.ReferencedColumnName)
		} else {
			foreignKey := &ForeignKey{
				Name:                  rawForeignKey.Name,
				ReferencedSchemaName:  rawForeignKey.ReferencedSchemaName,
				ReferencedTableName:   rawForeignKey.ReferencedTableName,
				UpdateRule:            rawForeignKey.UpdateRule,
				DeleteRule:            rawForeignKey.DeleteRule,
				ColumnNames:           []string{rawForeignKey.ColumnName},
				ReferencedColumnNames: []string{rawForeignKey.ReferencedColumnName},
			}
			foreignKeysByName[rawForeignKey.Name] = foreignKey
			foreignKeysByTableName[rawForeignKey.TableName] = append(foreignKeysByTableName[rawForeignKey.TableName], foreignKey)
		}
	}

	for _, t := range insp.schema.Tables {
		t.ForeignKeys = foreignKeysByTableName[t.Name]
	}
	return nil
}

func introspectChecks(ctx context.Context, insp *introspector) error {
	checksByTableName := make(map[string][]*Check)
	var rawChecks []struct {
		Name      string `db:"constraint_name"`
		Clause    string `db:"check_clause"`
		TableName string `db:"table_name"`
		Enforced  string `db:"enforced"`
	}

	// With MariaDB, information_schema.check_constraints has what we need. But
	// nothing in I_S reveals differences between inline-column checks and regular
	// checks, so that is handled separately by parsing SHOW CREATE TABLE later in
	// a fixup function. Also intentionally no ORDER BY in this query; the returned
	// order matches that of SHOW CREATE TABLE (which isn't usually alphabetical).
	//
	// With MySQL, we need to get table names and enforcement status from
	// information_schema.table_constraints. We don't even bother querying
	// information_schema.check_constraints because the clause value there has
	// broken double-escaping logic. Instead we parse bodies from SHOW CREATE
	// TABLE separately in a fixup function.
	var query string
	if insp.instance.Flavor().IsMariaDB() {
		query = `
			SELECT   SQL_BUFFER_RESULT
			         constraint_name AS constraint_name, check_clause AS check_clause,
			         table_name AS table_name, 'YES' AS enforced
			FROM     information_schema.check_constraints
			WHERE    constraint_schema = ?`
	} else {
		query = `
			SELECT   SQL_BUFFER_RESULT
			         constraint_name AS constraint_name, '' AS check_clause,
			         table_name AS table_name, enforced AS enforced
			FROM     information_schema.table_constraints
			WHERE    table_schema = ? AND constraint_type = 'CHECK'
			ORDER BY table_name, constraint_name`
	}
	if err := insp.db.SelectContext(ctx, &rawChecks, query, insp.schema.Name); err != nil {
		return fmt.Errorf("Error querying check constraints for schema %s: %s", insp.schema.Name, err)
	}
	for _, rawCheck := range rawChecks {
		check := &Check{
			Name:     rawCheck.Name,
			Clause:   rawCheck.Clause,
			Enforced: !strings.EqualFold(rawCheck.Enforced, "NO"),
		}
		checksByTableName[rawCheck.TableName] = append(checksByTableName[rawCheck.TableName], check)
	}

	for _, t := range insp.schema.Tables {
		t.Checks = checksByTableName[t.Name]
	}
	return nil
}

func introspectPartitioning(ctx context.Context, insp *introspector) error {
	var rawPartitioning []struct {
		TableName     string         `db:"table_name"`
		PartitionName string         `db:"partition_name"`
		SubName       sql.NullString `db:"subpartition_name"`
		Method        string         `db:"partition_method"`
		SubMethod     sql.NullString `db:"subpartition_method"`
		Expression    sql.NullString `db:"partition_expression"`
		SubExpression sql.NullString `db:"subpartition_expression"`
		Values        sql.NullString `db:"partition_description"`
		Comment       string         `db:"partition_comment"`
	}
	query := `
		SELECT   SQL_BUFFER_RESULT
		         p.table_name AS table_name, p.partition_name AS partition_name,
		         p.subpartition_name AS subpartition_name,
		         p.partition_method AS partition_method,
		         p.subpartition_method AS subpartition_method,
		         p.partition_expression AS partition_expression,
		         p.subpartition_expression AS subpartition_expression,
		         p.partition_description AS partition_description,
		         p.partition_comment AS partition_comment
		FROM     information_schema.partitions p
		WHERE    p.table_schema = ?
		AND      p.partition_name IS NOT NULL
		ORDER BY p.table_name, p.partition_ordinal_position,
		         p.subpartition_ordinal_position`
	if err := insp.db.SelectContext(ctx, &rawPartitioning, query, insp.schema.Name); err != nil {
		return fmt.Errorf("Error querying information_schema.partitions for schema %s: %s", insp.schema.Name, err)
	}

	partitioningByTableName := make(map[string]*TablePartitioning)
	for _, rawPart := range rawPartitioning {
		p, ok := partitioningByTableName[rawPart.TableName]
		if !ok {
			p = &TablePartitioning{
				Method:        rawPart.Method,
				SubMethod:     rawPart.SubMethod.String,
				Expression:    rawPart.Expression.String,
				SubExpression: rawPart.SubExpression.String,
				Partitions:    make([]*Partition, 0),
			}
			partitioningByTableName[rawPart.TableName] = p
		}
		p.Partitions = append(p.Partitions, &Partition{
			Name:    rawPart.PartitionName,
			SubName: rawPart.SubName.String,
			Values:  rawPart.Values.String,
			Comment: rawPart.Comment,
		})
	}

	for _, t := range insp.schema.Tables {
		if p, ok := partitioningByTableName[t.Name]; ok {
			for _, part := range p.Partitions {
				part.Engine = t.Engine
			}
			t.Partitioning = p
		}
	}
	return nil
}

var reIndexLine = regexp.MustCompile("^\\s+(?:UNIQUE |FULLTEXT |SPATIAL )?KEY `((?:[^`]|``)+)` (?:USING \\w+ )?\\([`(]")

// MySQL 8.0 uses a different index order in SHOW CREATE TABLE than in
// information_schema. This function fixes the struct to match SHOW CREATE
// TABLE's ordering.
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
	if cur != len(t.SecondaryIndexes) {
		panic(fmt.Errorf("Failed to parse indexes of %s for reordering: only matched %d of %d secondary indexes", t.Name, cur, len(t.SecondaryIndexes)))
	}
}

var reForeignKeyLine = regexp.MustCompile("^\\s+CONSTRAINT `((?:[^`]|``)+)` FOREIGN KEY")

// MySQL 5.5 doesn't alphabetize foreign keys; this function fixes the struct
// to match SHOW CREATE TABLE's order
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

// MySQL 8.0 uses a different order for table options in SHOW CREATE TABLE
// than in information_schema. This function fixes the struct to match SHOW
// CREATE TABLE's ordering.
func fixCreateOptionsOrder(t *Table, flavor Flavor) {
	if !strings.Contains(t.CreateOptions, " ") {
		return
	}

	// Use the generated (but incorrectly-ordered) create statement to build a
	// regexp that pulls out the create options from the actual create string
	genCreate := t.GeneratedCreateStatement(flavor)
	var template string
	for _, line := range strings.Split(genCreate, "\n") {
		if strings.HasPrefix(line, ") ENGINE=") {
			template = line
			break
		}
	}
	template = strings.Replace(template, t.CreateOptions, "!!!CREATEOPTS!!!", 1)
	template = regexp.QuoteMeta(template)
	template = strings.Replace(template, "!!!CREATEOPTS!!!", "(.+)", 1)
	re := regexp.MustCompile("^" + template + "$")

	for _, line := range strings.Split(t.CreateStatement, "\n") {
		if strings.HasPrefix(line, ") ENGINE=") {
			matches := re.FindStringSubmatch(line)
			if matches != nil {
				t.CreateOptions = matches[1]
				return
			}
		}
	}
}

// fixShowCharSets parses SHOW CREATE TABLE to set ShowCharSet and ShowCollation
// for columns when needed in MySQL 8:
//
// Prior to MySQL 8, the logic behind inclusion of column-level CHARACTER SET
// and COLLATE clauses in SHOW CREATE TABLE was weird but straightforward:
// CHARACTER SET was included whenever the col's *collation* differed from the
// table's default; COLLATION was included whenever the col's collation differed
// from the default collation *of the col's charset*.
//
// MySQL 8 includes these clauses unnecessarily in additional situations:
//   - 8.0 includes column-level character sets and collations whenever specified
//     explicitly in the original CREATE, even when equal to the table's defaults
//   - Tables upgraded from pre-8.0 may omit COLLATE if it's the default for the
//     charset, while tables created in 8.0 will generally include it whenever a
//     CHARACTER SET is shown in a column definition
func fixShowCharSets(t *Table) {
	lines := strings.Split(t.CreateStatement, "\n")
	for n, col := range t.Columns {
		if col.CharSet == "" || col.Collation == "" {
			continue // non-character-based column type, nothing to do
		}
		line := lines[n+1] // columns start on second line of CREATE TABLE
		if !col.ShowCharSet && strings.Contains(line, "CHARACTER SET "+col.CharSet) {
			col.ShowCharSet = true
		}
		if !col.ShowCollation && strings.Contains(line, "COLLATE "+col.Collation) {
			col.ShowCollation = true
		}
	}
}

// MySQL 5.7+ supports generated columns, but mangles them in I_S in various
// ways:
//   - 4-byte characters are not returned properly in I_S since it uses utf8mb3
//   - MySQL 8 incorrectly mangles escaping of single quotes in the I_S value
//   - MySQL 8 potentially uses different charsets introducers for string literals
//     in I_S vs SHOW CREATE
//
// This method modifies each generated Column.GenerationExpr to match SHOW
// CREATE's version.
func fixGenerationExpr(t *Table, flavor Flavor) {
	for _, col := range t.Columns {
		if col.GenerationExpr == "" {
			continue
		}
		if colDefinition := col.Definition(flavor); !strings.Contains(t.CreateStatement, colDefinition) {
			var genKind string
			if col.Virtual {
				genKind = "VIRTUAL"
			} else {
				genKind = "STORED"
			}
			reTemplate := `(?m)^\s*` + regexp.QuoteMeta(EscapeIdentifier(col.Name)) + `.+GENERATED ALWAYS AS \((.+)\) ` + genKind
			re := regexp.MustCompile(reTemplate)
			if matches := re.FindStringSubmatch(t.CreateStatement); matches != nil {
				col.GenerationExpr = matches[1]
			}
		}
	}
}

// fixPartitioningEdgeCases handles situations that are reflected in SHOW CREATE
// TABLE, but missing (or difficult to obtain) in information_schema.
func fixPartitioningEdgeCases(t *Table, flavor Flavor) {
	// Handle edge cases for how partitions are expressed in HASH or KEY methods:
	// typically this will just be a PARTITIONS N clause, but it could also be
	// nothing at all, or an explicit list of partitions, depending on how the
	// partitioning was originally created.
	if strings.HasSuffix(t.Partitioning.Method, "HASH") || strings.HasSuffix(t.Partitioning.Method, "KEY") {
		countClause := fmt.Sprintf("\nPARTITIONS %d", len(t.Partitioning.Partitions))
		if strings.Contains(t.CreateStatement, countClause) {
			t.Partitioning.ForcePartitionList = PartitionListCount
		} else if strings.Contains(t.CreateStatement, "\n(PARTITION ") {
			t.Partitioning.ForcePartitionList = PartitionListExplicit
		} else if len(t.Partitioning.Partitions) == 1 {
			t.Partitioning.ForcePartitionList = PartitionListNone
		}
	}

	// KEY methods support an optional ALGORITHM clause, which is present in SHOW
	// CREATE TABLE but not anywhere in information_schema
	if strings.HasSuffix(t.Partitioning.Method, "KEY") && strings.Contains(t.CreateStatement, "ALGORITHM") {
		re := regexp.MustCompile(fmt.Sprintf(`PARTITION BY %s ([^(]*)\(`, t.Partitioning.Method))
		if matches := re.FindStringSubmatch(t.CreateStatement); matches != nil {
			t.Partitioning.AlgoClause = matches[1]
		}
	}

	// Process DATA DIRECTORY clauses, which are easier to parse from SHOW CREATE
	// TABLE instead of information_schema.innodb_sys_tablespaces.
	if (t.Partitioning.ForcePartitionList == PartitionListDefault || t.Partitioning.ForcePartitionList == PartitionListExplicit) &&
		strings.Contains(t.CreateStatement, " DATA DIRECTORY = ") {
		for _, p := range t.Partitioning.Partitions {
			name := p.Name
			if flavor.MinMariaDB(10, 2) {
				name = EscapeIdentifier(name)
			}
			name = regexp.QuoteMeta(name)
			re := regexp.MustCompile(fmt.Sprintf(`PARTITION %s .*DATA DIRECTORY = '((?:\\\\|\\'|''|[^'])*)'`, name))
			if matches := re.FindStringSubmatch(t.CreateStatement); matches != nil {
				p.DataDir = matches[1]
			}
		}
	}
}

var rePerconaColCompressionLine = regexp.MustCompile("^\\s+`((?:[^`]|``)+)` .* /\\*!50633 COLUMN_FORMAT (COMPRESSED[^*]*) \\*/")

// fixPerconaColCompression parses the table's CREATE string in order to
// populate Column.Compression for columns that are using Percona Server's
// column compression feature, which isn't reflected in information_schema.
func fixPerconaColCompression(t *Table) {
	colsByName := t.ColumnsByName()
	for _, line := range strings.Split(t.CreateStatement, "\n") {
		matches := rePerconaColCompressionLine.FindStringSubmatch(line)
		if matches == nil {
			continue
		}
		colsByName[matches[1]].Compression = matches[2]
	}
}

// fixFulltextIndexParsers parses the table's CREATE string in order to
// populate Index.FullTextParser for any fulltext indexes that specify a parser.
func fixFulltextIndexParsers(t *Table, flavor Flavor) {
	for _, idx := range t.SecondaryIndexes {
		if idx.Type == "FULLTEXT" {
			// Obtain properly-formatted index definition without parser clause, and
			// then build a regex from this which captures the parser name.
			var template string
			if flavor.MinMariaDB(11, 7) {
				template = idx.Definition(flavor) + " WITH PARSER "
			} else {
				template = idx.Definition(flavor) + " /*!50100 WITH PARSER "
			}
			template = regexp.QuoteMeta(template)
			template += "`([^`]+)`"
			re := regexp.MustCompile(template)
			matches := re.FindStringSubmatch(t.CreateStatement)
			if matches != nil { // only matches if a parser is specified
				idx.FullTextParser = matches[1]
			}
		}
	}
}

// fixDefaultExpression parses the table's CREATE string in order to correct
// problems in Column.Default for columns using a default expression in MySQL 8:
//   - In MySQL 8.0.13-8.0.22, blob/text cols may have default expressions but
//     these are omitted from I_S due to a bug fixed in MySQL 8.0.23.
//   - 4-byte characters are not returned properly in I_S since it uses utf8mb3
//   - MySQL 8 incorrectly mangles escaping of single quotes in the I_S value
//   - MySQL 8 potentially uses different charsets introducers for string literals
//     in I_S vs SHOW CREATE
//
// It also fixes problems with BINARY / VARBINARY literal constant defaults in
// MySQL 8, as these are also mangled by I_S if a zero byte is present.
func fixDefaultExpression(t *Table, flavor Flavor) {
	for _, col := range t.Columns {
		if col.Default == "" {
			continue
		}
		var matcher string
		if col.Default[0] == '(' {
			matcher = `.+DEFAULT (\(.+\))`
		} else if strings.HasPrefix(col.Default, "'0x") && (col.Type.Base == "binary" || col.Type.Base == "varbinary") {
			matcher = `.+DEFAULT ('(''|[^'])*')`
		} else {
			continue
		}
		if colDefinition := col.Definition(flavor); !strings.Contains(t.CreateStatement, colDefinition) {
			defaultClause := " DEFAULT " + col.Default
			after := colDefinition[strings.Index(colDefinition, defaultClause)+len(defaultClause):]
			reTemplate := `(?m)^\s*` + regexp.QuoteMeta(EscapeIdentifier(col.Name)) + matcher + regexp.QuoteMeta(after)
			re := regexp.MustCompile(reTemplate)
			if matches := re.FindStringSubmatch(t.CreateStatement); matches != nil {
				col.Default = matches[1]
			}
		}
	}
}

// fixIndexExpression parses the table's CREATE string in order to correct
// problems in index expressions (functional indexes) in MySQL 8:
// * 4-byte characters are not returned properly in I_S since it uses utf8mb3
// * MySQL 8 incorrectly mangles escaping of single quotes in the I_S value
func fixIndexExpression(t *Table, flavor Flavor) {
	// Only need to check secondary indexes, since PK can't contain expressions
	for _, idx := range t.SecondaryIndexes {
		if !idx.Functional() {
			continue
		}
		if idxDefinition := idx.Definition(flavor); !strings.Contains(t.CreateStatement, idxDefinition) {
			exprParts := make([]*IndexPart, 0, len(idx.Parts))
			for n := range idx.Parts {
				if idx.Parts[n].Expression != "" {
					idxDefinition = strings.Replace(idxDefinition, idx.Parts[n].Expression, "!!!EXPR!!!", 1)
					exprParts = append(exprParts, &idx.Parts[n])
				}
			}
			// Build a regex which captures just the index expression(s) for this index
			reTemplate := regexp.QuoteMeta(idxDefinition)
			reTemplate = `(?m)^\s*` + strings.ReplaceAll(reTemplate, "!!!EXPR!!!", "(.*)") + `,?$`
			re := regexp.MustCompile(reTemplate)
			matches := re.FindStringSubmatch(t.CreateStatement)
			for n := 1; n < len(matches); n++ {
				exprParts[n-1].Expression = matches[n]
			}
		}
	}
}

// fixChecks handles the problematic information_schema data for check
// constraints, which is faulty in both MySQL and MariaDB but in different ways.
func fixChecks(t *Table, flavor Flavor) {
	// MariaDB handles CHECKs differently when they're defined inline in a column
	// definition: in this case I_S shows them having a name equal to the column
	// name, but cannot be manipulated using this name directly, nor does this
	// prevent explicitly-named checks from also having that same name.
	// MariaDB also truncates the check clause at 64 bytes in I_S, so we must
	// parse longer checks from SHOW CREATE TABLE.
	if flavor.IsMariaDB() {
		colsByName := t.ColumnsByName()
		var keep []*Check
		for _, cc := range t.Checks {
			if len(cc.Clause) == 64 {
				// This regex is designed to match regular checks as well as inline-column
				template := fmt.Sprintf(`%s[^\n]+CHECK \((%s[^\n]*)\),?\n`,
					regexp.QuoteMeta(EscapeIdentifier(cc.Name)),
					regexp.QuoteMeta(cc.Clause))
				re := regexp.MustCompile(template)
				if matches := re.FindStringSubmatch(t.CreateStatement); matches != nil {
					cc.Clause = matches[1]
				}
			}
			if col, ok := colsByName[cc.Name]; ok && !strings.Contains(t.CreateStatement, cc.Definition(flavor)) {
				col.CheckClause = cc.Clause
			} else {
				keep = append(keep, cc)
			}
		}
		t.Checks = keep
		return
	}

	// Meanwhile, MySQL butchers the escaping of special characters in check
	// clauses I_S, so we parse them from SHOW CREATE TABLE instead
	for _, cc := range t.Checks {
		cc.Clause = "!!!CHECKCLAUSE!!!"
		template := cc.Definition(flavor)
		template = regexp.QuoteMeta(template)
		template = strings.Replace(template, cc.Clause, "(.+?)", 1) + ",?\n"
		re := regexp.MustCompile(template)
		matches := re.FindStringSubmatch(t.CreateStatement)
		if matches != nil {
			cc.Clause = matches[1]
		}
	}
}

// fixVectorIndexes examines SHOW CREATE TABLE to obtain the optional M and
// DISTANCE attributes for MariaDB vector indexes. These attributes are not
// currently exposed in information_schema.
func fixVectorIndexes(t *Table, flavor Flavor) {
	for _, idx := range t.SecondaryIndexes {
		if idx.Type != "VECTOR" {
			continue
		}
		definition := idx.Definition(flavor)
		if indexStartPos := strings.Index(t.CreateStatement, definition+" "); indexStartPos > -1 {
			attribStart := indexStartPos + len(definition) + 1
			if newlinePos := strings.IndexByte(t.CreateStatement[attribStart:], '\n'); newlinePos > -1 {
				attribEnd := attribStart + newlinePos
				if t.CreateStatement[attribEnd-1] == ',' {
					attribEnd--
				}
				idx.Attributes = t.CreateStatement[attribStart:attribEnd]
			}
		}
	}
}
