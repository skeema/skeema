package tengo

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strings"

	"github.com/VividCortex/mysqlerr"
	"github.com/jmoiron/sqlx"
	"golang.org/x/sync/errgroup"
)

/*
	Important note on information_schema queries in this file: MySQL 8.0 changes
	information_schema column names to come back from queries in all caps, so we
	need to explicitly use AS clauses in order to get them back as lowercase and
	have sqlx Select() work.
*/

var reExtraOnUpdate = regexp.MustCompile(`(?i)\bon update (current_timestamp(?:\(\d*\))?)`)

func querySchemaTables(ctx context.Context, db *sqlx.DB, schema string, flavor Flavor) ([]*Table, error) {
	tables, havePartitions, err := queryTablesInSchema(ctx, db, schema, flavor)
	if err != nil {
		return nil, err
	}

	g, subCtx := errgroup.WithContext(ctx)

	for n := range tables {
		t := tables[n] // avoid issues with goroutines and loop iterator values
		g.Go(func() (err error) {
			t.CreateStatement, err = showCreateTable(subCtx, db, t.Name)
			if err != nil {
				err = fmt.Errorf("Error executing SHOW CREATE TABLE for %s.%s: %s", EscapeIdentifier(schema), EscapeIdentifier(t.Name), err)
			}
			return err
		})
	}

	var columnsByTableName map[string][]*Column
	g.Go(func() (err error) {
		columnsByTableName, err = queryColumnsInSchema(subCtx, db, schema, flavor)
		return err
	})

	var primaryKeyByTableName map[string]*Index
	var secondaryIndexesByTableName map[string][]*Index
	g.Go(func() (err error) {
		primaryKeyByTableName, secondaryIndexesByTableName, err = queryIndexesInSchema(subCtx, db, schema, flavor)
		return err
	})

	var foreignKeysByTableName map[string][]*ForeignKey
	g.Go(func() (err error) {
		foreignKeysByTableName, err = queryForeignKeysInSchema(subCtx, db, schema, flavor)
		return err
	})

	var partitioningByTableName map[string]*TablePartitioning
	if havePartitions {
		g.Go(func() (err error) {
			partitioningByTableName, err = queryPartitionsInSchema(subCtx, db, schema, flavor)
			return err
		})
	}

	// Await all of the async queries
	if err := g.Wait(); err != nil {
		return nil, err
	}

	// Assemble all the data, fix edge cases, and determine if SHOW CREATE TABLE
	// matches expectation
	for _, t := range tables {
		t.Columns = columnsByTableName[t.Name]
		t.PrimaryKey = primaryKeyByTableName[t.Name]
		t.SecondaryIndexes = secondaryIndexesByTableName[t.Name]
		t.ForeignKeys = foreignKeysByTableName[t.Name]

		if p, ok := partitioningByTableName[t.Name]; ok {
			for _, part := range p.Partitions {
				part.Engine = t.Engine
			}
			t.Partitioning = p
			fixPartitioningEdgeCases(t, flavor)
		}

		// Avoid issues from data dictionary weirdly caching a NULL next auto-inc
		if t.NextAutoIncrement == 0 && t.HasAutoIncrement() {
			t.NextAutoIncrement = 1
		}
		// Remove create options which don't affect InnoDB
		if t.Engine == "InnoDB" {
			t.CreateStatement = NormalizeCreateOptions(t.CreateStatement)
		}
		// Index order is unpredictable with new MySQL 8 data dictionary, so reorder
		// indexes based on parsing SHOW CREATE TABLE if needed
		if flavor.HasDataDictionary() && len(t.SecondaryIndexes) > 1 {
			fixIndexOrder(t)
		}
		// Foreign keys order is unpredictable in MySQL before 5.6, so reorder
		// foreign keys based on parsing SHOW CREATE TABLE if needed
		if !flavor.SortedForeignKeys() && len(t.ForeignKeys) > 1 {
			fixForeignKeyOrder(t)
		}
		// Create options order is unpredictable with the new MySQL 8 data dictionary
		// Also need to fix generated column expression string literals
		if flavor.HasDataDictionary() {
			fixCreateOptionsOrder(t, flavor)
			fixGenerationExpr(t, flavor)
		}
		// Percona Server column compression can only be parsed from SHOW CREATE
		// TABLE. (Although it also has new I_S tables, their name differs pre-8.0
		// vs post-8.0, and cols that aren't using a COMPRESSION_DICTIONARY are not
		// even present there.)
		if flavor.VendorMinVersion(VendorPercona, 5, 6, 33) && strings.Contains(t.CreateStatement, "COLUMN_FORMAT COMPRESSED") {
			fixPerconaColCompression(t)
		}
		// FULLTEXT indexes may have a PARSER clause, which isn't exposed in I_S
		if strings.Contains(t.CreateStatement, "WITH PARSER") {
			fixFulltextIndexParsers(t, flavor)
		}
		// Fix blob/text default expressions in MySQL 8.0.13-8.0.22, missing from I_S
		if flavor.MySQLishMinVersion(8, 0, 13) && !flavor.MySQLishMinVersion(8, 0, 23) {
			fixBlobDefaultExpression(t, flavor)
		}
		// Compare what we expect the create DDL to be, to determine if we support
		// diffing for the table. Ignore next-auto-increment differences in this
		// comparison, since the value may have changed between our previous
		// information_schema introspection and our current SHOW CREATE TABLE call!
		actual, _ := ParseCreateAutoInc(t.CreateStatement)
		expected, _ := ParseCreateAutoInc(t.GeneratedCreateStatement(flavor))
		if actual != expected {
			t.UnsupportedDDL = true
		}
	}
	return tables, nil
}

func queryTablesInSchema(ctx context.Context, db *sqlx.DB, schema string, flavor Flavor) ([]*Table, bool, error) {
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
		SELECT t.table_name AS table_name, t.table_type AS table_type, t.engine AS engine,
		       t.auto_increment AS auto_increment, t.table_collation AS table_collation,
		       t.create_options AS create_options, t.table_comment AS table_comment,
		       c.character_set_name AS character_set_name, c.is_default AS is_default
		FROM   information_schema.tables t
		JOIN   information_schema.collations c ON t.table_collation = c.collation_name
		WHERE  t.table_schema = ?
		AND    t.table_type = 'BASE TABLE'`
	if err := db.SelectContext(ctx, &rawTables, query, schema); err != nil {
		return nil, false, fmt.Errorf("Error querying information_schema.tables for schema %s: %s", schema, err)
	}
	if len(rawTables) == 0 {
		return []*Table{}, false, nil
	}
	tables := make([]*Table, len(rawTables))
	var havePartitions bool
	for n, rawTable := range rawTables {
		tables[n] = &Table{
			Name:               rawTable.Name,
			Engine:             rawTable.Engine.String,
			CharSet:            rawTable.CharSet,
			Collation:          rawTable.TableCollation.String,
			CollationIsDefault: rawTable.CollationIsDefault != "",
			Comment:            rawTable.Comment,
		}
		if rawTable.AutoIncrement.Valid {
			tables[n].NextAutoIncrement = uint64(rawTable.AutoIncrement.Int64)
		}
		if rawTable.CreateOptions.Valid && rawTable.CreateOptions.String != "" {
			if strings.Contains(strings.ToUpper(rawTable.CreateOptions.String), "PARTITIONED") {
				havePartitions = true
			}
			tables[n].CreateOptions = reformatCreateOptions(rawTable.CreateOptions.String)
		}
	}
	return tables, havePartitions, nil
}

func queryColumnsInSchema(ctx context.Context, db *sqlx.DB, schema string, flavor Flavor) (map[string][]*Column, error) {
	stripDisplayWidth := flavor.OmitIntDisplayWidth()
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
		CollationIsDefault sql.NullString `db:"is_default"`
	}
	query := `
		SELECT    c.table_name AS table_name, c.column_name AS column_name,
		          c.column_type AS column_type, c.is_nullable AS is_nullable,
		          c.column_default AS column_default, c.extra AS extra,
		          %s AS generation_expression,
		          c.column_comment AS column_comment,
		          c.character_set_name AS character_set_name,
		          c.collation_name AS collation_name, co.is_default AS is_default
		FROM      information_schema.columns c
		LEFT JOIN information_schema.collations co ON co.collation_name = c.collation_name
		WHERE     c.table_schema = ?
		ORDER BY  c.table_name, c.ordinal_position`
	genExpr := "NULL"
	if flavor.GeneratedColumns() {
		genExpr = "c.generation_expression"
	}
	query = fmt.Sprintf(query, genExpr)
	if err := db.SelectContext(ctx, &rawColumns, query, schema); err != nil {
		return nil, fmt.Errorf("Error querying information_schema.columns for schema %s: %s", schema, err)
	}
	columnsByTableName := make(map[string][]*Column)
	for _, rawColumn := range rawColumns {
		col := &Column{
			Name:          rawColumn.Name,
			TypeInDB:      rawColumn.Type,
			Nullable:      strings.ToUpper(rawColumn.IsNullable) == "YES",
			AutoIncrement: strings.Contains(rawColumn.Extra, "auto_increment"),
			Comment:       rawColumn.Comment,
			Invisible:     strings.Contains(rawColumn.Extra, "INVISIBLE"),
		}
		// If db was upgraded from a pre-8.0.19 version (but still 8.0+) to 8.0.19+,
		// I_S may still contain int display widths even though SHOW CREATE TABLE
		// omits them. Strip to avoid incorrectly flagging the table as unsupported
		// for diffs.
		if stripDisplayWidth && (strings.Contains(col.TypeInDB, "int(") || col.TypeInDB == "year(4)") {
			col.TypeInDB = StripDisplayWidth(col.TypeInDB)
		}
		if pos := strings.Index(col.TypeInDB, " /*!100301 COMPRESSED"); pos > -1 {
			// MariaDB includes compression attribute in column type; remove it
			col.Compression = "COMPRESSED"
			col.TypeInDB = col.TypeInDB[0:pos]
		}
		if rawColumn.GenerationExpr.Valid {
			col.GenerationExpr = rawColumn.GenerationExpr.String
			col.Virtual = strings.Contains(rawColumn.Extra, "VIRTUAL GENERATED")
		}
		if !rawColumn.Default.Valid {
			allowNullDefault := col.Nullable && !col.AutoIncrement && col.GenerationExpr == ""
			if !flavor.AllowBlobDefaults() && (strings.HasSuffix(col.TypeInDB, "blob") || strings.HasSuffix(col.TypeInDB, "text")) {
				allowNullDefault = false
				// MySQL 8.0.13-8.0.22 omits blob/text default expressions from
				// information_schema due to a bug, so in this case flag the column to
				// have its default parsed out from SHOW CREATE TABLE later
				if strings.Contains(rawColumn.Extra, "DEFAULT_GENERATED") {
					col.Default = "!!!BLOBDEFAULT!!!"
				}
			}
			if allowNullDefault {
				col.Default = "NULL"
			}
		} else if flavor.VendorMinVersion(VendorMariaDB, 10, 2) {
			if !col.AutoIncrement && col.GenerationExpr == "" {
				// MariaDB 10.2+ exposes defaults as expressions / quote-wrapped strings
				col.Default = rawColumn.Default.String
			}
		} else if strings.HasPrefix(rawColumn.Default.String, "CURRENT_TIMESTAMP") && (strings.HasPrefix(rawColumn.Type, "timestamp") || strings.HasPrefix(rawColumn.Type, "datetime")) {
			col.Default = rawColumn.Default.String
		} else if strings.HasPrefix(rawColumn.Type, "bit") && strings.HasPrefix(rawColumn.Default.String, "b'") {
			col.Default = rawColumn.Default.String
		} else if strings.Contains(rawColumn.Extra, "DEFAULT_GENERATED") {
			// MySQL/Percona 8.0.13+ added default expressions, which are paren-wrapped
			// in SHOW CREATE TABLE, possibly in addition to any paren-wrapping which
			// is already in information_schema. However, quotes get oddly mangled in
			// information_schema's representation.
			col.Default = fmt.Sprintf("(%s)", strings.ReplaceAll(rawColumn.Default.String, "\\'", "'"))
		} else {
			col.Default = fmt.Sprintf("'%s'", EscapeValueForCreateTable(rawColumn.Default.String))
		}
		if matches := reExtraOnUpdate.FindStringSubmatch(rawColumn.Extra); matches != nil {
			col.OnUpdate = matches[1]
			// Some flavors omit fractional precision from ON UPDATE in
			// information_schema only, despite it being present everywhere else
			if openParen := strings.IndexByte(rawColumn.Type, '('); openParen > -1 && !strings.Contains(col.OnUpdate, "(") {
				col.OnUpdate = fmt.Sprintf("%s%s", col.OnUpdate, rawColumn.Type[openParen:])
			}
		}
		if rawColumn.Collation.Valid { // only text-based column types have a notion of charset and collation
			col.CharSet = rawColumn.CharSet.String
			col.Collation = rawColumn.Collation.String
			col.CollationIsDefault = (rawColumn.CollationIsDefault.String != "")
		}
		if columnsByTableName[rawColumn.TableName] == nil {
			columnsByTableName[rawColumn.TableName] = make([]*Column, 0)
		}
		columnsByTableName[rawColumn.TableName] = append(columnsByTableName[rawColumn.TableName], col)
	}
	return columnsByTableName, nil
}

func queryIndexesInSchema(ctx context.Context, db *sqlx.DB, schema string, flavor Flavor) (map[string]*Index, map[string][]*Index, error) {
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
		SELECT   index_name AS index_name, table_name AS table_name,
		         non_unique AS non_unique, seq_in_index AS seq_in_index,
		         column_name AS column_name, sub_part AS sub_part,
		         index_comment AS index_comment, index_type AS index_type,
		         collation AS collation, %s AS expression, %s AS is_visible
		FROM     information_schema.statistics
		WHERE    table_schema = ?`
	exprSelect, visSelect := "NULL", "'YES'"
	if flavor.MySQLishMinVersion(8, 0) {
		// Index expressions added in 8.0.13
		if flavor.MySQLishMinVersion(8, 0, 13) {
			exprSelect = "expression"
		}
		visSelect = "is_visible" // available in all 8.0
	}
	query = fmt.Sprintf(query, exprSelect, visSelect)
	if err := db.SelectContext(ctx, &rawIndexes, query, schema); err != nil {
		return nil, nil, fmt.Errorf("Error querying information_schema.statistics for schema %s: %s", schema, err)
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
		for len(index.Parts) < int(rawIndex.SeqInIndex) {
			index.Parts = append(index.Parts, IndexPart{})
		}
		index.Parts[rawIndex.SeqInIndex-1] = IndexPart{
			ColumnName:   rawIndex.ColumnName.String,
			Expression:   rawIndex.Expression.String,
			PrefixLength: uint16(rawIndex.SubPart.Int64),
			Descending:   (rawIndex.Collation.String == "D"),
		}
	}
	return primaryKeyByTableName, secondaryIndexesByTableName, nil
}

func queryForeignKeysInSchema(ctx context.Context, db *sqlx.DB, schema string, flavor Flavor) (map[string][]*ForeignKey, error) {
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
		SELECT   rc.constraint_name AS constraint_name, rc.table_name AS table_name,
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
	if err := db.SelectContext(ctx, &rawForeignKeys, query, schema, schema); err != nil {
		return nil, fmt.Errorf("Error querying foreign key constraints for schema %s: %s", schema, err)
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
	return foreignKeysByTableName, nil
}

func queryPartitionsInSchema(ctx context.Context, db *sqlx.DB, schema string, flavor Flavor) (map[string]*TablePartitioning, error) {
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
		SELECT   p.table_name AS table_name, p.partition_name AS partition_name,
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
	if err := db.SelectContext(ctx, &rawPartitioning, query, schema); err != nil {
		return nil, fmt.Errorf("Error querying information_schema.partitions for schema %s: %s", schema, err)
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
	return partitioningByTableName, nil
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
	re := regexp.MustCompile(fmt.Sprintf("^%s$", template))

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

// MySQL 8 has nonsensical behavior regarding string literals in generated col
// expressions: the literals are expressed using a different charset in SHOW
// CREATE TABLE vs information_schema.columns.generation_expression. This method
// modifies each generated Column.GenerationExpr to match SHOW CREATE's version.
func fixGenerationExpr(t *Table, flavor Flavor) {
	for _, col := range t.Columns {
		if col.GenerationExpr != "" {
			// Approach: dynamically build a regexp that captures the generation expr
			// from the correct line of the full SHOW CREATE TABLE output
			origExpr := col.GenerationExpr
			col.GenerationExpr = "!!!GENEXPR!!!"
			reTemplate := regexp.QuoteMeta(col.Definition(flavor, t))
			reTemplate = strings.Replace(reTemplate, col.GenerationExpr, "(.*)", -1)
			re := regexp.MustCompile(reTemplate)
			matches := re.FindStringSubmatch(t.CreateStatement)
			if matches == nil {
				// If we somehow failed to match correctly, fall back to using the
				// uncorrected value from information_schema; unsupported diff is
				// preferable to a nil pointer panic
				col.GenerationExpr = origExpr
			} else {
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
			if flavor.VendorMinVersion(VendorMariaDB, 10, 2) {
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
			template := fmt.Sprintf("%s /*!50100 WITH PARSER ", idx.Definition(flavor))
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

// fixBlobDefaultExpression parses the table's CREATE string in order to
// populate Column.Default for blob/text columns using a default expression
// in MySQLish 8.0.13-8.0.22, which omits this from information_schema due
// to a bug fixed in MySQL 8.0.23.
func fixBlobDefaultExpression(t *Table, flavor Flavor) {
	for _, col := range t.Columns {
		if col.Default == "!!!BLOBDEFAULT!!!" {
			template := col.Definition(flavor, t)
			template = regexp.QuoteMeta(template)
			template = fmt.Sprintf("%s,?\n", strings.Replace(template, col.Default, "(.+?)", 1))
			re := regexp.MustCompile(template)
			matches := re.FindStringSubmatch(t.CreateStatement)
			if matches != nil {
				col.Default = matches[1]
			}
		}
	}
}

func querySchemaRoutines(ctx context.Context, db *sqlx.DB, schema string, flavor Flavor) ([]*Routine, error) {
	// Obtain the routines in the schema
	// We completely exclude routines that the user can call, but not examine --
	// e.g. user has EXECUTE priv but missing other vital privs. In this case
	// routine_definition will be NULL.
	var rawRoutines []struct {
		Name              string         `db:"routine_name"`
		Type              string         `db:"routine_type"`
		Body              sql.NullString `db:"routine_definition"`
		IsDeterministic   string         `db:"is_deterministic"`
		SQLDataAccess     string         `db:"sql_data_access"`
		SecurityType      string         `db:"security_type"`
		SQLMode           string         `db:"sql_mode"`
		Comment           string         `db:"routine_comment"`
		Definer           string         `db:"definer"`
		DatabaseCollation string         `db:"database_collation"`
	}
	query := `
		SELECT r.routine_name AS routine_name, UPPER(r.routine_type) AS routine_type,
		       r.routine_definition AS routine_definition,
		       UPPER(r.is_deterministic) AS is_deterministic,
		       UPPER(r.sql_data_access) AS sql_data_access,
		       UPPER(r.security_type) AS security_type,
		       r.sql_mode AS sql_mode, r.routine_comment AS routine_comment,
		       r.definer AS definer, r.database_collation AS database_collation
		FROM   information_schema.routines r
		WHERE  r.routine_schema = ? AND routine_definition IS NOT NULL`
	if err := db.SelectContext(ctx, &rawRoutines, query, schema); err != nil {
		return nil, fmt.Errorf("Error querying information_schema.routines for schema %s: %s", schema, err)
	}
	if len(rawRoutines) == 0 {
		return []*Routine{}, nil
	}
	routines := make([]*Routine, len(rawRoutines))
	dict := make(map[ObjectKey]*Routine, len(rawRoutines))
	for n, rawRoutine := range rawRoutines {
		routines[n] = &Routine{
			Name:              rawRoutine.Name,
			Type:              ObjectType(strings.ToLower(rawRoutine.Type)),
			Body:              rawRoutine.Body.String, // This contains incorrect formatting conversions; overwritten later
			Definer:           rawRoutine.Definer,
			DatabaseCollation: rawRoutine.DatabaseCollation,
			Comment:           rawRoutine.Comment,
			Deterministic:     rawRoutine.IsDeterministic == "YES",
			SQLDataAccess:     rawRoutine.SQLDataAccess,
			SecurityType:      rawRoutine.SecurityType,
			SQLMode:           rawRoutine.SQLMode,
		}
		if routines[n].Type != ObjectTypeProc && routines[n].Type != ObjectTypeFunc {
			return nil, fmt.Errorf("Unsupported routine type %s found in %s.%s", rawRoutine.Type, schema, rawRoutine.Name)
		}
		key := ObjectKey{Type: routines[n].Type, Name: routines[n].Name}
		dict[key] = routines[n]
	}

	// Obtain param string, return type string, and full create statement:
	// We can't rely only on information_schema, since it doesn't have the param
	// string formatted in the same way as the original CREATE, nor does
	// routines.body handle strings/charsets correctly for re-runnable SQL.
	// In flavors without the new data dictionary, we first try querying mysql.proc
	// to bulk-fetch sufficient info to rebuild the CREATE without needing to run
	// a SHOW CREATE per routine.
	// If mysql.proc doesn't exist or that query fails, we then run a SHOW CREATE
	// per routine, using multiple goroutines for performance reasons.
	var alreadyObtained int
	if !flavor.HasDataDictionary() {
		var rawRoutineMeta []struct {
			Name      string `db:"name"`
			Type      string `db:"type"`
			Body      string `db:"body"`
			ParamList string `db:"param_list"`
			Returns   string `db:"returns"`
		}
		query := `
			SELECT name, type, body, param_list, returns
			FROM   mysql.proc
			WHERE  db = ?`
		// Errors here are non-fatal. No need to even check; slice will be empty which is fine
		db.SelectContext(ctx, &rawRoutineMeta, query, schema)
		for _, meta := range rawRoutineMeta {
			key := ObjectKey{Type: ObjectType(strings.ToLower(meta.Type)), Name: meta.Name}
			if routine, ok := dict[key]; ok {
				routine.ParamString = strings.Replace(meta.ParamList, "\r\n", "\n", -1)
				routine.ReturnDataType = meta.Returns
				routine.Body = strings.Replace(meta.Body, "\r\n", "\n", -1)
				routine.CreateStatement = routine.Definition(flavor)
				alreadyObtained++
			}
		}
	}

	var err error
	if alreadyObtained < len(routines) {
		g, subCtx := errgroup.WithContext(ctx)
		for n := range routines {
			r := routines[n] // avoid issues with goroutines and loop iterator values
			if r.CreateStatement == "" {
				g.Go(func() (err error) {
					r.CreateStatement, err = showCreateRoutine(subCtx, db, r.Name, r.Type)
					if err == nil {
						r.CreateStatement = strings.Replace(r.CreateStatement, "\r\n", "\n", -1)
						err = r.parseCreateStatement(flavor, schema)
					} else {
						err = fmt.Errorf("Error executing SHOW CREATE %s for %s.%s: %s", r.Type.Caps(), EscapeIdentifier(schema), EscapeIdentifier(r.Name), err)
					}
					return err
				})
			}
		}
		err = g.Wait()
	}

	return routines, err
}

func showCreateRoutine(ctx context.Context, db *sqlx.DB, routine string, ot ObjectType) (create string, err error) {
	query := fmt.Sprintf("SHOW CREATE %s %s", ot.Caps(), EscapeIdentifier(routine))
	if ot == ObjectTypeProc {
		var createRows []struct {
			CreateStatement sql.NullString `db:"Create Procedure"`
		}
		err = db.SelectContext(ctx, &createRows, query)
		if (err == nil && len(createRows) != 1) || IsDatabaseError(err, mysqlerr.ER_SP_DOES_NOT_EXIST) {
			err = sql.ErrNoRows
		} else if err == nil {
			create = createRows[0].CreateStatement.String
		}
	} else if ot == ObjectTypeFunc {
		var createRows []struct {
			CreateStatement sql.NullString `db:"Create Function"`
		}
		err = db.SelectContext(ctx, &createRows, query)
		if (err == nil && len(createRows) != 1) || IsDatabaseError(err, mysqlerr.ER_SP_DOES_NOT_EXIST) {
			err = sql.ErrNoRows
		} else if err == nil {
			create = createRows[0].CreateStatement.String
		}
	} else {
		err = fmt.Errorf("Object type %s is not a routine", ot)
	}
	return
}
