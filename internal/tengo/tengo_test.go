package tengo

import (
	"fmt"
	"os"
	"strings"
	"testing"
)

func TestMain(m *testing.M) {
	UseFilteredDriverLogger()
	os.Exit(m.Run())
}

func TestIntegration(t *testing.T) {
	images := SkeemaTestImages(t)
	suite := &TengoIntegrationSuite{}
	RunSuite(suite, t, images)
}

type TengoIntegrationSuite struct {
	d *DockerizedInstance
}

func (s *TengoIntegrationSuite) Setup(t *testing.T, backend string) {
	opts := DockerizedInstanceOptions{
		Name:         fmt.Sprintf("skeema-test-%s", ContainerNameForImage(backend)),
		Image:        backend,
		RootPassword: "fakepw",
		DataTmpfs:    true,
	}
	var err error
	s.d, err = GetOrCreateDockerizedInstance(opts)
	if err != nil {
		t.Fatalf("Unable to setup backend %q: %v", backend, err)
	}
}

func (s *TengoIntegrationSuite) Teardown(t *testing.T) {
	s.d.Done(t)
}

func (s *TengoIntegrationSuite) BeforeTest(t *testing.T) {
	s.d.NukeData(t)
	s.d.SourceSQL(t, "testdata/integration.sql")
}

func (s *TengoIntegrationSuite) GetSchema(t *testing.T, schemaName string) *Schema {
	t.Helper()
	schema, err := s.d.Schema(schemaName)
	if schema == nil || err != nil {
		t.Fatalf("Unable to obtain schema %s: %s", schemaName, err)
	}
	return schema
}

func getTable(t *testing.T, schema *Schema, tableName string) *Table {
	t.Helper()
	table := schema.Table(tableName)
	if table == nil {
		t.Fatalf("Table %s.%s unexpectedly does not exist", schema.Name, tableName)
	}
	return table
}

// TestObjectKeyString confirms behavior of ObjectKey.String()
func TestObjectKeyString(t *testing.T) {
	key := ObjectKey{Type: ObjectTypeTable, Name: "weird`test"}
	if keyStr := key.String(); keyStr != "table `weird``test`" {
		t.Errorf("Unexpected result from ObjectKey.String(): %s", keyStr)
	}
}

// TestUnitTableFlavors confirms that our hard-coded fixture table methods
// (later on in this file) correctly adjust their output to match the specified
// flavors.
func TestUnitTableFlavors(t *testing.T) {
	orig := aTable(1)

	table := aTableForFlavor(ParseFlavor("mysql:5.7"), 1)
	if clauses, supported := table.Diff(&orig); !supported || len(clauses) != 0 {
		t.Errorf("MySQL 5.7: Expected no diff; instead found %d differences, supported=%t", len(clauses), supported)
	}
	table = aTableForFlavor(ParseFlavor("mariadb:10.1"), 1)
	if clauses, supported := table.Diff(&orig); !supported || len(clauses) != 0 {
		t.Errorf("MariaDB 10.1: Expected no diff; instead found %d differences, supported=%t", len(clauses), supported)
	}

	table = aTableForFlavor(ParseFlavor("mysql:5.5"), 1)
	if clauses, supported := table.Diff(&orig); !supported || len(clauses) != 1 {
		t.Errorf("MySQL 5.5: Expected 1 diff clause; instead found %d differences, supported=%t", len(clauses), supported)
	}
	for _, check := range []string{table.Columns[3].Type.String(), table.Columns[3].OnUpdate, table.Columns[3].Default} {
		if strings.HasSuffix(check, ")") {
			t.Error("MySQL 5.5: Expected all traces of fractional timestamp precision to be removed, but still present")
		}
	}

	table = aTableForFlavor(ParseFlavor("mariadb:10.3"), 1)
	if clauses, supported := table.Diff(&orig); !supported || len(clauses) != 2 {
		t.Errorf("MariaDB 10.3: Expected 2 diff clauses; instead found %d differences, supported=%t", len(clauses), supported)
	}
	if table.Columns[5].Default[0] == '\'' {
		t.Error("MariaDB 10.3: Expected int column to not have quoted default, but it still does")
	}
	if table.Columns[3].OnUpdate != "current_timestamp(2)" || table.Columns[3].Default != "current_timestamp(2)" {
		t.Error("MariaDB 10.3: Expected current_timestamp to be lowercased, but it is not")
	}
	if table.GeneratedCreateStatement(ParseFlavor("mariadb:10.3")) != table.CreateStatement {
		t.Error("MariaDB 10.3: Expected function to reset CreateStatement to GeneratedCreateStatement, but it did not")
	}

	orig2 := supportedTable()
	flavor := ParseFlavor("mariadb:10.2")
	table2 := supportedTableForFlavor(flavor)
	if table2.GeneratedCreateStatement(flavor) == orig2.GeneratedCreateStatement(FlavorUnknown) {
		t.Errorf("MariaDB 10.2: Expected GeneratedCreateStatement to differ vs FlavorUnknown, but it did not")
	}
	colClause := table2.Columns[3].Definition(flavor)
	if !strings.Contains(colClause, "DEFAULT NULL") {
		t.Errorf("MariaDB 10.2: Expected text column to now emit a default value, but it did not")
	}
}

// flavorTestFiles returns a slice of .sql filenames that could run for the
// supplied flavor. The supplied flavor should have a *non-zero* Patch field,
// as this is especially relevant for MySQL 8.
// The result omits integration.sql, since that is always run prior to each
// subtest.
func flavorTestFiles(flavor Flavor) []string {
	// Non-flavor-specific
	result := []string{"integration-ext.sql", "partition.sql", "rows.sql", "views.sql", "spatial.sql"}

	if flavor.MinMySQL(8, 0, 13) {
		result = append(result, "default-expr.sql")
	} else if flavor.MinMariaDB(10, 2) {
		result = append(result, "default-expr-maria.sql") // No support for 4-byte chars in the expressions
	}

	if flavor.GeneratedColumns() {
		if flavor.IsMariaDB() {
			result = append(result, "generatedcols-maria.sql") // no support for NOT NULL generated cols or 4-byte chars in generation expressions
		} else {
			result = append(result, "generatedcols.sql")
		}
	}

	if flavor.MinMySQL(8) {
		result = append(result, "index-mysql8.sql") // functional indexes, descending indexes, invisible indexes
	} else if flavor.MinMariaDB(10, 6) {
		result = append(result, "index-maria106.sql") // ignored indexes
	}

	if flavor.MinMariaDB(10, 3) || flavor.MinMySQL(8, 0, 23) {
		result = append(result, "inviscols.sql")
	}

	if flavor.MinMySQL(5, 7) {
		// other flavors may support FT parsers but don't ship with any alternatives;
		// other flavors do not support TABLESPACE clauses in InnoDB tables
		result = append(result, "ft-parser.sql", "inno-tablespace.sql")
	}

	if flavor.IsPercona() && flavor.MinMySQL(5, 6, 33) {
		result = append(result, "colcompression-percona.sql")
	} else if flavor.MinMariaDB(10, 3) {
		result = append(result, "colcompression-maria.sql")
	}

	if flavor.MinMySQL(5, 7) {
		result = append(result, "pagecompression.sql")
	} else if flavor.MinMariaDB(10, 2) {
		result = append(result, "pagecompression-maria.sql")
	}

	if flavor.HasCheckConstraints() {
		if flavor.IsMariaDB() {
			result = append(result, "check-maria.sql")
		} else {
			result = append(result, "check.sql")
		}
	}

	if flavor.MinMariaDB(10, 8) { // descending indexes, IN/OUT/INOUT func params
		result = append(result, "maria108.sql")
	}
	if flavor.MinMariaDB(10, 10) { // uca1400 collations
		result = append(result, "uca1400.sql")
	}
	if flavor.MinMariaDB(11, 7) { // vectors and vector indexes
		result = append(result, "vector-maria117.sql")
	}

	for n := range result {
		result[n] = "testdata/" + result[n]
	}
	return result
}

func primaryKey(cols ...*Column) *Index {
	parts := make([]IndexPart, len(cols))
	for n := range cols {
		parts[n] = IndexPart{
			ColumnName: cols[n].Name,
		}
	}
	return &Index{
		Name:       "PRIMARY",
		Parts:      parts,
		PrimaryKey: true,
		Unique:     true,
		Type:       "BTREE",
	}
}

func aTable(nextAutoInc uint64) Table {
	return aTableForFlavor(FlavorUnknown, nextAutoInc)
}

func aTableForFlavor(flavor Flavor, nextAutoInc uint64) Table {
	utf8mb3 := "utf8"
	// MySQL 8.0.29+ and MariaDB 10.6+ report the legacy utf8 charset as utf8mb3.
	if flavor.MinMySQL(8, 0, 29) || flavor.MinMariaDB(10, 6) {
		utf8mb3 = "utf8mb3"
	}
	utf8mb3DefaultCollation := characterSetsForFlavor(flavor)[utf8mb3].DefaultCollation
	lastUpdateCol := &Column{
		Name:     "last_update",
		Type:     ParseColumnType("timestamp(2)"),
		Default:  "CURRENT_TIMESTAMP(2)",
		OnUpdate: "CURRENT_TIMESTAMP(2)",
	}
	lastUpdateDef := "`last_update` timestamp(2) NOT NULL DEFAULT CURRENT_TIMESTAMP(2) ON UPDATE CURRENT_TIMESTAMP(2)"
	if flavor.IsMySQL(5, 5) { // No fractional timestamps in 5.5
		lastUpdateCol.Type = ParseColumnType("timestamp")
		lastUpdateCol.Default = "CURRENT_TIMESTAMP"
		lastUpdateCol.OnUpdate = "CURRENT_TIMESTAMP"
		lastUpdateDef = "`last_update` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"
	}
	if flavor.MinMariaDB(10, 2) {
		lastUpdateCol.Default = strings.ToLower(lastUpdateCol.Default)
		lastUpdateCol.OnUpdate = strings.ToLower(lastUpdateCol.OnUpdate)
		lastUpdateDef = strings.Replace(lastUpdateDef, "CURRENT_TIMESTAMP", "current_timestamp", 2)
	}

	aliveCol := &Column{
		Name:    "alive",
		Type:    ParseColumnType("tinyint(1) unsigned"),
		Default: "'1'",
	}
	aliveDef := "`alive` tinyint(1) unsigned NOT NULL DEFAULT '1'"
	if flavor.MinMariaDB(10, 2) {
		aliveCol.Default = "1"
		aliveDef = "`alive` tinyint(1) unsigned NOT NULL DEFAULT 1"
	}

	columns := []*Column{
		{
			Name:          "actor_id",
			Type:          ParseColumnType("smallint(5) unsigned"),
			AutoIncrement: true,
		},
		{
			Name:      "first_name",
			Type:      ParseColumnType("varchar(45)"),
			CharSet:   utf8mb3,
			Collation: utf8mb3DefaultCollation,
		},
		{
			Name:      "last_name",
			Nullable:  true,
			Type:      ParseColumnType("varchar(45)"),
			Default:   "NULL",
			CharSet:   utf8mb3,
			Collation: utf8mb3DefaultCollation,
		},
		lastUpdateCol,
		{
			Name:      "ssn",
			Type:      ParseColumnType("char(10)"),
			CharSet:   utf8mb3,
			Collation: utf8mb3DefaultCollation,
		},
		aliveCol,
		{
			Name:    "alive_bit",
			Type:    ParseColumnType("bit(1)"),
			Default: "b'1'",
		},
	}
	secondaryIndexes := []*Index{
		{
			Name: "idx_ssn",
			Parts: []IndexPart{
				{ColumnName: columns[4].Name},
			},
			Unique: true,
			Type:   "BTREE",
		},
		{
			Name: "idx_actor_name",
			Parts: []IndexPart{
				{ColumnName: columns[2].Name, PrefixLength: 10},
				{ColumnName: columns[1].Name, PrefixLength: 1},
			},
			Type: "BTREE",
		},
	}

	var autoIncClause string
	if nextAutoInc > 1 {
		autoIncClause = fmt.Sprintf(" AUTO_INCREMENT=%d", nextAutoInc)
	}

	utf8mb3Table := utf8mb3
	if flavor.MinMySQL(8, 0, 24) {
		// 8.0.24-8.0.28 changes how utf8mb3 is expressed for table-level default in
		// SHOW CREATE, but not anywhere else
		utf8mb3Table = "utf8mb3"
	} else if flavor.AlwaysShowCollate() {
		utf8mb3Table += " COLLATE=" + utf8mb3DefaultCollation
	}

	stmt := fmt.Sprintf(`CREATE TABLE `+"`"+`actor`+"`"+` (
  `+"`"+`actor_id`+"`"+` smallint(5) unsigned NOT NULL AUTO_INCREMENT,
  `+"`"+`first_name`+"`"+` varchar(45) NOT NULL,
  `+"`"+`last_name`+"`"+` varchar(45) DEFAULT NULL,
  `+lastUpdateDef+`,
  `+"`"+`ssn`+"`"+` char(10) NOT NULL,
  `+aliveDef+`,
  `+"`"+`alive_bit`+"`"+` bit(1) NOT NULL DEFAULT b'1',
  PRIMARY KEY (`+"`"+`actor_id`+"`"+`),
  UNIQUE KEY `+"`"+`idx_ssn`+"`"+` (`+"`"+`ssn`+"`"+`),
  KEY `+"`"+`idx_actor_name`+"`"+` (`+"`"+`last_name`+"`"+`(10),`+"`"+`first_name`+"`"+`(1))
) ENGINE=InnoDB%s DEFAULT CHARSET=%s`, autoIncClause, utf8mb3Table)
	table := Table{
		Name:              "actor",
		Engine:            "InnoDB",
		CharSet:           utf8mb3,
		Collation:         utf8mb3DefaultCollation,
		ShowCollation:     flavor.AlwaysShowCollate(),
		Columns:           columns,
		PrimaryKey:        primaryKey(columns[0]),
		SecondaryIndexes:  secondaryIndexes,
		NextAutoIncrement: nextAutoInc,
		CreateStatement:   stmt,
	}
	if flavor.OmitIntDisplayWidth() {
		stripIntDisplayWidths(&table, flavor)
	}
	return table
}

func anotherTable() Table {
	return anotherTableForFlavor(FlavorUnknown)
}

func anotherTableForFlavor(flavor Flavor) Table {
	columns := []*Column{
		{
			Name: "actor_id",
			Type: ParseColumnType("smallint(5) unsigned"),
		},
		{
			Name:      "film_name",
			Type:      ParseColumnType("varchar(60)"),
			CharSet:   "latin1",
			Collation: "latin1_swedish_ci",
		},
	}
	secondaryIndex := &Index{
		Name: "film_name",
		Parts: []IndexPart{
			{ColumnName: columns[1].Name},
		},
		Type: "BTREE",
	}
	stmt := `CREATE TABLE ` + "`" + `actor_in_film` + "`" + ` (
  ` + "`" + `actor_id` + "`" + ` smallint(5) unsigned NOT NULL,
  ` + "`" + `film_name` + "`" + ` varchar(60) NOT NULL,
  PRIMARY KEY (` + "`" + `actor_id` + "`" + `,` + "`" + `film_name` + "`" + `),
  KEY ` + "`" + `film_name` + "`" + ` (` + "`" + `film_name` + "`" + `)
) ENGINE=InnoDB DEFAULT CHARSET=latin1`
	table := Table{
		Name:             "actor_in_film",
		Engine:           "InnoDB",
		CharSet:          "latin1",
		Collation:        "latin1_swedish_ci",
		ShowCollation:    flavor.AlwaysShowCollate(),
		Columns:          columns,
		PrimaryKey:       primaryKey(columns[0], columns[1]),
		SecondaryIndexes: []*Index{secondaryIndex},
		CreateStatement:  stmt,
	}
	if flavor.OmitIntDisplayWidth() {
		stripIntDisplayWidths(&table, flavor)
	}
	if table.ShowCollation {
		table.CreateStatement += " COLLATE=latin1_swedish_ci"
	}
	return table
}

func unsupportedTable() Table {
	t := supportedTable()
	t.CreateStatement += `
/*!50100 PARTITION BY RANGE (user_id)
SUBPARTITION BY HASH (post_id)
SUBPARTITIONS 2
(PARTITION p0 VALUES LESS THAN (123) ENGINE = InnoDB,
 PARTITION p1 VALUES LESS THAN MAXVALUE ENGINE = InnoDB) */`
	t.Partitioning = &TablePartitioning{
		Method:        "RANGE",
		SubMethod:     "HASH",
		Expression:    "user_id",
		SubExpression: "post_id",
		Partitions: []*Partition{
			{
				Name:   "p0",
				Values: "123",
				Engine: "InnoDB",
			},
			{
				Name:   "p1",
				Values: "MAXVALUE",
				Engine: "InnoDB",
			},
		},
	}
	t.UnsupportedDDL = true
	return t
}

// Returns the same as unsupportedTable() but without any partitioning,
// so that the table is actually supported.
func supportedTable() Table {
	return supportedTableForFlavor(FlavorUnknown)
}

func supportedTableForFlavor(flavor Flavor) Table {
	columns := []*Column{
		{
			Name: "post_id",
			Type: ParseColumnType("bigint(20) unsigned"),
		},
		{
			Name: "user_id",
			Type: ParseColumnType("bigint(20) unsigned"),
		},
		{
			Name:     "subscribed_at",
			Type:     ParseColumnType("int(10) unsigned"),
			Default:  "NULL",
			Nullable: true,
		},
		{
			Name:      "metadata",
			Nullable:  true,
			Type:      ParseColumnType("text"),
			CharSet:   "latin1",
			Collation: "latin1_swedish_ci",
		},
	}
	stmt := strings.Replace(`CREATE TABLE ~followed_posts~ (
  ~post_id~ bigint(20) unsigned NOT NULL,
  ~user_id~ bigint(20) unsigned NOT NULL,
  ~subscribed_at~ int(10) unsigned DEFAULT NULL,
  ~metadata~ text,
  PRIMARY KEY (~post_id~,~user_id~)
) ENGINE=InnoDB DEFAULT CHARSET=latin1`, "~", "`", -1)
	if flavor.MinMariaDB(10, 2) { // allow explicit DEFAULT NULL for blob/text
		columns[3].Default = "NULL"
		stmt = strings.Replace(stmt, " text", " text DEFAULT NULL", 1)
	}

	table := Table{
		Name:             "followed_posts",
		Engine:           "InnoDB",
		CharSet:          "latin1",
		Collation:        "latin1_swedish_ci",
		ShowCollation:    flavor.AlwaysShowCollate(),
		Columns:          columns,
		PrimaryKey:       primaryKey(columns[0:2]...),
		SecondaryIndexes: []*Index{},
		CreateStatement:  stmt,
	}
	if flavor.OmitIntDisplayWidth() {
		stripIntDisplayWidths(&table, flavor)
	}
	if table.ShowCollation {
		table.CreateStatement += " COLLATE=latin1_swedish_ci"
	}
	return table
}

func foreignKeyTable() Table {
	columns := []*Column{
		{
			Name: "id",
			Type: ParseColumnType("int(10) unsigned"),
		},
		{
			Name:     "customer_id",
			Type:     ParseColumnType("int(10) unsigned"),
			Default:  "NULL",
			Nullable: true,
		},
		{
			Name:      "product_line",
			Type:      ParseColumnType("char(12)"),
			CharSet:   "latin1",
			Collation: "latin1_swedish_ci",
		},
		{
			Name: "model",
			Type: ParseColumnType("int(10) unsigned"),
		},
	}

	secondaryIndexes := []*Index{
		{
			Name: "customer",
			Parts: []IndexPart{
				{ColumnName: columns[1].Name},
			},
			Type: "BTREE",
		},
		{
			Name: "product",
			Parts: []IndexPart{
				{ColumnName: columns[2].Name},
				{ColumnName: columns[3].Name},
			},
			Unique: true,
			Type:   "BTREE",
		},
	}

	foreignKeys := []*ForeignKey{
		{
			Name:                  "customer_fk",
			ColumnNames:           []string{columns[1].Name},
			ReferencedSchemaName:  "purchasing",
			ReferencedTableName:   "customers",
			ReferencedColumnNames: []string{"id"},
			DeleteRule:            "SET NULL",
			UpdateRule:            "RESTRICT",
		},
		{
			Name:                  "product_fk",
			ColumnNames:           []string{columns[2].Name, columns[3].Name},
			ReferencedSchemaName:  "", // same schema as this table
			ReferencedTableName:   "products",
			ReferencedColumnNames: []string{"line", "model"},
			DeleteRule:            "CASCADE",
			UpdateRule:            "NO ACTION",
		},
	}

	// warning: haven't created Flavor-specific versions of this unit test fixture
	// table yet because the need hasn't come up, but there are actual flavor-
	// specific differences with FKs. In particular, MySQL 8+ squashes NO ACTION
	// clauses from SHOW CREATE TABLE; 8.0.19+ strips display widths; ordering of
	// FKs is different in 5.5 as well as 8.0.19+; recent MariaDB (Nov'22 onwards)
	// will include COLLATE after any CHARSET.
	stmt := strings.Replace(`CREATE TABLE ~warranties~ (
  ~id~ int(10) unsigned NOT NULL,
  ~customer_id~ int(10) unsigned DEFAULT NULL,
  ~product_line~ char(12) NOT NULL,
  ~model~ int(10) unsigned NOT NULL,
  PRIMARY KEY (~id~),
  UNIQUE KEY ~product~ (~product_line~,~model~),
  KEY ~customer~ (~customer_id~),
  CONSTRAINT ~customer_fk~ FOREIGN KEY (~customer_id~) REFERENCES ~purchasing~.~customers~ (~id~) ON DELETE SET NULL,
  CONSTRAINT ~product_fk~ FOREIGN KEY (~product_line~, ~model~) REFERENCES ~products~ (~line~, ~model~) ON DELETE CASCADE ON UPDATE NO ACTION
) ENGINE=InnoDB DEFAULT CHARSET=latin1`, "~", "`", -1)

	return Table{
		Name:             "warranties",
		Engine:           "InnoDB",
		CharSet:          "latin1",
		Collation:        "latin1_swedish_ci",
		Columns:          columns,
		PrimaryKey:       primaryKey(columns[0]),
		SecondaryIndexes: secondaryIndexes,
		ForeignKeys:      foreignKeys,
		CreateStatement:  stmt,
	}
}

func stripIntDisplayWidths(table *Table, flavor Flavor) {
	origGenCreate := table.GeneratedCreateStatement(flavor)
	for _, col := range table.Columns {
		col.Type.StripDisplayWidth()
	}
	if newGenCreate := table.GeneratedCreateStatement(flavor); newGenCreate != origGenCreate { // stripped at least one col
		if table.CreateStatement == origGenCreate { // CreateStatement was normalized properly
			table.CreateStatement = newGenCreate
		} else {
			table.CreateStatement = StripDisplayWidthsFromCreate(table.CreateStatement)
		}
	}
}

func aSchema(name string, tables ...*Table) Schema {
	if tables == nil {
		tables = []*Table{}
	}
	s := Schema{
		Name:      name,
		CharSet:   "latin1",
		Collation: "latin1_swedish_ci",
		Tables:    tables,
	}
	return s
}
