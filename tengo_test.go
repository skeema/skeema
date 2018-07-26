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
	images := SplitEnv("TENGO_TEST_IMAGES")
	if len(images) == 0 {
		fmt.Println("TENGO_TEST_IMAGES env var is not set, so integration tests will be skipped!")
		fmt.Println("To run integration tests, you may set TENGO_TEST_IMAGES to a comma-separated")
		fmt.Println("list of Docker images. Example:\n# TENGO_TEST_IMAGES=\"mysql:5.6,mysql:5.7\" go test")
	}
	RunSuite(&TengoIntegrationSuite{}, t, images)

	// Provide coverage for additional DockerizedInstance methods at this point,
	// when we know we're otherwise done with the images
	t.Run("TestDockerizedInstance", func(t *testing.T) {
		name := containerName(images[0])
		di, err := GetDockerizedInstance(name, images[0])
		if err != nil {
			t.Errorf("Unable to re-obtain Docker container %s: %s", name, err)
		}
		if os.Getenv("CI") == "true" {
			if err := di.Destroy(); err != nil {
				t.Errorf("Unable to destroy container %s: %s", name, err)
			}
		}
	})
}

type TengoIntegrationSuite struct {
	d *DockerizedInstance
}

func (s *TengoIntegrationSuite) Setup(backend string) (err error) {
	s.d, err = GetOrCreateDockerizedInstance(containerName(backend), backend)
	return err
}

func (s *TengoIntegrationSuite) Teardown(backend string) error {
	return s.d.Stop()
}

func (s *TengoIntegrationSuite) BeforeTest(method string, backend string) error {
	if err := s.d.NukeData(); err != nil {
		return err
	}
	_, err := s.d.SourceSQL("testdata/integration.sql")
	return err
}

func (s *TengoIntegrationSuite) GetSchema(t *testing.T, schemaName string) *Schema {
	t.Helper()
	schema, err := s.d.Schema(schemaName)
	if schema == nil || err != nil {
		t.Fatalf("Unable to obtain schema %s: %s", schemaName, err)
	}
	return schema
}

func (s *TengoIntegrationSuite) GetTable(t *testing.T, schemaName, tableName string) *Table {
	t.Helper()
	_, table := s.GetSchemaAndTable(t, schemaName, tableName)
	return table
}

func (s *TengoIntegrationSuite) GetSchemaAndTable(t *testing.T, schemaName, tableName string) (*Schema, *Table) {
	t.Helper()
	schema := s.GetSchema(t, schemaName)
	table := schema.Table(tableName)
	if table == nil {
		t.Fatalf("Table %s.%s unexpectedly does not exist", schemaName, tableName)
	}
	return schema, table
}

func containerName(backend string) string {
	return fmt.Sprintf("tengo-test-%s", strings.Replace(backend, ":", "-", -1))
}

// TestUnitTableFlavors confirms that our hard-coded fixture table methods
// (later on in this file) correctly adjust their output to match the specified
// flavors.
func TestUnitTableFlavors(t *testing.T) {
	orig := aTable(1)

	table := aTableForFlavor(FlavorPercona57, 1)
	if clauses, supported := table.Diff(&orig); !supported || len(clauses) != 0 {
		t.Errorf("Percona 5.7: Expected no diff; instead found %d differences, supported=%t", len(clauses), supported)
	}
	table = aTableForFlavor(FlavorMariaDB101, 1)
	if clauses, supported := table.Diff(&orig); !supported || len(clauses) != 0 {
		t.Errorf("MariaDB 10.1: Expected no diff; instead found %d differences, supported=%t", len(clauses), supported)
	}

	table = aTableForFlavor(FlavorMySQL55, 1)
	if clauses, supported := table.Diff(&orig); !supported || len(clauses) != 1 {
		t.Errorf("MySQL 5.5: Expected 1 diff clause; instead found %d differences, supported=%t", len(clauses), supported)
	}
	for _, check := range []string{table.Columns[3].TypeInDB, table.Columns[3].OnUpdate, table.Columns[3].Default.Value} {
		if strings.HasSuffix(check, ")") {
			t.Error("MySQL 5.5: Expected all traces of fractional timestamp precision to be removed, but still present")
		}
	}

	table = aTableForFlavor(FlavorMariaDB103, 1)
	if clauses, supported := table.Diff(&orig); !supported || len(clauses) != 2 {
		t.Errorf("MariaDB 10.3: Expected 2 diff clauses; instead found %d differences, supported=%t", len(clauses), supported)
	}
	if table.Columns[5].Default.Quoted {
		t.Error("MariaDB 10.3: Expected int column to not have quoted default, but it still does")
	}
	if table.Columns[3].OnUpdate != "current_timestamp(2)" || table.Columns[3].Default.Value != "current_timestamp(2)" {
		t.Error("MariaDB 10.3: Expected current_timestamp to be lowercased, but it is not")
	}
	if table.GeneratedCreateStatement(FlavorMariaDB103) != table.CreateStatement {
		t.Error("MariaDB 10.3: Expected function to reset CreateStatement to GeneratedCreateStatement, but it did not")
	}

	orig2 := supportedTable()
	table2 := supportedTableForFlavor(FlavorMariaDB102)
	if table2.GeneratedCreateStatement(FlavorMariaDB102) == orig2.GeneratedCreateStatement(FlavorUnknown) {
		t.Errorf("MariaDB 10.2: Expected GeneratedCreateStatement to differ vs FlavorUnknown, but it did not")
	}
	defaultClause := table2.Columns[2].Default.Clause(FlavorMariaDB102, table2.Columns[2])
	if !strings.Contains(defaultClause, "DEFAULT NULL") {
		t.Errorf("MariaDB 10.2: Expected text column to now emit a default value, but it did not")
	}
}

func primaryKey(cols ...*Column) *Index {
	return &Index{
		Name:       "PRIMARY",
		Columns:    cols,
		SubParts:   make([]uint16, len(cols)),
		PrimaryKey: true,
		Unique:     true,
	}
}

func aTable(nextAutoInc uint64) Table {
	return aTableForFlavor(FlavorUnknown, nextAutoInc)
}

func aTableForFlavor(flavor Flavor, nextAutoInc uint64) Table {
	lastUpdateCol := &Column{
		Name:     "last_update",
		TypeInDB: "timestamp(2)",
		Default:  ColumnDefaultExpression("CURRENT_TIMESTAMP(2)"),
		OnUpdate: "CURRENT_TIMESTAMP(2)",
	}
	lastUpdateDef := "`last_update` timestamp(2) NOT NULL DEFAULT CURRENT_TIMESTAMP(2) ON UPDATE CURRENT_TIMESTAMP(2)"
	if !flavor.FractionalTimestamps() {
		lastUpdateCol.TypeInDB = "timestamp"
		lastUpdateCol.Default = ColumnDefaultExpression("CURRENT_TIMESTAMP")
		lastUpdateCol.OnUpdate = "CURRENT_TIMESTAMP"
		lastUpdateDef = "`last_update` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"
	}
	if flavor.VendorMinVersion(VendorMariaDB, 10, 2) {
		lastUpdateCol.Default.Value = strings.ToLower(lastUpdateCol.Default.Value)
		lastUpdateCol.OnUpdate = strings.ToLower(lastUpdateCol.OnUpdate)
		lastUpdateDef = strings.Replace(lastUpdateDef, "CURRENT_TIMESTAMP", "current_timestamp", 2)
	}

	aliveCol := &Column{
		Name:     "alive",
		TypeInDB: "tinyint(1)",
		Default:  ColumnDefaultValue("1"),
	}
	aliveDef := "`alive` tinyint(1) NOT NULL DEFAULT '1'"
	if flavor.AllowDefaultExpression() {
		aliveCol.Default = ColumnDefaultExpression("1")
		aliveDef = "`alive` tinyint(1) NOT NULL DEFAULT 1"
	}

	columns := []*Column{
		{
			Name:          "actor_id",
			TypeInDB:      "smallint(5) unsigned",
			AutoIncrement: true,
			Default:       ColumnDefaultNull,
		},
		{
			Name:     "first_name",
			TypeInDB: "varchar(45)",
			Default:  ColumnDefaultNull,
			CharSet:  "utf8",
		},
		{
			Name:     "last_name",
			Nullable: true,
			TypeInDB: "varchar(45)",
			Default:  ColumnDefaultNull,
			CharSet:  "utf8",
		},
		lastUpdateCol,
		{
			Name:     "ssn",
			TypeInDB: "char(10)",
			Default:  ColumnDefaultNull,
			CharSet:  "utf8",
		},
		aliveCol,
		{
			Name:     "alive_bit",
			TypeInDB: "bit(1)",
			Default:  ColumnDefaultExpression("b'1'"),
		},
	}
	secondaryIndexes := []*Index{
		{
			Name:     "idx_ssn",
			Columns:  []*Column{columns[4]},
			SubParts: []uint16{0},
			Unique:   true,
		},
		{
			Name:     "idx_actor_name",
			Columns:  []*Column{columns[2], columns[1]},
			SubParts: []uint16{10, 1},
		},
	}

	var autoIncClause string
	if nextAutoInc > 1 {
		autoIncClause = fmt.Sprintf(" AUTO_INCREMENT=%d", nextAutoInc)
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
) ENGINE=InnoDB%s DEFAULT CHARSET=utf8`, autoIncClause)
	return Table{
		Name:              "actor",
		Engine:            "InnoDB",
		CharSet:           "utf8",
		Columns:           columns,
		PrimaryKey:        primaryKey(columns[0]),
		SecondaryIndexes:  secondaryIndexes,
		NextAutoIncrement: nextAutoInc,
		CreateStatement:   stmt,
	}
}

func anotherTable() Table {
	return anotherTableForFlavor(FlavorUnknown)
}

func anotherTableForFlavor(flavor Flavor) Table {
	columns := []*Column{
		{
			Name:     "actor_id",
			TypeInDB: "smallint(5) unsigned",
			Default:  ColumnDefaultNull,
		},
		{
			Name:     "film_name",
			TypeInDB: "varchar(60)",
			Default:  ColumnDefaultNull,
			CharSet:  "latin1",
		},
	}
	secondaryIndex := &Index{
		Name:     "film_name",
		Columns:  []*Column{columns[1]},
		SubParts: []uint16{0},
	}
	stmt := `CREATE TABLE ` + "`" + `actor_in_film` + "`" + ` (
  ` + "`" + `actor_id` + "`" + ` smallint(5) unsigned NOT NULL,
  ` + "`" + `film_name` + "`" + ` varchar(60) NOT NULL,
  PRIMARY KEY (` + "`" + `actor_id` + "`" + `,` + "`" + `film_name` + "`" + `),
  KEY ` + "`" + `film_name` + "`" + ` (` + "`" + `film_name` + "`" + `)
) ENGINE=InnoDB DEFAULT CHARSET=latin1`
	return Table{
		Name:             "actor_in_film",
		Engine:           "InnoDB",
		CharSet:          "latin1",
		Columns:          columns,
		PrimaryKey:       primaryKey(columns[0], columns[1]),
		SecondaryIndexes: []*Index{secondaryIndex},
		CreateStatement:  stmt,
	}
}

func unsupportedTable() Table {
	t := supportedTable()
	t.CreateStatement += ` ROW_FORMAT=REDUNDANT
   /*!50100 PARTITION BY RANGE (customer_id)
   (PARTITION p0 VALUES LESS THAN (123) ENGINE = InnoDB,
    PARTITION p1 VALUES LESS THAN MAXVALUE ENGINE = InnoDB) */`
	t.UnsupportedDDL = true
	return t
}

// Returns the same as unsupportedTable() but without partitioning, so that
// the table is actually supported.
func supportedTable() Table {
	return supportedTableForFlavor(FlavorUnknown)
}

func supportedTableForFlavor(flavor Flavor) Table {
	columns := []*Column{
		{
			Name:          "id",
			TypeInDB:      "int(10) unsigned",
			AutoIncrement: true,
			Default:       ColumnDefaultNull,
		},
		{
			Name:     "customer_id",
			TypeInDB: "int(10) unsigned",
			Default:  ColumnDefaultNull,
		},
		{
			Name:     "info",
			Nullable: true,
			TypeInDB: "text",
			Default:  ColumnDefaultNull,
		},
	}
	stmt := strings.Replace(`CREATE TABLE ~orders~ (
  ~id~ int(10) unsigned NOT NULL AUTO_INCREMENT,
  ~customer_id~ int(10) unsigned NOT NULL,
  ~info~ text,
  PRIMARY KEY (~id~,~customer_id~)
) ENGINE=InnoDB DEFAULT CHARSET=latin1`, "~", "`", -1)
	if flavor.AllowBlobDefaults() {
		stmt = strings.Replace(stmt, " text", " text DEFAULT NULL", 1)
	}

	return Table{
		Name:              "orders",
		Engine:            "InnoDB",
		CharSet:           "latin1",
		Columns:           columns,
		PrimaryKey:        primaryKey(columns[0:2]...),
		SecondaryIndexes:  []*Index{},
		NextAutoIncrement: 1,
		CreateStatement:   stmt,
	}
}

func foreignKeyTable() Table {
	columns := []*Column{
		{
			Name:     "id",
			TypeInDB: "int(10) unsigned",
			Default:  ColumnDefaultNull,
		},
		{
			Name:     "customer_id",
			TypeInDB: "int(10) unsigned",
			Default:  ColumnDefaultNull,
			Nullable: true,
		},
		{
			Name:     "product_line",
			TypeInDB: "char(12)",
			Default:  ColumnDefaultNull,
		},
		{
			Name:     "model",
			TypeInDB: "int(10) unsigned",
			Default:  ColumnDefaultNull,
		},
	}

	secondaryIndexes := []*Index{
		{
			Name:     "customer",
			Columns:  []*Column{columns[1]},
			SubParts: []uint16{0},
		},
		{
			Name:     "product",
			Columns:  []*Column{columns[2], columns[3]},
			Unique:   true,
			SubParts: []uint16{0, 0},
		},
	}

	foreignKeys := []*ForeignKey{
		{
			Name:                  "customer_fk",
			Columns:               columns[1:2],
			ReferencedSchemaName:  "purchasing",
			ReferencedTableName:   "customers",
			ReferencedColumnNames: []string{"id"},
			DeleteRule:            "SET NULL",
			UpdateRule:            "RESTRICT",
		},
		{
			Name:                  "product_fk",
			Columns:               columns[2:4],
			ReferencedSchemaName:  "", // same schema as this table
			ReferencedTableName:   "products",
			ReferencedColumnNames: []string{"line", "model"},
			DeleteRule:            "CASCADE",
			UpdateRule:            "CASCADE",
		},
	}

	stmt := strings.Replace(`CREATE TABLE ~warranties~ (
  ~id~ int(10) unsigned NOT NULL,
  ~customer_id~ int(10) unsigned DEFAULT NULL,
  ~product_line~ char(12) NOT NULL,
  ~model~ int(10) unsigned NOT NULL,
  PRIMARY KEY (~id~),
  UNIQUE KEY ~product~ (~product_line~,~model~),
  KEY ~customer~ (~customer_id~),
  CONSTRAINT ~customer_fk~ FOREIGN KEY (~customer_id~) REFERENCES ~purchasing~.~customers~ (~id~),
  CONSTRAINT ~product_fk~ FOREIGN KEY (~product_line~, ~model~) REFERENCES ~products~ (~line~, ~model~)
) ENGINE=InnoDB DEFAULT CHARSET=latin1`, "~", "`", -1)

	return Table{
		Name:             "warranties",
		Engine:           "InnoDB",
		CharSet:          "latin1",
		Columns:          columns,
		PrimaryKey:       primaryKey(columns[0]),
		SecondaryIndexes: secondaryIndexes,
		ForeignKeys:      foreignKeys,
		CreateStatement:  stmt,
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
