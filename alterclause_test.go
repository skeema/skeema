package tengo

import (
	"fmt"
	"testing"
)

// TestModifyColumnDisplayWidth provides coverage of edge cases relating to
// one side having an int display width and the other missing one.
func TestModifyColumnDisplayWidth(t *testing.T) {
	mc := ModifyColumn{
		Table: &Table{Name: "test"},
		OldColumn: &Column{
			Name:     "col",
			TypeInDB: "bigint(20) unsigned",
			Default:  "NULL",
			Nullable: true,
		},
		NewColumn: &Column{
			Name:     "col",
			TypeInDB: "bigint unsigned",
			Default:  "NULL",
			Nullable: true,
		},
	}

	assertNoOp := func() {
		t.Helper()
		if clause := mc.Clause(StatementModifiers{}); clause != "" {
			t.Errorf("Expected Clause() to return an empty string, instead found %q", clause)
		}
	}
	assertOp := func() {
		t.Helper()
		if mc.Clause(StatementModifiers{}) == "" {
			t.Error("Expected Clause() to return a non-empty string, but it was empty")
		}
	}

	assertNoOp() // starting setup just removes display width
	mc.OldColumn, mc.NewColumn = mc.NewColumn, mc.OldColumn
	assertNoOp()

	mc.OldColumn.TypeInDB = "int unsigned"
	assertOp()
	mc.OldColumn, mc.NewColumn = mc.NewColumn, mc.OldColumn
	assertOp()
	mc.NewColumn.TypeInDB = "bigint(19) unsigned"
	assertOp()
	mc.NewColumn.TypeInDB = "bigint(20)"
	assertOp()

	mc.OldColumn.TypeInDB, mc.NewColumn.TypeInDB = "bigint(20) unsigned", "bigint unsigned"
	assertNoOp()
	mc.NewColumn.Nullable = false
	mc.NewColumn.Default = ""
	assertOp()

	mc.OldColumn.TypeInDB, mc.NewColumn.TypeInDB = "timestamp(4)", "timestamp"
	mc.OldColumn.Nullable, mc.NewColumn.Nullable = false, false
	mc.OldColumn.Default, mc.NewColumn.Default = "", ""
	assertOp()
}

func TestModifyColumnUnsafe(t *testing.T) {
	assertUnsafe := func(type1, type2 string, expected bool) {
		mc := ModifyColumn{
			OldColumn: &Column{TypeInDB: type1},
			NewColumn: &Column{TypeInDB: type2},
		}
		if actual := mc.Unsafe(); actual != expected {
			t.Errorf("For %s -> %s, expected unsafe=%t, instead found unsafe=%t", type1, type2, expected, actual)
		}
	}

	expectUnsafe := [][]string{
		{"int unsigned", "int"},
		{"bigint(11)", "bigint(11) unsigned"},
		{"int(11)", "bigint(20) unsigned"},
		{"enum('a', 'b', 'c')", "enum('a', 'aa', 'b', 'c'"},
		{"set('abc', 'def', 'ghi')", "set('abc', 'def')"},
		{"decimal(10,5)", "decimal(10,4)"},
		{"decimal(10,5)", "decimal(9,5)"},
		{"decimal(10,5)", "decimal(9,6)"},
		{"decimal(9,4)", "decimal(10,5) unsigned"},
		{"varchar(20)", "varchar(19)"},
		{"varbinary(40)", "varbinary(35)"},
		{"varbinary(256)", "tinyblob"},
		{"blob", "varbinary(2000)"},
		{"varchar(20)", "varbinary(20)"},
		{"timestamp(5)", "timestamp"},
		{"datetime(4)", "datetime(3)"},
		{"float", "float(10,5)"},
		{"double", "float"},
		{"float(10,5)", "float(10,4)"},
		{"double(10,5)", "double(9,5)"},
		{"float(10,5)", "double(10,4)"},
		{"float(10,5)", "float(10,5) unsigned"},
		{"mediumint", "smallint"},
		{"mediumint(1)", "tinyint"},
		{"longblob", "blob"},
		{"mediumtext", "tinytext"},
		{"varchar(2000)", "tinytext"},
		{"tinytext", "char(200)"},
		{"tinyblob", "longtext"},
		{"binary(5)", "binary(10)"},
		{"bit(10)", "bit(9)"},
	}
	for _, types := range expectUnsafe {
		assertUnsafe(types[0], types[1], true)
	}

	expectSafe := [][]string{
		{"varchar(30)", "varchar(30)"},
		{"mediumint(4)", "mediumint(3)"},
		{"int zerofill", "int"},
		{"int(10) unsigned", "bigint(20)"},
		{"enum('a', 'b', 'c')", "enum('a', 'b', 'c', 'd')"},
		{"set('abc', 'def', 'ghi')", "set('abc', 'def', 'ghi', 'jkl')"},
		{"decimal(9,4)", "decimal(10,4)"},
		{"decimal(9,4)", "decimal(9,5)"},
		{"decimal(9,4) unsigned", "decimal(9,4)"},
		{"varchar(20)", "varchar(21)"},
		{"varbinary(40)", "varbinary(45)"},
		{"varbinary(255)", "tinyblob"},
		{"tinyblob", "varbinary(255)"},
		{"timestamp", "timestamp(5)"},
		{"datetime(3)", "datetime(4)"},
		{"float(10,5)", "float"},
		{"float", "double"},
		{"float(10,4)", "float(10,5)"},
		{"double(9,5)", "double(10,5)"},
		{"double(10,5) unsigned", "double(10,5)"},
		{"float(10,4)", "double(11,4)"},
		{"float(10,4)", "double"},
		{"smallint", "mediumint"},
		{"tinyint", "mediumint(1)"},
		{"int(4) unsigned", "int(5) unsigned"},
		{"blob", "longblob"},
		{"tinytext", "mediumtext"},
		{"tinytext", "char(255)"},
		{"char(10)", "char(15)"},
		{"varchar(200)", "tinytext"},
		{"char(30)", "varchar(30)"},
		{"bit(10)", "bit(11)"},
	}
	for _, types := range expectSafe {
		assertUnsafe(types[0], types[1], false)
	}

	// Special case: confirm changing the character set of a column is unsafe, but
	// changing collation within same character set is safe
	mc := ModifyColumn{
		OldColumn: &Column{TypeInDB: "varchar(30)", CharSet: "latin1"},
		NewColumn: &Column{TypeInDB: "varchar(30)", CharSet: "utf8mb4"},
	}
	if !mc.Unsafe() {
		t.Error("For changing character set, expected unsafe=true, instead found unsafe=false")
	}
	mc.NewColumn.CharSet = "latin1"
	mc.NewColumn.Collation = "latin1_bin"
	if mc.Unsafe() {
		t.Error("For changing collation but not character set, expected unsafe=false, instead found unsafe=true")
	}

	// Special case: confirm changing the type of a column is safe for virtual
	// generated columns but not stored generated columns
	mc = ModifyColumn{
		OldColumn: &Column{TypeInDB: "bigint(20)", GenerationExpr: "id * 2", Virtual: true},
		NewColumn: &Column{TypeInDB: "int(11)", GenerationExpr: "id * 2", Virtual: true},
	}
	if mc.Unsafe() {
		t.Error("Expected virtual column modification to be safe, but Unsafe() returned true")
	}
	mc.OldColumn.Virtual = false
	if !mc.Unsafe() {
		t.Error("Expected stored column modification to be unsafe, but Unsafe() returned false")
	}
}

func (s TengoIntegrationSuite) TestAlterPageCompression(t *testing.T) {
	flavor := s.d.Flavor()
	// Skip test if flavor doesn't support page compression
	// Note that although MariaDB 10.1 supports this feature, we exclude it here
	// since it does not seem to work out-of-the-box in Docker images
	if !flavor.MySQLishMinVersion(5, 7) && !flavor.VendorMinVersion(VendorMariaDB, 10, 2) {
		t.Skipf("InnoDB page compression not supported in flavor %s", flavor)
	}

	sqlPath := "testdata/pagecompression.sql"
	if flavor.Vendor == VendorMariaDB {
		sqlPath = "testdata/pagecompression-maria.sql"
	}
	if _, err := s.d.SourceSQL(sqlPath); err != nil {
		t.Fatalf("Unexpected error sourcing %s: %v", sqlPath, err)
	}
	uncompTable := s.GetTable(t, "testing", "actor_in_film")

	runAlter := func(clause TableAlterClause) {
		t.Helper()
		db, err := s.d.Connect("testing", "")
		if err != nil {
			t.Fatalf("Unable to connect to DockerizedInstance: %s", err)
		}
		tableName := uncompTable.Name
		query := fmt.Sprintf("ALTER TABLE %s %s", EscapeIdentifier(tableName), clause.Clause(StatementModifiers{}))
		if _, err := db.Exec(query); err != nil {
			t.Fatalf("Unexpected error from query %q: %v", query, err)
		}
	}

	compTable := s.GetTable(t, "testing", "actor_in_film_comp")
	if compTable.UnsupportedDDL {
		t.Fatal("Table with page compression is unexpectedly unsupported for diff")
	}
	compTable.Name = uncompTable.Name

	// Test diff generation for uncompressed -> compressed
	clauses, supported := uncompTable.Diff(compTable)
	if len(clauses) != 1 || !supported {
		t.Fatalf("Unexpected return from diff: %d clauses, supported=%t", len(clauses), supported)
	}
	runAlter(clauses[0])
	refetchedTable := s.GetTable(t, "testing", "actor_in_film")
	if refetchedTable.CreateOptions != compTable.CreateOptions {
		t.Fatalf("Expected refetched table to have create options %q, instead found %q", compTable.CreateOptions, refetchedTable.CreateOptions)
	}

	// Test diff generation and execution for compressed -> uncompressed
	clauses, supported = compTable.Diff(uncompTable)
	if len(clauses) != 1 || !supported {
		t.Fatalf("Unexpected return from diff: %d clauses, supported=%t", len(clauses), supported)
	}
	runAlter(clauses[0])
	refetchedTable = s.GetTable(t, "testing", "actor_in_film")
	if refetchedTable.CreateOptions != uncompTable.CreateOptions {
		t.Fatalf("Expected refetched table to have create options %q, instead found %q", uncompTable.CreateOptions, refetchedTable.CreateOptions)
	}
}
