package tengo

import (
	"fmt"
	"strings"
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
		{"binary(17)", "inet6"},
		{"inet6", "varbinary(16)"},
		{"inet6", "varchar(38)"},
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
		{"binary(16)", "inet6"},
		{"inet6", "binary(16)"},
		{"char(39)", "inet6"},
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
	if uncompTable.CreateOptions != "" {
		t.Fatal("Fixture table has changed without test logic being updated")
	}

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
	// Just comparing string length because the *order* of create options may
	// randomly differ from what was specified in DDL
	if len(refetchedTable.CreateOptions) != len(compTable.CreateOptions) {
		t.Fatalf("Expected refetched table to have create options %q, instead found %q", compTable.CreateOptions, refetchedTable.CreateOptions)
	}

	// Test diff generation and execution for compressed -> uncompressed
	clauses, supported = compTable.Diff(uncompTable)
	if len(clauses) != 1 || !supported {
		t.Fatalf("Unexpected return from diff: %d clauses, supported=%t", len(clauses), supported)
	}
	runAlter(clauses[0])
	refetchedTable = s.GetTable(t, "testing", "actor_in_film")
	if refetchedTable.CreateOptions != "" {
		t.Fatalf("Expected refetched table to have create options \"\", instead found %q", refetchedTable.CreateOptions)
	}
}

// TestAlterCheckConstraints provides unit test coverage relating to diffs of
// check constraints.
func TestAlterCheckConstraints(t *testing.T) {
	flavor := FlavorMySQL80
	flavor.Patch = 23
	mods := StatementModifiers{Flavor: flavor}

	// Test addition of checks
	tableNoChecks := aTableForFlavor(flavor, 1)
	tableChecks := aTableForFlavor(flavor, 1)
	tableChecks.Checks = []*Check{
		{Name: "alivecheck", Clause: "alive != 0", Enforced: true},
		{Name: "stringythings", Clause: "ssn <> '000000000'", Enforced: true},
	}
	tableChecks.CreateStatement = tableChecks.GeneratedCreateStatement(flavor)
	td := NewAlterTable(&tableNoChecks, &tableChecks)
	if len(td.alterClauses) != 2 {
		t.Errorf("Expected 2 alter clauses, instead found %d", len(td.alterClauses))
	} else {
		for _, clause := range td.alterClauses {
			str := clause.Clause(mods)
			if _, ok := clause.(AddCheck); !ok || !strings.Contains(str, "ADD CONSTRAINT") {
				t.Errorf("Found unexpected type %T", clause)
			}
		}
	}

	// Test removal of checks
	td = NewAlterTable(&tableChecks, &tableNoChecks)
	if len(td.alterClauses) != 2 {
		t.Errorf("Expected 2 alter clauses, instead found %d", len(td.alterClauses))
	} else {
		for _, clause := range td.alterClauses {
			if _, ok := clause.(DropCheck); !ok {
				t.Errorf("Found unexpected type %T", clause)
			}
			strMySQL := clause.Clause(mods)
			strMaria := clause.Clause(StatementModifiers{Flavor: FlavorMariaDB105})
			if strMySQL == strMaria || !strings.Contains(strMySQL, "DROP CHECK") || !strings.Contains(strMaria, "DROP CONSTRAINT") {
				t.Errorf("Unexpected clause differences between flavors; found MySQL %q, MariaDB %q", strMySQL, strMaria)
			}
		}
	}

	// Test change in check clause on first check. This should result in 4 clauses:
	// drop and re-add the first check to modify it, and drop and re-add the second
	// check but only for ordering (which is typically ignored)
	tableChecks2 := aTableForFlavor(flavor, 1)
	tableChecks2.Checks = []*Check{
		{Name: "alivecheck", Clause: "alive = 1", Enforced: true},
		{Name: "stringythings", Clause: "ssn <> '000000000'", Enforced: true},
	}
	tableChecks2.CreateStatement = tableChecks2.GeneratedCreateStatement(flavor)
	td = NewAlterTable(&tableChecks, &tableChecks2)
	if len(td.alterClauses) != 4 {
		t.Errorf("Expected 4 alterClauses, instead found %d", len(td.alterClauses))
	} else {
		if dcc, ok := td.alterClauses[0].(DropCheck); !ok || dcc.Check != tableChecks.Checks[0] || dcc.reorderOnly {
			t.Errorf("Found unexpected alterClause %+v", td.alterClauses[0])
		}
		if acc, ok := td.alterClauses[1].(AddCheck); !ok || acc.Check != tableChecks2.Checks[0] || acc.reorderOnly {
			t.Errorf("Found unexpected alterClause %+v", td.alterClauses[1])
		}
		if dcc, ok := td.alterClauses[2].(DropCheck); !ok || dcc.Check != tableChecks.Checks[1] || !dcc.reorderOnly {
			t.Errorf("Found unexpected alterClause %+v", td.alterClauses[2])
		}
		if acc, ok := td.alterClauses[3].(AddCheck); !ok || acc.Check != tableChecks2.Checks[1] || !acc.reorderOnly {
			t.Errorf("Found unexpected alterClause %+v", td.alterClauses[3])
		}
		for n, alterClause := range td.alterClauses {
			expectBlank := n > 1
			actualBlank := alterClause.Clause(mods) == ""
			if expectBlank != actualBlank {
				t.Errorf("Unexpected result from Clause() at n=%d", n)
			}
		}
	}

	// Test alteration of check enforcement
	tableChecks2.Checks = []*Check{
		{Name: "alivecheck", Clause: "alive != 0", Enforced: true},
		{Name: "stringythings", Clause: "ssn <> '000000000'", Enforced: false},
	}
	tableChecks2.CreateStatement = tableChecks2.GeneratedCreateStatement(flavor)
	td = NewAlterTable(&tableChecks, &tableChecks2)
	if len(td.alterClauses) != 1 {
		t.Errorf("Expected 1 alterClause, instead found %d", len(td.alterClauses))
	} else {
		str := td.alterClauses[0].Clause(mods)
		if _, ok := td.alterClauses[0].(AlterCheck); !ok || !strings.Contains(str, "ALTER CHECK") {
			t.Errorf("Found unexpected type %T", td.alterClauses[0])
		}
	}

	// Create a table with 5 checks. Reorder one of them and confirm result.
	flavor = FlavorMariaDB105
	tableChecks, tableChecks2 = aTableForFlavor(flavor, 1), aTableForFlavor(flavor, 1)
	tableChecks.Checks = []*Check{
		{Name: "check1", Clause: "ssn <> '111111111'", Enforced: true},
		{Name: "check2", Clause: "ssn <> '222222222'", Enforced: true},
		{Name: "check3", Clause: "ssn <> '333333333'", Enforced: true},
		{Name: "check4", Clause: "ssn <> '444444444'", Enforced: true},
		{Name: "check5", Clause: "ssn <> '555555555'", Enforced: true},
	}
	tableChecks.CreateStatement = tableChecks.GeneratedCreateStatement(flavor)
	tableChecks2.Checks = []*Check{
		{Name: "check1", Clause: "ssn <> '111111111'", Enforced: true},
		{Name: "check2", Clause: "ssn <> '222222222'", Enforced: true},
		{Name: "check4", Clause: "ssn <> '444444444'", Enforced: true},
		{Name: "check3", Clause: "ssn <> '333333333'", Enforced: true},
		{Name: "check5", Clause: "ssn <> '555555555'", Enforced: true},
	}
	tableChecks2.CreateStatement = tableChecks2.GeneratedCreateStatement(flavor)
	td = NewAlterTable(&tableChecks, &tableChecks2)
	modsStrict := StatementModifiers{Flavor: flavor, StrictCheckOrder: true}
	if len(td.alterClauses) != 4 {
		t.Errorf("Expected 4 alterClauses, instead found %d", len(td.alterClauses))
	}
	for n, alterClause := range td.alterClauses {
		str, strStrict := alterClause.Clause(mods), alterClause.Clause(modsStrict)
		if str != "" || strStrict == "" {
			t.Errorf("Clauses don't match expectations: %q / %q", str, strStrict)
		}
		if n%2 == 0 {
			if _, ok := alterClause.(DropCheck); !ok {
				t.Errorf("Unexpected type at clause[%d]: %T", n, alterClause)
			}
		} else {
			if _, ok := alterClause.(AddCheck); !ok {
				t.Errorf("Unexpected type at clause[%d]: %T", n, alterClause)
			}
		}
	}
}

// TestAlterCheckConstraints provides integration test coverage relating to
// diffs of check constraints. It is similar to the above function, but actually
// executes the generated ALTERs to confirm validity.
func (s TengoIntegrationSuite) TestAlterCheckConstraints(t *testing.T) {
	flavor := s.d.Flavor()
	if !flavor.HasCheckConstraints() {
		t.Skipf("Check constraints not supported in flavor %s", flavor)
	}

	db, err := s.d.ConnectionPool("testing", "")
	if err != nil {
		t.Fatalf("Unable to establish connection pool: %v", err)
	}
	execAlter := func(td *TableDiff) {
		t.Helper()
		if td == nil {
			t.Fatal("diff was unexpectedly nil")
		} else if td.Type != DiffTypeAlter {
			t.Fatalf("Expected Type to be DiffTypeAlter, instead found %s", td.Type)
		}
		stmt, err := td.Statement(StatementModifiers{Flavor: flavor})
		if err != nil {
			t.Fatalf("Unexpected error from Statement: %v", err)
		} else if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("Unexpected error executing statement %q: %v", stmt, err)
		}
	}

	// Test addition of checks
	tableNoChecks := s.GetTable(t, "testing", "grab_bag")
	tableChecks := s.GetTable(t, "testing", "grab_bag")
	tableChecks.Checks = []*Check{
		{Name: "alivecheck", Clause: "alive != 0", Enforced: true},
		{Name: "stringythings", Clause: "code != 'ABCD1234' AND name != code", Enforced: true},
	}
	tableChecks.CreateStatement = tableChecks.GeneratedCreateStatement(flavor)
	td := NewAlterTable(tableNoChecks, tableChecks)
	execAlter(td)
	tableChecks = s.GetTable(t, "testing", "grab_bag")
	if tableChecks.UnsupportedDDL {
		t.Fatal("Table is unexpectedly unsupported for diffs now")
	}

	// Confirm that modifying a check's name or clause = drop and re-add
	tableChecks2 := s.GetTable(t, "testing", "grab_bag")
	tableChecks2.Checks[0].Clause = "alive = 1"
	tableChecks2.Checks[1].Name = "stringycheck"
	tableChecks2.CreateStatement = tableChecks2.GeneratedCreateStatement(flavor)
	td = NewAlterTable(tableChecks, tableChecks2)
	if len(td.alterClauses) != 4 {
		t.Errorf("Expected 4 alterClauses, instead found %d", len(td.alterClauses))
	}
	execAlter(td)
	tableChecks = s.GetTable(t, "testing", "grab_bag")
	if tableChecks.UnsupportedDDL {
		t.Fatal("Table is unexpectedly unsupported for diffs now")
	}
	if len(tableChecks.Checks) != 2 {
		t.Errorf("Expected 2 check constraints, instead found %d", len(tableChecks.Checks))
	}

	// Confirm functionality related to MySQL's ALTER CHECK clause and the NOT
	// ENFORCED modifier
	if flavor.Vendor != VendorMariaDB {
		tableChecks2 = s.GetTable(t, "testing", "grab_bag")
		tableChecks2.Checks[1].Enforced = false
		tableChecks2.CreateStatement = tableChecks2.GeneratedCreateStatement(flavor)
		td = NewAlterTable(tableChecks, tableChecks2)
		if len(td.alterClauses) != 1 {
			t.Errorf("Expected 1 alterClause, instead found %d", len(td.alterClauses))
		}
		execAlter(td)
		tableChecks2 = s.GetTable(t, "testing", "grab_bag")
		if tableChecks2.UnsupportedDDL {
			t.Fatal("Table is unexpectedly unsupported for diffs now")
		}
		if tableChecks2.Checks[0].Enforced == tableChecks2.Checks[1].Enforced {
			t.Error("Altering enforcement of check did not work as expected")
		}

		// Now do the reverse: set the check back to enforced
		td = NewAlterTable(tableChecks2, tableChecks)
		execAlter(td)
		tableChecks = s.GetTable(t, "testing", "grab_bag")
		if tableChecks.UnsupportedDDL {
			t.Fatal("Table is unexpectedly unsupported for diffs now")
		}
		if !tableChecks.Checks[0].Enforced || !tableChecks.Checks[1].Enforced {
			t.Error("Altering enforcement of check did not work as expected")
		}
	}
}
