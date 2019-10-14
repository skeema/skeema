package tengo

import (
	"testing"
)

func TestTableCreatePartitioning(t *testing.T) {
	unpartitioned := unpartitionedTable(FlavorUnknown)
	partitioned := partitionedTable(FlavorUnknown)
	s1 := aSchema("s1")
	s2 := aSchema("s2", &partitioned)
	sd := NewSchemaDiff(&s1, &s2)
	if len(sd.TableDiffs) != 1 {
		t.Fatalf("Incorrect number of table diffs: expected 1, found %d", len(sd.TableDiffs))
	}
	td := sd.TableDiffs[0]
	if td.DiffType() != DiffTypeCreate || td.Type.String() != "CREATE" {
		t.Fatalf("Incorrect type of table diff returned: expected %s, found %s", DiffTypeCreate, td.Type)
	}

	mods := StatementModifiers{}
	expected := partitioned.CreateStatement
	if actual, err := td.Statement(mods); err != nil {
		t.Fatalf("Unexpected error from Statement: %+v", err)
	} else if actual != expected {
		t.Errorf("Unexpected return from Statement: expected %q, found %q", expected, actual)
	}

	mods.Partitioning = PartitioningRemove
	expected = unpartitioned.CreateStatement
	if actual, err := td.Statement(mods); err != nil {
		t.Fatalf("Unexpected error from Statement: %+v", err)
	} else if actual != expected {
		t.Errorf("Unexpected return from Statement: expected %q, found %q", expected, actual)
	}
}

func TestTableAlterPartitioningStatus(t *testing.T) {
	unpartitioned := unpartitionedTable(FlavorUnknown)
	partitioned := partitionedTable(FlavorUnknown)

	tableAlters, supported := unpartitioned.Diff(&partitioned)
	if !supported {
		t.Error("ALTER to add partitioning unexpectedly unsupported")
	} else if len(tableAlters) != 1 {
		t.Errorf("Wrong number of alter clauses: expected 1, found %d: %+v", len(tableAlters), tableAlters)
	} else if clause, ok := tableAlters[0].(AddPartitioning); !ok {
		t.Errorf("Wrong type of alter clause: expected %T, found %T", clause, tableAlters[0])
	} else {
		mods := StatementModifiers{}
		if expected, actual := partitioned.Partitioning.Definition(FlavorUnknown), clause.Clause(mods); expected != actual {
			t.Errorf("Unexpected return from Clause(): expected %q, found %q", expected, actual)
		}
		mods.Partitioning = PartitioningRemove
		if expected, actual := "", clause.Clause(mods); expected != actual {
			t.Errorf("Unexpected return from Clause(): expected %q, found %q", expected, actual)
		}
	}

	tableAlters, supported = partitioned.Diff(&unpartitioned)
	if !supported {
		t.Error("ALTER to remove partitioning unexpectedly unsupported")
	} else if len(tableAlters) != 1 {
		t.Errorf("Wrong number of alter clauses: expected 1, found %d: %+v", len(tableAlters), tableAlters)
	} else if clause, ok := tableAlters[0].(RemovePartitioning); !ok {
		t.Errorf("Wrong type of alter clause: expected %T, found %T", clause, tableAlters[0])
	} else {
		mods := StatementModifiers{}
		if expected, actual := "REMOVE PARTITIONING", clause.Clause(mods); expected != actual {
			t.Errorf("Unexpected return from Clause(): expected %q, found %q", expected, actual)
		}
		mods.Partitioning = PartitioningKeep
		if expected, actual := "", clause.Clause(mods); expected != actual {
			t.Errorf("Unexpected return from Clause(): expected %q, found %q", expected, actual)
		}
	}
}

func TestTableAlterPartitioningOther(t *testing.T) {
	assertIgnored := func(t1, t2 *Table) {
		t.Helper()
		t2.CreateStatement = "" // bypass diff logic short-circuit on matching CreateStatement
		tableAlters, supported := t1.Diff(t2)
		if !supported || len(tableAlters) != 1 {
			t.Errorf("Unexpected return from Diff: %d alters / %t supported", len(tableAlters), supported)
		} else {
			_, ok := tableAlters[0].(ModifyPartitions)
			clause := tableAlters[0].Clause(StatementModifiers{})
			if !ok || clause != "" {
				t.Errorf("Unexpected type or clause returned from diff: %T %s", tableAlters[0], clause)
			}
		}
	}

	assertUnsupported := func(t1, t2 *Table) {
		t.Helper()
		t2.CreateStatement = "" // bypass diff logic short-circuit on matching CreateStatement
		_, supported := t1.Diff(t2)
		if supported {
			t.Error("Expected diff to be unsupported, but it was supported")
		}
	}

	// Changes to the partition list are ignored (via placeholder
	// ModifyPartitions clause) for unit test table since it has RANGE partitioning
	p1, p2 := partitionedTable(FlavorUnknown), partitionedTable(FlavorUnknown)
	p2.Partitioning.Partitions[1].Comment = "hello world"
	assertIgnored(&p1, &p2)
	p2.Partitioning.Partitions = []*Partition{p2.Partitioning.Partitions[0], p2.Partitioning.Partitions[2]}
	assertIgnored(&p1, &p2)
	assertIgnored(&p2, &p1)

	// Changes to the partition list are unsupported for HASH partitioning
	p1.Partitioning.Method, p2.Partitioning.Method = "HASH", "HASH"
	assertUnsupported(&p1, &p2)
	assertUnsupported(&p2, &p1)

	// Changing the method of partitioning is unsupported
	p1.Partitioning.Method = "RANGE"
	assertUnsupported(&p1, &p2)
	assertUnsupported(&p2, &p1)
}

func TestTableUnpartitionedCreateStatement(t *testing.T) {
	flavors := []Flavor{FlavorUnknown, FlavorMySQL55, FlavorPercona56, FlavorMySQL80, FlavorMariaDB102}
	for _, flavor := range flavors {
		unpartitioned := unpartitionedTable(flavor)
		partitioned := partitionedTable(flavor)
		partitioned.UnsupportedDDL = false
		expected, actual := unpartitioned.CreateStatement, partitioned.UnpartitionedCreateStatement(flavor)
		if actual != expected {
			t.Errorf("Unexpected return from UnpartitionedCreateStatement(%s): expected %q, found %q", flavor, expected, actual)
		}

		// Test separate code path for tables using unsupported features
		partitioned.UnsupportedDDL = true
		actual = partitioned.UnpartitionedCreateStatement(flavor)
		if actual != expected {
			t.Errorf("Unexpected return from UnpartitionedCreateStatement(%s): expected %q, found %q", flavor, expected, actual)
		}

		// Confirm correct return value for already-unpartitioned table
		expected, actual = unpartitioned.CreateStatement, unpartitioned.UnpartitionedCreateStatement(flavor)
		if actual != expected {
			t.Errorf("Unexpected return from UnpartitionedCreateStatement(%s): expected %q, found %q", flavor, expected, actual)
		}
	}
}

func (s TengoIntegrationSuite) TestPartitionedIntrospection(t *testing.T) {
	if _, err := s.d.SourceSQL("testdata/partition.sql"); err != nil {
		t.Fatalf("Unexpected error sourcing testdata/partition.sql: %v", err)
	}
	schema := s.GetSchema(t, "partitionparty")
	flavor := s.d.Flavor()

	// Ensure our unit test fixture and integration test fixture match
	tableFromDB := schema.Table("prange")
	tableFromUnit := partitionedTable(flavor)
	tableFromUnit.CreateStatement = "" // Prevent diff from short-circuiting on equivalent CREATEs
	clauses, supported := tableFromDB.Diff(&tableFromUnit)
	if !supported {
		t.Error("Diff unexpectedly not supported for unit test partitioned table")
	} else if len(clauses) > 0 {
		t.Errorf("Diff of partitioned table unexpectedly found %d clauses; expected 0. Clauses: %+v", len(clauses), clauses)
	}

	// ensure partitioned tables are introspected correctly by confirming that
	// they are supported for diffs. Additionally confirm that
	// UnpartitionedCreateStatement returns the expected value.
	for _, table := range schema.Tables {
		if table.UnsupportedDDL {
			t.Errorf("Table %s unexpectedly has UnsupportedDDL==true\nExpected SHOW CREATE TABLE:\n%s\nActual SHOW CREATE TABLE:\n%s", table.Name, table.GeneratedCreateStatement(flavor), table.CreateStatement)
		}
		actual := table.UnpartitionedCreateStatement(flavor)
		table.Partitioning = nil
		expected := table.GeneratedCreateStatement(flavor)
		if actual != expected {
			t.Errorf("Table %s unexpected result from UnpartitionedCreateStatement: expected %q, found %q", table.Name, expected, actual)
		}
	}
}

// Keep this definition in sync with table prange in partition.sql
func partitionedTable(flavor Flavor) Table {
	t := unpartitionedTable(flavor)
	t.Partitioning = &TablePartitioning{
		Method:     "RANGE",
		Expression: "customer_id",
		Partitions: []*Partition{
			{Name: "p0", Values: "123", method: "RANGE", engine: "InnoDB"},
			{Name: "p1", Values: "456", method: "RANGE", engine: "InnoDB"},
			{Name: "p2", Values: "MAXVALUE", method: "RANGE", engine: "InnoDB"},
		},
	}
	t.CreateStatement = t.GeneratedCreateStatement(flavor)
	return t
}

func unpartitionedTable(flavor Flavor) Table {
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
			TypeInDB: "text",
			Nullable: true,
			Default:  ColumnDefaultNull,
			CharSet:  "latin1", Collation: "latin1_swedish_ci",
			CollationIsDefault: true,
		},
	}
	t := Table{
		Name:               "prange",
		Engine:             "InnoDB",
		CharSet:            "latin1",
		Collation:          "latin1_swedish_ci",
		CollationIsDefault: true,
		CreateOptions:      "ROW_FORMAT=REDUNDANT",
		Columns:            columns,
		PrimaryKey:         primaryKey(columns[0], columns[1]),
		SecondaryIndexes:   []*Index{},
		ForeignKeys:        []*ForeignKey{},
		NextAutoIncrement:  1,
	}
	t.CreateStatement = t.GeneratedCreateStatement(flavor)
	return t
}
