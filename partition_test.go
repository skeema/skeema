package tengo

import (
	"testing"
)

func (s TengoIntegrationSuite) TestPartitionedIntrospection(t *testing.T) {
	if _, err := s.d.SourceSQL("testdata/partition.sql"); err != nil {
		t.Fatalf("Unexpected error sourcing testdata/partition.sql: %v", err)
	}
	schema := s.GetSchema(t, "partitionparty")
	flavor := s.d.Flavor()

	// ensure partitioned tables are introspected correctly by confirming that
	// they are supported for diffs
	for _, table := range schema.Tables {
		if table.UnsupportedDDL {
			t.Errorf("Table %s unexpectedly has UnsupportedDDL==true\nExpected SHOW CREATE TABLE:\n%s\nActual SHOW CREATE TABLE:\n%s", table.Name, table.GeneratedCreateStatement(flavor), table.CreateStatement)
		}
	}
}
