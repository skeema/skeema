package tengo

import (
	"testing"
)

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
		{"enum('a', 'b', 'c')", "enum('a', 'aa', 'b', 'c'"},
		{"set('abc', 'def', 'ghi')", "set('abc', 'def')"},
		{"decimal(10,5)", "decimal(10,4)"},
		{"decimal(10,5)", "decimal(9,5)"},
		{"decimal(10,5)", "decimal(9,6)"},
		{"varchar(20)", "varchar(19)"},
		{"varbinary(40)", "varbinary(35)"},
		{"varchar(20)", "varbinary(20)"},
		{"char(10)", "char(15)"},
		{"timestamp(5)", "timestamp"},
		{"datetime(4)", "datetime(3)"},
		{"float", "float(10,5)"},
		{"double", "float"},
		{"float(10,5)", "float(10,4)"},
		{"double(10,5)", "double(9,5)"},
		{"float(10,5)", "double(10,4)"},
		{"mediumint", "smallint"},
		{"mediumint(1)", "tinyint"},
		{"longblob", "blob"},
		{"mediumtext", "tinytext"},
		{"tinyblob", "longtext"},
		{"varchar(200)", "text"},
		{"char(30)", "varchar(30)"},
	}
	for _, types := range expectUnsafe {
		assertUnsafe(types[0], types[1], true)
	}

	expectSafe := [][]string{
		{"varchar(30)", "varchar(30)"},
		{"mediumint(4)", "mediumint(3)"},
		{"int zerofill", "int"},
		{"enum('a', 'b', 'c')", "enum('a', 'b', 'c', 'd')"},
		{"set('abc', 'def', 'ghi')", "set('abc', 'def', 'ghi', 'jkl')"},
		{"decimal(9,4)", "decimal(10,4)"},
		{"decimal(9,4)", "decimal(9,5)"},
		{"varchar(20)", "varchar(21)"},
		{"varbinary(40)", "varbinary(45)"},
		{"timestamp", "timestamp(5)"},
		{"datetime(3)", "datetime(4)"},
		{"float(10,5)", "float"},
		{"float", "double"},
		{"float(10,4)", "float(10,5)"},
		{"double(9,5)", "double(10,5)"},
		{"float(10,4)", "double(11,4)"},
		{"float(10,4)", "double"},
		{"smallint", "mediumint"},
		{"tinyint", "mediumint(1)"},
		{"int(4) unsigned", "int(5) unsigned"},
		{"blob", "longblob"},
		{"tinytext", "mediumtext"},
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
}
