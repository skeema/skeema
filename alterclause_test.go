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
