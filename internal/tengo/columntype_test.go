package tengo

import (
	"testing"
)

func TestParseColumnType(t *testing.T) {
	cases := map[string]ColumnType{
		"int":                          {Base: "int"},
		"int unsigned":                 {Base: "int", Unsigned: true},
		"bigint(11)":                   {Base: "bigint", Size: 11},
		"bigint(8) zerofill":           {Base: "bigint", Size: 8, Zerofill: true},
		"int unsigned zerofill":        {Base: "int", Unsigned: true, Zerofill: true},
		"tinyint(1)":                   {Base: "tinyint", Size: 1},
		"tinyint(3) unsigned zerofill": {Base: "tinyint", Size: 3, Unsigned: true, Zerofill: true},
		"enum('a','b','c')":            {Base: "enum('a','b','c')"},
		"set('abc','def','ghi')":       {Base: "set('abc','def','ghi')"},
		"decimal(10,5)":                {Base: "decimal", Size: 10, Scale: 5},
		"varchar(20)":                  {Base: "varchar", Size: 20},
		"blob":                         {Base: "blob"},
		"timestamp(5)":                 {Base: "timestamp", Size: 5},
		"float":                        {Base: "float"},
		"double(10,0)":                 {Base: "double", Size: 10, Scale: 0},
		"vector(10)":                   {Base: "vector", Size: 10},

		// These are useless, but the server supports them and still shows (0) in the type!
		"varchar(0)":   {Base: "varchar"},
		"char(0)":      {Base: "char"},
		"varbinary(0)": {Base: "varbinary"},
		"binary(0)":    {Base: "binary"},
	}
	for input, expected := range cases {
		expected.str = input
		actual := ParseColumnType(input)
		if actual != expected {
			t.Errorf("ParseColumnType(%q) returned %+v, expected %+v", input, actual, expected)
		} else if recreateStr := actual.generatedString(); recreateStr != input {
			t.Errorf("Expected ParseColumnType(%q).generatedString() to return original input, instead got %q", input, recreateStr)
		}
	}

	// Confirm that a manually-constructed ColumnType panics upon call to String()
	ct := ColumnType{Base: "int", Unsigned: true}
	var didPanic bool
	defer func() {
		if recover() != nil {
			didPanic = true
		}
	}()
	_ = ct.String()
	if !didPanic {
		t.Errorf("Expected ColumnType.String() to panic on a non-ParseColumnType-created value, but it did not")
	}
}

func TestColumnTypeStripDisplayWidth(t *testing.T) {
	cases := map[string]string{
		"tinyint(1)":          "tinyint(1)",
		"tinyint(2)":          "tinyint",
		"tinyint(1) unsigned": "tinyint unsigned",
		"year(4)":             "year",
		"year":                "year",
		"int(11)":             "int",
		"int(11) zerofill":    "int(11) zerofill",
		"int(10) unsigned":    "int unsigned",
		"bigint(20)":          "bigint",
		"varchar(30)":         "varchar(30)",
		"char(99)":            "char(99)",
		"mediumtext":          "mediumtext",
		"decimal(10,5)":       "decimal(10,5)",
		"timestamp(5)":        "timestamp(5)",
		"double(10,0)":        "double(10,0)",
		"vector(10)":          "vector(10)",
		"varchar(0)":          "varchar(0)",
	}
	for input, expected := range cases {
		expectStripped := (input != expected)
		actual := ParseColumnType(input)
		if didStrip := actual.StripDisplayWidth(); actual.String() != expected || didStrip != expectStripped {
			t.Errorf("Expected StripDisplayWidth on %q to return %t with new string %q; instead found %t with new string %q", input, expectStripped, expected, didStrip, actual)
		}
	}
}
