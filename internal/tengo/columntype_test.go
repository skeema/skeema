package tengo

import (
	"slices"
	"strings"
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
		"enum('a','b','c')":            {Base: "enum", values: "'a','b','c'"},
		"set('abc','def','gh''s')":     {Base: "set", values: "'abc','def','gh''s'"},
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

func TestColumnTypeIntegerRange(t *testing.T) {
	cases := []struct {
		input     string
		expectMin int64
		expectMax uint64
		expectOK  bool
	}{
		{"tinyint(1)", -128, 127, true},
		{"tinyint unsigned", 0, 255, true},
		{"smallint", -32768, 32767, true},
		{"smallint unsigned", 0, 65535, true},
		{"mediumint", -8388608, 8388607, true},
		{"mediumint unsigned", 0, 16777215, true},
		{"int", -2147483648, 2147483647, true},
		{"int(4) unsigned", 0, 4294967295, true},
		{"bigint(8) zerofill", -9223372036854775808, 9223372036854775807, true},
		{"bigint unsigned", 0, 18446744073709551615, true},
		{"binary(3)", 0, 0, false},
		{"enum('hello','world')", 0, 0, false},
		{"float", 0, 0, false},
		{"timestamp(5)", 0, 0, false},
	}
	for _, tc := range cases {
		actualMin, actualMax, actualOK := ParseColumnType(tc.input).IntegerRange()
		if actualMin != tc.expectMin || actualMax != tc.expectMax || actualOK != tc.expectOK {
			t.Errorf("Unexpected return from ParseColumnType(%q).IntegerRange(): found %d, %d, %t", tc.input, actualMin, actualMax, actualOK)
		}
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

func TestColumnTypeValues(t *testing.T) {
	cases := map[string][]string{
		"enum('a','b','c')":        {"a", "b", "c"},
		"enum('x')":                {"x"},
		"set('abc','def','gh''s')": {"abc", "def", "gh's"},
		"bigint(11)":               nil,
		"year":                     nil,
	}
	for input, expected := range cases {
		parsed := ParseColumnType(input)
		actual := parsed.Values()
		if !slices.Equal(actual, expected) {
			t.Errorf("ParseColumnType(%q).Values() returned %v, expected %v", input, actual, expected)
		} else if expected != nil {
			// Confirm we can round-trip the original quoted/escaped value
			var escapedValues []string
			for _, v := range actual {
				escapedValues = append(escapedValues, "'"+EscapeValueForCreateTable(v)+"'")
			}
			roundTrip := parsed.Base + "(" + strings.Join(escapedValues, ",") + ")"
			if roundTrip != input {
				t.Errorf("Unable to round-trip regenerate input of %q: got back %q", input, roundTrip)
			}
		}
	}
}
