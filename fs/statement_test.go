package fs

import (
	"testing"
)

func TestStatementLocation(t *testing.T) {
	stmt := Statement{
		File:   "some/path/file.sql",
		LineNo: 123,
		CharNo: 45,
	}
	if stmt.Location() != "some/path/file.sql:123:45" {
		t.Errorf("Location() returned unexpected result: %s", stmt.Location())
	}

	// Test without known file
	stmt.File = ""
	if stmt.Location() != "unknown:123:45" {
		t.Errorf("Location() returned unexpected result: %s", stmt.Location())
	}

	// Test blank return if no location-related fields
	stmt = Statement{}
	if stmt.Location() != "" {
		t.Errorf("Location() returned unexpected result: %s", stmt.Location())
	}
}

func TestStatementSchema(t *testing.T) {
	statements := []*Statement{
		{DefaultDatabase: "", ObjectQualifier: ""},
		{DefaultDatabase: "foo", ObjectQualifier: ""},
		{DefaultDatabase: "", ObjectQualifier: "bar"},
		{DefaultDatabase: "foo", ObjectQualifier: "bar"},
	}
	expectSchema := []string{
		"",
		"foo",
		"bar",
		"bar",
	}
	for n, stmt := range statements {
		expect, actual := expectSchema[n], stmt.Schema()
		if expect != actual {
			t.Errorf("Unexpected Schema(): expected %s, found %s", expect, actual)
		}
	}
}

func TestStatementSplitTextBody(t *testing.T) {
	cases := map[string][]string{
		"":    {"", ""},
		";\n": {"", ";\n"},
		"CREATE TABLE foo (\n\tid int\n) ;\n": {"CREATE TABLE foo (\n\tid int\n)", " ;\n"},
		"INSERT INTO foo VALUES (';');":       {"INSERT INTO foo VALUES (';')", ";"},
		"USE some_db":                         {"USE some_db", ""},
		"USE some_db\n\n":                     {"USE some_db", "\n\n"},
	}
	for input, expected := range cases {
		stmt := &Statement{Text: input, delimiter: ";"}
		actualBody, actualSuffix := stmt.SplitTextBody()
		if actualBody != expected[0] || actualSuffix != expected[1] {
			t.Errorf("SplitTextBody on %s: Expected %#v,%#v; found %#v,%#v", input, expected[0], expected[1], actualBody, actualSuffix)
		}
		if stmt.Body() != actualBody {
			t.Errorf("Body on %s returned different result than first returned value of SplitTextBody", input)
		}
	}
}

func TestStripAnyQuote(t *testing.T) {
	cases := map[string]string{
		"":                "",
		"'":               "'",
		"''":              "",
		`"x"`:             "x",
		"'nope\"":         "'nope\"",
		"nope''nopen":     "nope''nopen",
		"'he''s here'":    "he's here",
		"'she\\'s here'":  "she's here",
		`"nope''s nope"`:  "nope''s nope",
		"`nope\\`s nope`": "nope\\`s nope",
	}
	for input, expected := range cases {
		if actual := stripAnyQuote(input); actual != expected {
			t.Errorf("stripAnyQuote on %s: Expected %s, found %s", input, expected, actual)
		}
	}
}

func TestCanParse(t *testing.T) {
	cases := map[string]bool{
		"CREATE TABLE foo (\n\t`id` int unsigned DEFAULT '0'\n) ;\n": true,
		"CREATE TABLE   IF  not EXISTS  foo (\n\tid int\n) ;\n":      true,
		"USE some_db\n\n":                                            true,
		"INSERT INTO foo VALUES (';')":                               false,
		"bork bork bork":                                             false,
		"# hello":                                                    false,
		"CREATE TEMPORARY TABLE foo (\n\tid int\n) ;\n":   false,
		"CREATE TABLE foo LIKE bar":                       false,
		"CREATE TABLE foo (like bar)":                     false,
		"CREATE TABLE foo2 select * from foo":             false,
		"CREATE TABLE foo2 (id int) AS select * from foo": false,
	}
	for input, expected := range cases {
		if actual := CanParse(input); actual != expected {
			t.Errorf("CanParse on %s: Expected %t, found %t", input, expected, actual)
		}
	}
}
