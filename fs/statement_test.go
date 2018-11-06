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
		stmt := &Statement{Text: input}
		actualBody, actualSuffix := stmt.SplitTextBody()
		if actualBody != expected[0] || actualSuffix != expected[1] {
			t.Errorf("SplitTextBody on %s: Expected %#v,%#v; found %#v,%#v", input, expected[0], expected[1], actualBody, actualSuffix)
		}
	}
}
