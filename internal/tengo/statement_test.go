package tengo

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
		"":                                    {"", ""},
		";\n":                                 {"", ";\n"},
		"CREATE TABLE foo (\n\tid int\n) ;\n": {"CREATE TABLE foo (\n\tid int\n)", " ;\n"},
		"INSERT INTO foo VALUES (';');":       {"INSERT INTO foo VALUES (';')", ";"},
		"USE some_db":                         {"USE some_db", ""},
		"USE some_db\n\n":                     {"USE some_db", "\n\n"},
	}
	for input, expected := range cases {
		stmt := &Statement{Text: input, Delimiter: ";"}
		actualBody, actualSuffix := stmt.SplitTextBody()
		if actualBody != expected[0] || actualSuffix != expected[1] {
			t.Errorf("SplitTextBody on %s: Expected %#v,%#v; found %#v,%#v", input, expected[0], expected[1], actualBody, actualSuffix)
		}
		if stmt.Body() != actualBody {
			t.Errorf("Body on %s returned different result than first returned value of SplitTextBody", input)
		}
	}
}

// TestStatementBody tests regex replacement of schema name qualifiers in Body().
func TestStatementBody(t *testing.T) {
	filePath := "testdata/statements.sql"
	// extracted from relevant lines of sqlfile_test.go's expectedStatements()
	statements := []*Statement{
		{File: filePath, LineNo: 31, CharNo: 1, DefaultDatabase: "product", Type: StatementTypeCreate, ObjectType: ObjectTypeFunc, ObjectName: "funcdefquote2", ObjectQualifier: "analytics", Text: "create definer=foo@'localhost' /*lol*/ FUNCTION analytics.funcdefquote2() RETURNS int RETURN 42;\n", Delimiter: ";", nameClause: "analytics.funcdefquote2"},
		{File: filePath, LineNo: 48, CharNo: 1, DefaultDatabase: "product", Type: StatementTypeCreate, ObjectType: ObjectTypeTable, ObjectName: "tbl1", ObjectQualifier: "uhoh", Text: "CREATE TABLE `uhoh` . tbl1 (id int unsigned not null primary key);\n", Delimiter: ";", nameClause: "`uhoh` . tbl1"},
		{File: filePath, LineNo: 49, CharNo: 1, DefaultDatabase: "product", Type: StatementTypeCreate, ObjectType: ObjectTypeTable, ObjectName: "tbl2", ObjectQualifier: "uhoh", Text: "CREATE TABLE uhoh.tbl2 (id int unsigned not null primary key);\n", Delimiter: ";", nameClause: "uhoh.tbl2"},
		{File: filePath, LineNo: 50, CharNo: 1, DefaultDatabase: "product", Type: StatementTypeCreate, ObjectType: ObjectTypeTable, ObjectName: "tbl3", ObjectQualifier: "uhoh", Text: "CREATE TABLE /*lol*/ uhoh  .  `tbl3`  \n  (id int unsigned not null primary key);\n", Delimiter: ";", nameClause: "uhoh  .  `tbl3`"},
		{File: filePath, LineNo: 51, CharNo: 1, DefaultDatabase: "product", Type: StatementTypeCreate, ObjectType: ObjectTypeFunc, ObjectName: "funcdefquote3", ObjectQualifier: "foo", Text: "create definer=foo@'localhost' /*lol*/ FUNCTION foo.funcdefquote3() RETURNS int RETURN 42;\n", Delimiter: ";", nameClause: "foo.funcdefquote3"},
	}
	allowedBodies := map[string]bool{
		"create definer=foo@'localhost' /*lol*/ FUNCTION `funcdefquote2`() RETURNS int RETURN 42": true,
		"CREATE TABLE `tbl1` (id int unsigned not null primary key)":                              true,
		"CREATE TABLE `tbl2` (id int unsigned not null primary key)":                              true,
		"CREATE TABLE /*lol*/ `tbl3`  \n  (id int unsigned not null primary key)":                 true,
		"create definer=foo@'localhost' /*lol*/ FUNCTION `funcdefquote3`() RETURNS int RETURN 42": true,
	}
	for n, stmt := range statements {
		body := stmt.Body()
		if !allowedBodies[body] {
			t.Errorf("Unexpected Body() result for statement[%d]: %q", n, body)
		}
	}
}

func TestStatementNormalizeTrailer(t *testing.T) {
	cases := []struct {
		text      string
		typ       StatementType
		delimiter string
		expected  string
	}{
		{"DELIMITER ;", StatementTypeCommand, "\000", "DELIMITER ;\n"},
		{"DELIMITER ;\n", StatementTypeCommand, "\000", "DELIMITER ;\n"},
		{"DELIMITER ;;", StatementTypeCommand, "\000", "DELIMITER ;;\n"},
		{"USE foo", StatementTypeCommand, ";", "USE foo;\n"},
		{"USE foo\n", StatementTypeCommand, ";", "USE foo;\n"},
		{"USE foo   \t\n", StatementTypeCommand, ";", "USE foo;   \t\n"},
		{"USE foo;", StatementTypeCommand, ";", "USE foo;\n"},
		{"USE foo;\n", StatementTypeCommand, ";", "USE foo;\n"},
		{"USE foo;  \r\n", StatementTypeCommand, ";", "USE foo;  \r\n"},
		{"# this is a comment\n#this too   ", StatementTypeNoop, ";", "# this is a comment\n#this too   \n"},
		{"   ", StatementTypeNoop, ";", "   \n"},
		{"LOAD DATA INFILE BIP BLOOP BLOOP", StatementTypeUnknown, ";", "LOAD DATA INFILE BIP BLOOP BLOOP\n"},
	}

	for n, tc := range cases {
		stmt := &Statement{
			Text:      tc.text,
			Type:      tc.typ,
			Delimiter: tc.delimiter,
		}
		stmt.NormalizeTrailer()
		if stmt.Text != tc.expected {
			t.Errorf("cases[%d]: Expected normalized text to be %q, instead found %q", n, tc.expected, stmt.Text)
		}
	}
}
