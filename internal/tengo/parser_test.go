package tengo

import (
	"bufio"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestParseStatementsInFileSuccess(t *testing.T) {
	filePath := "testdata/statements.sql"
	statements, err := ParseStatementsInFile(filePath)
	if err != nil {
		t.Fatalf("Unexpected error from ParseStatementsInFile(): %v", err)
	}
	expected := expectedStatements(filePath)
	if len(statements) != len(expected) {
		t.Errorf("Expected %d statements, instead found %d", len(expected), len(statements))
	}
	for n := range statements {
		if n >= len(expected) || n >= len(statements) {
			break
		}
		if *statements[n] != *expected[n] {
			t.Errorf("statement[%d] fields did not all match expected values.\nExpected:\n%+v\n\nActual:\n%+v", n, expected[n], statements[n])
		}
	}

	// Test again, this time with CRLF line-ends in the .sql file
	var contents string
	if byteContents, err := os.ReadFile(filePath); err != nil {
		t.Fatalf("Failed to read %s: %v", filePath, err)
	} else {
		contents = strings.ReplaceAll(string(byteContents), "\n", "\r\n")
	}
	filePath = filepath.Join(t.TempDir(), "statements_crlf.sql")
	if err := os.WriteFile(filePath, []byte(contents), 0777); err != nil {
		t.Fatalf("Unable to write %s: %v", filePath, err)
	}
	statements, err = ParseStatementsInFile(filePath)
	if err != nil {
		t.Fatalf("Unexpected error from ParseStatementsInFile(): %v", err)
	}
	if len(statements) != len(expected) {
		t.Errorf("Expected %d statements, instead found %d", len(expected), len(statements))
	} else {
		for n := range statements {
			expect := expected[n]
			expect.File = filePath
			expect.Text = strings.ReplaceAll(expect.Text, "\n", "\r\n")
			expect.nameClause = strings.ReplaceAll(expect.nameClause, "\n", "\r\n")
			if *statements[n] != *expect {
				t.Errorf("statement[%d] fields did not all match expected values.\nExpected:\n%+v\n\nActual:\n%+v", n, expect, statements[n])
			}
		}
	}
}

func TestParseStatementsInFileFail(t *testing.T) {
	var origContents string
	if byteContents, err := os.ReadFile("testdata/statements.sql"); err != nil {
		t.Fatalf("Failed to read testdata/statements.sql: %v", err)
	} else {
		origContents = string(byteContents)
	}
	tempDir := t.TempDir()

	// Test error returns for unterminated quote or unterminated C-style comment
	contents := strings.Replace(origContents, "use /*wtf*/`analytics`", "use /*wtf*/`analytics", 1)
	filePath := filepath.Join(tempDir, "openbacktick.sql")
	if err := os.WriteFile(filePath, []byte(contents), 0777); err != nil {
		t.Fatalf("Unable to write %s: %v", filePath, err)
	}
	if _, err := ParseStatementsInFile(filePath); err == nil {
		t.Error("Expected to get an error about unterminated quote, but err was nil")
	} else if msg := err.Error(); !strings.Contains(msg, "openbacktick.sql") {
		t.Errorf("Expected error message to include file path, but it did not: %s", msg)
	} else if mse, ok := err.(*MalformedSQLError); !ok {
		t.Errorf("Expected error to be a *MalformedSQLError, instead type is %T", err)
	} else if mse.lineNumber != 59 || mse.colNumber != 19 {
		t.Errorf("Unexpected line/col numbers in error: expected line 59, column 19; instead found line %d, column %d", mse.lineNumber, mse.colNumber)
	}

	contents = strings.Replace(origContents, "use /*wtf*/`analytics`", "use /*wtf`analytics", 1)
	filePath = filepath.Join(tempDir, "opencomment.sql")
	if err := os.WriteFile(filePath, []byte(contents), 0777); err != nil {
		t.Fatalf("Unable to write %s: %v", filePath, err)
	}
	if _, err := ParseStatementsInFile(filePath); err == nil {
		t.Error("Expected to get an error about unterminated comment, but err was nil")
	}

	// Test error return for nonexistent file
	filePath = filepath.Join(tempDir, "not-here.sql")
	if _, err := ParseStatementsInFile(filePath); err == nil {
		t.Error("Expected to get an error about nonexistent file, but err was nil")
	}

	// Test handling of files that just contain a single routine definition, but
	// without using the DELIMITER command
	if statements, err := ParseStatementsInFile("testdata/nodelimiter1.sql"); err != nil {
		t.Errorf("Unexpected error parsing nodelimiter1.sql: %s", err)
	} else if len(statements) != 2 {
		t.Errorf("Expected file to contain 2 statements, instead found %d", len(statements))
	} else if statements[0].Type != StatementTypeNoop || statements[1].Type != StatementTypeCreate {
		t.Error("Correct count of statements found, but incorrect types parsed")
	}

	// Repeat previous test, but this time using a reader which doesn't support
	// seeking
	f, err := os.Open("testdata/nodelimiter1.sql")
	if err != nil {
		t.Fatalf("Unexpected error from os.Open: %v", err)
	}
	r := bufio.NewReader(f)
	if statements, err := ParseStatements(r, filePath); err != nil {
		t.Errorf("Unexpected error parsing nodelimiter1.sql: %s", err)
	} else if len(statements) != 2 {
		t.Errorf("Expected file to contain 2 statements, instead found %d", len(statements))
	} else if statements[0].Type != StatementTypeNoop || statements[1].Type != StatementTypeCreate {
		t.Error("Correct count of statements found, but incorrect types parsed")
	}

	// Now try parsing a file that contains a multi-line routine (but no DELIMITER
	// command) followed by another CREATE, and confirm the parsing is "incorrect"
	// in the expected way
	if statements, err := ParseStatementsInFile("testdata/nodelimiter2.sql"); err != nil {
		t.Errorf("Unexpected error parsing nodelimiter2.sql: %s", err)
	} else {
		if len(statements) != 8 {
			t.Errorf("Expected file to contain 8 statements, instead found %d", len(statements))
		}
		var seenUnknown bool
		for _, stmt := range statements {
			if stmt.Type == StatementTypeUnknown {
				seenUnknown = true
			}
		}
		if !seenUnknown {
			t.Error("Expected to find a statement that could not be parsed, but did not")
		}
	}
}

func TestParseStatementsInFileBadCreate(t *testing.T) {
	statements, err := ParseStatementsInFile("testdata/statements-badcreate.sql")
	if err != nil {
		t.Fatalf("Unexpected error from ParseStatementsInFile(): %v", err)
	}

	// All statements should come out as StatementTypeUnknown
	for _, stmt := range statements {
		if stmt.Type != StatementTypeUnknown || stmt.ObjectName != "" {
			t.Errorf("Unexpected field values in statement at %s: %+v", stmt.Location(), stmt)
		}
	}
}

func TestParseStatementsInFileWithBOM(t *testing.T) {
	statements, err := ParseStatementsInFile("testdata/statements-utf8bom.sql")
	if err != nil {
		t.Fatalf("Unexpected error from ParseStatementsInFile(): %v", err)
	}

	// We should find 2 StatementTypeNoop (one with just the BOM, and then one with
	// a comment), followed by 2 StatementTypeCreate for the two CREATE TABLEs.
	// The BOM's noop statement should have a special CharNo of 0, which normally
	// is not used; the subsequent statement should start at CharNo of 1 as usual.
	if len(statements) != 4 {
		t.Fatalf("Unexpected statement count in testdata/statements-utf8bom.sql: expected 4, found %d", len(statements))
	}
	if stmt := statements[0]; stmt.Type != StatementTypeNoop || stmt.Text != "\uFEFF" || stmt.LineNo != 1 || stmt.CharNo != 0 {
		t.Errorf("Unexpected field values in statements[0]: %+v", stmt)
	}
	if stmt := statements[1]; stmt.Type != StatementTypeNoop || !strings.HasPrefix(stmt.Text, "-- ") || stmt.LineNo != 1 || stmt.CharNo != 1 {
		t.Errorf("Unexpected field values in statements[1]: %+v", stmt)
	}
	if stmt := statements[2]; stmt.Type != StatementTypeCreate || stmt.ObjectType != ObjectTypeTable || stmt.ObjectName != "one" || stmt.LineNo != 2 || stmt.CharNo != 1 {
		t.Errorf("Unexpected field values in statements[2]: %+v", stmt)
	}
	if stmt := statements[3]; stmt.Type != StatementTypeCreate || stmt.ObjectType != ObjectTypeTable || stmt.ObjectName != "two" || stmt.LineNo != 7 || stmt.CharNo != 1 {
		t.Errorf("Unexpected field values in statements[3]: %+v", stmt)
	}
}

func TestParseStatementsInString(t *testing.T) {
	if stmts, err := ParseStatementsInString("/* hello */\nCREATE TABLE foo (id int);\n"); err != nil || len(stmts) != 2 {
		t.Errorf("Unexpected return from ParseStatementsInString: %+v, %v", stmts, err)
	}
	if stmts, err := ParseStatementsInString("LOAD XML LOCAL INFILE 'unexpected-eof"); err == nil || len(stmts) != 1 || stmts[0].Type != StatementTypeUnknown {
		t.Errorf("Unexpected return from ParseStatementsInString: %+v, %v", stmts, err)
	}
	if stmts, err := ParseStatementsInString(""); err != nil || len(stmts) != 0 {
		t.Errorf("Unexpected return from ParseStatementsInString: %+v, %v", stmts, err)
	}
}

func TestParseStatementInString(t *testing.T) {
	cases := map[string]ObjectKey{
		"":      {},
		"x y z": {},
		"/* hello */\nCREATE TABLE foo (id int);\n":                {},
		"CREATE TABLE foo (id int);\n":                             {Type: ObjectTypeTable, Name: "foo"},
		"CREATE TABLE foo (id int);\nCREATE TABLE bar (id int);\n": {Type: ObjectTypeTable, Name: "foo"},
	}
	for input, expected := range cases {
		if actual := ParseStatementInString(input).ObjectKey(); actual != expected {
			t.Errorf("For input %q, expected resulting statement to have ObjectKey %s, instead found %s", input, expected, actual)
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

// expectedStatements returns the expected contents of testdata/statements.sql
// in the form of a slice of statement pointers
func expectedStatements(filePath string) []*Statement {
	return []*Statement{
		{File: filePath, LineNo: 1, CharNo: 1, DefaultDatabase: "", Type: StatementTypeNoop, Text: "  -- this file exists for testing statement tokenization of *.sql files\n\n", Delimiter: ";"},
		{File: filePath, LineNo: 3, CharNo: 1, DefaultDatabase: "", Type: StatementTypeUnknown, Text: "CREATE DATABASE /*!32312 IF NOT EXISTS*/ `product` /*!40100 DEFAULT CHARACTER SET latin1 */;\n", Delimiter: ";"},
		{File: filePath, LineNo: 4, CharNo: 1, DefaultDatabase: "", Type: StatementTypeNoop, Text: "/* hello */   ", Delimiter: ";"},
		{File: filePath, LineNo: 4, CharNo: 15, DefaultDatabase: "", Type: StatementTypeCommand, Text: "USE product # this is a comment\n", Delimiter: ";"},
		{File: filePath, LineNo: 5, CharNo: 1, DefaultDatabase: "product", Type: StatementTypeNoop, Text: "\n", Delimiter: ";"},
		{File: filePath, LineNo: 6, CharNo: 1, DefaultDatabase: "product", Type: StatementTypeCreate, ObjectType: ObjectTypeTable, ObjectName: "users", Text: "CREATE #fun interruption\nTABLE `users` (\n  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,\n  `na``me` varchar(30) NOT NULL DEFAULT 'it\\'s complicated \"escapes''',--\tend of line comment with tab\n  `credits` decimal(9,2) DEFAULT '10.00', --\u3000end of line; \" comment with ideographic space\n  `last_modified` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, # another end-of-line comment;\n  PRIMARY KEY (`id`),\n  UNIQUE KEY `name` (`name`)\n) ENGINE=InnoDB DEFAULT CHARSET=latin1;\n", Delimiter: ";", nameClause: "`users`"},
		{File: filePath, LineNo: 15, CharNo: 1, DefaultDatabase: "product", Type: StatementTypeNoop, Text: "          ", Delimiter: ";"},
		{File: filePath, LineNo: 15, CharNo: 11, DefaultDatabase: "product", Type: StatementTypeCreate, ObjectType: ObjectTypeTable, ObjectName: "posts with spaces", Text: "CREATE TABLE `posts with spaces` (\n  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,\n  `user_id` bigint(20) unsigned NOT NULL,\n  `body` varchar(50) DEFAULT '/* lol\\'',\n  `created_at` datetime /*!50601 DEFAULT CURRENT_TIMESTAMP*/,\n  `edited_at` datetime /*!50601 DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP*/,\n  PRIMARY KEY (`id`),\n  KEY `user_created` (`user_id`,`created_at`)\n) ENGINE=InnoDB DEFAULT CHARSET=latin1;\n", Delimiter: ";", nameClause: "`posts with spaces`"},
		{File: filePath, LineNo: 24, CharNo: 1, DefaultDatabase: "product", Type: StatementTypeNoop, Text: "\n\n--\n", Delimiter: ";"},
		{File: filePath, LineNo: 27, CharNo: 1, DefaultDatabase: "product", Type: StatementTypeCreate, ObjectType: ObjectTypeFunc, ObjectName: "funcnodefiner", Text: "create function funcnodefiner() RETURNS varchar(30) RETURN \"hello\";\n", Delimiter: ";", nameClause: "funcnodefiner"},
		{File: filePath, LineNo: 28, CharNo: 1, DefaultDatabase: "product", Type: StatementTypeCreate, ObjectType: ObjectTypeFunc, ObjectName: "funccuruserparens", Text: "CREATE DEFINER = CURRENT_USER() FUNCTION funccuruserparens() RETURNS int RETURN 42;\n", Delimiter: ";", nameClause: "funccuruserparens"},
		{File: filePath, LineNo: 29, CharNo: 1, DefaultDatabase: "product", Type: StatementTypeCreate, ObjectType: ObjectTypeProc, ObjectName: "proccurusernoparens", Text: "CREATE DEFINER=CURRENT_USER PROCEDURE proccurusernoparens() # this is a comment!\n\tSELECT 1;\n", Delimiter: ";", nameClause: "proccurusernoparens"},
		{File: filePath, LineNo: 31, CharNo: 1, DefaultDatabase: "product", Type: StatementTypeCreate, ObjectType: ObjectTypeFunc, ObjectName: "funcdefquote2", ObjectQualifier: "analytics", Text: "create definer=foo@'localhost' /*lol*/ FUNCTION analytics.funcdefquote2() RETURNS int RETURN 42;\n", Delimiter: ";", nameClause: "analytics.funcdefquote2"},
		{File: filePath, LineNo: 32, CharNo: 1, DefaultDatabase: "product", Type: StatementTypeCreate, ObjectType: ObjectTypeProc, ObjectName: "procdefquote1", Text: "create DEFINER = 'foo'@localhost PROCEDURE `procdefquote1`() SELECT 42;\n", Delimiter: ";", nameClause: "`procdefquote1`"},
		{File: filePath, LineNo: 33, CharNo: 1, DefaultDatabase: "product", Type: StatementTypeNoop, Text: "\t", Delimiter: ";"},
		{File: filePath, LineNo: 33, CharNo: 2, DefaultDatabase: "product", Type: StatementTypeCommand, Text: "delimiter    \"💩💩💩\"\n", Delimiter: "\000"},
		{File: filePath, LineNo: 34, CharNo: 1, DefaultDatabase: "product", Type: StatementTypeCreate, ObjectType: ObjectTypeTable, ObjectName: "uhoh", Text: "CREATE TABLE uhoh (ummm varchar(20) default 'ok 💩💩💩 cool')💩💩💩\n", Delimiter: "💩💩💩", nameClause: "uhoh"},
		{File: filePath, LineNo: 35, CharNo: 1, DefaultDatabase: "product", Type: StatementTypeCommand, Text: "DELIMITER $$ -- cool\n", Delimiter: "\000"},
		{File: filePath, LineNo: 36, CharNo: 1, DefaultDatabase: "product", Type: StatementTypeCreate, ObjectType: ObjectTypeProc, ObjectName: "whatever", Text: "CREATE PROCEDURE whatever(name varchar(10))\nBEGIN\n\tDECLARE v1 INT; -- comment with \"normal space\" in front!\n\tSET v1=loops;--\u00A0comment with `nbsp' in front?!?\n\tWHILE v1 > 0 DO\n\t\tINSERT INTO users (name) values ('\\xF0\\x9D\\x8C\\x86');\n\t\tSET v1 = v1 - (2 / 2); /* testing // testing */\n\tEND WHILE;\nEND$$\n", Delimiter: "$$", nameClause: "whatever"},
		{File: filePath, LineNo: 45, CharNo: 1, DefaultDatabase: "product", Type: StatementTypeCommand, Text: "delimiter ;\n", Delimiter: "\000"},
		{File: filePath, LineNo: 46, CharNo: 1, DefaultDatabase: "product", Type: StatementTypeNoop, Text: "\n\n", Delimiter: ";"},
		{File: filePath, LineNo: 48, CharNo: 1, DefaultDatabase: "product", Type: StatementTypeCreate, ObjectType: ObjectTypeTable, ObjectName: "tbl1", ObjectQualifier: "uhoh", Text: "CREATE TABLE `uhoh` . tbl1 (id int unsigned not null primary key);\n", Delimiter: ";", nameClause: "`uhoh` . tbl1"},
		{File: filePath, LineNo: 49, CharNo: 1, DefaultDatabase: "product", Type: StatementTypeCreate, ObjectType: ObjectTypeTable, ObjectName: "tbl2", ObjectQualifier: "uhoh", Text: "CREATE TABLE uhoh.tbl2 (id int unsigned not null primary key);\n", Delimiter: ";", nameClause: "uhoh.tbl2"},
		{File: filePath, LineNo: 50, CharNo: 1, DefaultDatabase: "product", Type: StatementTypeCreate, ObjectType: ObjectTypeTable, ObjectName: "tbl3", ObjectQualifier: "uhoh", Text: "CREATE TABLE /*lol*/ uhoh  .  `tbl3` (id int unsigned not null primary key);\n", Delimiter: ";", nameClause: "uhoh  .  `tbl3`"},
		{File: filePath, LineNo: 51, CharNo: 1, DefaultDatabase: "product", Type: StatementTypeCreate, ObjectType: ObjectTypeFunc, ObjectName: "funcdefquote3", ObjectQualifier: "foo", Text: "create definer=foo@'localhost' /*lol*/ FUNCTION foo.funcdefquote3() RETURNS int RETURN 42;\n", Delimiter: ";", nameClause: "foo.funcdefquote3"},
		{File: filePath, LineNo: 52, CharNo: 1, DefaultDatabase: "product", Type: StatementTypeNoop, Text: "\n", Delimiter: ";"},
		{File: filePath, LineNo: 53, CharNo: 1, DefaultDatabase: "product", Type: StatementTypeCommand, Text: "use /*wtf*/`analytics`;", Delimiter: ";"},
		{File: filePath, LineNo: 53, CharNo: 24, DefaultDatabase: "analytics", Type: StatementTypeCreate, ObjectType: ObjectTypeTable, ObjectName: "comments", Text: "CREATE TABLE  if  NOT    eXiStS     `comments` (\n  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,\n  `post_id` bigint(20) unsigned NOT NULL,\n  `user_id` bigint(20) unsigned NOT NULL,\n  `created_at` datetime DEFAULT NULL,\n  `body` text,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=latin1;\n", Delimiter: ";", nameClause: "`comments`"},
		{File: filePath, LineNo: 61, CharNo: 1, DefaultDatabase: "analytics", Type: StatementTypeCreate, ObjectType: ObjectTypeTable, ObjectName: "subscriptions", Text: "CREATE TABLE subscriptions (id int unsigned not null primary key)", Delimiter: ";", nameClause: "subscriptions"},
	}
}

func BenchmarkParseStatementsInFile(b *testing.B) {
	b.ReportAllocs()
	for n := 0; n < b.N; n++ {
		_, err := ParseStatementsInFile("testdata/statements.sql")
		if err != nil {
			panic(err)
		}
	}
}
