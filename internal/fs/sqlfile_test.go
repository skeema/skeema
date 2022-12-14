package fs

import (
	"fmt"
	"runtime"
	"strings"
	"testing"

	"github.com/skeema/skeema/internal/tengo"
)

func TestSQLFileExists(t *testing.T) {
	sf := SQLFile{FilePath: "testdata/statements.sql"}
	ok, err := sf.Exists()
	if err != nil {
		t.Errorf("Unexpected error from Exists(): %s", err)
	}
	if !ok {
		t.Errorf("Expected Exists() to return true for %s, but it returned false", sf.FilePath)
	}
	sf.FilePath = "testdata/statements2.sql"
	ok, err = sf.Exists()
	if err != nil {
		t.Errorf("Unexpected error from Exists(): %s", err)
	}
	if ok {
		t.Errorf("Expected Exists() to return false for %s, but it returned true", sf.FilePath)
	}
}

func TestSQLFileAddCreateStatement(t *testing.T) {
	sf := &SQLFile{}

	// Add a simple CREATE TABLE
	key := tengo.ObjectKey{Type: tengo.ObjectTypeTable, Name: "subscriptions"}
	create := "CREATE TABLE subscriptions (id int unsigned not null primary key)"
	sf.AddCreateStatement(key, create)
	if len(sf.Statements) != 1 || !sf.Dirty || sf.Statements[0].Text != create+";\n" {
		t.Fatalf("Unexpected values in SQLFile: dirty=%t, len(statements)=%d", sf.Dirty, len(sf.Statements))
	}

	// Add a proc that requires special delimiter
	key = tengo.ObjectKey{Type: tengo.ObjectTypeProc, Name: "whatever"}
	create = `CREATE PROCEDURE whatever(name varchar(10))
	BEGIN
		DECLARE v1 INT;
		SET v1=loops;
		WHILE v1 > 0 DO
			INSERT INTO users (name) values ('\xF0\x9D\x8C\x86');
			SET v1 = v1 - (2 / 2); /* testing // testing */
		END WHILE;
	END;`
	sf.AddCreateStatement(key, create)
	if len(sf.Statements) != 4 || !sf.Dirty || sf.Statements[2].Text != create+"//\n" {
		t.Fatalf("Unexpected values in SQLFile: dirty=%t, len(statements)=%d", sf.Dirty, len(sf.Statements))
	}

	// Add another proc that requires a special delimiter. This should effectively
	// move the previous trailing "DELIMITER ;" back to the end of the file.
	key.Name = "whatever2"
	create = strings.Replace(create, "whatever", "Whatever2", 1)
	sf.AddCreateStatement(key, create)
	if len(sf.Statements) != 5 || !sf.Dirty || sf.Statements[3].Text != create+"//\n" {
		t.Fatalf("Unexpected values in SQLFile: dirty=%t, len(statements)=%d", sf.Dirty, len(sf.Statements))
	}

	// Add a func that does not require a special delimiter. This should just add
	// the statement at the end of the file, leaving the previously-trailing
	// "DELIMITER ;" where it was
	key = tengo.ObjectKey{Type: tengo.ObjectTypeFunc, Name: "foo"}
	create = `CREATE FUNCTION foo() RETURNS varchar(30) RETURN "hello"`
	sf.AddCreateStatement(key, create)
	if len(sf.Statements) != 6 || !sf.Dirty || sf.Statements[5].Text != create+";\n" {
		t.Fatalf("Unexpected values in SQLFile: dirty=%t, len(statements)=%d", sf.Dirty, len(sf.Statements))
	}
}

func TestSQLFileEditStatementText(t *testing.T) {
	// Initial setup: two statements in one file, both with standard semicolon
	// delimiter
	create1 := `CREATE FUNCTION whatever() RETURNS varchar(30) RETURN "hello"`
	stmt1 := &Statement{
		File:       "whatever.sql",
		Text:       create1 + ";\n",
		Type:       StatementTypeCreate,
		ObjectType: tengo.ObjectTypeFunc,
		ObjectName: "whatever",
		Delimiter:  ";",
	}
	create2 := "CREATE TABLE subscriptions (id int unsigned not null primary key)"
	stmt2 := &Statement{File: "subscriptions.sql",
		Text:       create2 + ";\n",
		Type:       StatementTypeCreate,
		ObjectType: tengo.ObjectTypeTable,
		ObjectName: "subscriptions",
		Delimiter:  ";",
	}
	sf := &SQLFile{
		Statements: []*Statement{stmt1, stmt2},
	}

	// Adjust the second statement. This should not involve DELIMITER commands
	// in any way.
	sf.EditStatementText(stmt2, "CREATE TABLE subscriptions (subID int unsigned not null primary key)")
	if !sf.Dirty {
		t.Error("Expected file to be marked as dirty, but it was not")
	}
	if len(sf.Statements) != 2 {
		t.Fatalf("Wrong statement count in file: expected 2, found %d", len(sf.Statements))
	} else if sf.Statements[0] != stmt1 || sf.Statements[1] != stmt2 {
		t.Fatal("Unexpected CREATE statement positions in file")
	}

	// Adjust the first statement to require a special delimiter. File should
	// now have 4 statements incl the delimiter wrappers around the first
	// statement.
	sf.EditStatementText(stmt1, `CREATE FUNCTION whatever() RETURNS varchar(30)
	BEGIN
		RETURN "hello";
	END;`)
	if len(sf.Statements) != 4 {
		t.Fatalf("Wrong statement count in file: expected 4, found %d", len(sf.Statements))
	} else if sf.Statements[1] != stmt1 || sf.Statements[3] != stmt2 {
		t.Fatal("Unexpected CREATE statement positions in file")
	} else if sf.Statements[0].Type != StatementTypeCommand || sf.Statements[2].Type != StatementTypeCommand {
		t.Fatal("Unexpected DELIMITER statement positions in file")
	}

	// Adjust the second statement back to its original text. DELIMITERs should
	// remain in place since we do not currently clean them up!
	sf.EditStatementText(stmt1, create1)
	if len(sf.Statements) != 4 {
		t.Fatalf("Wrong statement count in file: expected 4, found %d", len(sf.Statements))
	} else if sf.Statements[1] != stmt1 || sf.Statements[3] != stmt2 {
		t.Fatal("Unexpected CREATE statement positions in file")
	} else if sf.Statements[0].Type != StatementTypeCommand || sf.Statements[2].Type != StatementTypeCommand {
		t.Fatal("Unexpected DELIMITER statement positions in file")
	}
}

func TestParseStatementsInFileSuccess(t *testing.T) {
	filePath := "testdata/statements.sql"
	statements, err := ParseStatementsInFile(filePath)
	if err != nil {
		t.Fatalf("Unexpected error from ParseStatementsInFile(): %v", err)
	}
	expected := expectedStatements(filePath)
	if len(statements) != len(expected) {
		t.Errorf("Expected %d statements, instead found %d", len(expected), len(statements))
	} else {
		for n := range statements {
			if expected[n].Error != nil && statements[n].Error != nil {
				expected[n].Error = statements[n].Error // for Error, only verify nil/non-nil
			}
			if *statements[n] != *expected[n] {
				t.Errorf("statement[%d] fields did not all match expected values.\nExpected:\n%+v\n\nActual:\n%+v", n, expected[n], statements[n])
			}
		}
	}

	// Test again, this time with CRLF line-ends in the .sql file
	contents := ReadTestFile(t, filePath)
	contents = strings.ReplaceAll(contents, "\n", "\r\n")
	filePath = "testdata/statements_crlf.sql"
	WriteTestFile(t, filePath, contents)
	defer RemoveTestFile(t, filePath)
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
	filePath := "testdata/statements.sql"
	origContents := ReadTestFile(t, filePath)

	// Test error returns for unterminated quote or unterminated C-style comment
	contents := strings.Replace(origContents, "use /*wtf*/`analytics`", "use /*wtf*/`analytics", 1)
	filePath = "testdata/statements2.sql"
	WriteTestFile(t, filePath, contents)
	if _, err := ParseStatementsInFile(filePath); err == nil {
		t.Error("Expected to get an error about unterminated quote, but err was nil")
	}

	contents = strings.Replace(origContents, "use /*wtf*/`analytics`", "use /*wtf`analytics", 1)
	WriteTestFile(t, filePath, contents)
	if _, err := ParseStatementsInFile(filePath); err == nil {
		t.Error("Expected to get an error about unterminated comment, but err was nil")
	}

	// Test error return for nonexistent file
	RemoveTestFile(t, filePath)
	if _, err := ParseStatementsInFile(filePath); err == nil {
		t.Error("Expected to get an error about nonexistent file, but err was nil")
	}

	// Test handling of files that just contain a single routine definition, but
	// without using the DELIMITER command
	filePath = "testdata/nodelimiter1.sql"
	if statements, err := ParseStatementsInFile(filePath); err != nil {
		t.Errorf("Unexpected error parsing nodelimiter1.sql: %s", err)
	} else if len(statements) != 2 {
		t.Errorf("Expected file to contain 2 statements, instead found %d", len(statements))
	} else if statements[0].Type != StatementTypeNoop || statements[1].Type != StatementTypeCreate {
		t.Error("Correct count of statements found, but incorrect types parsed")
	}

	// Now try parsing a file that contains a multi-line routine (but no DELIMITER
	// command) followed by another CREATE, and confirm the parsing is "incorrect"
	// in the expected way
	filePath = "testdata/nodelimiter2.sql"
	if statements, err := ParseStatementsInFile(filePath); err != nil {
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

func TestSQLFileWrite(t *testing.T) {
	// Use Write() to write file statements2.sql with same contents as statements.sql
	contents := ReadTestFile(t, "testdata/statements.sql")
	statements, err := ParseStatementsInFile("testdata/statements.sql")
	if err != nil {
		t.Fatalf("Unexpected error from ParseStatementsInFile: %v", err)
	}
	sqlFile := &SQLFile{
		FilePath:   "testdata/statements2.sql",
		Statements: statements,
	}
	bytesWritten, err := sqlFile.Write()
	if err != nil {
		t.Fatalf("Unexpected error from Write: %s", err)
	}
	contents2 := ReadTestFile(t, "testdata/statements2.sql")
	if len(contents2) != bytesWritten {
		t.Errorf("Expected bytes written to be %d, instead found %d", len(contents2), bytesWritten)
	}
	if contents2 != contents {
		t.Error("File contents differ from expectation")
	}

	// Remove everything except commands and whitespace/comments. Write should
	// now delete the file.
	for n := len(sqlFile.Statements) - 1; n >= 0; n-- {
		stmt := sqlFile.Statements[n]
		if stmt.Type != StatementTypeNoop && stmt.Type != StatementTypeCommand {
			sqlFile.RemoveStatement(stmt)
		}
	}
	bytesWritten, err = sqlFile.Write()
	if bytesWritten != 0 || err != nil {
		t.Errorf("Unexpected return values from Write: %d / %v", bytesWritten, err)
	}
	if exists, err := sqlFile.Exists(); exists || err != nil {
		t.Errorf("Unexpected return values from Exists: %t / %v", exists, err)
		sqlFile.Delete()
	}
}

// expectedStatements returns the expected contents of testdata/statements.sql
// in the form of a slice of statement pointers
func expectedStatements(filePath string) []*Statement {
	return []*Statement{
		{File: filePath, LineNo: 1, CharNo: 1, DefaultDatabase: "", Type: StatementTypeNoop, Text: "  -- this file exists for testing statement tokenization of *.sql files\n\n", Delimiter: ";"},
		{File: filePath, LineNo: 3, CharNo: 1, DefaultDatabase: "", Type: StatementTypeUnknown, Text: "CREATE DATABASE /*!32312 IF NOT EXISTS*/ `product` /*!40100 DEFAULT CHARACTER SET latin1 */;\n", Delimiter: ";"},
		{File: filePath, LineNo: 4, CharNo: 1, DefaultDatabase: "", Type: StatementTypeNoop, Text: "/* hello */   ", Delimiter: ";"},
		{File: filePath, LineNo: 4, CharNo: 15, DefaultDatabase: "", Type: StatementTypeCommand, Text: "USE product\n", Delimiter: ";"},
		{File: filePath, LineNo: 5, CharNo: 1, DefaultDatabase: "product", Type: StatementTypeNoop, Text: "\n", Delimiter: ";"},
		{File: filePath, LineNo: 6, CharNo: 1, DefaultDatabase: "product", Type: StatementTypeCreate, ObjectType: tengo.ObjectTypeTable, ObjectName: "users", Text: "CREATE #fun interruption\nTABLE `users` (\n  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,\n  `na``me` varchar(30) NOT NULL DEFAULT 'it\\'s complicated \"escapes''',--\tend of line comment with tab\n  `credits` decimal(9,2) DEFAULT '10.00', --\u3000end of line; \" comment with ideographic space\n  `last_modified` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, # another end-of-line comment;\n  PRIMARY KEY (`id`),\n  UNIQUE KEY `name` (`name`)\n) ENGINE=InnoDB DEFAULT CHARSET=latin1;\n", Delimiter: ";", nameClause: "`users`"},
		{File: filePath, LineNo: 15, CharNo: 1, DefaultDatabase: "product", Type: StatementTypeNoop, Text: "          ", Delimiter: ";"},
		{File: filePath, LineNo: 15, CharNo: 11, DefaultDatabase: "product", Type: StatementTypeCreate, ObjectType: tengo.ObjectTypeTable, ObjectName: "posts with spaces", Text: "CREATE TABLE `posts with spaces` (\n  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,\n  `user_id` bigint(20) unsigned NOT NULL,\n  `body` varchar(50) DEFAULT '/* lol\\'',\n  `created_at` datetime /*!50601 DEFAULT CURRENT_TIMESTAMP*/,\n  `edited_at` datetime /*!50601 DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP*/,\n  PRIMARY KEY (`id`),\n  KEY `user_created` (`user_id`,`created_at`)\n) ENGINE=InnoDB DEFAULT CHARSET=latin1;\n", Delimiter: ";", nameClause: "`posts with spaces`"},
		{File: filePath, LineNo: 24, CharNo: 1, DefaultDatabase: "product", Type: StatementTypeNoop, Text: "\n\n--\n", Delimiter: ";"},
		{File: filePath, LineNo: 27, CharNo: 1, DefaultDatabase: "product", Type: StatementTypeCreate, ObjectType: tengo.ObjectTypeFunc, ObjectName: "funcnodefiner", Text: "create function funcnodefiner() RETURNS varchar(30) RETURN \"hello\";\n", Delimiter: ";", nameClause: "funcnodefiner"},
		{File: filePath, LineNo: 28, CharNo: 1, DefaultDatabase: "product", Type: StatementTypeCreate, ObjectType: tengo.ObjectTypeFunc, ObjectName: "funccuruserparens", Text: "CREATE DEFINER = CURRENT_USER() FUNCTION funccuruserparens() RETURNS int RETURN 42;\n", Delimiter: ";", nameClause: "funccuruserparens"},
		{File: filePath, LineNo: 29, CharNo: 1, DefaultDatabase: "product", Type: StatementTypeCreate, ObjectType: tengo.ObjectTypeProc, ObjectName: "proccurusernoparens", Text: "CREATE DEFINER=CURRENT_USER PROCEDURE proccurusernoparens() # this is a comment!\n\tSELECT 1;\n", Delimiter: ";", nameClause: "proccurusernoparens"},
		{File: filePath, LineNo: 31, CharNo: 1, DefaultDatabase: "product", Type: StatementTypeCreate, ObjectType: tengo.ObjectTypeFunc, ObjectName: "funcdefquote2", ObjectQualifier: "analytics", Text: "create definer=foo@'localhost' /*lol*/ FUNCTION analytics.funcdefquote2() RETURNS int RETURN 42;\n", Delimiter: ";", nameClause: "analytics.funcdefquote2"},
		{File: filePath, LineNo: 32, CharNo: 1, DefaultDatabase: "product", Type: StatementTypeCreate, ObjectType: tengo.ObjectTypeProc, ObjectName: "procdefquote1", Text: "create DEFINER = 'foo'@localhost PROCEDURE `procdefquote1`() SELECT 42;\n", Delimiter: ";", nameClause: "`procdefquote1`"},
		{File: filePath, LineNo: 33, CharNo: 1, DefaultDatabase: "product", Type: StatementTypeNoop, Text: "\t", Delimiter: ";"},
		{File: filePath, LineNo: 33, CharNo: 2, DefaultDatabase: "product", Type: StatementTypeCommand, Text: "delimiter    \"💩💩💩\"\n", Delimiter: "\000"},
		{File: filePath, LineNo: 34, CharNo: 1, DefaultDatabase: "product", Type: StatementTypeCreate, ObjectType: tengo.ObjectTypeTable, ObjectName: "uhoh", Text: "CREATE TABLE uhoh (ummm varchar(20) default 'ok 💩💩💩 cool')💩💩💩\n", Delimiter: "💩💩💩", nameClause: "uhoh"},
		{File: filePath, LineNo: 35, CharNo: 1, DefaultDatabase: "product", Type: StatementTypeCommand, Text: "DELIMITER //\n", Delimiter: "\000"},
		{File: filePath, LineNo: 36, CharNo: 1, DefaultDatabase: "product", Type: StatementTypeCreate, ObjectType: tengo.ObjectTypeProc, ObjectName: "whatever", Text: "CREATE PROCEDURE whatever(name varchar(10))\nBEGIN\n\tDECLARE v1 INT; -- comment with \"normal space\" in front!\n\tSET v1=loops;--\u00A0comment with `nbsp' in front?!?\n\tWHILE v1 > 0 DO\n\t\tINSERT INTO users (name) values ('\\xF0\\x9D\\x8C\\x86');\n\t\tSET v1 = v1 - (2 / 2); /* testing // testing */\n\tEND WHILE;\nEND\n//\n", Delimiter: "//", nameClause: "whatever"},
		{File: filePath, LineNo: 46, CharNo: 1, DefaultDatabase: "product", Type: StatementTypeCommand, Text: "delimiter ;\n", Delimiter: "\000"},
		{File: filePath, LineNo: 47, CharNo: 1, DefaultDatabase: "product", Type: StatementTypeNoop, Text: "\n", Delimiter: ";"},
		{File: filePath, LineNo: 48, CharNo: 1, DefaultDatabase: "product", Type: StatementTypeCreate, ObjectType: tengo.ObjectTypeTable, ObjectName: "tbl1", ObjectQualifier: "uhoh", Text: "CREATE TABLE `uhoh` . tbl1 (id int unsigned not null primary key);\n", Delimiter: ";", nameClause: "`uhoh` . tbl1"},
		{File: filePath, LineNo: 49, CharNo: 1, DefaultDatabase: "product", Type: StatementTypeCreate, ObjectType: tengo.ObjectTypeTable, ObjectName: "tbl2", ObjectQualifier: "uhoh", Text: "CREATE TABLE uhoh.tbl2 (id int unsigned not null primary key);\n", Delimiter: ";", nameClause: "uhoh.tbl2"},
		{File: filePath, LineNo: 50, CharNo: 1, DefaultDatabase: "product", Type: StatementTypeCreate, ObjectType: tengo.ObjectTypeTable, ObjectName: "tbl3", ObjectQualifier: "uhoh", Text: "CREATE TABLE /*lol*/ uhoh  .  `tbl3` (id int unsigned not null primary key);\n", Delimiter: ";", nameClause: "uhoh  .  `tbl3`"},
		{File: filePath, LineNo: 51, CharNo: 1, DefaultDatabase: "product", Type: StatementTypeCreate, ObjectType: tengo.ObjectTypeFunc, ObjectName: "funcdefquote3", ObjectQualifier: "foo", Text: "create definer=foo@'localhost' /*lol*/ FUNCTION foo.funcdefquote3() RETURNS int RETURN 42;\n", Delimiter: ";", nameClause: "foo.funcdefquote3"},
		{File: filePath, LineNo: 52, CharNo: 1, DefaultDatabase: "product", Type: StatementTypeNoop, Text: "\n", Delimiter: ";"},
		{File: filePath, LineNo: 53, CharNo: 1, DefaultDatabase: "product", Type: StatementTypeCommand, Text: "use /*wtf*/`analytics`;", Delimiter: ";"},
		{File: filePath, LineNo: 53, CharNo: 24, DefaultDatabase: "analytics", Type: StatementTypeCreate, ObjectType: tengo.ObjectTypeTable, ObjectName: "comments", Text: "CREATE TABLE  if  NOT    eXiStS     `comments` (\n  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,\n  `post_id` bigint(20) unsigned NOT NULL,\n  `user_id` bigint(20) unsigned NOT NULL,\n  `created_at` datetime DEFAULT NULL,\n  `body` text,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=latin1;\n", Delimiter: ";", nameClause: "`comments`"},
		{File: filePath, LineNo: 61, CharNo: 1, DefaultDatabase: "analytics", Type: StatementTypeCreate, ObjectType: tengo.ObjectTypeTable, ObjectName: "subscriptions", Text: "CREATE TABLE subscriptions (id int unsigned not null primary key)", Delimiter: ";", nameClause: "subscriptions"},
	}
}

func TestPathForObject(t *testing.T) {
	cases := []struct {
		DirPath    string
		ObjectName string
		Expected   string
	}{
		{"", "foobar", "foobar.sql"},
		{"/foo/bar", "baz", "/foo/bar/baz.sql"},
		{"/var/schemas", "", "/var/schemas/symbols.sql"},
		{"/var/schemas", "[*]. ({`'\"})", "/var/schemas/symbols.sql"},
		{"/var/schemas", "foo_bar", "/var/schemas/foo_bar.sql"},
		{"/var/schemas", "foo-bar", "/var/schemas/foobar.sql"},
		{"/var/schemas", "../../etc/passwd", "/var/schemas/etcpasswd.sql"},
	}
	for _, c := range cases {
		if runtime.GOOS == "windows" {
			if c.DirPath != "" {
				c.DirPath = fmt.Sprintf("C:%s", strings.ReplaceAll(c.DirPath, "/", "\\"))
				c.Expected = fmt.Sprintf("C:%s", strings.ReplaceAll(c.Expected, "/", "\\"))
			}
		}
		if actual := PathForObject(c.DirPath, c.ObjectName); actual != c.Expected {
			t.Errorf("Expected PathForObject(%q, %q) to return %q, instead found %q", c.DirPath, c.ObjectName, c.Expected, actual)
		}
	}
}

func TestAddDelimiter(t *testing.T) {
	proc := `CREATE PROCEDURE whatever(name varchar(10))
BEGIN
	DECLARE v1 INT;
	SET v1=loops;
	WHILE v1 > 0 DO
		INSERT INTO users (name) values ('\xF0\x9D\x8C\x86');
		SET v1 = v1 - (2 / 2); /* testing // testing */
	END WHILE;
END;`
	result := AddDelimiter(proc)
	if result == proc || !strings.Contains(result, "DELIMITER") {
		t.Errorf("Unexpected result from AddDelimiter: %s", result)
	}

	proc = `CREATE FUNCTION foo() RETURNS varchar(30) RETURN "hello"`
	result = AddDelimiter(proc)
	if result == proc || strings.Contains(result, "DELIMITER") || !strings.HasSuffix(result, ";\n") {
		t.Errorf("Unexpected result from AddDelimiter: %s", result)
	}
}
