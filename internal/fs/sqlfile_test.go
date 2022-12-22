package fs

import (
	"fmt"
	"runtime"
	"strings"
	"testing"

	"github.com/skeema/skeema/internal/tengo"
)

func TestSQLFileExists(t *testing.T) {
	sf := SQLFile{FilePath: "testdata/host/db/posts.sql"}
	ok, err := sf.Exists()
	if err != nil {
		t.Errorf("Unexpected error from Exists(): %s", err)
	}
	if !ok {
		t.Errorf("Expected Exists() to return true for %s, but it returned false", sf.FilePath)
	}
	sf.FilePath = "testdata/host/db/doesnotexist.sql"
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
	stmt1 := &tengo.Statement{
		File:       "whatever.sql",
		Text:       create1 + ";\n",
		Type:       tengo.StatementTypeCreate,
		ObjectType: tengo.ObjectTypeFunc,
		ObjectName: "whatever",
		Delimiter:  ";",
	}
	create2 := "CREATE TABLE subscriptions (id int unsigned not null primary key)"
	stmt2 := &tengo.Statement{File: "subscriptions.sql",
		Text:       create2 + ";\n",
		Type:       tengo.StatementTypeCreate,
		ObjectType: tengo.ObjectTypeTable,
		ObjectName: "subscriptions",
		Delimiter:  ";",
	}
	sf := &SQLFile{
		Statements: []*tengo.Statement{stmt1, stmt2},
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
	} else if sf.Statements[0].Type != tengo.StatementTypeCommand || sf.Statements[2].Type != tengo.StatementTypeCommand {
		t.Fatal("Unexpected DELIMITER statement positions in file")
	}

	// Adjust the second statement back to its original text. DELIMITERs should
	// remain in place since we do not currently clean them up!
	sf.EditStatementText(stmt1, create1)
	if len(sf.Statements) != 4 {
		t.Fatalf("Wrong statement count in file: expected 4, found %d", len(sf.Statements))
	} else if sf.Statements[1] != stmt1 || sf.Statements[3] != stmt2 {
		t.Fatal("Unexpected CREATE statement positions in file")
	} else if sf.Statements[0].Type != tengo.StatementTypeCommand || sf.Statements[2].Type != tengo.StatementTypeCommand {
		t.Fatal("Unexpected DELIMITER statement positions in file")
	}
}

func TestSQLFileWrite(t *testing.T) {
	// Use Write() to write file statements2.sql with same contents as statements.sql
	contents := ReadTestFile(t, "../tengo/testdata/statements.sql")
	statements, err := tengo.ParseStatementsInFile("../tengo/testdata/statements.sql")
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
		if stmt.Type != tengo.StatementTypeNoop && stmt.Type != tengo.StatementTypeCommand {
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
