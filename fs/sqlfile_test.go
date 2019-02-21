package fs

import (
	"strings"
	"testing"

	"github.com/skeema/tengo"
)

func TestSQLFileExists(t *testing.T) {
	sf := SQLFile{
		Dir:      "../testdata",
		FileName: "statements.sql",
	}
	ok, err := sf.Exists()
	if err != nil {
		t.Errorf("Unexpected error from Exists(): %s", err)
	}
	if !ok {
		t.Errorf("Expected Exists() to return true for %s, but it returned false", sf)
	}
	sf.FileName = "statements2.sql"
	ok, err = sf.Exists()
	if err != nil {
		t.Errorf("Unexpected error from Exists(): %s", err)
	}
	if ok {
		t.Errorf("Expected Exists() to return false for %s, but it returned true", sf)
	}
}

func TestSQLFileCreate(t *testing.T) {
	sf := SQLFile{
		Dir:      "../testdata",
		FileName: "statements.sql",
	}
	if err := sf.Create("# hello world"); err == nil {
		t.Error("Expected error from Create() on preexisting file, but err is nil")
	}
	sf.FileName = "statements2.sql"
	if err := sf.Create("# hello world"); err != nil {
		t.Errorf("Unexpected error from Create() on new file: %s", err)
	} else if err := sf.Delete(); err != nil {
		t.Errorf("Unexpected error from Delete(): %s", err)
	}
}

func TestSQLFileTokenize(t *testing.T) {
	sf := SQLFile{
		Dir:      "../testdata",
		FileName: "statements.sql",
	}
	tokenizedFile, err := sf.Tokenize()
	if err != nil {
		t.Fatalf("Unexpected error from Tokenize(): %s", err)
	}
	expected := expectedStatements(sf.String())
	if len(tokenizedFile.Statements) != len(expected) {
		t.Errorf("Expected %d statements, instead found %d", len(expected), len(tokenizedFile.Statements))
	} else {
		for n := range tokenizedFile.Statements {
			actual, expect := tokenizedFile.Statements[n], expected[n]
			if actual.File != expect.File {
				t.Errorf("statement[%d]: Expected file %s, instead found %s", n, expect.File, actual.File)
			}
			if actual.LineNo != expect.LineNo {
				t.Errorf("statement[%d]: Expected line %d, instead found %d", n, expect.LineNo, actual.LineNo)
			}
			if actual.CharNo != expect.CharNo {
				t.Errorf("statement[%d]: Expected char %d, instead found %d", n, expect.CharNo, actual.CharNo)
			}
			if actual.Text != expect.Text {
				t.Errorf("statement[%d]: Expected text %s, instead found %s", n, expect.Text, actual.Text)
			}
			if actual.DefaultDatabase != expect.DefaultDatabase {
				t.Errorf("statement[%d]: Expected default db %s, instead found %s", n, expect.DefaultDatabase, actual.DefaultDatabase)
			}
			if actual.Type != expect.Type {
				t.Errorf("statement[%d]: Expected statement type %d, instead found %d", n, expect.Type, actual.Type)
			}
			if actual.ObjectType != expect.ObjectType {
				t.Errorf("statement[%d]: Expected object type %s, instead found %s", n, expect.ObjectType, actual.ObjectType)
			}
			if actual.ObjectQualifier != expect.ObjectQualifier {
				t.Errorf("statement[%d]: Expected object qualifier %s, instead found %s", n, expect.ObjectQualifier, actual.ObjectQualifier)
			}
			if actual.ObjectName != expect.ObjectName {
				t.Errorf("statement[%d]: Expected object name %s, instead found %s", n, expect.ObjectName, actual.ObjectName)
			}
			if actual.FromFile != tokenizedFile {
				t.Errorf("statement[%d]: Expected FromFile %p, instead found %p", n, tokenizedFile, actual.FromFile)
			}
		}
	}

	// Test error returns for unterminated quote or unterminated C-style comment
	sf2 := SQLFile{
		Dir:      "../testdata",
		FileName: "statements2.sql",
	}
	origContents := ReadTestFile(t, sf.Path())
	contents := strings.Replace(origContents, "use /*wtf*/`analytics`", "use /*wtf*/`analytics", 1)
	WriteTestFile(t, sf2.Path(), contents)
	if _, err := sf2.Tokenize(); err == nil {
		t.Error("Expected to get an error about unterminated quote, but err was nil")
	}

	contents = strings.Replace(origContents, "use /*wtf*/`analytics`", "use /*wtf`analytics", 1)
	WriteTestFile(t, sf2.Path(), contents)
	if _, err := sf2.Tokenize(); err == nil {
		t.Error("Expected to get an error about unterminated comment, but err was nil")
	}

	// Test error return for nonexistent file
	sf2.Delete()
	if _, err := sf2.Tokenize(); err == nil {
		t.Error("Expected to get an error about nonexistent file, but err was nil")
	}
}

func TestTokenizedSQLFileRewrite(t *testing.T) {
	contents := ReadTestFile(t, "../testdata/statements.sql")
	sf2 := SQLFile{
		Dir:      "../testdata",
		FileName: "statements2.sql",
	}
	tokenizedFile := &TokenizedSQLFile{
		SQLFile:    sf2,
		Statements: expectedStatements(sf2.Path()),
	}
	bytesWritten, err := tokenizedFile.Rewrite()
	if err != nil {
		t.Fatalf("Unexpected error from Rewrite: %s", err)
	}
	contents2 := ReadTestFile(t, sf2.Path())
	if len(contents2) != bytesWritten {
		t.Errorf("Expected bytes written to be %d, instead found %d", len(contents2), bytesWritten)
	}
	if contents2 != contents {
		t.Error("File contents differ from expectation")
	}
	sf2.Delete()
}

// expectedStatements returns the expected contents of ../testdata/statements.sql
// in the form of a slice of statement pointers
func expectedStatements(filePath string) []*Statement {
	return []*Statement{
		{File: filePath, LineNo: 1, CharNo: 1, DefaultDatabase: "", Type: StatementTypeNoop, Text: "  -- this file exists for testing statement tokenization of *.sql files\n\n"},
		{File: filePath, LineNo: 3, CharNo: 1, DefaultDatabase: "", Type: StatementTypeUnknown, Text: "CREATE DATABASE /*!32312 IF NOT EXISTS*/ `product` /*!40100 DEFAULT CHARACTER SET latin1 */;\n"},
		{File: filePath, LineNo: 4, CharNo: 1, DefaultDatabase: "", Type: StatementTypeNoop, Text: "/* hello */   "},
		{File: filePath, LineNo: 4, CharNo: 15, DefaultDatabase: "", Type: StatementTypeCommand, Text: "USE product\n"},
		{File: filePath, LineNo: 5, CharNo: 1, DefaultDatabase: "product", Type: StatementTypeNoop, Text: "\n"},
		{File: filePath, LineNo: 6, CharNo: 1, DefaultDatabase: "product", Type: StatementTypeCreate, ObjectType: tengo.ObjectTypeTable, ObjectName: "users", Text: "CREATE #fun interruption\nTABLE `users` (\n  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,\n  `na``me` varchar(30) NOT NULL DEFAULT 'it\\'s complicated \"escapes''',\n  `credits` decimal(9,2) DEFAULT '10.00', -- end of line; \" comment\n  `last_modified` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, # another end-of-line comment;\n  PRIMARY KEY (`id`),\n  UNIQUE KEY `name` (`name`)\n) ENGINE=InnoDB DEFAULT CHARSET=latin1;\n"},
		{File: filePath, LineNo: 15, CharNo: 1, DefaultDatabase: "product", Type: StatementTypeNoop, Text: "          "},
		{File: filePath, LineNo: 15, CharNo: 11, DefaultDatabase: "product", Type: StatementTypeCreate, ObjectType: tengo.ObjectTypeTable, ObjectName: "posts with spaces", Text: "CREATE TABLE `posts with spaces` (\n  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,\n  `user_id` bigint(20) unsigned NOT NULL,\n  `body` varchar(50) DEFAULT '/* lol\\'',\n  `created_at` datetime /*!50601 DEFAULT CURRENT_TIMESTAMP*/,\n  `edited_at` datetime /*!50601 DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP*/,\n  PRIMARY KEY (`id`),\n  KEY `user_created` (`user_id`,`created_at`)\n) ENGINE=InnoDB DEFAULT CHARSET=latin1;\n"},
		{File: filePath, LineNo: 24, CharNo: 1, DefaultDatabase: "product", Type: StatementTypeNoop, Text: "\n\n\n\t"},
		{File: filePath, LineNo: 27, CharNo: 2, DefaultDatabase: "product", Type: StatementTypeCommand, Text: "delimiter    \"💩💩💩\"\n"},
		{File: filePath, LineNo: 28, CharNo: 1, DefaultDatabase: "product", Type: StatementTypeCreate, ObjectType: tengo.ObjectTypeTable, ObjectName: "uhoh", Text: "CREATE TABLE uhoh (ummm varchar(20) default 'ok 💩💩💩 cool')💩💩💩\n"},
		{File: filePath, LineNo: 29, CharNo: 1, DefaultDatabase: "product", Type: StatementTypeCommand, Text: "delimiter ;\n"},
		{File: filePath, LineNo: 30, CharNo: 1, DefaultDatabase: "product", Type: StatementTypeNoop, Text: "\n"},
		{File: filePath, LineNo: 31, CharNo: 1, DefaultDatabase: "product", Type: StatementTypeCommand, Text: "use /*wtf*/`analytics`;"},
		{File: filePath, LineNo: 31, CharNo: 24, DefaultDatabase: "analytics", Type: StatementTypeCreate, ObjectType: tengo.ObjectTypeTable, ObjectName: "comments", Text: "CREATE TABLE  if  NOT    eXiStS     `comments` (\n  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,\n  `post_id` bigint(20) unsigned NOT NULL,\n  `user_id` bigint(20) unsigned NOT NULL,\n  `created_at` datetime DEFAULT NULL,\n  `body` text,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=latin1;\n"},
		{File: filePath, LineNo: 39, CharNo: 1, DefaultDatabase: "analytics", Type: StatementTypeCreate, ObjectType: tengo.ObjectTypeTable, ObjectName: "subscriptions", Text: "CREATE TABLE subscriptions (id int unsigned not null primary key)"},
	}
}

func TestAppendToFile(t *testing.T) {
	assertAppend := func(filePath, contents string, expectBytes int, expectCreated bool) {
		t.Helper()
		bytesWritten, created, err := AppendToFile(filePath, contents)
		if err != nil {
			t.Errorf("Unexpected error from AppendToFile on %s: %s", filePath, err)
		}
		if bytesWritten != expectBytes {
			t.Errorf("Incorrect bytes-written from AppendToFile: expected %d, found %d", expectBytes, bytesWritten)
		}
		if created != expectCreated {
			t.Error("created did not match expectation")
		}
	}

	WriteTestFile(t, "../testdata/.scratch/fs/append-test1", "")
	assertAppend("../testdata/.scratch/fs/append-test1", "hello world", 11, false)
	assertAppend("../testdata/.scratch/fs/append-test2", "hello world", 11, true)
	assertAppend("../testdata/.scratch/fs/append-test2", "hello world", 23, false)
	if contents := ReadTestFile(t, "../testdata/.scratch/fs/append-test2"); contents != "hello world\nhello world" {
		t.Errorf("Unexpected contents: %s", contents)
	}
	RemoveTestFile(t, "../testdata/.scratch/fs/append-test1")
	RemoveTestFile(t, "../testdata/.scratch/fs/append-test2")
	RemoveTestFile(t, "../testdata/.scratch/fs")
}
