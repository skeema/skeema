package fs

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/skeema/mybase"
	"github.com/skeema/skeema/internal/tengo"
	"github.com/skeema/skeema/internal/util"
)

func TestParseDir(t *testing.T) {
	dir := getDir(t, "testdata/host/db")
	if dir.Config.Get("default-collation") != "latin1_swedish_ci" {
		t.Errorf("dir.Config not working as expected; default-collation is %s", dir.Config.Get("default-collation"))
	}
	if dir.Config.Get("host") != "127.0.0.1" {
		t.Errorf("dir.Config not working as expected; host is %s", dir.Config.Get("host"))
	}
	if len(dir.UnparsedStatements) > 0 {
		t.Errorf("Expected 0 UnparsedStatements, instead found %d", len(dir.UnparsedStatements))
	}
	if len(dir.LogicalSchemas) != 1 {
		t.Fatalf("Expected 1 LogicalSchema; instead found %d", len(dir.LogicalSchemas))
	}
	if expectRepoBase, _ := filepath.Abs("../.."); expectRepoBase != dir.repoBase {
		// expected repo base is ../.. due to presence of .git there
		t.Errorf("dir repoBase %q does not match expectation %q", dir.repoBase, expectRepoBase)
	}
	logicalSchema := dir.LogicalSchemas[0]
	if logicalSchema.CharSet != "latin1" || logicalSchema.Collation != "latin1_swedish_ci" {
		t.Error("LogicalSchema not correctly populated with charset/collation from .skeema file")
	}
	expectTableNames := []string{"comments", "posts", "subscriptions", "users"}
	if len(logicalSchema.Creates) != len(expectTableNames) {
		t.Errorf("Unexpected object count: found %d, expected %d", len(logicalSchema.Creates), len(expectTableNames))
	} else {
		for _, name := range expectTableNames {
			key := tengo.ObjectKey{Type: tengo.ObjectTypeTable, Name: name}
			if logicalSchema.Creates[key] == nil {
				t.Errorf("Did not find Create for table %s in LogicalSchema", name)
			}
		}
	}
	if dir.retainMapKeyCasing {
		t.Error("dir retainMapKeyCasing is unexpectedly true")
	}

	// Confirm that parsing ~ should cause it to be its own repoBase, since we
	// do not search beyond HOME for .skeema files or .git dirs
	home, _ := os.UserHomeDir()
	dir = getDir(t, home)
	if dir.repoBase != home {
		t.Errorf("Unexpected repoBase for $HOME: expected %s, found %s", home, dir.repoBase)
	}
}

func TestParseDirErrors(t *testing.T) {
	// Confirm error cases: nonexistent dir; non-dir file; dir with *.sql files
	// creating same table multiple times
	for _, dirPath := range []string{"../../bestdata", "../../testdata/setup.sql", "../../testdata"} {
		dir, err := ParseDir(dirPath, getValidConfig(t))
		if err == nil || (dir != nil && dir.ParseError == nil) {
			t.Errorf("Expected ParseDir to return nil dir and non-nil error, but dir=%v err=%v", dir, err)
		}
	}

	// Undefined options should cause an error
	cmd := mybase.NewCommand("fstest", "", "", nil)
	cmd.AddArg("environment", "production", false)
	cfg := mybase.ParseFakeCLI(t, cmd, "fstest")
	if _, err := ParseDir("../../testdata/golden/init/mydb", cfg); err == nil {
		t.Error("Expected error from ParseDir(), but instead err is nil")
	}
	if _, err := ParseDir("../../testdata/golden/init/mydb/product", cfg); err == nil {
		t.Error("Expected error from ParseDir(), but instead err is nil")
	}
}

func TestParseDirSymlinks(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Not testing symlink behavior on Windows")
	}

	dir := getDir(t, "testdata/sqlsymlinks")

	// Confirm symlinks to dirs are ignored by Subdirs
	subs, err := dir.Subdirs()
	if err != nil || countParseErrors(subs) > 0 || len(subs) != 2 {
		t.Fatalf("Unexpected error from Subdirs(): %v, %v; %d parse errors", subs, err, countParseErrors(subs))
	}

	dir = getDir(t, "testdata/sqlsymlinks/product")
	logicalSchema := dir.LogicalSchemas[0]
	expectTableNames := []string{"comments", "posts", "subscriptions", "users", "activity", "rollups"}
	if len(logicalSchema.Creates) != len(expectTableNames) {
		t.Errorf("Unexpected object count: found %d, expected %d", len(logicalSchema.Creates), len(expectTableNames))
	} else {
		for _, name := range expectTableNames {
			key := tengo.ObjectKey{Type: tengo.ObjectTypeTable, Name: name}
			if logicalSchema.Creates[key] == nil {
				t.Errorf("Did not find Create for table %s in LogicalSchema", name)
			}
		}
	}

	// .skeema files that are symlinks pointing within same repo are OK
	getDir(t, "testdata/cfgsymlinks1/validrel")
	dir = getDir(t, "testdata/cfgsymlinks1")
	subs, err = dir.Subdirs()
	if badCount := countParseErrors(subs); err != nil || badCount != 2 || len(subs)-badCount != 1 {
		t.Errorf("Expected Subdirs() to return 1 valid sub and 2 bad ones; instead found %d good, %d bad, %v", len(subs)-badCount, badCount, err)
	}

	// Otherwise, .skeema files that are symlinks pointing outside the repo, or
	// to non-regular files, generate errors
	expectErrors := []string{
		"testdata/cfgsymlinks1/invalidrel",
		"testdata/cfgsymlinks1/invalidabs",
		"testdata/cfgsymlinks2/product",
		"testdata/cfgsymlinks2",
	}
	cfg := getValidConfig(t)
	for _, dirPath := range expectErrors {
		if _, err := ParseDir(dirPath, cfg); err == nil {
			t.Errorf("For path %s, expected error from ParseDir(), but instead err is nil", dirPath)
		}
	}
}

// TestParseDirNamedSchemas tests parsing of dirs that have explicit schema
// names in the *.sql files, in various combinations.
func TestParseDirNamedSchemas(t *testing.T) {
	// named1 has one table in the nameless schema and one in a named schema
	dir := getDir(t, "testdata/named1")
	if len(dir.LogicalSchemas) != 2 {
		t.Errorf("Expected 2 logical schemas in testdata/named1, instead found %d", len(dir.LogicalSchemas))
	} else {
		if dir.LogicalSchemas[0].Name != "" || dir.LogicalSchemas[0].CharSet != "latin1" || len(dir.LogicalSchemas[0].Creates) != 1 {
			t.Errorf("Unexpected field values in dir.LogicalSchemas[0]: %+v", dir.LogicalSchemas[0])
		}
		if dir.LogicalSchemas[1].Name != "bar" || dir.LogicalSchemas[1].CharSet != "" || len(dir.LogicalSchemas[1].Creates) != 1 {
			t.Errorf("Unexpected field values in dir.LogicalSchemas[1]: %+v", dir.LogicalSchemas[1])
		}
	}

	// named2 has one table in the nameless schema, and one in each of two
	// different named schemas
	dir = getDir(t, "testdata/named2")
	if len(dir.LogicalSchemas) != 3 {
		t.Errorf("Expected 3 logical schemas in testdata/named2, instead found %d", len(dir.LogicalSchemas))
	} else {
		if dir.LogicalSchemas[0].Name != "" || dir.LogicalSchemas[0].CharSet != "latin1" || len(dir.LogicalSchemas[0].Creates) != 1 {
			t.Errorf("Unexpected field values in dir.LogicalSchemas[0]: %+v", dir.LogicalSchemas[0])
		}
		if (dir.LogicalSchemas[1].Name != "bar" && dir.LogicalSchemas[1].Name != "glorp") || dir.LogicalSchemas[1].CharSet != "" || len(dir.LogicalSchemas[1].Creates) != 1 {
			t.Errorf("Unexpected field values in dir.LogicalSchemas[1]: %+v", dir.LogicalSchemas[1])
		}
		if (dir.LogicalSchemas[2].Name != "bar" && dir.LogicalSchemas[2].Name != "glorp") || dir.LogicalSchemas[2].CharSet != "" || len(dir.LogicalSchemas[2].Creates) != 1 {
			t.Errorf("Unexpected field values in dir.LogicalSchemas[2]: %+v", dir.LogicalSchemas[2])
		}
	}

	// named3 has two different named schemas, each with one table. It has a
	// .skeema file definining a schema name, but no CREATEs are put into the
	// nameless schema.
	dir = getDir(t, "testdata/named3")
	if len(dir.LogicalSchemas) != 2 {
		t.Errorf("Expected 2 logical schemas in testdata/named3, instead found %d", len(dir.LogicalSchemas))
	} else {
		if (dir.LogicalSchemas[0].Name != "bar" && dir.LogicalSchemas[0].Name != "glorp") || dir.LogicalSchemas[0].CharSet != "" || len(dir.LogicalSchemas[0].Creates) != 1 {
			t.Errorf("Unexpected field values in dir.LogicalSchemas[0]: %+v", dir.LogicalSchemas[0])
		}
		if (dir.LogicalSchemas[1].Name != "bar" && dir.LogicalSchemas[1].Name != "glorp") || dir.LogicalSchemas[1].CharSet != "" || len(dir.LogicalSchemas[1].Creates) != 1 {
			t.Errorf("Unexpected field values in dir.LogicalSchemas[1]: %+v", dir.LogicalSchemas[1])
		}
	}

	// named4 has two different named schemas, each with one table. It has no
	// .skeema file at all.
	dir = getDir(t, "testdata/named4")
	if len(dir.LogicalSchemas) != 2 {
		t.Errorf("Expected 2 logical schemas in testdata/named4, instead found %d", len(dir.LogicalSchemas))
	} else {
		if (dir.LogicalSchemas[0].Name != "bar" && dir.LogicalSchemas[0].Name != "glorp") || dir.LogicalSchemas[0].CharSet != "" || len(dir.LogicalSchemas[0].Creates) != 1 {
			t.Errorf("Unexpected field values in dir.LogicalSchemas[0]: %+v", dir.LogicalSchemas[0])
		}
		if (dir.LogicalSchemas[1].Name != "bar" && dir.LogicalSchemas[1].Name != "glorp") || dir.LogicalSchemas[1].CharSet != "" || len(dir.LogicalSchemas[1].Creates) != 1 {
			t.Errorf("Unexpected field values in dir.LogicalSchemas[1]: %+v", dir.LogicalSchemas[1])
		}
	}
}

func TestDirGenerator(t *testing.T) {
	dir := getDir(t, "testdata/host/db")
	major, minor, patch, edition := dir.Generator()
	if major != 1 || minor != 5 || patch != 0 || edition != "community" {
		t.Errorf("Incorrect result from Generator: %d, %d, %d, %q", major, minor, patch, edition)
	}

	dir = getDir(t, "testdata/named1")
	major, minor, patch, edition = dir.Generator()
	if major != 0 || minor != 0 || patch != 0 || edition != "" {
		t.Errorf("Incorrect result from Generator: %d, %d, %d, %q", major, minor, patch, edition)
	}
}

func TestDirNamedSchemaStatements(t *testing.T) {
	// Test against a dir that has no named-schema statements
	dir := getDir(t, "../../testdata/golden/init/mydb/product")
	if len(dir.NamedSchemaStatements) > 0 {
		t.Errorf("Expected dir %s to have no named schema statements; instead found %d", dir, len(dir.NamedSchemaStatements))
	}

	// named1 has 2 USE statements, and no schema-qualified CREATEs
	dir = getDir(t, "testdata/named1")
	if len(dir.NamedSchemaStatements) != 2 {
		t.Errorf("Expected dir %s to have 2 named schema statements; instead found %d", dir, len(dir.NamedSchemaStatements))
	} else if dir.NamedSchemaStatements[0].Type != tengo.StatementTypeCommand || dir.NamedSchemaStatements[1].Type != tengo.StatementTypeCommand {
		t.Errorf("Unexpected statements found in result of NamedSchemaStatements: [0]=%+v, [1]=%+v", *dir.NamedSchemaStatements[0], *dir.NamedSchemaStatements[1])
	}

	// named2 has 1 schema-qualified CREATE, and 1 USE statement
	dir = getDir(t, "testdata/named2")
	if len(dir.NamedSchemaStatements) != 2 {
		t.Errorf("Expected dir %s to have 2 named schema statements; instead found %d", dir, len(dir.NamedSchemaStatements))
	} else if dir.NamedSchemaStatements[0].Type != tengo.StatementTypeCreate || dir.NamedSchemaStatements[1].Type != tengo.StatementTypeCommand {
		t.Errorf("Unexpected statements found in result of NamedSchemaStatements: [0]=%+v, [1]=%+v", *dir.NamedSchemaStatements[0], *dir.NamedSchemaStatements[1])
	}

	// named3 has 2 schema-qualified CREATEs
	dir = getDir(t, "testdata/named3")
	if len(dir.NamedSchemaStatements) != 2 {
		t.Errorf("Expected dir %s to have 2 named schema statements; instead found %d", dir, len(dir.NamedSchemaStatements))
	} else if dir.NamedSchemaStatements[0].Type != tengo.StatementTypeCreate || dir.NamedSchemaStatements[1].Type != tengo.StatementTypeCreate {
		t.Errorf("Unexpected statements found in result of NamedSchemaStatements: [0]=%+v, [1]=%+v", *dir.NamedSchemaStatements[0], *dir.NamedSchemaStatements[1])
	}

	// named4 has 2 USE statements, and no schema-qualified CREATEs
	dir = getDir(t, "testdata/named4")
	if len(dir.NamedSchemaStatements) != 2 {
		t.Errorf("Expected dir %s to have 2 named schema statements; instead found %d", dir, len(dir.NamedSchemaStatements))
	} else if dir.NamedSchemaStatements[0].Type != tengo.StatementTypeCommand || dir.NamedSchemaStatements[1].Type != tengo.StatementTypeCommand {
		t.Errorf("Unexpected statements found in result of NamedSchemaStatements: [0]=%+v, [1]=%+v", *dir.NamedSchemaStatements[0], *dir.NamedSchemaStatements[1])
	}
}

func TestParseDirNoSchemas(t *testing.T) {
	// empty1 has no *.sql and no schema defined in the .skeema file
	dir := getDir(t, "testdata/empty1")
	if len(dir.LogicalSchemas) != 0 {
		t.Errorf("Expected no logical schemas in testdata/empty1, instead found %d", len(dir.LogicalSchemas))
	}

	// empty2 has no *.sql, but does define a schema in the .skeema file, so it
	// should have a single LogicalSchema with no CREATEs in it.
	dir = getDir(t, "testdata/empty2")
	if len(dir.LogicalSchemas) != 1 {
		t.Errorf("Expected 1 logical schema in testdata/empty2, instead found %d", len(dir.LogicalSchemas))
	} else {
		if dir.LogicalSchemas[0].Name != "" || dir.LogicalSchemas[0].CharSet != "latin1" || len(dir.LogicalSchemas[0].Creates) != 0 {
			t.Errorf("Unexpected field values in dir.LogicalSchemas[0]: %+v", dir.LogicalSchemas[0])
		}
	}
}

func TestParseDirUnterminated(t *testing.T) {
	// These 3 dirs have unterminated quotes, identifiers (backticks), and multi-
	// line comments, respectively. Each should surface as dir.ParseError.
	for _, subdir := range []string{"unterminatedcomment", "unterminatedident", "unterminatedquote"} {
		if _, err := ParseDir("testdata/"+subdir, getValidConfig(t)); err == nil {
			t.Errorf("In dir testdata/%s, expected error from ParseDir(), but instead err is nil", subdir)
		}
	}
}

func TestParseDirUnparsedStatements(t *testing.T) {
	// This dir contains an INSERT statement among the valid CREATEs. This should
	// be tracked in dir.UnparsedStatements but isn't a fatal error.
	if dir, err := ParseDir("testdata/unknownstatement", getValidConfig(t)); err != nil {
		t.Fatalf("In dir testdata/unknownstatement, unexpected error from ParseDir(): %v", err)
	} else if len(dir.LogicalSchemas) != 1 {
		t.Fatalf("In dir testdata/unknownstatement, expected 1 logical schema, instead found %d", len(dir.LogicalSchemas))
	} else if dir.ParseError != nil {
		t.Fatalf("In dir testdata/unknownstatement, expected nil ParseError, instead found %v", dir.ParseError)
	} else if len(dir.UnparsedStatements) != 1 {
		t.Errorf("In dir testdata/unknownstatement, expected 1 UnparsedStatements, instead found %d", len(dir.UnparsedStatements))
	}
}

func TestParseDirRedundantDelimiter(t *testing.T) {
	// This dir contains two special cases of DELIMITER commands:
	// * Setting a delimiter that is already the current delimiter, e.g. from ; to ;
	// * Setting a delimiter that is double the previous delimiter, e.g. from ; to ;;
	// These cases should not cause errors or UnparsedStatements.
	if dir, err := ParseDir("testdata/redundantdelimiter", getValidConfig(t)); err != nil {
		t.Fatalf("In dir testdata/redundantdelimiter, unexpected error from ParseDir(): %v", err)
	} else if len(dir.LogicalSchemas) != 1 {
		t.Fatalf("In dir testdata/redundantdelimiter, expected 1 logical schema, instead found %d", len(dir.LogicalSchemas))
	} else if len(dir.LogicalSchemas[0].Creates) != 3 {
		t.Fatalf("In dir testdata/redundantdelimiter, expected 3 CREATEs, instead found %d", len(dir.LogicalSchemas[0].Creates))
	} else if dir.ParseError != nil {
		t.Fatalf("In dir testdata/redundantdelimiter, expected nil ParseError, instead found %v", dir.ParseError)
	} else if len(dir.UnparsedStatements) > 0 {
		t.Errorf("In dir testdata/redundantdelimiter, expected 0 UnparsedStatements, instead found %d, first is %+v", len(dir.UnparsedStatements), *dir.UnparsedStatements[0])
	}
}

func TestParseDirCreateSelect(t *testing.T) {
	// This dir contains a CREATE ... SELECT statement, which is explicitly not
	// supported at this time.
	_, err := ParseDir("testdata/createselect", getValidConfig(t))
	if err == nil {
		t.Fatal("In dir testdata/createselect, expected error from ParseDir(), but instead err is nil")
	}
}

func TestParseDirBOM(t *testing.T) {
	// The .skeema file and tables.sql file in this dir both have a UTF8 byte-order
	// marker prefix char, which should not interfere with the ability to parse the
	// dir or its contents
	if dir, err := ParseDir("testdata/utf8bom", getValidConfig(t)); err != nil {
		t.Fatalf("In dir testdata/utf8bom, unexpected error from ParseDir(): %v", err)
	} else if len(dir.LogicalSchemas) != 1 {
		t.Fatalf("In dir testdata/utf8bom, expected 1 logical schema, instead found %d", len(dir.LogicalSchemas))
	} else if len(dir.LogicalSchemas[0].Creates) != 2 {
		t.Fatalf("In dir testdata/utf8bom, expected 2 CREATEs, instead found %d", len(dir.LogicalSchemas[0].Creates))
	} else if dir.ParseError != nil {
		t.Fatalf("In dir testdata/utf8bom, expected nil ParseError, instead found %v", dir.ParseError)
	} else if len(dir.UnparsedStatements) != 0 {
		t.Errorf("In dir testdata/utf8bom, expected 0 UnparsedStatements, instead found %d", len(dir.UnparsedStatements))
	}
}

func TestParseDirIgnorePatterns(t *testing.T) {
	// Confirm behavior of ignore pattern blocking all procs
	dir := getDir(t, "testdata/ignore/invalidsql")
	if len(dir.LogicalSchemas) != 1 {
		t.Errorf("Expected 1 logical schema, instead found %d", len(dir.LogicalSchemas))
	}
	if len(dir.LogicalSchemas[0].Creates) != 2 {
		t.Errorf("Expected 2 non-ignored CREATES in logical schema, instead found %d", len(dir.LogicalSchemas[0].Creates))
	}
	for key := range dir.LogicalSchemas[0].Creates {
		if key.Type == tengo.ObjectTypeProc {
			t.Errorf("Expected all procs to be ignored by ignore-proc=., but found proc with name %s", key.Name)
		}
	}

	// Confirm behavior of invalid regex for ignore pattern
	_, err := ParseDir("testdata/ignore/invalidregex", getValidConfig(t))
	if err == nil {
		t.Fatal("In dir testdata/ignore/invalidregex, expected error from ParseDir(), but instead err is nil")
	}
	var ce ConfigError
	if !errors.As(err, &ce) {
		t.Errorf("Expected err to be ConfigError, instead type is %T and it does not unwrap to ConfigError", err)
	}
}

// TestDirParseDirCasingConflict covers situations where object names or file
// names only differ by casing. Normally we downcase filenames for use as
// map keys to avoid introducing files which only differ by casing, UNLESS a dir
// ALREADY has files with such a conflict (which can only happen on a case-
// sensitive filesystem anyway).
func TestDirParseDirCasingConflict(t *testing.T) {
	// Test behavior with empty dir: new conflicts won't be created
	dirPath := t.TempDir()
	dir := getDir(t, dirPath)
	if dir.retainMapKeyCasing {
		t.Fatal("Expected retainMapKeyCasing to be false, but it was true")
	}
	mixedCase := dir.FileFor(tengo.ObjectKey{Type: tengo.ObjectTypeTable, Name: "Foo"})
	lowerCase := dir.FileFor(tengo.ObjectKey{Type: tengo.ObjectTypeTable, Name: "foo"})
	if expected := filepath.Join(dirPath, "Foo.sql"); mixedCase.FilePath != expected {
		t.Errorf("Unexpected FilePath: expected %s, found %s", expected, mixedCase.FilePath)
	} else if lowerCase.FilePath != mixedCase.FilePath {
		t.Errorf("Unexpected FilePath: expected %s, found %s", mixedCase.FilePath, lowerCase.FilePath)
	} else if len(dir.SQLFiles) != 1 {
		t.Errorf("Expected 1 SQL file (both tables using same file), instead found %d", len(dir.SQLFiles))
	}
	if another := dir.FileFor(&tengo.Statement{File: filepath.Join(dirPath, "FOO.sql")}); another.FilePath != mixedCase.FilePath {
		t.Errorf("Unexpected FilePath: expected %s, found %s", mixedCase.FilePath, another.FilePath)
	}
	if another := dir.FileFor(&tengo.Statement{File: filepath.Join(dirPath, "foo.sql")}); another.FilePath != mixedCase.FilePath {
		t.Errorf("Unexpected FilePath: expected %s, found %s", mixedCase.FilePath, another.FilePath)
	}

	if runtime.GOOS == "darwin" || runtime.GOOS == "windows" {
		// Do a variant of above test in which some files already exist. Confirm that
		// attempts to introduce a conflict just re-use the existing file with
		// whatever casing it already had.
		WriteTestFile(t, filepath.Join(dirPath, "AAA.sql"), "CREATE TABLE AAA (id int);\nCREATE TABLE aaa (id int);")
		WriteTestFile(t, filepath.Join(dirPath, "foobar.sql"), "CREATE TABLE foobar (id int);")
		dir = getDir(t, dirPath) // need to re-parse dir now that files present
		if dir.retainMapKeyCasing {
			t.Fatal("Expected retainMapKeyCasing to be false, but it was true")
		}
		if len(dir.SQLFiles) != 2 {
			t.Errorf("Expected 2 SQL files, instead found %d", len(dir.SQLFiles))
		}
		if len(dir.LogicalSchemas[0].Creates) != 3 {
			t.Errorf("Expected 3 CREATEs, instead found %d", len(dir.LogicalSchemas[0].Creates))
		}
		another := dir.FileFor(tengo.ObjectKey{Type: tengo.ObjectTypeTable, Name: "aAa"})
		if expected := filepath.Join(dirPath, "AAA.sql"); another.FilePath != expected {
			t.Errorf("Unexpected FilePath: expected %s, found %s", expected, another.FilePath)
		}
		another = dir.FileFor(tengo.ObjectKey{Type: tengo.ObjectTypeTable, Name: "FOObar"})
		if expected := filepath.Join(dirPath, "foobar.sql"); another.FilePath != expected {
			t.Errorf("Unexpected FilePath: expected %s, found %s", expected, another.FilePath)
		}
		if len(dir.SQLFiles) != 2 {
			t.Errorf("Expected 2 SQL files, instead found %d", len(dir.SQLFiles))
		}

	} else {
		// On case-sensitive filesystems, we can do a more thorough test by actually
		// creating casing conflicts. Test behavior of a dir that has 3 SQL files, 2
		// of which only differ by casing
		WriteTestFile(t, filepath.Join(dirPath, "AAA.sql"), "CREATE TABLE AAA (id int);")
		WriteTestFile(t, filepath.Join(dirPath, "FooBar.sql"), "CREATE TABLE FooBar (id int);")
		WriteTestFile(t, filepath.Join(dirPath, "foobar.sql"), "CREATE TABLE foobar (id int);")
		dir = getDir(t, dirPath) // need to re-parse dir now that files present
		if !dir.retainMapKeyCasing {
			t.Fatal("Expected retainMapKeyCasing to be true, but it was false")
		}
		if len(dir.SQLFiles) != 3 {
			t.Errorf("Expected 3 SQL files, instead found %d", len(dir.SQLFiles))
		}
		mixedCase := dir.FileFor(tengo.ObjectKey{Type: tengo.ObjectTypeTable, Name: "FooBar"})
		lowerCase := dir.FileFor(tengo.ObjectKey{Type: tengo.ObjectTypeTable, Name: "foobar"})
		if mixedCase.FilePath == lowerCase.FilePath {
			t.Errorf("Expected paths to differ, but both are %s", mixedCase.FilePath)
		}
		upperAAA := dir.FileFor(tengo.ObjectKey{Type: tengo.ObjectTypeTable, Name: "AAA"})
		lowerAAA := dir.FileFor(tengo.ObjectKey{Type: tengo.ObjectTypeTable, Name: "aaa"})
		if upperAAA.FilePath == lowerAAA.FilePath {
			t.Errorf("Expected paths to differ, but both are %s", mixedCase.FilePath)
		}
		if exists, err := upperAAA.Exists(); !exists || err != nil {
			t.Errorf("Unexpected return from Exists: %t, %v", exists, err)
		}
		if exists, err := lowerAAA.Exists(); exists || err != nil {
			t.Errorf("Unexpected return from Exists: %t, %v", exists, err)
		}
	}
}

func TestDirBaseName(t *testing.T) {
	dir := getDir(t, "../../testdata/golden/init/mydb/product")
	if bn := dir.BaseName(); bn != "product" {
		t.Errorf("Unexpected base name: %s", bn)
	}
}

func TestDirRelPath(t *testing.T) {
	dir := getDir(t, "../../testdata/golden/init/mydb/product")
	expected := "testdata/golden/init/mydb/product"
	if runtime.GOOS == "windows" {
		expected = strings.ReplaceAll(expected, "/", `\`)
	}
	if rel := dir.RelPath(); rel != expected {
		t.Errorf("Unexpected rel path: %s", rel)
	}
	dir = getDir(t, "../..")
	if rel := dir.RelPath(); rel != "." {
		t.Errorf("Unexpected rel path: %s", rel)
	}

	// Force a relative path into dir.Path (shouldn't normally be possible) and
	// confirm just the basename (bar) is returned
	dir.Path = "foo/bar"
	if rel := dir.RelPath(); rel != "bar" {
		t.Errorf("Unexpected rel path: %s", rel)
	}
}

func TestDirSubdirs(t *testing.T) {
	dir := getDir(t, "../../testdata/golden/init/mydb")
	subs, err := dir.Subdirs()
	if err != nil || countParseErrors(subs) > 0 {
		t.Fatalf("Unexpected error from Subdirs(): %s", err)
	}
	if len(subs) < 2 {
		t.Errorf("Unexpectedly low subdir count returned: found %d, expected at least 2", len(subs))
	}

	dir = getDir(t, "testdata")
	subs, err = dir.Subdirs()
	// Expect at least 5 parse errors: 3 with unterminated quotes/comments, 1 bad
	// symlink in cfgsymlinks2, 1 forbidden statement in createselect
	if len(subs) < 19 || err != nil || countParseErrors(subs) < 5 {
		t.Errorf("Unexpected return from Subdirs(): %d subs, %d parse errors, err=%v", len(subs), countParseErrors(subs), err)
	}
}

func TestDirFileFor(t *testing.T) {
	dir := getDir(t, "testdata/host/db")

	// test FileFor with an object that does not yet exist: it should still return
	// an SQLFile based on the object's default location, but with zero statements
	key := tengo.ObjectKey{Type: tengo.ObjectTypeTable, Name: "doesnt_exist"}
	sf := dir.FileFor(key)
	if sf == nil {
		t.Error("Dir.FileFor unexpectedly returned nil SQLFile")
	} else if len(sf.Statements) > 0 {
		t.Errorf("Expected new SQLFile to have 0 statements, but instead found %d", len(sf.Statements))
	}

	// test FileFor with an object that already exists: it should return an SQLFile
	// with one statement
	key.Name = "comments"
	sf = dir.FileFor(key)
	if sf == nil {
		t.Fatal("Dir.FileFor unexpectedly returned nil SQLFile")
	} else if len(sf.Statements) != 1 {
		t.Fatalf("Expected SQLFile to have 1 statement, but instead found %d", len(sf.Statements))
	}

	// test FileFor with a statement: its return should be based on the File field
	// of the statement, returning back a pointer to the exact same SQLFile
	stmt := sf.Statements[0]
	if sf2 := dir.FileFor(stmt); sf2 != sf {
		t.Errorf("Unexpected return from FileFor on a statement: expected %+v, found %+v", sf, sf2)
	}

	// Artificially manipulate the statement: change its name and empty its file
	// field. FileFor should fall back to the object's default location.
	origName := stmt.ObjectName
	stmt.ObjectName += "2"
	stmt.Text = strings.Replace(stmt.Text, origName, stmt.ObjectName, 1)
	stmt.File = ""
	sf3 := dir.FileFor(stmt)
	if expectedPath := strings.Replace(sf.FilePath, origName, stmt.ObjectName, 1); expectedPath != sf3.FilePath {
		t.Errorf("Expected return from FileFor to have path %s, instead found %s", expectedPath, sf3.FilePath)
	}
}

func TestDirDirtyFiles(t *testing.T) {
	dir := getDir(t, "testdata/host/db")

	// Expect no dirty file initially
	if dirties := dir.DirtyFiles(); len(dirties) > 0 {
		t.Errorf("Expected no dirty files initially, instead found %d", len(dirties))
	}

	// Artificially mark two files as dirty and confirm they are returned now
	for filePath, sf := range dir.SQLFiles {
		if strings.HasSuffix(filePath, "comments.sql") || strings.HasSuffix(filePath, "posts.sql") {
			sf.Dirty = true
		}
	}
	if dirties := dir.DirtyFiles(); len(dirties) != 2 {
		t.Errorf("Expected 2 dirty files, instead found %d", len(dirties))
	} else if dirties[0].FilePath == dirties[1].FilePath {
		t.Error("Same file returned twice by dir.DirtyFiles()")
	} else {
		for _, sf := range dirties {
			if !strings.HasSuffix(sf.FilePath, "comments.sql") && !strings.HasSuffix(sf.FilePath, "posts.sql") {
				t.Errorf("Unexpected file returned from dir.DirtyFiles(): %s", sf.FilePath)
			}
		}
	}
}

func TestDirInstances(t *testing.T) {
	assertInstances := func(optionValues map[string]string, expectError bool, expectedInstances ...string) []*tengo.Instance {
		cmd := mybase.NewCommand("test", "1.0", "this is for testing", nil)
		cmd.AddArg("environment", "production", false)
		util.AddGlobalOptions(cmd)
		cli := &mybase.CommandLine{
			Command: cmd,
		}
		cfg := mybase.NewConfig(cli, mybase.SimpleSource(optionValues))
		dir := &Dir{
			Path:   "/tmp/dummydir",
			Config: cfg,
		}
		instances, err := dir.Instances()
		if expectError && err == nil {
			t.Errorf("With option values %v, expected error to be returned, but it was nil", optionValues)
		} else if !expectError && err != nil {
			t.Errorf("With option values %v, expected nil error, but found %s", optionValues, err)
		} else {
			var foundInstances []string
			for _, inst := range instances {
				foundInstances = append(foundInstances, inst.String())
			}
			if !reflect.DeepEqual(expectedInstances, foundInstances) {
				t.Errorf("With option values %v, expected instances %#v, but found instances %#v", optionValues, expectedInstances, foundInstances)
			}
		}
		return instances
	}

	// no host defined
	assertInstances(nil, false)

	// static host with various combinations of other options
	assertInstances(map[string]string{"host": "some.db.host"}, false, "some.db.host:3306")
	assertInstances(map[string]string{"host": "some.db.host:3307"}, false, "some.db.host:3307")
	assertInstances(map[string]string{"host": "some.db.host", "port": "3307"}, false, "some.db.host:3307")
	assertInstances(map[string]string{"host": "some.db.host:3307", "port": "3307"}, false, "some.db.host:3307")
	assertInstances(map[string]string{"host": "some.db.host:3307", "port": "3306"}, true) // mismatched port option not ignored if supplied explicitly, even if default
	assertInstances(map[string]string{"host": "localhost"}, false, "localhost:/tmp/mysql.sock")
	assertInstances(map[string]string{"host": "localhost", "port": "1234"}, false, "localhost:1234")
	assertInstances(map[string]string{"host": "localhost", "socket": "/var/run/mysql.sock"}, false, "localhost:/var/run/mysql.sock")
	assertInstances(map[string]string{"host": "localhost", "port": "1234", "socket": "/var/lib/mysql/mysql.sock"}, false, "localhost:/var/lib/mysql/mysql.sock")

	// list of static hosts
	assertInstances(map[string]string{"host": "some.db.host,other.db.host"}, false, "some.db.host:3306", "other.db.host:3306")
	assertInstances(map[string]string{"host": `"some.db.host, other.db.host"`, "port": "3307"}, false, "some.db.host:3307", "other.db.host:3307")
	assertInstances(map[string]string{"host": "'some.db.host:3308', 'other.db.host'"}, false, "some.db.host:3308", "other.db.host:3306")

	// invalid option values or combinations
	assertInstances(map[string]string{"host": "some.db.host", "connect-options": ","}, true)
	assertInstances(map[string]string{"host": "some.db.host:3306", "port": "3307"}, true)
	assertInstances(map[string]string{"host": "@@@@@"}, true)
	assertInstances(map[string]string{"host-wrapper": "`echo {INVALID_VAR}`", "host": "irrelevant"}, true)

	// dynamic hosts via host-wrapper command execution
	if runtime.GOOS == "windows" {
		assertInstances(map[string]string{"host-wrapper": "echo '{HOST}:3306'", "host": "some.db.host"}, false, "some.db.host:3306")
		assertInstances(map[string]string{"host-wrapper": "echo \"{HOST}`r`n\"", "host": "some.db.host:3306"}, false, "some.db.host:3306")
		assertInstances(map[string]string{"host-wrapper": "echo \"some.db.host`r`nother.db.host\"", "host": "ignored", "port": "3333"}, false, "some.db.host:3333", "other.db.host:3333")
		assertInstances(map[string]string{"host-wrapper": "echo \"some.db.host`tother.db.host:3316\"", "host": "ignored", "port": "3316"}, false, "some.db.host:3316", "other.db.host:3316")
		assertInstances(map[string]string{"host-wrapper": "echo \"localhost,remote.host:3307,other.host\"", "host": "ignored", "socket": "/var/lib/mysql/mysql.sock"}, false, "localhost:/var/lib/mysql/mysql.sock", "remote.host:3307", "other.host:3306")
		assertInstances(map[string]string{"host-wrapper": "echo \" \"", "host": "ignored"}, false)
	} else {
		assertInstances(map[string]string{"host-wrapper": "/usr/bin/printf '{HOST}:3306'", "host": "some.db.host"}, false, "some.db.host:3306")
		assertInstances(map[string]string{"host-wrapper": "`/usr/bin/printf '{HOST}\n'`", "host": "some.db.host:3306"}, false, "some.db.host:3306")
		assertInstances(map[string]string{"host-wrapper": "/usr/bin/printf 'some.db.host\nother.db.host'", "host": "ignored", "port": "3333"}, false, "some.db.host:3333", "other.db.host:3333")
		assertInstances(map[string]string{"host-wrapper": "/usr/bin/printf 'some.db.host\tother.db.host:3316'", "host": "ignored", "port": "3316"}, false, "some.db.host:3316", "other.db.host:3316")
		assertInstances(map[string]string{"host-wrapper": "/usr/bin/printf 'localhost,remote.host:3307,other.host'", "host": "ignored", "socket": "/var/lib/mysql/mysql.sock"}, false, "localhost:/var/lib/mysql/mysql.sock", "remote.host:3307", "other.host:3306")
		assertInstances(map[string]string{"host-wrapper": "/bin/echo -n", "host": "ignored"}, false)
	}
}

func TestDirInstanceDefaultParams(t *testing.T) {
	getFakeDir := func(connectOptions string) *Dir {
		return &Dir{
			Path:   "/tmp/dummydir",
			Config: mybase.SimpleConfig(map[string]string{"connect-options": connectOptions, "ssl-mode": "preferred"}),
		}
	}

	assertDefaultParams := func(connectOptions, expected string) {
		t.Helper()
		dir := getFakeDir(connectOptions)
		if parsed, err := url.ParseQuery(expected); err != nil {
			t.Fatalf("Bad expected value \"%s\": %s", expected, err)
		} else {
			expected = parsed.Encode() // re-sort expected so we can just compare strings
		}
		actual, err := dir.InstanceDefaultParams()
		if err != nil {
			t.Errorf("Unexpected error from connect-options=\"%s\": %s", connectOptions, err)
		} else if actual != expected {
			t.Errorf("Expected connect-options=\"%s\" to yield default params \"%s\", instead found \"%s\"", connectOptions, expected, actual)
		}
	}
	baseDefaults := "interpolateParams=true&foreign_key_checks=0&timeout=5s&writeTimeout=5s&readTimeout=20s&tls=preferred&default_storage_engine=%27InnoDB%27"
	expectParams := map[string]string{
		"":                          baseDefaults,
		"foo='bar'":                 baseDefaults + "&foo=%27bar%27",
		"bool=true,quotes='yes,no'": baseDefaults + "&bool=true&quotes=%27yes,no%27",
		`escaped=we\'re ok`:         baseDefaults + "&escaped=we%5C%27re ok",
		`escquotes='we\'re still quoted',this=that`: baseDefaults + "&escquotes=%27we%5C%27re still quoted%27&this=that",
		"ok=1,writeTimeout=12ms":                    strings.Replace(baseDefaults, "writeTimeout=5s", "writeTimeout=12ms&ok=1", 1),
	}
	for connOpts, expected := range expectParams {
		assertDefaultParams(connOpts, expected)
	}

	expectError := []string{
		"totally_benign=1,allowAllFiles=true",
		"FOREIGN_key_CHECKS='on'",
		"bad_parse",
	}
	for _, connOpts := range expectError {
		dir := getFakeDir(connOpts)
		if _, err := dir.InstanceDefaultParams(); err == nil {
			t.Errorf("Did not get expected error from connect-options=\"%s\"", connOpts)
		}
	}

	// Test valid ssl-mode values, along with an invalid one and then an invalid combination with tls in connect-options
	expectTLS := map[string]string{
		"disabled":  strings.Replace(baseDefaults, "tls=preferred", "tls=false", 1),
		"preferred": baseDefaults,
		"required":  strings.Replace(baseDefaults, "tls=preferred", "tls=skip-verify", 1),
	}
	dir := getFakeDir("")
	for sslMode, expected := range expectTLS {
		dir.Config = mybase.SimpleConfig(map[string]string{"connect-options": "", "ssl-mode": sslMode})
		if parsed, err := url.ParseQuery(expected); err != nil {
			t.Fatalf("Bad expected value %q: %s", expected, err)
		} else {
			expected = parsed.Encode() // re-sort expected so we can just compare strings
		}
		actual, err := dir.InstanceDefaultParams()
		if err != nil {
			t.Errorf("Unexpected error from ssl-mode=%q: %s", sslMode, err)
		} else if actual != expected {
			t.Errorf("Expected ssl-mode=%q to yield default params %q, instead found %q", sslMode, expected, actual)
		}
	}
	dir.Config = mybase.SimpleConfig(map[string]string{"connect-options": "", "ssl-mode": "invalid-enum"})
	if _, err := dir.InstanceDefaultParams(); err == nil {
		t.Error("Expected an error from dir.InstanceDefaultParams() with invalid ssl-mode, but err was nil")
	}
	dir.Config = mybase.SimpleConfig(map[string]string{"connect-options": "tls=preferred", "ssl-mode": "required"})
	if _, err := dir.InstanceDefaultParams(); err == nil {
		t.Error("Expected an error from dir.InstanceDefaultParams() with tls in connect-options while also setting ssl-mode, but err was nil")
	}
}

func TestHostDefaultDirName(t *testing.T) {
	cases := []struct {
		Hostname string
		Port     int
		Expected string
	}{
		{"localhost", 3306, "localhost"},
		{"localhost", 3307, "localhost:3307"},
		{"1.2.3.4", 0, "1.2.3.4"},
		{"1.2.3.4", 3333, "1.2.3.4:3333"},
	}
	for _, c := range cases {
		if runtime.GOOS == "windows" {
			c.Expected = strings.Replace(c.Expected, ":", "_", 1)
		}
		if actual := HostDefaultDirName(c.Hostname, c.Port); actual != c.Expected {
			t.Errorf("Expected HostDefaultDirName(%q, %d) to return %q, instead found %q", c.Hostname, c.Port, c.Expected, actual)
		}
	}
}

func TestAncestorPaths(t *testing.T) {
	type testcase struct {
		input    string
		expected []string
	}
	var cases []testcase
	if runtime.GOOS == "windows" {
		cases = []testcase{
			{`C:\`, []string{`C:\`}},
			{`Z:\foo`, []string{`Z:\foo`, `Z:\`}},
			{`Z:\foo\`, []string{`Z:\foo`, `Z:\`}},
			{`C:\foo\bar\baz`, []string{`C:\foo\bar\baz`, `C:\foo\bar`, `C:\foo`, `C:\`}},
			{`\\host\share\`, []string{`\\host\share\`}},
			{`\\host\share\dir`, []string{`\\host\share\dir`, `\\host\share\`}},
			{`\\host\share\dir\subdir\`, []string{`\\host\share\dir\subdir`, `\\host\share\dir`, `\\host\share\`}},
		}
	} else {
		cases = []testcase{
			{"/", []string{"/"}},
			{"///", []string{"/"}},
			{"/foo", []string{"/foo", "/"}},
			{"/foo/", []string{"/foo", "/"}},
			{"/foo//bar/.", []string{"/foo/bar", "/foo", "/"}},
			{"/foo/bar/baz/..", []string{"/foo/bar", "/foo", "/"}},
		}
	}
	for _, c := range cases {
		if actual := ancestorPaths(c.input); !reflect.DeepEqual(actual, c.expected) {
			t.Errorf("Expected ancestorPaths(%q) to return %q, instead found %q", c.input, c.expected, actual)
		}
	}
}

func TestDirPassword(t *testing.T) {
	defer func() {
		util.PasswordPromptInput = util.PasswordInputSource(util.NoInteractiveInput)
		cachedInteractivePasswords = make(map[string]string)
	}()

	// If a parent dir .skeema file has a bare "password" line, pw should be
	// prompted there, but not redundantly for its subdirs, since it gets cached
	// in the dir's config as a runtime override, at dir parsing time (e.g. getDir)
	util.PasswordPromptInput = util.NewMockPasswordInput("basedir")
	dir := getDir(t, "testdata/pwprompt/basedir")
	util.PasswordPromptInput = util.PasswordInputSource(util.NoInteractiveInput)
	if pw, err := dir.Password(); pw != "basedir" || err != nil {
		t.Errorf("Unexpected return values from dir.Password(): %q, %v", pw, err)
	}
	util.PasswordPromptInput = util.NewMockPasswordInput("different value to ensure not re-prompted")
	subdirs, err := dir.Subdirs()
	if err != nil {
		t.Fatalf("Unexpected error from Subdirs: %v", err)
	}
	for _, subdir := range subdirs {
		if pw, err := subdir.Password(); pw != "basedir" || err != nil {
			t.Errorf("Unexpected return values from subdir.Password(): %q, %v", pw, err)
		}
	}

	// Same situation as above, but verify that a blank interactive password won't
	// re-prompt redundantly for subdirs
	util.PasswordPromptInput = util.NewMockPasswordInput("")
	dir = getDir(t, "testdata/pwprompt/basedir")
	util.PasswordPromptInput = util.NewMockPasswordInput("different value to ensure not re-prompted")
	if pw, err := dir.Password(); pw != "" || err != nil {
		t.Errorf("Unexpected return values from dir.Password(): %q, %v", pw, err)
	}
	subdirs, err = dir.Subdirs()
	if err != nil {
		t.Fatalf("Unexpected error from Subdirs: %v", err)
	}
	for _, subdir := range subdirs {
		if pw, err := subdir.Password(); pw != "" || err != nil {
			t.Errorf("Unexpected return values from subdir.Password(): %q, %v", pw, err)
		}
	}

	// If parent dir doesn't have bare "password" but both subdirs do, they should
	// each prompt password separately
	util.PasswordPromptInput = util.NewMockPasswordInput("basedir")
	dir = getDir(t, "testdata/pwprompt/leafdir")
	if pw, err := dir.Password(); pw != "" || err != nil {
		t.Errorf("Unexpected return values from dir.Password(): %q, %v", pw, err)
	}
	var counter int
	util.PasswordPromptInput = func() (string, error) {
		val := fmt.Sprintf("leaf-%d", counter)
		counter++
		return val, nil
	}
	subdirs, err = dir.Subdirs()
	if err != nil {
		t.Fatalf("Unexpected error from Subdirs: %v", err)
	}
	for n, subdir := range subdirs {
		if pw, err := subdir.Password(); pw != fmt.Sprintf("leaf-%d", n) || err != nil {
			t.Errorf("Unexpected return values from subdir.Password(): %q, %v", pw, err)
		}
	}

	// If an equals sign is present, but no value or an empty string, this means
	// empty password (not prompt) for compat with MySQL client behavior
	util.PasswordPromptInput = util.NewMockPasswordInput("this should not show up")
	dir = getDir(t, "testdata/pwprompt/noprompt/a")
	if pw, err := dir.Password(); pw != "" || err != nil {
		t.Errorf("Unexpected return values from dir.Password(): %q, %v", pw, err)
	}
	dir = getDir(t, "testdata/pwprompt/noprompt/b")
	if pw, err := dir.Password(); pw != "" || err != nil {
		t.Errorf("Unexpected return values from dir.Password(): %q, %v", pw, err)
	}

	// Now test prompting with hostnames:
	// The first dir here contains 3 hosts, but a prompt should only occur once.
	// Set the mock input to return a value once, followed by errors on subsequent
	// calls.
	util.PasswordPromptInput = func() (string, error) {
		util.PasswordPromptInput = util.NoInteractiveInput
		return "success", nil
	}
	dir = getDir(t, "testdata/pwprompt/hosts/a")
	if pw, err := dir.Password(dir.Config.GetSlice("host", ',', true)...); pw != "success" || err != nil {
		t.Errorf("Unexpected return values from dir.Password(): %q, %v", pw, err)
	}

	// The second dir has only one host, but it's one that also existed in first
	// dir, so its pw should be cached since it also has same username.
	dir = getDir(t, "testdata/pwprompt/hosts/b")
	if pw, err := dir.Password(dir.Config.GetSlice("host", ',', true)...); pw != "success" || err != nil {
		t.Errorf("Unexpected return values from dir.Password(): %q, %v", pw, err)
	}

	// The third dir has a single host that also appeared in first dir, but a
	// different user name, so pw prompt should error instead of using cache!
	dir = getDir(t, "testdata/pwprompt/hosts/c")
	if pw, err := dir.Password(dir.Config.GetSlice("host", ',', true)...); pw != "" || err == nil {
		t.Errorf("Unexpected return values from dir.Password(): %q, %v", pw, err)
	}

	// The fourth dir has a single host that did not appear previously, altho
	// same user name as first two dirs. PW prompt should error instead of using
	// cache.
	dir = getDir(t, "testdata/pwprompt/hosts/d")
	if pw, err := dir.Password(dir.Config.GetSlice("host", ',', true)...); pw != "" || err == nil {
		t.Errorf("Unexpected return values from dir.Password(): %q, %v", pw, err)
	}
}

func getValidConfigWithCLI(t *testing.T, cliOptions string) *mybase.Config {
	t.Helper()
	cmd := mybase.NewCommand("fstest", "", "", nil)
	util.AddGlobalOptions(cmd)
	cmd.AddArg("environment", "production", false)
	return mybase.ParseFakeCLI(t, cmd, "fstest "+cliOptions)
}

func getDirWithCLI(t *testing.T, dirPath, cliOptions string) *Dir {
	t.Helper()
	dir, err := ParseDir(dirPath, getValidConfigWithCLI(t, cliOptions))
	if err != nil {
		t.Fatalf("Unexpected error parsing dir %s: %s", dirPath, err)
	}
	return dir
}

func getValidConfig(t *testing.T) *mybase.Config {
	t.Helper()
	return getValidConfigWithCLI(t, "")
}

func getDir(t *testing.T, dirPath string) *Dir {
	t.Helper()
	return getDirWithCLI(t, dirPath, "")
}

func countParseErrors(subs []*Dir) (count int) {
	for _, sub := range subs {
		if sub.ParseError != nil {
			count++
		}
	}
	return
}
