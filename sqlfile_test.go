package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/skeema/mybase"
)

func getWorkspaceDir(t *testing.T) *Dir {
	t.Helper()
	cmd := mybase.NewCommand("test", "1.0", "this is for testing", nil)
	cli := &mybase.CommandLine{
		Command: cmd,
	}
	cfg := mybase.NewConfig(cli)
	dirPath, _ := filepath.Abs("testdata/.workspace")
	if _, err := os.Stat(dirPath); err == nil { // dir exists
		if err := os.RemoveAll(dirPath); err != nil {
			t.Fatalf("Unable to remove workspace dir: %s", err)
		}
	}
	if err := os.MkdirAll(dirPath, 0777); err != nil {
		t.Fatalf("Unable to create workspace dir: %s", err)
	}
	return &Dir{
		Path:    dirPath,
		Config:  cfg,
		section: "production",
	}
}

func TestSQLFileIO(t *testing.T) {
	sf := SQLFile{
		Dir:      getWorkspaceDir(t),
		FileName: "something.sql",
	}

	// Read should fail if file does not exist
	if _, err := sf.Read(); err == nil {
		t.Error("Expected read on nonexistent SQLFile to return error, but it did not")
	}

	// Write should fail if file has no contents
	if bytes, err := sf.Write(); err == nil {
		t.Errorf("Expected write to fail with no contents, but instead %d bytes written without error", bytes)
	}

	// Write should fail if file has contents but no .sql extension
	sf.FileName = "something.nosql"
	sf.Contents = "contents not validated by write"
	if bytes, err := sf.Write(); err == nil {
		t.Errorf("Expected write to fail with non-.sql extension, but instead %d bytes written without error", bytes)
	}

	// Write should fail if the file's dir doesn't exist
	oldDirPath := sf.Dir.Path
	sf.Dir.Path += "xyz"
	if bytes, err := sf.Write(); err == nil {
		t.Errorf("Expected write to fail with invalid dir path, but instead %d bytes written without error", bytes)
	}
	sf.Dir.Path = oldDirPath

	// Write should succeed if non-empty (but invalid) contents and .sql extension,
	// but subsequent read should error due to the invalid contents
	sf.FileName = "something.sql"
	if _, err := sf.Write(); err != nil {
		t.Fatalf("Expected write to succeed, but instead returned error %s", err)
	}
	if _, err := sf.Read(); err == nil {
		t.Error("Expected read on SQLFile with invalid contents to return error, but it did not")
	}

	// Read should succeed if contents are a valid CREATE TABLE
	writeFile(t, sf.Path(), "CREATE TABLE whatever (id int NOT NULL) ENGINE=InnoDB;\n")
	if _, err := sf.Read(); err != nil {
		t.Errorf("Expected read to succeed, but instead returned error %s", err)
	}

	// Delete should succeed when file exists, and fail when it does not
	if err := sf.Delete(); err != nil {
		t.Errorf("Expected delete to succeed, but instead returned error %s", err)
	}
	if _, err := sf.Read(); err == nil {
		t.Error("Expected read on deleted SQLFile to return error, but it did not")
	}
	if err := sf.Delete(); err == nil {
		t.Error("Expected delete on already-deleted SQLFile to return error, but it did not")
	}

	// Clean up
	if err := sf.Dir.Delete(); err != nil {
		t.Fatalf("Unexpected error from deleting workspace dir: %s", err)
	}
}

func TestSQLFileValidateContents(t *testing.T) {
	sf := SQLFile{
		Dir:      getWorkspaceDir(t),
		FileName: "something.sql",
	}
	assertValidation := func(contents string, expectedErrors, expectedWarnings int) {
		t.Helper()
		sf.Contents = contents
		err := sf.validateContents()
		if err != sf.Error {
			t.Fatalf("Expected returned error to match field, but found return=%s, sf.Error=%s", err, sf.Error)
		}
		if err == nil && expectedErrors != 0 {
			t.Errorf("Expected %d error, but instead found 0", expectedErrors)
		} else if err != nil && expectedErrors != 1 {
			t.Errorf("Expected %d errors, but instead found 1: %s", expectedErrors, err)
		}
		if len(sf.Warnings) != expectedWarnings {
			t.Errorf("Expected %d warnings, but instead found %d: %v", expectedWarnings, len(sf.Warnings), sf.Warnings)
		}
	}

	assertValidation(strings.Repeat("x", MaxSQLFileSize+1), 1, 0)
	assertValidation("no create table here", 1, 0)
	assertValidation("# comment\nCREATE TABLE something (id int);\nrandom stuff after\n", 0, 1)
	assertValidation("CREATE TABLE foo (id int);\n", 0, 1)
	assertValidation("CREATE TABLE something AS SELECT 1;", 1, 0)
	assertValidation("CREATE TABLE something \n  SELECT   1;", 1, 0)
	assertValidation("CREATE TABLE something    LIKE   other_table;", 1, 0)
	assertValidation("CREATE TABLE something (LIKE other_table);", 1, 0)
	assertValidation("# comment\nCREATE TABLE IF NOT EXISTS foo SELECT 1", 1, 2)
	assertValidation("CREATE TABLE something (id int)", 0, 0)
	assertValidation("CREATE TABLE something (id int);\n", 0, 0)
	assertValidation("CREATE  TaBLE  iF   NOT    exists `something` (id int);", 0, 0)
}
