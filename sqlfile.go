package main

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"regexp"
	"strings"
)

// Regexp for parsing CREATE TABLE statements. Submatches:
// [1] is any text preceeding the CREATE TABLE -- we ignore this
// [2] is the table name -- note we do not allow whitespace even if backtick-escaped
// [3] is the table body -- later we scan this for disallowed things
// [4] is any text after the table body -- we ignore this
var reParseCreate = regexp.MustCompile(`(?i)^(.*)\s*create\s+table\s+(?:if\s+not\s+exists\s+)?` + "`?([^\\s`]+)`?" + `\s+([^;]+);?\s*(.*)$`)

// We disallow CREATE TABLE SELECT and CREATE TABLE LIKE expressions
var reBodyDisallowed = regexp.MustCompile(`^(as\s+select|select|like|[(]\s+like)`)

type SQLFile struct {
	Dir      *SkeemaDir
	FileName string
	Contents string
	Error    error
	Warnings []error
	fileInfo os.FileInfo
}

func (sf *SQLFile) Path() string {
	return path.Join(sf.Dir.Path, sf.FileName)
}

func (sf *SQLFile) TableName() string {
	if !strings.HasSuffix(sf.FileName, ".sql") {
		return ""
	}
	return sf.FileName[0 : len(sf.FileName)-4]
}

func (sf *SQLFile) Read() (string, error) {
	byteContents, err := ioutil.ReadFile(sf.Path())
	if err != nil {
		sf.Error = err
		return "", err
	}
	sf.Contents = string(byteContents)
	if sf.ValidateContents() != nil {
		return "", sf.Error
	}
	return sf.Contents, nil
}

func (sf SQLFile) Write() (int, error) {
	if sf.ValidatePath(false) != nil {
		return 0, sf.Error
	}
	if sf.Contents == "" {
		return 0, errors.New("SQLFile.Write: refusing to write blank / unpopulated file contents")
	}
	value := fmt.Sprintf("%s;\n", sf.Contents)
	err := ioutil.WriteFile(sf.Path(), []byte(value), 0666)
	if err == nil {
		return len(value), nil
	} else {
		return 0, err
	}
}

func (sf SQLFile) Delete() error {
	return os.Remove(sf.Path())
}

func (sf *SQLFile) FileInfo() (os.FileInfo, error) {
	if sf.fileInfo != nil {
		return sf.fileInfo, nil
	}
	var err error
	sf.fileInfo, err = os.Stat(sf.Path())
	if err != nil {
		sf.Error = err
	}
	return sf.fileInfo, sf.Error
}

// ValidatePath sanity-checks the value of sf.Path, both in terms of its value and
// what existing file (if any) is at that path.
func (sf *SQLFile) ValidatePath(mustExist bool) error {
	// First, validations that are run regardless of whether the file exists
	if !strings.HasSuffix(sf.FileName, ".sql") {
		sf.Error = errors.New("SQLFile.ValidatePath: Filename does not end in .sql")
		return sf.Error
	}

	// Any validations from here down are only run if the file exists
	fi, err := sf.FileInfo()
	if os.IsNotExist(err) && !mustExist {
		return nil
	} else if err != nil {
		sf.Error = err
		return sf.Error
	}

	// TODO: add support for symlinks?
	if !fi.Mode().IsRegular() {
		sf.Error = errors.New("SQLFile.ValidatePath: Existing file is not a regular file")
		return sf.Error
	}
	if fi.Size() > MaxSQLFileSize {
		sf.Error = errors.New("SQLFile.ValidatePath: Existing file is too large")
		return sf.Error
	}

	return nil
}

// ValidateContents sanity-checks, and normalizes, the value of sf.Contents.
// It is the caller's responsibility to populate sf.Contents prior to calling
// this method.
func (sf *SQLFile) ValidateContents() error {
	matches := reParseCreate.FindStringSubmatch(sf.Contents)
	if matches == nil {
		sf.Error = errors.New("SQLFile.ValidateContents: Cannot parse a valid CREATE TABLE statement")
		return sf.Error
	}
	if len(matches[1]) > 0 || len(matches[4]) > 0 {
		warning := fmt.Errorf("SQLFile.ValidateContents: Ignoring %d chars before CREATE TABLE and %d chars after CREATE TABLE", len(matches[1]), len(matches[4]))
		sf.Warnings = append(sf.Warnings, warning)
	}
	if sf.FileName != fmt.Sprintf("%s.sql", matches[2]) {
		warning := fmt.Errorf("SQLFile.ValidateContents: filename does not match table name of %s", matches[2])
		sf.Warnings = append(sf.Warnings, warning)
	}
	if reBodyDisallowed.MatchString(matches[3]) {
		sf.Error = errors.New("SQLFile.ValidateContents: This form of CREATE TABLE statement is disallowed for security reasons")
		return sf.Error
	}

	sf.Contents = fmt.Sprintf("CREATE TABLE `%s` %s", matches[2], matches[3])
	return nil
}
