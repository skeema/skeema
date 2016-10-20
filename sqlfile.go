package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"regexp"
	"strings"

	"github.com/skeema/tengo"
)

// Regexp for parsing CREATE TABLE statements. Submatches:
// [1] is any text preceeding the CREATE TABLE -- we ignore this
// [2] is the table name -- note we do not allow whitespace even if backtick-escaped
// [3] is the table body -- later we scan this for disallowed things
// [4] is any text after the table body -- we ignore this
var reParseCreate = regexp.MustCompile(`(?i)^(.*)\s*create\s+table\s+(?:if\s+not\s+exists\s+)?` + "`?([^\\s`]+)`?" + `\s+([^;]+);?\s*(.*)$`)

// We disallow CREATE TABLE SELECT and CREATE TABLE LIKE expressions
var reBodyDisallowed = regexp.MustCompile(`(?i)^(as\s+select|select|like|[(]\s+like)`)

// Forbid reading SQL files that are larger than 16KB; we assume legit CREATE TABLE statements should be smaller than this
const MaxSQLFileSize = 16 * 1024

// IsSQLFile returns true if the supplied os.FileInfo has a .sql extension and
// is a regular file. It is the caller's responsibility to resolve symlinks
// prior to passing them to this function.
func IsSQLFile(fi os.FileInfo) bool {
	if !strings.HasSuffix(fi.Name(), ".sql") {
		return false
	}
	if !fi.Mode().IsRegular() {
		return false
	}
	return true
}

type SQLFile struct {
	Dir      *Dir
	FileName string
	Contents string
	Error    error
	Warnings []error
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
		sf.Error = fmt.Errorf("%s: Error reading file: %s", sf.Path(), err)
		return "", sf.Error
	}
	sf.Contents = string(byteContents)
	if sf.validateContents() != nil {
		return "", sf.Error
	}
	return sf.Contents, nil
}

func (sf *SQLFile) Write() (int, error) {
	if !strings.HasSuffix(sf.FileName, ".sql") {
		return 0, fmt.Errorf("Filename %s does not end in .sql extension", sf.FileName)
	}
	if sf.Contents == "" {
		return 0, fmt.Errorf("SQLFile.Write: refusing to write blank / unpopulated file contents to %s", sf.Path())
	}
	value := fmt.Sprintf("%s;\n", sf.Contents)
	err := ioutil.WriteFile(sf.Path(), []byte(value), 0666)
	if err != nil {
		return 0, err
	}
	return len(value), nil
}

func (sf *SQLFile) Delete() error {
	return os.Remove(sf.Path())
}

// ValidateContents sanity-checks, and normalizes, the value of sf.Contents.
// It is the caller's responsibility to populate sf.Contents prior to calling
// this method.
func (sf *SQLFile) validateContents() error {
	if len(sf.Contents) > MaxSQLFileSize {
		sf.Error = fmt.Errorf("%s: file is too large; size of %d bytes exceeds max of %d bytes", sf.Path(), len(sf.Contents), MaxSQLFileSize)
		return sf.Error
	}

	matches := reParseCreate.FindStringSubmatch(sf.Contents)
	if matches == nil {
		sf.Error = fmt.Errorf("%s: cannot parse a valid CREATE TABLE statement", sf.Path())
		return sf.Error
	}
	if len(matches[1]) > 0 || len(matches[4]) > 0 {
		warning := fmt.Errorf("%s: stripping and ignoring %d chars before CREATE TABLE and %d chars after CREATE TABLE", sf.Path(), len(matches[1]), len(matches[4]))
		sf.Warnings = append(sf.Warnings, warning)
	}
	if sf.FileName != fmt.Sprintf("%s.sql", matches[2]) {
		warning := fmt.Errorf("%s: filename does not match table name of %s", sf.Path(), matches[2])
		sf.Warnings = append(sf.Warnings, warning)
	}
	if reBodyDisallowed.MatchString(matches[3]) {
		sf.Error = fmt.Errorf("%s: this form of CREATE TABLE statement is disallowed for security reasons", sf.Path())
		return sf.Error
	}

	sf.Contents = fmt.Sprintf("CREATE TABLE %s %s", tengo.EscapeIdentifier(matches[2]), matches[3])
	return nil
}
