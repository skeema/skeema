package fs

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"unicode"

	"github.com/skeema/skeema/internal/tengo"
)

// SQLFile represents a file containing SQL statements.
type SQLFile struct {
	FilePath   string
	Statements []*Statement
	Dirty      bool
}

// FileName returns the file name of sqlFile without its directory path.
func (sqlFile *SQLFile) FileName() string {
	return filepath.Base(sqlFile.FilePath)
}

// Exists returns true if sqlFile already exists in the filesystem, false if not.
func (sqlFile *SQLFile) Exists() (bool, error) {
	_, err := os.Stat(sqlFile.FilePath)
	if err == nil {
		return true, nil
	} else if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

// Delete unlinks the file.
func (sqlFile *SQLFile) Delete() error {
	return os.Remove(sqlFile.FilePath)
}

// Write creates or replaces the SQLFile with the current statements, returning
// the number of bytes written. If the file's statements now only consist of
// comments, whitespace, and commands (e.g. USE, DELIMITER) then the file will
// be deleted instead, and a length of 0 will be returned. The file will be
// unmarked as dirty if the operation was successful.
func (sqlFile *SQLFile) Write() (n int, err error) {
	var b bytes.Buffer
	var keepFile bool
	for _, stmt := range sqlFile.Statements {
		b.WriteString(stmt.Text)
		if stmt.Type != StatementTypeNoop && stmt.Type != StatementTypeCommand {
			keepFile = true
		}
	}
	if keepFile {
		n, err = b.Len(), os.WriteFile(sqlFile.FilePath, b.Bytes(), 0666)
	} else {
		err = sqlFile.Delete()
	}
	if err == nil {
		sqlFile.Dirty = false
	}
	return n, err
}

// AddCreateStatement appends a CREATE statement to the file's list of
// statements, using the supplied create statement string. create should not
// include a delimiter or trailing newline.
// This method marks the file as dirty, but does not rewrite the file.
func (sqlFile *SQLFile) AddCreateStatement(key tengo.ObjectKey, create string) {
	// Prune any trailing DELIMITER or USE commands from the file, as these have
	// no effect anyway.
	for len(sqlFile.Statements) > 0 && sqlFile.Statements[len(sqlFile.Statements)-1].Type == StatementTypeCommand {
		sqlFile.Statements = sqlFile.Statements[:len(sqlFile.Statements)-1]
	}

	// If there are any statements left, examine the last statement to see what
	// the delimiter and default database are at the end of the file.
	currentDelimiter := ";"
	var defaultDatabase string
	if len(sqlFile.Statements) > 0 {
		currentDelimiter = sqlFile.Statements[len(sqlFile.Statements)-1].Delimiter
		defaultDatabase = sqlFile.Statements[len(sqlFile.Statements)-1].DefaultDatabase
	}

	makeDelimiterCommand := func(newDelim string) *Statement {
		stmt := &Statement{
			File:            sqlFile.FilePath,
			Text:            "DELIMITER " + newDelim + "\n",
			DefaultDatabase: defaultDatabase,
			Type:            StatementTypeCommand,
			Delimiter:       "\000",
		}
		currentDelimiter = newDelim
		return stmt
	}

	if NeedSpecialDelimiter(key, create) {
		if currentDelimiter == ";" {
			sqlFile.Statements = append(sqlFile.Statements, makeDelimiterCommand("//"))
		}
		create += "//\n"
	} else {
		if currentDelimiter != ";" {
			sqlFile.Statements = append(sqlFile.Statements, makeDelimiterCommand(";"))
		}
		create += ";\n"
	}
	sqlFile.Statements = append(sqlFile.Statements, &Statement{
		File:            sqlFile.FilePath,
		Text:            create,
		DefaultDatabase: defaultDatabase,
		Type:            StatementTypeCreate,
		ObjectType:      key.Type,
		ObjectName:      key.Name,
		Delimiter:       currentDelimiter,
	})
	if currentDelimiter != ";" {
		sqlFile.Statements = append(sqlFile.Statements, makeDelimiterCommand(";"))
	}

	sqlFile.Dirty = true
}

// EditStatementText sets stmt.Text to a new value consisting of newText plus
// an appropriate delimiter and newline. It marks the file as dirty, and (if
// needed) adds DELIMITER commands around stmt in the file's list of statements.
// The supplied newText should NOT have a delimiter or trailing newline. This
// method panics if stmt's address is not actually found among the file's
// statement pointers slice.
func (sqlFile *SQLFile) EditStatementText(stmt *Statement, newText string) {
	prevSpecialDelim := (stmt.Delimiter != ";")
	newSpecialDelim := NeedSpecialDelimiter(stmt.ObjectKey(), newText)
	sqlFile.Dirty = true
	i := sqlFile.statementIndex(stmt)

	// TODO: remove extraneous DELIMITER commands if they are unnecessary.
	// currently we only add them if needed, but never remove them, nor avoid
	// introducing duplicate ones in a multi-statement file.
	if prevSpecialDelim || !newSpecialDelim {
		_, oldFooter := stmt.SplitTextBody()
		stmt.Text = newText + oldFooter
		return
	}

	newStatements := make([]*Statement, len(sqlFile.Statements)+2)
	copy(newStatements, sqlFile.Statements[0:i])
	newStatements[i] = &Statement{
		File:            sqlFile.FilePath,
		Text:            "DELIMITER //\n",
		DefaultDatabase: stmt.DefaultDatabase,
		Type:            StatementTypeCommand,
		Delimiter:       "\000",
	}
	stmt.Delimiter = "//"
	stmt.Text = newText + "//\n"
	newStatements[i+1] = stmt
	newStatements[i+2] = &Statement{
		File:            sqlFile.FilePath,
		Text:            "DELIMITER ;\n",
		DefaultDatabase: stmt.DefaultDatabase,
		Type:            StatementTypeCommand,
		Delimiter:       "\000",
	}
	copy(newStatements[i+3:], sqlFile.Statements[i+1:])
	sqlFile.Statements = newStatements
}

// RemoveStatement removes stmt from the file's in-memory list of statements,
// and marks the file as dirty. Panics if the address of stmt is not actually
// found in its expected file's in-memory representation.
func (sqlFile *SQLFile) RemoveStatement(stmt *Statement) {
	i := sqlFile.statementIndex(stmt)
	sqlFile.Dirty = true
	copy(sqlFile.Statements[i:], sqlFile.Statements[i+1:])
	sqlFile.Statements[len(sqlFile.Statements)-1] = nil
	sqlFile.Statements = sqlFile.Statements[:len(sqlFile.Statements)-1]
}

func (sqlFile *SQLFile) statementIndex(stmt *Statement) int {
	for n := range sqlFile.Statements {
		if sqlFile.Statements[n] == stmt {
			return n
		}
	}
	panic(fmt.Errorf("Statement previously at %s not actually found in file", stmt.Location()))
}

// NormalizeFileName forces name to lowercase on operating systems that
// traditionally have case-insensitive operating systems. This is intended for
// use in string-keyed maps, to avoid the possibility of having multiple
// distinct map keys which actually refer to the same file.
func NormalizeFileName(name string) string {
	if runtime.GOOS == "darwin" || runtime.GOOS == "windows" {
		return strings.ToLower(name)
	}
	return name
}

// FileNameForObject returns a string containing the filename to use for the
// SQLFile representing the supplied object name. Special characters in the
// objectName will be removed; however, there is no risk of "conflicts" since
// a single SQLFile can store definitions for multiple objects.
func FileNameForObject(objectName string) string {
	objectName = strings.Map(removeSpecialChars, objectName)
	if objectName == "" {
		objectName = "symbols"
	}
	return NormalizeFileName(objectName) + ".sql"
}

// PathForObject returns a string containing a path to use for the SQLFile
// representing the supplied object name. Special characters in the objectName
// will be removed; however, there is no risk of "conflicts" since a single
// SQLFile can store definitions for multiple objects.
func PathForObject(dirPath, objectName string) string {
	return filepath.Join(dirPath, FileNameForObject(objectName))
}

func removeSpecialChars(r rune) rune {
	if unicode.IsSpace(r) {
		return -1
	}
	banned := []rune{
		'.',
		'\\', '/',
		'"', '\'', '`',
		':', '*', '?', '|', '~', '#', '&', '-',
		'<', '>', '{', '}', '[', ']', '(', ')',
	}
	for _, bad := range banned {
		if r == bad {
			return -1
		}
	}
	return r
}

var reIsMultiStatement = regexp.MustCompile(`(?is)begin.*;.*end`)

// NeedSpecialDelimiter returns true if the statement requires used of a
// nonstandard delimiter.
func NeedSpecialDelimiter(key tengo.ObjectKey, stmt string) bool {
	return key.Type != tengo.ObjectTypeTable && reIsMultiStatement.MatchString(stmt)
}

// AddDelimiter takes the supplied string and appends a delimiter to the end.
// If the supplied string is a multi-statement routine, delimiter commands will
// be prepended and appended to the string appropriately.
// TODO devise a way to avoid using special delimiter for single-routine files
func AddDelimiter(stmt string) string {
	if reIsMultiStatement.MatchString(stmt) {
		return fmt.Sprintf("DELIMITER //\n%s//\nDELIMITER ;\n", stmt)
	}
	return fmt.Sprintf("%s;\n", stmt)
}

// SQLContentsError represents a fatal problem parsing a .sql file: the file
// contains an unterminated quote, unterminated multi-line comment, forbidden
// statement, or a special character outside of a string/identifier/comment.
type SQLContentsError string

// Error satisfies the builtin error interface.
func (sce SQLContentsError) Error() string {
	return string(sce)
}
