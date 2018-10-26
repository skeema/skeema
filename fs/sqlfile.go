package fs

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strings"
)

// SQLFile represents a file containing zero or more SQL statements.
type SQLFile struct {
	Dir      string
	FileName string
}

// TokenizedSQLFile represents a SQLFile that has been tokenized into
// statements successfully.
type TokenizedSQLFile struct {
	SQLFile
	Statements []*Statement
}

// Path returns the full absolute path to a SQLFile.
func (sf SQLFile) Path() string {
	return path.Join(sf.Dir, sf.FileName)
}

func (sf SQLFile) String() string {
	return sf.Path()
}

// Exists returns true if sf already exists in the filesystem, false if not.
func (sf SQLFile) Exists() (bool, error) {
	_, err := os.Stat(sf.Path())
	if err == nil {
		return true, nil
	} else if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

// Create writes a new file, erroring if it already exists.
func (sf SQLFile) Create(contents string) error {
	if exists, err := sf.Exists(); err != nil {
		return err
	} else if exists {
		return fmt.Errorf("Cannot create %s: already exists", sf)
	}
	return ioutil.WriteFile(sf.Path(), []byte(contents), 0666)
}

// Delete unlinks the file.
func (sf SQLFile) Delete() error {
	return os.Remove(sf.Path())
}

// Tokenize reads the file and splits it into statements, returning a
// TokenizedSQLFile that wraps sf with the statements added. Statements preserve
// their whitespace and semicolons; the return value exactly represents the
// entire file. Some of the returned "statements" may just be comments and/or
// whitespace, since any comments and/or whitespace between SQL statements gets
// split into separate Statement values.
func (sf SQLFile) Tokenize() (*TokenizedSQLFile, error) {
	file, err := os.Open(sf.Path())
	if err != nil {
		return nil, err
	}
	defer file.Close()
	tokenizer := newStatementTokenizer(file, sf.Path())
	statements, err := tokenizer.statements()
	if err != nil {
		return nil, err
	}
	return NewTokenizedSQLFile(sf, statements), nil
}

// WriteStatements writes (or re-writes) the file using the contents of the
// supplied statements. The number of bytes written is returned.
func (sf SQLFile) WriteStatements(statements []*Statement) (int, error) {
	lines := make([]string, len(statements))
	for n := range statements {
		lines[n] = string(statements[n].Text)
	}
	value := strings.Join(lines, "")
	err := ioutil.WriteFile(sf.Path(), []byte(value), 0666)
	if err != nil {
		return 0, err
	}
	return len(value), nil
}

// NewTokenizedSQLFile creates a TokenizedSQLFile whose statements have a
// FromFile pointer linking back to the TokenizedSQLFile. This permits easy
// mutation of the statements and rewriting of the file.
func NewTokenizedSQLFile(sf SQLFile, statements []*Statement) *TokenizedSQLFile {
	result := &TokenizedSQLFile{
		SQLFile:    sf,
		Statements: statements,
	}
	for _, stmt := range statements {
		stmt.FromFile = result
	}
	return result
}

// Rewrite rewrites the SQLFile with the current statements, returning the
// number of bytes written. If the file's statements now only consist of noop
// (comments and whitespace) statements, the file will be deleted instead, and
// a length of 0 will be returned.
func (tsf *TokenizedSQLFile) Rewrite() (int, error) {
	var keepFile bool
	for _, stmt := range tsf.Statements {
		if stmt.Type != StatementTypeNoop {
			keepFile = true
			break
		}
	}
	if keepFile {
		return tsf.WriteStatements(tsf.Statements)
	}
	return 0, tsf.Delete()
}

// AppendToFile appends the supplied string to the file at the given path. If the
// file already exists and is not newline-terminated, a newline will be added
// before contents are appended. If the file does not exist, it will be created.
func AppendToFile(filePath, contents string) (bytesWritten int, created bool, err error) {
	_, err = os.Stat(filePath)
	if os.IsNotExist(err) {
		return len(contents), true, ioutil.WriteFile(filePath, []byte(contents), 0666)
	} else if err != nil {
		return
	}

	byteContents, err := ioutil.ReadFile(filePath)
	if err != nil {
		return 0, false, fmt.Errorf("%s: Cannot append: %s", filePath, err)
	}
	var whitespace string
	if len(byteContents) > 0 && byteContents[len(byteContents)-1] != '\n' {
		whitespace = "\n"
	}
	newContents := fmt.Sprintf("%s%s%s", string(byteContents), whitespace, contents)
	return len(newContents), false, ioutil.WriteFile(filePath, []byte(newContents), 0666)
}
