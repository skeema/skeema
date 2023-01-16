package tengo

import (
	"fmt"
	"strings"
)

// StatementType indicates the type of a SQL statement found in a SQLFile.
// Parsing of types is very rudimentary, which can be advantageous for linting
// purposes. Otherwise, SQL errors or typos would prevent type detection.
type StatementType int

// Constants enumerating different types of statements
const (
	StatementTypeUnknown StatementType = iota
	StatementTypeNoop                  // entirely whitespace and/or comments
	StatementTypeCommand               // currently just USE or DELIMITER
	StatementTypeCreate
	StatementTypeAlter     // not actually ever parsed yet
	StatementTypeForbidden // disallowed statement such as CREATE TABLE ... SELECT
	// Other types will be added once they are supported by the package
)

// Statement represents a logical instruction in a file, consisting of either
// an SQL statement, a command (e.g. "USE some_database"), or whitespace and/or
// comments between two separate statements or commands.
type Statement struct {
	File            string
	LineNo          int
	CharNo          int
	Text            string // includes trailing Delimiter and newline
	DefaultDatabase string // only populated if an explicit USE command was encountered
	Type            StatementType
	ObjectType      ObjectType
	ObjectName      string
	ObjectQualifier string
	Error           error  // any problem lexing or parsing this statement, populated when Type is StatementTypeUnknown or StatementTypeForbidden
	Delimiter       string // delimiter in use at the time of statement; not necessarily present in Text though
	nameClause      string // raw version, potentially with schema name qualifier and/or surrounding backticks
}

// Location returns the file, line number, and character number where the
// statement was obtained from
func (stmt *Statement) Location() string {
	if stmt.File == "" && stmt.LineNo == 0 && stmt.CharNo == 0 {
		return ""
	}
	if stmt.File == "" {
		return fmt.Sprintf("unknown:%d:%d", stmt.LineNo, stmt.CharNo)
	}
	return fmt.Sprintf("%s:%d:%d", stmt.File, stmt.LineNo, stmt.CharNo)
}

// ObjectKey returns an ObjectKey for the object affected by this
// statement.
func (stmt *Statement) ObjectKey() ObjectKey {
	return ObjectKey{
		Type: stmt.ObjectType,
		Name: stmt.ObjectName,
	}
}

// Schema returns the schema name that this statement impacts.
func (stmt *Statement) Schema() string {
	if stmt.ObjectQualifier != "" {
		return stmt.ObjectQualifier
	}
	return stmt.DefaultDatabase
}

// Body returns the Statement's Text, without any trailing delimiter,
// whitespace, or qualified schema name.
func (stmt *Statement) Body() string {
	body, _ := stmt.SplitTextBody()
	if stmt.ObjectQualifier == "" || stmt.nameClause == "" {
		return body
	}
	return strings.Replace(body, stmt.nameClause, EscapeIdentifier(stmt.ObjectName), 1)
}

// SplitTextBody returns Text with its trailing delimiter and whitespace (if
// any) separated out into a separate string.
func (stmt *Statement) SplitTextBody() (body string, suffix string) {
	if stmt == nil {
		return "", ""
	}
	body = strings.TrimRight(stmt.Text, "\n\r\t ")
	body = strings.TrimSuffix(body, stmt.Delimiter)
	body = strings.TrimRight(body, "\n\r\t ")
	return body, stmt.Text[len(body):]
}

// isCreateWithBegin is useful for identifying multi-line statements that may
// have been mis-parsed (for example, due to lack of DELIMITER commands)
func (stmt *Statement) isCreateWithBegin() bool {
	return stmt.Type == StatementTypeCreate &&
		(stmt.ObjectType == ObjectTypeProc || stmt.ObjectType == ObjectTypeFunc) &&
		strings.Contains(strings.ToLower(stmt.Text), "begin")
}
