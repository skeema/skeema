package tengo

import (
	"strings"

	"github.com/alecthomas/participle/lexer"
)

// IMPORTANT: the lexer/parser here is going to be completely replaced in 2023.
// Implementation is nearly complete in a separate branch. Outside pull requests
// should avoid touching this code until then.

// CanParse returns true if the supplied string can be parsed as a type of
// SQL statement understood by this package. The supplied string should NOT
// have a delimiter. Note that this method returns false for strings that are
// entirely whitespace and/or comments.
func CanParse(input string) (bool, error) {
	sqlStmt := &sqlStatement{}
	err := nameParser.ParseString(input, sqlStmt)
	return err == nil && !sqlStmt.forbidden(), err
}

// ParseStatementsInFile splits the contents of the supplied file path into
// distinct SQL statements. Statements preserve their whitespace and semicolons;
// the return value exactly represents the entire file. Some of the returned
// "statements" may just be comments and/or whitespace, since any comments and/
// or whitespace between SQL statements gets split into separate Statement
// values.
func ParseStatementsInFile(filePath string) (result []*Statement, err error) {
	tokenizer := newStatementTokenizer(filePath, ";")
	result, err = tokenizer.statements()

	// As a special case, if a file contains a single routine but no DELIMITER
	// command, re-parse it as a single statement. This avoids user error from
	// lack of DELIMITER usage in a multi-statement routine.
	tryReparse := true
	var seenRoutine, unknownAfterRoutine bool
	for _, stmt := range result {
		switch stmt.Type {
		case StatementTypeNoop:
			// nothing to do for StatementTypeNoop, just excluding it from the default case
		case StatementTypeCreate:
			if !seenRoutine && stmt.isCreateWithBegin() {
				seenRoutine = true
			} else {
				tryReparse = false
			}
		case StatementTypeUnknown:
			if seenRoutine {
				unknownAfterRoutine = true
			}
		default:
			tryReparse = false
		}
		if !tryReparse {
			break
		}
	}
	if seenRoutine && unknownAfterRoutine && tryReparse {
		tokenizer := newStatementTokenizer(filePath, "\000")
		if result2, err2 := tokenizer.statements(); err2 == nil {
			result = result2
			err = nil
		}
	}
	return
}

// sqlStatement is the top-level struct for the name parser.
type sqlStatement struct {
	CreateTable      *createTable      `parser:"@@"`
	CreateProc       *createProc       `parser:"| @@"`
	CreateFunc       *createFunc       `parser:"| @@"`
	UseCommand       *useCommand       `parser:"| @@"`
	DelimiterCommand *delimiterCommand `parser:"| @@"`
}

// forbidden returns true if the statement can be parsed, but is of a disallowed
// form by this package.
func (sqlStmt *sqlStatement) forbidden() bool {
	// Forbid CREATE TABLE...SELECT since it also mixes DML, violating the
	// "workspace tables must be empty" validation upon workspace cleanup
	if sqlStmt.CreateTable != nil {
		for _, token := range sqlStmt.CreateTable.Body.Contents {
			if len(token) == 6 && strings.ToUpper(token) == "SELECT" {
				return true
			}
		}
	}
	return false
}

// objectName represents the name of an object, which may or may not be
// backtick-wrapped, and may or may not have multiple qualifier parts (each
// also potentially backtick-wrapped).
type objectName struct {
	Qualifiers []string `parser:"(@Word '.')*"`
	Name       string   `parser:"@Word"`
	Pos        lexer.Position
	EndPos     lexer.Position
}

// schemaAndTable interprets the objectName as a table name which may optionally
// have a schema name qualifier. The first return value is the schema name, or
// empty string if none was specified; the second return value is the table name.
func (n *objectName) schemaAndTable() (string, string) {
	if len(n.Qualifiers) > 0 {
		return stripBackticks(n.Qualifiers[0]), stripBackticks(n.Name)
	}
	return "", stripBackticks(n.Name)
}

// clause returns the portion of the supplied raw SQL string that makes up the
// object name clause, including any optional schema name qualifier and
// backticks, but without any trailing whitespace or trailing comments. If a
// schema name qualifier is present, whitespace and/or comments may still be
// present in the middle of the clause though.
func (n *objectName) clause(statementText string) string {
	startObjectName := n.Pos.Offset
	if len(n.Qualifiers) > 0 {
		startObjectName += len(n.Qualifiers[0])
	}
	endObjectName := startObjectName + strings.Index(statementText[startObjectName:n.EndPos.Offset], n.Name) + len(n.Name)
	return statementText[n.Pos.Offset:endObjectName]
}

// body slurps all body contents of a statement. Note that "body" and
// "statement" here are used with respect to the parser internals, and do NOT
// refer to Statement or Statement.Body().
type body struct {
	Contents []string `parser:"(@Word | @String | @Number | @Operator)*"`
}

// definer represents a user who is the definer of a routine or view.
type definer struct {
	User string `parser:"((@String | @Word) '@'"`
	Host string `parser:"(@String | @Word))"`
	Func string `parser:"| ('CURRENT_USER' ('(' ')')?)"`
}

// createTable represents a CREATE TABLE statement.
type createTable struct {
	Name objectName `parser:"'CREATE' 'TABLE' ('IF' 'NOT' 'EXISTS')? @@"`
	Body body       `parser:"@@"`
}

// createProc represents a CREATE PROCEDURE statement.
type createProc struct {
	Definer *definer   `parser:"'CREATE' ('DEFINER' '=' @@)?"`
	Name    objectName `parser:"'PROCEDURE' @@"`
	Body    body       `parser:"@@"`
}

// createFunc represents a CREATE FUNCTION statement.
type createFunc struct {
	Definer *definer   `parser:"'CREATE' ('DEFINER' '=' @@)?"`
	Name    objectName `parser:"'FUNCTION' @@"`
	Body    body       `parser:"@@"`
}

// useCommand represents a USE command.
type useCommand struct {
	DefaultDatabase string `parser:"'USE' @Word"`
}

// delimiterCommand represents a DELIMITER command.
type delimiterCommand struct {
	NewDelimiter string `parser:"'DELIMITER' (@Word | @String | @Operator+)"`
}
