package fs

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/alecthomas/participle"
	"github.com/alecthomas/participle/lexer"
	"github.com/skeema/skeema/internal/tengo"
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
	StatementTypeLexError  // something went horribly wrong, caller should treat as fatal
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
	ObjectType      tengo.ObjectType
	ObjectName      string
	ObjectQualifier string
	FromFile        *TokenizedSQLFile
	Error           error // any problem lexing or parsing this statement, populated when Type is StatementTypeUnknown, StatementTypeLexError, or StatementTypeForbidden
	delimiter       string
	nameClause      string // raw version, potentially with schema name qualifier and/or surrounding backticks; also any trailing whitespace/comments
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

// ObjectKey returns a tengo.ObjectKey for the object affected by this
// statement.
func (stmt *Statement) ObjectKey() tengo.ObjectKey {
	return tengo.ObjectKey{
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
	// stmt.nameClause includes trailing whitespace, but we want to leave that intact
	replaceClause := strings.TrimRight(stmt.nameClause, "\n\r\t ")
	return strings.Replace(body, replaceClause, tengo.EscapeIdentifier(stmt.ObjectName), 1)
}

// SplitTextBody returns Text with its trailing delimiter and whitespace (if
// any) separated out into a separate string.
func (stmt *Statement) SplitTextBody() (body string, suffix string) {
	if stmt == nil {
		return "", ""
	}
	body = strings.TrimRight(stmt.Text, "\n\r\t ")
	body = strings.TrimSuffix(body, stmt.delimiter)
	body = strings.TrimRight(body, "\n\r\t ")
	return body, stmt.Text[len(body):]
}

// Remove removes the statement from the list of statements in stmt.FromFile.
// It does not rewrite the file though.
func (stmt *Statement) Remove() {
	for i, comp := range stmt.FromFile.Statements {
		if stmt == comp {
			// from go wiki slicetricks -- delete slice element without leaking memory
			copy(stmt.FromFile.Statements[i:], stmt.FromFile.Statements[i+1:])
			stmt.FromFile.Statements[len(stmt.FromFile.Statements)-1] = nil
			stmt.FromFile.Statements = stmt.FromFile.Statements[:len(stmt.FromFile.Statements)-1]
			return
		}
	}
	panic(fmt.Errorf("Statement previously at %s not actually found in file", stmt.Location()))
}

// isCreateWithBegin is useful for identifying multi-line statements that may
// have been mis-parsed (for example, due to lack of DELIMITER commands)
func (stmt *Statement) isCreateWithBegin() bool {
	return stmt.Type == StatementTypeCreate &&
		(stmt.ObjectType == tengo.ObjectTypeProc || stmt.ObjectType == tengo.ObjectTypeFunc) &&
		strings.Contains(strings.ToLower(stmt.Text), "begin")
}

// CanParse returns true if the supplied string can be parsed as a type of
// SQL statement understood by this package. The supplied string should NOT
// have a delimiter. Note that this method returns false for strings that are
// entirely whitespace and/or comments.
func CanParse(input string) (bool, error) {
	sqlStmt := &sqlStatement{}
	err := nameParser.ParseString(input, sqlStmt)
	return err == nil && !sqlStmt.forbidden(), err
}

//////////// lexing/parsing internals from here to end of this file ////////////

// IMPORTANT: the lexer/parser here is going to be completely replaced in 2023.
// Implementation is nearly complete in a separate branch. Outside pull requests
// should avoid touching this code until then.
type statementTokenizer struct {
	filePath  string
	delimiter string // statement delimiter, typically ";" or sometimes "//" for routines

	result []*Statement // completed statements
	stmt   *Statement   // tracking current (not yet completely tokenized) statement
	buf    bytes.Buffer // tracking text to eventually put into stmt

	lineNo          int    // human-readable line number, starting at 1
	inRelevant      bool   // true if current statement contains something other than just whitespace and comments
	inCComment      bool   // true if in a C-style comment
	inQuote         rune   // nonzero if inside of a quoted string; value indicates which quote rune
	defaultDatabase string // tracks most recent USE command
}

type lineState struct {
	*statementTokenizer
	line   string // current line of text, including trailing newline
	pos    int    // current byte offset within line
	charNo int    // human-readable column number, starting at 1
}

// newStatementTokenizer creates a tokenizer for splitting the contents of the
// file at the supplied path into statements.
func newStatementTokenizer(filePath, delimiter string) *statementTokenizer {
	return &statementTokenizer{
		filePath:  filePath,
		delimiter: delimiter,
	}
}

func (st *statementTokenizer) statements() ([]*Statement, error) {
	file, err := os.Open(st.filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	reader := bufio.NewReader(file)

	for err != io.EOF {
		var line string
		line, err = reader.ReadString('\n')
		if err != nil && err != io.EOF {
			return st.result, err
		}
		st.processLine(line, err == io.EOF)
	}
	if st.inQuote != 0 {
		err = SQLContentsError(fmt.Sprintf("File %s has unterminated quote %c", st.filePath, st.inQuote))
	} else if st.inCComment {
		err = SQLContentsError(fmt.Sprintf("File %s has unterminated C-style comment", st.filePath))
	} else {
		err = nil
	}
	return st.result, err
}

func (st *statementTokenizer) processLine(line string, eof bool) {
	st.lineNo++

	// Trim UTF8 BOM prefix if present at beginning of file
	if st.lineNo == 1 {
		line = strings.TrimPrefix(line, "\uFEFF")
	}

	ls := &lineState{
		statementTokenizer: st,
		line:               line,
	}

	for ls.pos < len(ls.line) {
		c, cLen := ls.nextRune()
		if ls.stmt == nil {
			ls.beginStatement()
		}
		if ls.inCComment {
			if c == '*' && ls.peekRune() == '/' {
				ls.nextRune()
				ls.inCComment = false
			}
			continue
		} else if ls.inQuote > 0 {
			if c == '\\' {
				ls.nextRune()
			} else if c == ls.inQuote {
				if ls.peekRune() == ls.inQuote {
					ls.nextRune()
				} else {
					ls.inQuote = 0
				}
			}
			continue
		}

		// C-style comment can be multi-line
		if c == '/' && ls.peekRune() == '*' {
			ls.inCComment = true
			ls.nextRune()
			continue
		}

		// Comment until end of line: Just put the rest of the line in the buffer
		// and move on to next line
		if c == '#' {
			ls.buf.WriteString(ls.line[ls.pos:])
			break
		}
		if c == '-' && ls.peekRune() == '-' {
			ls.nextRune()
			if unicode.IsSpace(ls.peekRune()) {
				ls.buf.WriteString(ls.line[ls.pos:])
				break
			}
		}

		// When transitioning from whitespace and/or comments, to something that
		// isn't whitespace or comments, split the whitespace/comments into its own
		// statement. That way, future file manipulations that change individual
		// statements won't remove any preceding whitespace or comments.
		if !ls.inRelevant && !unicode.IsSpace(c) {
			ls.doneStatement(cLen)
			ls.inRelevant = true
		}

		// Commands are special-cases in terms of delimiter vs newline handling: USE
		// command has optional delimiter, and DELIMITER command has no delimiter
		// (and requires special care to handle transititions like e.g. going from
		// a single semicolon delimiter to double-semicolon delimiter!)
		if bufLen := ls.buf.Len(); (bufLen == 4 || bufLen == 10) && unicode.IsSpace(c) { // potentially USE or DELIMITER followed by whitespace
			// Treat this line as a command if the current char is a space and the
			// preceeding line content is "use" or "delimiter", case-insensitive. For
			// "use", we only do this if there's no delimiter later on the line though,
			// since it can optionally be present (potentially followed by other
			// separate statements, which we should not slurp up!)
			bufStr := strings.ToLower(ls.buf.String()[0 : bufLen-1])
			if bufStr == "delimiter" || (bufStr == "use" && !ls.containsDelimiter()) {
				ls.buf.WriteString(ls.line[ls.pos:])
				ls.stmt.delimiter = "\000" // prevent SplitTextBody from stripping previous delimiter from command arg
				ls.doneStatement(0)
				return
			}
		}

		delimFirstRune, delimFirstRuneLen := utf8.DecodeRuneInString(st.delimiter)
		delimRuneCount := utf8.RuneCountInString(st.delimiter)
		switch c {
		case '"', '`', '\'':
			ls.inQuote = c
		case delimFirstRune:
			// Multi-rune delimiter: peek ahead to see if we've matched the full
			// delimiter. If so, slurp up the rest of the delimiter's runes.
			if delimRuneCount > 1 {
				if ls.peekRunes(delimRuneCount-1) != st.delimiter[delimFirstRuneLen:] {
					break
				}
				for n := 0; n < delimRuneCount-1; n++ {
					ls.nextRune()
				}
			}
			// Slurp up a single trailing newline (LF or CRLF) if present
			if ls.peekRune() == '\n' {
				ls.nextRune()
			} else if ls.peekRunes(2) == "\r\n" {
				ls.nextRune()
				ls.nextRune()
			}
			ls.doneStatement(0)
		}
	}

	// handle final statement before EOF, if anything left in buffer
	if eof {
		ls.doneStatement(0)
	}
}

// nextRune returns the rune at the current position, along with its length
// in bytes. It also advances to the next position.
func (ls *lineState) nextRune() (rune, int) {
	if ls.pos >= len(ls.line) {
		return 0, 0
	}
	c, cLen := utf8.DecodeRuneInString(ls.line[ls.pos:])
	ls.buf.WriteRune(c)
	ls.pos += cLen
	ls.charNo++
	return c, cLen
}

// peekRune returns the rune at the current position, without advancing.
func (ls *lineState) peekRune() rune {
	if ls.pos >= len(ls.line) {
		return 0
	}
	c, _ := utf8.DecodeRuneInString(ls.line[ls.pos:])
	return c
}

// peekRunes returns a string, made of at most n runes, from the current
// position without advancing.
func (ls *lineState) peekRunes(n int) string {
	pos := ls.pos
	for n > 0 && pos < len(ls.line) {
		_, runeLen := utf8.DecodeRuneInString(ls.line[pos:])
		pos += runeLen
		n--
	}
	return ls.line[ls.pos:pos]
}

// containsDelimiter returns true if the line contains the current delimiter
// string beyond the current position.
func (ls *lineState) containsDelimiter() bool {
	return strings.Contains(ls.line[ls.pos:], ls.delimiter)
}

// beginStatement records the starting position of the next (not yet fully
// tokenized) statement.
func (ls *lineState) beginStatement() {
	ls.stmt = &Statement{
		File:            ls.filePath,
		LineNo:          ls.lineNo,
		CharNo:          ls.charNo,
		DefaultDatabase: ls.defaultDatabase,
		delimiter:       ls.delimiter,
	}
}

// doneStatement finalizes the current statement by filling in its text
// field with the buffer contents, optionally excluding the last omitEndBytes
// bytes of the buffer. It then puts this statement onto the result slice,
// and cleans up bookkeeping state in preparation for the next statement.
func (ls *lineState) doneStatement(omitEndBytes int) {
	bufLen := ls.buf.Len()
	if ls.stmt == nil || bufLen <= omitEndBytes {
		return
	}
	ls.stmt.Text = fmt.Sprintf("%s", ls.buf.Next(bufLen-omitEndBytes))
	ls.parseStatement()
	ls.result = append(ls.result, ls.stmt)
	ls.stmt = nil
	if omitEndBytes == 0 {
		ls.buf.Reset()
		ls.inRelevant = false
	} else {
		ls.beginStatement()
	}
}

func (ls *lineState) parseStatement() {
	txt, _ := ls.stmt.SplitTextBody()
	if !ls.inRelevant || txt == "" {
		ls.stmt.Type = StatementTypeNoop
	} else {
		sqlStmt := &sqlStatement{}
		var name *objectName
		if err := nameParser.ParseString(txt, sqlStmt); err != nil || sqlStmt.forbidden() {
			if err == nil { // forbidden statement
				ls.stmt.Type = StatementTypeForbidden
				ls.stmt.Error = fmt.Errorf("%s: Statements of the form CREATE TABLE...SELECT are not supported", ls.stmt.File)
			} else if lexErr, ok := err.(*lexer.Error); ok { // lexer error, potentially bad
				ls.stmt.Type = StatementTypeLexError
				fileLine, fileCol := ls.stmt.LineNo+lexErr.Tok.Pos.Line-1, lexErr.Tok.Pos.Column
				if lexErr.Tok.Pos.Line == 1 && ls.stmt.CharNo > 1 { // error is on first line of statement, and statement started mid-line
					fileCol += ls.stmt.CharNo - 1
				}
				ls.stmt.Error = fmt.Errorf("%s:%d:%d: %s", ls.stmt.File, fileLine, fileCol, lexErr.Msg)
			} else { // unsupported statement, often benign
				ls.stmt.Error = err // pass through the error as-is (often a participle.UnexpectedTokenError which isn't particularly useful)
			}
			return
		} else if sqlStmt.UseCommand != nil {
			ls.stmt.Type = StatementTypeCommand
			ls.defaultDatabase = stripBackticks(sqlStmt.UseCommand.DefaultDatabase)
		} else if sqlStmt.DelimiterCommand != nil {
			ls.stmt.Type = StatementTypeCommand
			ls.delimiter = stripAnyQuote(sqlStmt.DelimiterCommand.NewDelimiter)
		} else if sqlStmt.CreateTable != nil {
			ls.stmt.Type = StatementTypeCreate
			ls.stmt.ObjectType = tengo.ObjectTypeTable
			name = &sqlStmt.CreateTable.Name
		} else if sqlStmt.CreateProc != nil {
			ls.stmt.Type = StatementTypeCreate
			ls.stmt.ObjectType = tengo.ObjectTypeProc
			name = &sqlStmt.CreateProc.Name
		} else if sqlStmt.CreateFunc != nil {
			ls.stmt.Type = StatementTypeCreate
			ls.stmt.ObjectType = tengo.ObjectTypeFunc
			name = &sqlStmt.CreateFunc.Name
		}
		if name != nil {
			ls.stmt.ObjectQualifier, ls.stmt.ObjectName = name.schemaAndTable()
			ls.stmt.nameClause = txt[name.Pos.Offset:name.EndPos.Offset]
		}
	}
}

func stripBackticks(input string) string {
	if len(input) < 2 || input[0] != '`' || input[len(input)-1] != '`' {
		return input
	}
	input = input[1 : len(input)-1]
	return strings.Replace(input, "``", "`", -1)
}

func stripAnyQuote(input string) string {
	if len(input) < 2 || input[0] != input[len(input)-1] {
		return input
	}
	if input[0] == '`' {
		return stripBackticks(input)
	} else if input[0] != '"' && input[0] != '\'' {
		return input
	}
	quoteStr := input[0:1]
	input = input[1 : len(input)-1]
	input = strings.Replace(input, strings.Repeat(quoteStr, 2), quoteStr, -1)
	return strings.Replace(input, fmt.Sprintf("\\%s", quoteStr), quoteStr, -1)
}

// Note: this lexer and parser is not intended to line up 1:1 with SQL; its
// purpose is simply to parse *statement types* and either *object names* or
// *simple args*. The definition of Word intentionally matches keywords,
// barewords, and backtick-quoted identifiers. The definition of Operator
// intentionally matches several non-operator symbols in case they are used
// as delimiters (via the delimiter command).
var (
	sqlLexer = lexer.Must(lexer.Regexp(`(#[^\n]*(?:\n|$))` +
		`|(--([\s\p{Zs}][^\n]*)??(?:\n|$))` +
		`|(/\*(.|\n)*?\*/)` +
		`|([\s\p{Zs}]+)` +
		"|(?P<Word>[0-9a-zA-Z\u0080-\uFFFF$_]+|`(?:[^`]|``)+`)" +
		`|(?P<String>('(\\\\|\\'|''|[^'])*')|("(\\\\|\\"|""|[^"])*"))` +
		`|(?P<Number>[-+]?\d*\.?\d+([eE][-+]?\d+)?)` +
		`|(?P<Operator><>|!=|<=|>=|:=|[-+*/%,.()=<>@;~!^&:|])`,
	))
	nameParser = participle.MustBuild(&sqlStatement{},
		participle.Lexer(sqlLexer),
		participle.CaseInsensitive("Word"),
		participle.UseLookahead(10),
	)
)

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

// schemaAndTable interprets the ObjectName as a table name which may optionally
// have a schema name qualifier. The first return value is the schema name, or
// empty string if none was specified; the second return value is the table name.
func (n *objectName) schemaAndTable() (string, string) {
	if len(n.Qualifiers) > 0 {
		return stripBackticks(n.Qualifiers[0]), stripBackticks(n.Name)
	}
	return "", stripBackticks(n.Name)
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
