package tengo

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
)

// MalformedSQLError represents a fatal problem parsing or lexing SQL: the input
// contains an unterminated quote, unterminated multi-line comment, forbidden
// statement, or a special character outside of a string/identifier/comment.
type MalformedSQLError string

// Error satisfies the builtin error interface.
func (mse MalformedSQLError) Error() string {
	return string(mse)
}

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
		err = MalformedSQLError(fmt.Sprintf("File %s has unterminated quote %c", st.filePath, st.inQuote))
	} else if st.inCComment {
		err = MalformedSQLError(fmt.Sprintf("File %s has unterminated C-style comment", st.filePath))
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
				if bufStr == "delimiter" {
					// prevent SplitTextBody from stripping previous delimiter from command arg
					ls.stmt.Delimiter = "\000"
				}
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
		Delimiter:       ls.delimiter,
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
			}
			// Note we no longer populate ls.stmt.Error in unsupported statement cases,
			// as setting it to a participle.UnexpectedTokenError isn't particularly useful
			return
		} else if sqlStmt.UseCommand != nil {
			ls.stmt.Type = StatementTypeCommand
			ls.defaultDatabase = stripBackticks(sqlStmt.UseCommand.DefaultDatabase)
		} else if sqlStmt.DelimiterCommand != nil {
			ls.stmt.Type = StatementTypeCommand
			ls.delimiter = stripAnyQuote(sqlStmt.DelimiterCommand.NewDelimiter)
		} else if sqlStmt.CreateTable != nil {
			ls.stmt.Type = StatementTypeCreate
			ls.stmt.ObjectType = ObjectTypeTable
			name = &sqlStmt.CreateTable.Name
		} else if sqlStmt.CreateProc != nil {
			ls.stmt.Type = StatementTypeCreate
			ls.stmt.ObjectType = ObjectTypeProc
			name = &sqlStmt.CreateProc.Name
		} else if sqlStmt.CreateFunc != nil {
			ls.stmt.Type = StatementTypeCreate
			ls.stmt.ObjectType = ObjectTypeFunc
			name = &sqlStmt.CreateFunc.Name
		}
		if name != nil {
			ls.stmt.ObjectQualifier, ls.stmt.ObjectName = name.schemaAndTable()
			ls.stmt.nameClause = name.clause(txt)
		}
	}
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
